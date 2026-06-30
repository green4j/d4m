/*
 * MIT License
 *
 * Copyright (c) 2024-2026 Anatoly Gudkov and others.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package io.github.green4j.d4m.sequence;

import io.github.green4j.d4m.common.AtomicBuffer;
import io.github.green4j.d4m.common.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Lock-free allocator for heap-backed chunks. Pre-allocates slabs of
 * contiguous memory at construction time and manages a Treiber stack
 * of free chunks. Chunks pending reclamation (pinned by readers)
 * are retried via CAS with exponential deferral to avoid contention.
 */
public final class HeapChunkAllocator {
    private static final int MAX_RECLAIM_RETRIES = 16;
    private static final int DEFERRED_DRAIN_INTERVAL = 64;

    private static final class Node {
        final Chunk chunk;
        final Node next;
        final int retryCount;

        Node(final Chunk c, final Node n, final int r) {
            chunk = c;
            next = n;
            retryCount = r;
        }
    }

    private final int chunkSize;
    private final AtomicLong epochCounter;

    private final AtomicReference<Node> freeStack = new AtomicReference<>();
    private final AtomicReference<Node> pendingReclamation = new AtomicReference<>();
    private final AtomicReference<Node> deferredPending = new AtomicReference<>();
    private final AtomicLong drainCallCount = new AtomicLong();

    /**
     * Creates a heap chunk allocator, pre-allocating slabs up to the
     * specified maximum heap budget.
     *
     * @param chunkSize    size of each chunk in bytes
     * @param maxHeapBytes maximum total heap memory to allocate
     * @param slabSize     size of each contiguous slab allocation
     * @param epochCounter shared epoch counter for stamping chunks
     * @throws IllegalArgumentException if {@code slabSize < chunkSize}
     */
    public HeapChunkAllocator(final int chunkSize,
                              final long maxHeapBytes,
                              final int slabSize,
                              final AtomicLong epochCounter) {
        this.chunkSize = chunkSize;
        this.epochCounter = epochCounter;

        final int perSlab = slabSize / chunkSize;
        if (perSlab < 1) {
            throw new IllegalArgumentException("slabSize < chunkSize");
        }

        final int realSlab = perSlab * chunkSize;
        final int numSlabs = (int) ((maxHeapBytes + realSlab - 1) / realSlab);
        for (int s = 0; s < numSlabs; s++) {
            final ByteBuffer slab = ByteBuffer.allocate(realSlab);
            for (int i = 0; i < perSlab; i++) {
                final int off = i * chunkSize;
                slab.position(off).limit(off + chunkSize);
                final AtomicBuffer buf = new UnsafeBuffer(slab.slice());
                push(freeStack, new Chunk(buf), 0);
                slab.clear();
            }
        }
    }

    /**
     * Returns the configured chunk size in bytes.
     *
     * @return chunk size
     */
    public int chunkSize() {
        return chunkSize;
    }

    /**
     * Attempts to allocate a chunk from the free stack, draining pending
     * reclamations first. Returns {@code null} if no chunks are available.
     *
     * @return a freshly stamped chunk, or {@code null} if the pool is empty
     */
    public Chunk tryAllocate() {
        drainPendingReclamation();
        final Chunk chunk = pop(freeStack);
        if (chunk != null) {
            stampEpoch(chunk);
        }
        return chunk;
    }

    /**
     * Submits a chunk for deferred reclamation. The chunk will be
     * returned to the free pool once its reference count reaches zero.
     *
     * @param c the chunk to reclaim
     */
    public void submitPendingReclamation(final Chunk c) {
        push(pendingReclamation, c, 0);
    }

    /**
     * Drains the pending reclamation queue, returning chunks with zero
     * ref-count to the free stack. Chunks pinned by readers are
     * re-enqueued; after exceeding the retry threshold they are moved
     * to a deferred queue to reduce hot-path contention.
     */
    public void drainPendingReclamation() {
        if ((drainCallCount.incrementAndGet() & (DEFERRED_DRAIN_INTERVAL - 1)) == 0) {
            drainDeferred();
        }

        Node retryHot = null;
        Node retryDeferred = null;

        while (true) {
            final Node h = pendingReclamation.get();
            if (h == null) {
                break;
            }
            if (!pendingReclamation.compareAndSet(h, h.next)) {
                continue;
            }

            if (h.chunk.casRefCount(0, -1)) {
                push(freeStack, h.chunk, 0);
            } else {
                final int next = h.retryCount + 1;
                if (next >= MAX_RECLAIM_RETRIES) {
                    retryDeferred = new Node(h.chunk, retryDeferred, next);
                } else {
                    retryHot = new Node(h.chunk, retryHot, next);
                }
            }
        }

        for (Node n = retryHot; n != null; n = n.next) {
            push(pendingReclamation, n.chunk, n.retryCount);
        }
        for (Node n = retryDeferred; n != null; n = n.next) {
            push(deferredPending, n.chunk, n.retryCount);
        }
    }

    private void drainDeferred() {
        while (true) {
            final Node h = deferredPending.get();
            if (h == null) {
                break;
            }
            if (!deferredPending.compareAndSet(h, h.next)) {
                continue;
            }
            push(pendingReclamation, h.chunk, 0);
        }
    }

    private void stampEpoch(final Chunk chunk) {
        chunk.zeroWriterMeta();                                      // plain bulk zero
        chunk.putEvictionStateOrdered(Chunk.EVICTION_NONE);          // ordered
        chunk.putRefCountOrdered(0);                                 // ordered
        chunk.putChunkEpoch(epochCounter.incrementAndGet());         // volatile = final release
    }

    private static void push(final AtomicReference<Node> s,
                             final Chunk c,
                             final int retryCount) {
        while (true) {
            final Node h = s.get();
            if (s.compareAndSet(h, new Node(c, h, retryCount))) {
                return;
            }
        }
    }

    private static Chunk pop(final AtomicReference<Node> s) {
        while (true) {
            final Node h = s.get();
            if (h == null) {
                return null;
            }
            if (s.compareAndSet(h, h.next)) {
                return h.chunk;
            }
        }
    }
}
