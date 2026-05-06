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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Allocates chunks from memory-mapped files.
 *
 * <p>Mapped regions are never unmapped — the OS reclaims pages via
 * its virtual-memory lifecycle.  Files and mappings grow monotonically;
 * this is intentional so that readers holding pinned chunks never risk
 * accessing unmapped memory.</p>
 */
public final class MmapChunkAllocator {
    public static final String MMAP_FILE_PREFIX = "mmap-sequences-";
    public static final String MMAP_FILE_EXTENSION = ".tmp";

    private static final long MAX_FILE_SIZE = 2L * 1024 * 1024 * 1024;

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
    private final int chunksPerFile;
    private final long fileSize;
    private final File memoryMappedFileFolder;
    private final AtomicLong epochCounter;
    private final AtomicInteger fileSeq = new AtomicInteger();

    private final ConcurrentLinkedQueue<Chunk> freeList = new ConcurrentLinkedQueue<>();
    private final AtomicReference<Node> pendingReclamation = new AtomicReference<>();
    private final AtomicReference<Node> deferredPending = new AtomicReference<>();
    private final AtomicLong drainCallCount = new AtomicLong();

    private final ReentrantLock regionLock = new ReentrantLock();
    private MappedByteBuffer currentRegion;
    private int nextIdx;

    /**
     * Creates a memory-mapped chunk allocator. Cleans up any leftover
     * files from previous runs and optionally pre-allocates the first
     * mapped region.
     *
     * @param chunkSize              size of each chunk in bytes
     * @param memoryMappedFileFolder directory to store memory-mapped files
     * @param preAlloc               if {@code true}, the first region is mapped eagerly
     * @param epochCounter           shared epoch counter for stamping chunks
     * @throws UncheckedIOException if pre-allocation or cleanup fails
     */
    public MmapChunkAllocator(final int chunkSize,
                              final File memoryMappedFileFolder,
                              final boolean preAlloc,
                              final AtomicLong epochCounter) {
        this.chunkSize = chunkSize;
        this.epochCounter = epochCounter;
        this.memoryMappedFileFolder = memoryMappedFileFolder;

        final long cpf = MAX_FILE_SIZE / chunkSize;
        this.chunksPerFile = (int) Math.min(cpf, (long) Integer.MAX_VALUE / chunkSize);
        this.fileSize = (long) this.chunksPerFile * chunkSize;

        cleanup();

        if (preAlloc) {
            regionLock.lock();
            try {
                openNewRegion();
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                regionLock.unlock();
            }
        }
    }

    /**
     * Allocates a chunk, first trying the free list (recycled chunks)
     * and falling back to carving a new chunk from the current or a
     * newly opened memory-mapped region.
     *
     * @return a freshly initialized mmap-backed chunk
     * @throws UncheckedIOException if a new region cannot be opened
     */
    public Chunk allocate() {
        drainPendingReclamation();
        final Chunk c = freeList.poll();
        if (c != null) {
            initChunk(c);
            return c;
        }
        try {
            return allocateFromRegion();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Returns a chunk to the free list for future reuse.
     *
     * @param c the chunk to free, or {@code null} (no-op)
     */
    public void free(final Chunk c) {
        if (c != null) {
            freeList.offer(c);
        }
    }

    /**
     * Submits a chunk for deferred reclamation. The chunk will be
     * returned to the free list once its reference count reaches zero.
     *
     * @param c the chunk to reclaim
     */
    public void submitPendingReclamation(final Chunk c) {
        pushNode(pendingReclamation, c, 0);
    }

    private void drainPendingReclamation() {
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
                freeList.offer(h.chunk);
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
            pushNode(pendingReclamation, n.chunk, n.retryCount);
        }
        for (Node n = retryDeferred; n != null; n = n.next) {
            pushNode(deferredPending, n.chunk, n.retryCount);
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
            pushNode(pendingReclamation, h.chunk, 0);
        }
    }

    private Chunk allocateFromRegion() throws IOException {
        regionLock.lock();
        try {
            if (currentRegion == null || nextIdx >= chunksPerFile) {
                openNewRegion();
            }
            final int chunkOffset = Math.toIntExact((long) nextIdx * chunkSize);
            final AtomicBuffer buffer = new UnsafeBuffer(currentRegion, chunkOffset, chunkSize);
            final Chunk chunk = new Chunk(buffer);
            nextIdx++;
            initChunk(chunk);
            return chunk;
        } finally {
            regionLock.unlock();
        }
    }

    private void initChunk(final Chunk chunk) {
        chunk.zeroWriterMeta();
        chunk.putEvictionStateOrdered(Chunk.EVICTION_NONE);
        chunk.putRefCountOrdered(0);
        chunk.putChunkEpoch(epochCounter.incrementAndGet());
    }

    private void cleanup() {
        if (memoryMappedFileFolder.exists()) {
            if (!memoryMappedFileFolder.isDirectory()) {
                throw new IllegalArgumentException(memoryMappedFileFolder + " is not a folder");
            }
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(
                    memoryMappedFileFolder.toPath(),
                    MMAP_FILE_PREFIX + "*" + MMAP_FILE_EXTENSION
            )) {
                for (final Path filePath : stream) {
                    if (Files.isRegularFile(filePath)) {
                        final File file = filePath.toFile();
                        if (!file.delete()) {
                            throw new RuntimeException(
                                    "Failed to delete " + file);
                        }
                    }
                }
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            memoryMappedFileFolder.mkdirs();
        }
    }

    private void openNewRegion() throws IOException {
        final File file = new File(
                memoryMappedFileFolder,
                MMAP_FILE_PREFIX + fileSeq.getAndIncrement()
                        + MMAP_FILE_EXTENSION
        );
        file.createNewFile();
        file.deleteOnExit();

        final AtomicBuffer result;

        try (FileChannel fileChannel = FileChannel.open(file.toPath(),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)) {

            fileChannel.write(
                    ByteBuffer.allocate(1),
                    fileSize - 1
            );

            currentRegion = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE, 0, fileSize
            );
            nextIdx = 0;
        }
    }

    private static void pushNode(final AtomicReference<Node> s,
                                 final Chunk c, final int retryCount) {
        while (true) {
            final Node h = s.get();
            if (s.compareAndSet(h, new Node(c, h, retryCount))) {
                return;
            }
        }
    }
}
