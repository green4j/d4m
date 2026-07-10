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

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test-only sequence storage backed by heap and mmap allocators.
 */
public final class TestSequenceStorage {
    private final int chunkSize;
    private final HeapChunkAllocator heap;
    private final MmapChunkAllocator mmap;
    private final EvictionQueue evictQ;
    private final ConcurrentHashMap<String, Sequence> sequenceByName = new ConcurrentHashMap<>();

    /**
     * Creates a storage instance with the given allocator configuration.
     *
     * @param chunkSize     the size of each chunk in bytes
     * @param maxHeap       maximum heap memory for chunk allocation
     * @param chunksPerSlab the number of chunks per slab for the heap allocator
     * @param mmapDir       directory for memory-mapped chunk files
     * @param preAlloc      whether to pre-allocate mmap chunks
     */
    public TestSequenceStorage(final int chunkSize,
                               final long maxHeap,
                               final int chunksPerSlab,
                               final File mmapDir,
                               final boolean preAlloc) {
        this.chunkSize = chunkSize;

        final AtomicLong epoch = new AtomicLong();
        this.heap = new HeapChunkAllocator(chunkSize, chunksPerSlab, maxHeap, epoch);
        this.mmap = new MmapChunkAllocator(chunkSize, mmapDir, preAlloc, epoch);

        this.evictQ = new EvictionQueue();
    }

    /**
     * Returns the sequence with the given name, creating it if absent.
     *
     * @param name the sequence name
     * @return the existing or newly created sequence
     */
    public Sequence getOrCreate(final String name) {
        return sequenceByName.computeIfAbsent(name,
                n -> new Sequence(n, chunkSize, heap, mmap, evictQ));
    }

    /**
     * Creates a {@link ForwardCursor} for the named sequence.
     *
     * @param name the sequence name
     * @return a new forward cursor
     */
    public ForwardCursor forwardCursor(final String name) {
        return new ForwardCursor(findSequenceByName(name));
    }

    /**
     * Creates a {@link BackwardCursor} for the named sequence.
     *
     * @param n the sequence name
     * @return a new backward cursor
     */
    public BackwardCursor backwardCursor(final String n) {
        return new BackwardCursor(findSequenceByName(n));
    }

    /**
     * Creates a {@link MergedForwardCursor} spanning the named sequences.
     *
     * @param names the sequence names to merge
     * @return a new merged forward cursor
     */
    public MergedForwardCursor mergedForwardCursor(final String... names) {
        final ForwardCursor[] cursors = new ForwardCursor[names.length];
        for (int cursorIndex = 0; cursorIndex < names.length; cursorIndex++) {
            cursors[cursorIndex] = new ForwardCursor(findSequenceByName(names[cursorIndex]));
        }
        return new MergedForwardCursor(cursors);
    }

    /**
     * Creates a {@link MergedBackwardCursor} spanning the named sequences.
     *
     * @param names the sequence names to merge
     * @return a new merged backward cursor
     */
    public MergedBackwardCursor mergedBackwardCursor(final String... names) {
        final BackwardCursor[] cursors = new BackwardCursor[names.length];
        for (int cursorIndex = 0; cursorIndex < names.length; cursorIndex++) {
            cursors[cursorIndex] = new BackwardCursor(findSequenceByName(names[cursorIndex]));
        }
        return new MergedBackwardCursor(cursors);
    }

    private Sequence findSequenceByName(final String name) {
        final Sequence sequence = sequenceByName.get(name);
        if (sequence == null) {
            throw new IllegalArgumentException("Unknown: " + name);
        }
        return sequence;
    }

    /**
     * Returns the underlying {@link HeapChunkAllocator}.
     *
     * @return the heap chunk allocator
     */
    public HeapChunkAllocator heap() {
        return heap;
    }
}
