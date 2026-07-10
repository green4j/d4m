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
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared test infrastructure for creating Sequence with
 * heap and mmap allocators without real filesystem dependencies.
 */
final class TestHarness {
    private static final int SLAB_SIZE = 16 * 1024;

    final int chunkSize;
    final AtomicLong epochCounter;
    final HeapChunkAllocator heap;
    final MmapChunkAllocator mmap;
    final EvictionQueue evictQ;

    /**
     * Creates a harness with the given chunk size and default heap limit.
     *
     * @param chunkSize the size of each chunk in bytes
     */
    TestHarness(final int chunkSize) {
        this(chunkSize, 64 * 1024L, null);
    }

    /**
     * Creates a harness with configurable chunk size, heap limit, and optional mmap directory.
     *
     * @param chunkSize the size of each chunk in bytes
     * @param maxHeap   maximum heap memory for chunk allocation
     * @param mmapDir   directory for memory-mapped files, or {@code null} for stub
     */
    TestHarness(final int chunkSize,
                final long maxHeap,
                final File mmapDir) {
        this.chunkSize = chunkSize;
        this.epochCounter = new AtomicLong();
        this.heap = new HeapChunkAllocator(chunkSize, SLAB_SIZE / chunkSize, maxHeap, epochCounter);
        if (mmapDir != null) {
            this.mmap = new MmapChunkAllocator(chunkSize, mmapDir, false, epochCounter);
        } else {
            this.mmap = null;
        }
        this.evictQ = new EvictionQueue();
    }

    /**
     * Creates a new {@link Sequence} with the given name.
     *
     * @param name the sequence name
     * @return a new sequence instance
     */
    Sequence createSequence(final String name) {
        final MmapChunkAllocator mmapAlloc = mmap != null ? mmap : createStubMmap();
        return new Sequence(name, chunkSize, heap, mmapAlloc, evictQ);
    }

    /**
     * Allocates a single heap chunk from the underlying allocator.
     *
     * @return a newly allocated heap chunk
     */
    Chunk allocHeapChunk() {
        return heap.tryAllocate();
    }

    /**
     * Creates a payload buffer of the given size filled with a repeating byte pattern.
     *
     * @param size the buffer size in bytes
     * @return a new buffer with patterned data
     */
    static AtomicBuffer payload(final int size) {
        final byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        return new UnsafeBuffer(data);
    }

    /**
     * Creates an 8-byte payload buffer encoding the given id in little-endian format.
     *
     * @param id the identifier to encode
     * @return a new 8-byte buffer containing the encoded id
     */
    static AtomicBuffer payloadWithId(final int id) {
        final byte[] data = new byte[8];
        data[0] = (byte) (id & 0xFF);
        data[1] = (byte) ((id >> 8) & 0xFF);
        data[2] = (byte) ((id >> 16) & 0xFF);
        data[3] = (byte) ((id >> 24) & 0xFF);
        return new UnsafeBuffer(data);
    }

    /**
     * Decodes the id from a payload byte array produced by {@link #payloadWithId(int)}.
     *
     * @param payload the payload byte array to decode
     * @return the decoded id
     */
    static int idFromPayload(final byte[] payload) {
        return (payload[0] & 0xFF)
                | ((payload[1] & 0xFF) << 8)
                | ((payload[2] & 0xFF) << 16)
                | ((payload[3] & 0xFF) << 24);
    }

    /**
     * Stub mmap allocator backed by direct ByteBuffers (no filesystem).
     *
     * @return allocator over a temporary directory
     */
    private MmapChunkAllocator createStubMmap() {
        try {
            final File tmpDir = Files.createTempDirectory("mstore-test-").toFile();
            tmpDir.deleteOnExit();
            return new MmapChunkAllocator(chunkSize, tmpDir, false, epochCounter);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}