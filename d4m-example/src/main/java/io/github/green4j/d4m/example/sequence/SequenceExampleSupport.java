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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package io.github.green4j.d4m.example.sequence;

import io.github.green4j.d4m.common.AtomicBuffer;
import io.github.green4j.d4m.common.UnsafeBuffer;
import io.github.green4j.d4m.example.ExampleSupport;
import io.github.green4j.d4m.sequence.Chunk;
import io.github.green4j.d4m.sequence.ChunkSnapshot;
import io.github.green4j.d4m.sequence.EvictionQueue;
import io.github.green4j.d4m.sequence.HeapChunkAllocator;
import io.github.green4j.d4m.sequence.MmapChunkAllocator;
import io.github.green4j.d4m.sequence.Sequence;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared helpers for {@code d4m-example} sequence samples.
 */
final class SequenceExampleSupport extends ExampleSupport {
    static final int TOTAL_ENTRIES = 10_000_000;
    static final int PAYLOAD_BYTES = 200;
    static final int ENTRY_BYTES = Chunk.entrySize(PAYLOAD_BYTES);
    static final int CHUNK_SIZE = 262_144;

    private SequenceExampleSupport() {
    }

    static AtomicBuffer newPayloadBuffer() {
        return new UnsafeBuffer(new byte[PAYLOAD_BYTES]);
    }

    static Sequence newSequence(final String name,
                                final long maxHeapBytes,
                                final File mmapDir) {
        final AtomicLong epoch = new AtomicLong();
        final HeapChunkAllocator heap =
                new HeapChunkAllocator(CHUNK_SIZE, maxHeapBytes, CHUNK_SIZE, epoch);
        final MmapChunkAllocator mmap =
                new MmapChunkAllocator(CHUNK_SIZE, mmapDir, false, epoch);
        final EvictionQueue evictQ = new EvictionQueue();
        return new Sequence(name, CHUNK_SIZE, heap, mmap, evictQ);
    }

    static File createMmapDir(final String prefix) throws IOException {
        final File dir = Files.createTempDirectory(prefix).toFile();
        dir.deleteOnExit();
        return dir;
    }

    /**
     * Heap budget tuned so roughly ~70% of logical entry bytes can live on heap chunks;
     * the remainder is expected to land on mmap-backed chunks after eviction.
     *
     * @return maximum heap bytes for the heap chunk allocator
     */
    static long maxHeapBytesForRoughlyThirtyPercentMmap() {
        return (long) TOTAL_ENTRIES * ENTRY_BYTES * 7L / 10L;
    }

    static void printSequenceStatistics(final Sequence sequence) {
        final ChunkSnapshot snap = sequence.snapshot();
        long heapEntries = 0;
        long mmapEntries = 0;
        int heapChunks = 0;
        int mmapChunks = 0;

        for (int i = 0; i < snap.size(); i++) {
            final Chunk chunk = snap.chunk(i);
            final int ec = chunk.getEntryCount();
            if (chunk.buffer().isDirect()) {
                mmapEntries += ec;
                mmapChunks++;
            } else {
                heapEntries += ec;
                heapChunks++;
            }
        }

        final long totalEntries = heapEntries + mmapEntries;
        final double mmapSharePct =
                totalEntries == 0 ? 0.0 : (100.0 * mmapEntries) / totalEntries;

        System.out.printf(
                "%s%s%s%n",
                "-".repeat(11), "[ Sequence Statistics ]", "-".repeat(11)
        );
        System.out.printf("%-28s: %13d%n", "Total entries", totalEntries);
        System.out.printf("%-28s: %13d%n", "Heap chunks", heapChunks);
        System.out.printf("%-28s: %13d%n", "Mmap chunks", mmapChunks);
        System.out.printf("%-28s: %13d%n", "Entries on heap chunks", heapEntries);
        System.out.printf("%-28s: %13d%n", "Entries on mmap chunks", mmapEntries);
        System.out.printf("%-28s: %12.2f%%%n", "Mmap entry share (approx.)", mmapSharePct);

        final long approxChunkBytes = (long) snap.size() * CHUNK_SIZE;
        System.out.printf(
                "%-28s: %13s%n",
                "Approx. chunk storage",
                formatBytesToHumanReadable(approxChunkBytes)
        );
        final long logicalPayloadBytes = totalEntries * PAYLOAD_BYTES;
        System.out.printf(
                "%-28s: %13s%n",
                "Logical payload total",
                formatBytesToHumanReadable(logicalPayloadBytes)
        );
        System.out.println(BR);
    }

    static void printThroughputLine(final String label, final long nanos, final long count) {
        final double nanosPerSecond = 1_000_000_000.0;
        final double perSecond = count / (nanos / nanosPerSecond);
        System.out.printf("%-2s%-18s: %10.4f per sec%n", " ", label, perSecond);
    }
}
