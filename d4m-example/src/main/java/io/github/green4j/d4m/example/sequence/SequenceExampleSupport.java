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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared helpers for {@code d4m-example} sequence samples.
 */
final class SequenceExampleSupport extends ExampleSupport {
    // Overridable so the example can run small (e.g. for the README
    // memory-consumption figures) without changing its default.
    static final int TOTAL_ENTRIES = getInt("d4m.seq.entries", 10_000_000);
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
                new HeapChunkAllocator(CHUNK_SIZE, 1, maxHeapBytes, epoch,
                        HEAP_LOGGING_LISTENER);
        final MmapChunkAllocator mmap =
                new MmapChunkAllocator(CHUNK_SIZE, mmapDir, false, epoch,
                        MMAP_LOGGING_LISTENER);
        final EvictionQueue evictQ = new EvictionQueue();
        return new Sequence(name, CHUNK_SIZE, heap, mmap, evictQ,
                SEQUENCE_LOGGING_LISTENER);
    }

    /**
     * Creates {@code count} sequences that <em>share</em> one heap allocator,
     * one mmap allocator and one eviction queue (the recommended layout for
     * cooperative eviction). Used to demonstrate how many sequences affect the
     * total chunk footprint via partially-filled tail chunks.
     *
     * @param prefix       name prefix for the sequences
     * @param count        number of sequences to create
     * @param maxHeapBytes total heap budget shared across all sequences
     * @param mmapDir       folder for mmap overflow files
     * @return the created sequences, all backed by the same allocators
     */
    static Sequence[] newSharedSequences(final String prefix,
                                         final int count,
                                         final long maxHeapBytes,
                                         final File mmapDir) {
        final AtomicLong epoch = new AtomicLong();
        final HeapChunkAllocator heap =
                new HeapChunkAllocator(CHUNK_SIZE, 1, maxHeapBytes, epoch,
                        HEAP_LOGGING_LISTENER);
        final MmapChunkAllocator mmap =
                new MmapChunkAllocator(CHUNK_SIZE, mmapDir, false, epoch,
                        MMAP_LOGGING_LISTENER);
        final EvictionQueue evictQ = new EvictionQueue();
        final Sequence[] sequences = new Sequence[count];
        for (int i = 0; i < count; i++) {
            sequences[i] = new Sequence(prefix + i, CHUNK_SIZE, heap, mmap, evictQ,
                    SEQUENCE_LOGGING_LISTENER);
        }
        return sequences;
    }

    /**
     * Demonstrates the observability hooks by logging structural events to
     * {@code System.out} (equivalent of the KV store's default listener).
     *
     * <p>These listeners are attached on the writer thread and some events
     * (snapshot publish, chunk seal/evict) fire on every chunk roll-over. To
     * keep this throughput sample readable and avoid perturbing the timed loop,
     * the frequent callbacks are logged only on their <em>first</em> occurrence
     * via {@link #logFirst(String, String)}; a production listener would instead
     * forward every event to a logger or a metrics registry.</p>
     */
    private static final Set<String> LOGGED_ONCE = ConcurrentHashMap.newKeySet();

    private static void logFirst(final String key, final String message) {
        if (LOGGED_ONCE.add(key)) {
            System.out.println(message + " (further occurrences suppressed)");
        }
    }

    private static final HeapChunkAllocator.Listener HEAP_LOGGING_LISTENER =
            new HeapChunkAllocator.Listener() {
                @Override
                public void onSlabAllocated(final HeapChunkAllocator notifier,
                                            final int slabIndex,
                                            final int slabBytes,
                                            final int chunksInSlab) {
                    System.out.println("Heap slab allocated: index=" + slabIndex
                            + ", bytes=" + slabBytes + ", chunks=" + chunksInSlab);
                }

                @Override
                public void onPoolExhausted(final HeapChunkAllocator notifier) {
                    logFirst("heap.exhausted", "Heap pool exhausted");
                }

                @Override
                public void onChunkReclaimed(final HeapChunkAllocator notifier,
                                             final long chunkEpoch) {
                    logFirst("heap.reclaimed",
                            "Heap chunk reclaimed: epoch=" + chunkEpoch);
                }
            };

    private static final MmapChunkAllocator.Listener MMAP_LOGGING_LISTENER =
            new MmapChunkAllocator.Listener() {
                @Override
                public void onMemoryMappedFileFolderCleanup(final MmapChunkAllocator notifier,
                                                            final File folder,
                                                            final File deletedFile) {
                    System.out.println("Mmap file cleanup: " + deletedFile.getAbsolutePath());
                }

                @Override
                public void onMemoryMappedRegionCreated(final MmapChunkAllocator notifier,
                                                        final File file,
                                                        final long fileSize) {
                    System.out.println("Mmap region created: " + file.getAbsolutePath()
                            + ", size=" + fileSize + " bytes");
                }

                @Override
                public void onChunkReclaimed(final MmapChunkAllocator notifier,
                                             final long chunkEpoch) {
                    logFirst("mmap.reclaimed",
                            "Mmap chunk reclaimed: epoch=" + chunkEpoch);
                }
            };

    private static final Sequence.Listener SEQUENCE_LOGGING_LISTENER =
            new Sequence.Listener() {
                @Override
                public void onChunkSealedForEviction(final Sequence notifier,
                                                     final long chunkEpoch) {
                    logFirst("seq.sealed",
                            "Chunk sealed for eviction: epoch=" + chunkEpoch);
                }

                @Override
                public void onChunkEvictedToMmap(final Sequence evictor,
                                                 final Sequence owner,
                                                 final long heapEpoch,
                                                 final long mmapEpoch) {
                    logFirst("seq.evicted", "Chunk evicted to mmap: owner=" + owner.name()
                            + ", heapEpoch=" + heapEpoch + ", mmapEpoch=" + mmapEpoch);
                }

                @Override
                public void onHeapChunkSwapped(final Sequence notifier,
                                               final int chunkIndex,
                                               final long mmapEpoch) {
                    logFirst("seq.swapped", "Heap chunk swapped: index=" + chunkIndex
                            + ", mmapEpoch=" + mmapEpoch);
                }

                @Override
                public void onCowRebuild(final Sequence notifier,
                                         final int oldChunkIndex,
                                         final int newChunkCount) {
                    logFirst("seq.cow", "COW rebuild: oldIndex=" + oldChunkIndex
                            + ", newChunks=" + newChunkCount);
                }

                @Override
                public void onSnapshotPublished(final Sequence notifier,
                                                final long version,
                                                final int chunkCount) {
                    logFirst("seq.snapshot", "Snapshot published: version=" + version
                            + ", chunks=" + chunkCount);
                }
            };

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
        final ChunkSnapshot snapshot = sequence.snapshot();
        long heapEntries = 0;
        long mmapEntries = 0;
        int heapChunks = 0;
        int mmapChunks = 0;

        for (int i = 0; i < snapshot.size(); i++) {
            final Chunk chunk = snapshot.chunk(i);
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

        final long approxChunkBytes = (long) snapshot.size() * CHUNK_SIZE;
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

    /**
     * Prints aggregate chunk statistics across a set of sequences that share
     * allocators. Chunks are never shared between sequences, so the total
     * footprint is the sum of each sequence's whole-chunk allocation.
     *
     * @param sequences the sequences to aggregate over
     */
    static void printMultiSequenceStatistics(final Sequence[] sequences) {
        long totalEntries = 0;
        int totalChunks = 0;
        int heapChunks = 0;
        int mmapChunks = 0;

        for (final Sequence sequence : sequences) {
            final ChunkSnapshot snapshot = sequence.snapshot();
            totalChunks += snapshot.size();
            for (int i = 0; i < snapshot.size(); i++) {
                final Chunk chunk = snapshot.chunk(i);
                totalEntries += chunk.getEntryCount();
                if (chunk.buffer().isDirect()) {
                    mmapChunks++;
                } else {
                    heapChunks++;
                }
            }
        }

        System.out.printf(
                "%s%s%s%n",
                "-".repeat(9), "[ Multi-Sequence Statistics ]", "-".repeat(9)
        );
        System.out.printf("%-28s: %13d%n", "Number of sequences", sequences.length);
        System.out.printf("%-28s: %13d%n", "Total entries", totalEntries);
        System.out.printf("%-28s: %13d%n", "Total chunks", totalChunks);
        System.out.printf("%-28s: %13d%n", "Heap chunks", heapChunks);
        System.out.printf("%-28s: %13d%n", "Mmap chunks", mmapChunks);

        final long approxChunkBytes = (long) totalChunks * CHUNK_SIZE;
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
