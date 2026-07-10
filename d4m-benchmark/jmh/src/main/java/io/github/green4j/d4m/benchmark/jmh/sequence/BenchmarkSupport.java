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
package io.github.green4j.d4m.benchmark.jmh.sequence;

import io.github.green4j.d4m.common.AtomicBuffer;
import io.github.green4j.d4m.common.UnsafeBuffer;
import io.github.green4j.d4m.sequence.EvictionQueue;
import io.github.green4j.d4m.sequence.HeapChunkAllocator;
import io.github.green4j.d4m.sequence.MmapChunkAllocator;
import io.github.green4j.d4m.sequence.Sequence;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared utilities and constants for JMH benchmarks in the d4m-sequence module.
 */
public final class BenchmarkSupport {
    public static final int PAYLOAD_SIZE = 200;
    public static final int READ_BATCH = 256;

    static final int RAMP_UP_ENTRIES = 100;
    static final int ENTRIES_1_SEQUENCE = 100_000;
    static final int ENTRIES_PER_1000_SEQUENCES = 5_000;

    private BenchmarkSupport() {
    }

    /**
     * Defines the mix of append, insert, and update operations used by write benchmarks.
     */
    public enum WriteProfile {
        APPEND_100(100, false),
        APPEND_90_INSERT_10(90, false),
        APPEND_50_INSERT_50(50, false),
        APPEND_90_UPDATE_10(90, true),
        APPEND_50_UPDATE_50(50, true);

        final int appendPercent;
        final boolean updateMode;

        WriteProfile(final int appendPercent, final boolean updateMode) {
            this.appendPercent = appendPercent;
            this.updateMode = updateMode;
        }
    }

    /**
     * Enumerates cursor variants available for read benchmarks.
     */
    public enum CursorType {
        FORWARD,
        BACKWARD,
        MERGED_FORWARD,
        MERGED_BACKWARD
    }

    /**
     * Creates an array of {@link Sequence} instances backed by heap and mmap allocators.
     *
     * @param count        the number of sequences to create
     * @param chunkSize    the chunk size in bytes for each sequence
     * @param maxHeapBytes the maximum number of bytes available for heap allocation
     * @return an array of newly created sequences
     */
    static Sequence[] createSequences(final int count,
                                      final int chunkSize,
                                      final long maxHeapBytes) {
        final AtomicLong epoch = new AtomicLong();
        final EvictionQueue evictQ = new EvictionQueue();
        final HeapChunkAllocator heap = new HeapChunkAllocator(
                chunkSize, 1, maxHeapBytes, epoch);
        final MmapChunkAllocator mmap = createMmap(chunkSize, epoch);
        final Sequence[] result = new Sequence[count];
        for (int i = 0; i < count; i++) {
            result[i] = new Sequence("seq-" + i, chunkSize, heap, mmap, evictQ);
        }
        return result;
    }

    /**
     * Returns a default maximum heap budget based on sequence count and chunk size.
     *
     * @param sequenceCount the number of sequences
     * @param chunkSize   the chunk size in bytes
     * @return the maximum heap budget in bytes
     */
    static long defaultMaxHeap(final int sequenceCount, final int chunkSize) {
        if (sequenceCount <= 1) {
            return (long) chunkSize * 200;
        }
        return Math.min(3L * 1024 * 1024 * 1024,
                (long) sequenceCount * chunkSize * 10);
    }

    /**
     * Creates a deterministic payload buffer of {@link #PAYLOAD_SIZE} bytes.
     *
     * @return a buffer filled with a repeating byte pattern
     */
    static AtomicBuffer createPayload() {
        final byte[] data = new byte[PAYLOAD_SIZE];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        return new UnsafeBuffer(data);
    }

    /**
     * Writes a single entry to the given sequence using the specified {@link WriteProfile}.
     *
     * @param seq           the target sequence
     * @param orderCounters per-sequence order counters, updated on append
     * @param sequenceIndex the index into {@code orderCounters} for the current sequence
     * @param opCount       the cumulative operation count, used to select append vs insert/update
     * @param profile       the write profile controlling the operation mix
     * @param payload       the payload buffer to write
     */
    static void writeEntry(final Sequence seq,
                           final long[] orderCounters,
                           final int sequenceIndex,
                           final long opCount,
                           final WriteProfile profile,
                           final AtomicBuffer payload) {
        final long sequenceOps = orderCounters[sequenceIndex];
        if (sequenceOps < RAMP_UP_ENTRIES
                || opCount % 100 < profile.appendPercent) {
            seq.append(orderCounters[sequenceIndex]++, payload, 0, PAYLOAD_SIZE);
        } else if (profile.updateMode) {
            final long order = ThreadLocalRandom.current().nextLong(sequenceOps);
            seq.insertOrUpdateUnique(order, payload, 0, PAYLOAD_SIZE);
        } else {
            final long order = ThreadLocalRandom.current().nextLong(
                    Math.max(1, sequenceOps - 1));
            seq.insert(order, payload, 0, PAYLOAD_SIZE);
        }
    }

    /**
     * Append-only fast path used by the write benchmarks when
     * {@code writeProfile == APPEND_100}. Strips the profile-decision
     * branch and the {@code appendPercent} field load that
     * {@link #writeEntry} carries for the mixed profiles.
     *
     * @param seq            the target sequence
     * @param orderCounters  per-sequence order counters; the entry at
     *                       {@code sequenceIndex} is post-incremented
     * @param sequenceIndex  the index into {@code orderCounters}
     * @param payload        the payload buffer to append
     */
    static void appendOnlyEntry(final Sequence seq,
                                final long[] orderCounters,
                                final int sequenceIndex,
                                final AtomicBuffer payload) {
        seq.append(orderCounters[sequenceIndex]++, payload, 0, PAYLOAD_SIZE);
    }

    /**
     * Pre-populates all sequences with the given number of entries per sequence.
     *
     * @param sequences        the sequences to populate
     * @param entriesPerSeries the number of entries to write into each sequence
     * @param profile          the write profile controlling the operation mix
     */
    static void populateSequences(final Sequence[] sequences,
                                  final int entriesPerSeries,
                                  final WriteProfile profile) {
        final AtomicBuffer payload = createPayload();
        for (final Sequence seq : sequences) {
            final long[] counter = {0};
            for (long op = 0; op < entriesPerSeries; op++) {
                writeEntry(seq, counter, 0, op, profile, payload);
            }
        }
    }

    /**
     * Returns the number of entries to populate per sequence based on the total sequence count.
     *
     * @param sequenceCount the number of sequences
     * @return the number of entries per sequence
     */
    static int entriesPerSeries(final int sequenceCount) {
        return sequenceCount <= 1 ? ENTRIES_1_SEQUENCE : ENTRIES_PER_1000_SEQUENCES;
    }

    private static MmapChunkAllocator createMmap(final int chunkSize,
                                                 final AtomicLong epoch) {
        try {
            final File dir = Files.createTempDirectory("d4m-jmh-").toFile();
            dir.deleteOnExit();
            return new MmapChunkAllocator(chunkSize, dir, false, epoch);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
