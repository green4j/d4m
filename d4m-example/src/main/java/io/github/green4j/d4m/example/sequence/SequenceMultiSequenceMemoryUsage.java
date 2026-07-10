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
import io.github.green4j.d4m.example.ExampleSupport;
import io.github.green4j.d4m.sequence.Sequence;

import static io.github.green4j.d4m.example.sequence.SequenceExampleSupport.ENTRY_BYTES;
import static io.github.green4j.d4m.example.sequence.SequenceExampleSupport.PAYLOAD_BYTES;
import static io.github.green4j.d4m.example.sequence.SequenceExampleSupport.TOTAL_ENTRIES;
import static io.github.green4j.d4m.example.sequence.SequenceExampleSupport.createMmapDir;
import static io.github.green4j.d4m.example.sequence.SequenceExampleSupport.newPayloadBuffer;
import static io.github.green4j.d4m.example.sequence.SequenceExampleSupport.newSharedSequences;
import static io.github.green4j.d4m.example.sequence.SequenceExampleSupport.printMultiSequenceStatistics;

/**
 * Single-threaded sample: distributes {@code TOTAL_ENTRIES} entries round-robin
 * across {@code SEQUENCE_COUNT} sequences that share one heap allocator, one
 * mmap allocator and one eviction queue, then reports the aggregate chunk
 * footprint. Demonstrates how splitting the same number of entries across more
 * sequences increases the footprint (each sequence rounds up to whole chunks,
 * so more sequences means more partially-filled tail chunks).
 *
 * <p><b>Module flags required:</b>
 * {@code --add-opens java.base/jdk.internal.misc=ALL-UNNAMED}
 * {@code --add-opens java.base/java.nio=ALL-UNNAMED}
 */
public final class SequenceMultiSequenceMemoryUsage {
    private static final int SEQUENCE_COUNT = ExampleSupport.getInt("d4m.seq.count", 100);
    private static final long MAX_HEAP_BYTES = (long) TOTAL_ENTRIES * ENTRY_BYTES * 2L; // heap budget large enough to
    // keep this small demo mostly on heap

    private SequenceMultiSequenceMemoryUsage() {
    }

    public static void main(final String[] args) throws Exception {
        final Sequence[] sequences = newSharedSequences(
                "multi-seq-",
                SEQUENCE_COUNT,
                MAX_HEAP_BYTES,
                createMmapDir("d4m-seq-multi-")
        );

        appendRoundRobin(sequences);

        System.out.printf(
                "Distributed %,d entries across %,d sequences%n",
                TOTAL_ENTRIES, SEQUENCE_COUNT);
        printMultiSequenceStatistics(sequences);
    }

    private static void appendRoundRobin(final Sequence[] sequences) {
        final AtomicBuffer payload = newPayloadBuffer();
        final long[] perSequenceOrder = new long[sequences.length];
        for (long i = 0; i < TOTAL_ENTRIES; i++) {
            final int s = (int) (i % sequences.length);
            final long order = perSequenceOrder[s]++;
            if (!sequences[s].append(order, payload, 0, PAYLOAD_BYTES)) {
                throw new IllegalStateException(
                        "Append failed on sequence " + s + " at order " + order);
            }
        }
    }
}
