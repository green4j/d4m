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
import io.github.green4j.d4m.sequence.BackwardCursor;
import io.github.green4j.d4m.sequence.EntryConsumer;
import io.github.green4j.d4m.sequence.ForwardCursor;
import io.github.green4j.d4m.sequence.Sequence;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static io.github.green4j.d4m.example.kv.ExampleSupport.PERFORMANCE_RESULT_TITLE;
import static io.github.green4j.d4m.example.sequence.SequenceExampleSupport.BR;
import static io.github.green4j.d4m.example.sequence.SequenceExampleSupport.PAYLOAD_BYTES;
import static io.github.green4j.d4m.example.sequence.SequenceExampleSupport.TOTAL_ENTRIES;
import static io.github.green4j.d4m.example.sequence.SequenceExampleSupport.createMmapDir;
import static io.github.green4j.d4m.example.sequence.SequenceExampleSupport.maxHeapBytesForRoughlyThirtyPercentMmap;
import static io.github.green4j.d4m.example.sequence.SequenceExampleSupport.newPayloadBuffer;
import static io.github.green4j.d4m.example.sequence.SequenceExampleSupport.newSequence;
import static io.github.green4j.d4m.example.sequence.SequenceExampleSupport.printSequenceStatistics;
import static io.github.green4j.d4m.example.sequence.SequenceExampleSupport.printThroughputLine;

/**
 * Single-threaded sample: bulk append, then forward and backward cursor scans.
 *
 * <p><b>Module flags required:</b>
 * {@code --add-opens java.base/jdk.internal.misc=ALL-UNNAMED}
 * {@code --add-opens java.base/java.nio=ALL-UNNAMED}
 */
public final class SequenceBulkAppendAndCursorScan {
    private static final int CURSOR_BATCH = 4096;

    public static void main(final String[] args) throws IOException {
        final Sequence sequence =
                newSequence(
                        "bulk-cursors",
                        maxHeapBytesForRoughlyThirtyPercentMmap(),
                        createMmapDir("d4m-seq-bulk-")
                );
        final AtomicBuffer payload = newPayloadBuffer();

        final long appendNanos = appendAll(sequence, payload);
        final long forwardNanos = scanForward(sequence);
        final long backwardNanos = scanBackward(sequence);

        printPerformance(
                appendNanos,
                forwardNanos,
                backwardNanos
        );
        printSequenceStatistics(sequence);
    }

    private static long appendAll(final Sequence sequence,
                                  final AtomicBuffer payload) {
        final long t0 = System.nanoTime();
        for (long order = 0; order < TOTAL_ENTRIES; order++) {
            if (!sequence.append(order, payload, 0, PAYLOAD_BYTES)) {
                throw new IllegalStateException("Append failed at order " + order);
            }
        }
        return System.nanoTime() - t0;
    }

    private static long scanForward(final Sequence sequence) {
        final ForwardCursor cursor = new ForwardCursor(sequence);
        final AtomicLong seen = new AtomicLong();
        final EntryConsumer counter = (owner,
                                       order,
                                       buffer,
                                       offset,
                                       size) -> seen.incrementAndGet();

        final long t0 = System.nanoTime();
        while (seen.get() < TOTAL_ENTRIES) {
            cursor.next(CURSOR_BATCH, counter);
        }
        return System.nanoTime() - t0;
    }

    private static long scanBackward(final Sequence sequence) {
        final BackwardCursor cursor = new BackwardCursor(sequence);
        cursor.seekToEnd();
        final AtomicLong seen = new AtomicLong();
        final EntryConsumer counter = (owner,
                                       order,
                                       buffer,
                                       offset,
                                       size) -> seen.incrementAndGet();

        final long t0 = System.nanoTime();
        while (seen.get() < TOTAL_ENTRIES) {
            cursor.next(CURSOR_BATCH, counter);
        }
        return System.nanoTime() - t0;
    }

    private static void printPerformance(final long appendNanos,
                                         final long forwardNanos,
                                         final long backwardNanos) {
        System.out.println(PERFORMANCE_RESULT_TITLE);
        printThroughputLine("APPEND", appendNanos, TOTAL_ENTRIES);
        printThroughputLine("FORWARD_READ", forwardNanos, TOTAL_ENTRIES);
        printThroughputLine("BACKWARD_READ", backwardNanos, TOTAL_ENTRIES);
        System.out.println(BR);
    }
}
