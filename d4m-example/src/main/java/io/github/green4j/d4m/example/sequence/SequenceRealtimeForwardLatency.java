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
import io.github.green4j.d4m.sequence.EntryConsumer;
import io.github.green4j.d4m.sequence.ForwardCursor;
import io.github.green4j.d4m.sequence.Sequence;
import org.HdrHistogram.Histogram;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static io.github.green4j.d4m.example.ExampleSupport.PERFORMANCE_RESULT_TITLE;
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
 * Writer appends with {@link System#nanoTime()} as order; reader tails forward and
 * records reader-writer latency in an {@link Histogram}.
 *
 * <p><b>Module flags required:</b>
 * {@code --add-opens java.base/jdk.internal.misc=ALL-UNNAMED}
 * {@code --add-opens java.base/java.nio=ALL-UNNAMED}
 */
public final class SequenceRealtimeForwardLatency {
    private static final int CURSOR_BATCH = 1024;
    /**
     * Target ~60s wall time for 10M appends (60e9/10e6 ns per append).
     * Wall time is often higher because {@link LockSupport#parkNanos(long)} is imprecise.
     */
    private static final long PAUSE_NANOS_PER_APPEND = 60_000_000_000L / TOTAL_ENTRIES;
    private static final long HISTOGRAM_HIGHEST_NS = 60_000_000_000L;
    private static final int HISTOGRAM_SIG_FIGS = 3;
    private static final int PRINT_STATUS_EACH_COUNT = 100_000;

    public static void main(final String[] args) throws Exception {
        final Sequence sequence =
                newSequence(
                        "realtime-latency",
                        maxHeapBytesForRoughlyThirtyPercentMmap(),
                        createMmapDir("d4m-seq-rt-")
                );
        final AtomicBuffer payload = newPayloadBuffer();
        final AtomicLong readerDelivered = new AtomicLong();

        final Histogram histogram =
                new Histogram(HISTOGRAM_HIGHEST_NS, HISTOGRAM_SIG_FIGS);

        final ForwardCursor forward = new ForwardCursor(sequence);
        final Thread reader = new Thread(
                () -> runReader(
                        forward,
                        histogram,
                        readerDelivered
                ),
                "seq-reader"
        );

        final long writeStart = System.nanoTime();
        reader.start();

        final Thread writer = new Thread(
                () -> runWriter(
                        sequence,
                        payload
                ),
                "seq-writer"
        );
        writer.start();
        writer.join();
        reader.join();

        final long writeWallNanos = System.nanoTime() - writeStart;

        printResults(writeWallNanos, histogram, readerDelivered.get());
        printSequenceStatistics(sequence);
    }

    private static void runWriter(final Sequence sequence,
                                  final AtomicBuffer payload) {
        for (long i = 0; i < TOTAL_ENTRIES; i++) {
            final long order = System.nanoTime();
            if (!sequence.append(order, payload, 0, PAYLOAD_BYTES)) {
                throw new IllegalStateException("Append failed at index " + i);
            }
            LockSupport.parkNanos(PAUSE_NANOS_PER_APPEND);
            if (i % PRINT_STATUS_EACH_COUNT == 0) {
                System.out.println("Appended  " + i + " of " + TOTAL_ENTRIES);
            }
        }
        System.out.println("Appended  " + TOTAL_ENTRIES + " total");
    }

    private static void runReader(final ForwardCursor forward,
                                  final Histogram histogram,
                                  final AtomicLong readerDelivered) {
        final EntryConsumer entryConsumer = (owner,
                                             order,
                                             buffer,
                                             offset,
                                             size) -> {
            final long now = System.nanoTime();
            final long latency = now - order;
            if (latency >= 0 && latency <= HISTOGRAM_HIGHEST_NS) {
                histogram.recordValue(latency);
            }
            final long delivered = readerDelivered.incrementAndGet();
            if (delivered % PRINT_STATUS_EACH_COUNT == 0) {
                System.out.println("Delivered " + delivered + " of " + TOTAL_ENTRIES);
            }
        };

        while (readerDelivered.get() < TOTAL_ENTRIES) {
            forward.next(CURSOR_BATCH, entryConsumer);
        }
        System.out.println("Delivered " + TOTAL_ENTRIES + " total");
    }

    private static void printResults(final long writeWallNanos,
                                     final Histogram histogram,
                                     final long delivered) {
        System.out.println(PERFORMANCE_RESULT_TITLE);
        printThroughputLine("WRITER_WALL", writeWallNanos, TOTAL_ENTRIES);
        System.out.printf(
                "%-2s%-18s: %13s%n",
                " ",
                "Reader deliveries",
                delivered
        );
        System.out.println(BR);

        System.out.printf(
                "%s%s%s%n",
                "-".repeat(9), "[ Latency Histogram (ns) ]", "-".repeat(10)
        );
        System.out.printf("%-28s: %13d%n", "Recorded samples", histogram.getTotalCount());
        if (histogram.getTotalCount() > 0) {
            System.out.printf("%-28s: %13.0f%n", "Mean", histogram.getMean());
            System.out.printf("%-28s: %13d%n", "Min", histogram.getMinValue());
            System.out.printf("%-28s: %13d%n", "Max", histogram.getMaxValue());
            printPercentileLine("p50", histogram.getValueAtPercentile(50.0));
            printPercentileLine("p90", histogram.getValueAtPercentile(90.0));
            printPercentileLine("p99", histogram.getValueAtPercentile(99.0));
            printPercentileLine("p99.9", histogram.getValueAtPercentile(99.9));
            printPercentileLine("p99.99", histogram.getValueAtPercentile(99.99));
        }
        System.out.println(BR);
    }

    private static void printPercentileLine(final String label, final double valueNs) {
        System.out.printf("%-28s: %13.0f%n", label, valueNs);
    }
}
