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
package io.github.green4j.d4m.example.queue;

import io.github.green4j.d4m.common.UnsafeBuffer;
import io.github.green4j.d4m.queue.MessageHandler;
import io.github.green4j.d4m.queue.QueueReader;
import io.github.green4j.d4m.queue.QueueWriter;
import io.github.green4j.d4m.queue.ReadMode;
import io.github.green4j.d4m.queue.SegmentManager;
import io.github.green4j.d4m.queue.SegmentedQueue;
import org.HdrHistogram.Histogram;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.green4j.d4m.example.ExampleSupport.BR;
import static io.github.green4j.d4m.example.ExampleSupport.PERFORMANCE_RESULT_TITLE;
import static io.github.green4j.d4m.example.ExampleSupport.getInt;
import static io.github.green4j.d4m.example.ExampleSupport.getLong;
import static io.github.green4j.d4m.example.queue.QueueExampleSupport.MAX_MEMORY_BYTES;
import static io.github.green4j.d4m.example.queue.QueueExampleSupport.PAYLOAD_BYTES;
import static io.github.green4j.d4m.example.queue.QueueExampleSupport.SEGMENT_SIZE;
import static io.github.green4j.d4m.example.queue.QueueExampleSupport.checkHelpers;
import static io.github.green4j.d4m.example.queue.QueueExampleSupport.deleteDirectory;
import static io.github.green4j.d4m.example.queue.QueueExampleSupport.startHelper;
import static io.github.green4j.d4m.example.queue.QueueExampleSupport.startManager;

/**
 * Writer-to-reader latency under low load: the writer offers a 200-byte message
 * with {@link System#nanoTime()} inside every {@code d4m.queue.pauseNs}
 * nanoseconds (busy-waiting between messages), the reader busy-spins on
 * {@link QueueReader#poll} and records the latency in an {@link Histogram}.
 *
 * <pre>
 *   -Dd4m.queue.messages=1000000   messages to send (default 1 000 000)
 *   -Dd4m.queue.pauseNs=10000      pause between messages (default 10 000 ns)
 * </pre>
 *
 * <p>The reader and the writer each need a core of their own for meaningful numbers.</p>
 */
public final class QueueRealtimeLatency {
    private static final int MESSAGES = getInt("d4m.queue.messages", 1_000_000);
    private static final long PAUSE_NANOS = getLong("d4m.queue.pauseNs", 10_000L);
    private static final int WARMUP_MESSAGES = Math.min(100_000, MESSAGES / 10);
    private static final long HISTOGRAM_HIGHEST_NS = 10_000_000_000L;

    private QueueRealtimeLatency() {
    }

    public static void main(final String[] args) throws Exception {
        final Path dir = Files.createTempDirectory("d4m-queue-latency-");
        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        try (SegmentManager manager = SegmentManager.builder()
                .withSegmentSize(SEGMENT_SIZE)
                .withMaxMemoryBytes(MAX_MEMORY_BYTES)
                .withSpareSegments(2)
                .build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            final Thread managerThread = startManager(manager, running, failure);
            final Histogram histogram = new Histogram(HISTOGRAM_HIGHEST_NS, 3);

            final Thread reader = startHelper("queue-reader", failure,
                    () -> runReader(queue.reader(), histogram, running, failure));
            final long start = System.nanoTime();
            try {
                runWriter(queue.writer(), failure);
                reader.join();
            } finally {
                // the reader and the manager stop before the manager closes the queue
                running.set(false);
                reader.join();
                managerThread.join();
            }
            final long wallNanos = System.nanoTime() - start;
            checkHelpers(failure);
            printResults(histogram, wallNanos);
        } finally {
            deleteDirectory(dir);
        }
    }

    private static void runWriter(final QueueWriter writer,
                                  final AtomicReference<Throwable> failure) {
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[PAYLOAD_BYTES]);
        long next = System.nanoTime();
        for (int i = 0; i < MESSAGES; i++) {
            next += PAUSE_NANOS;
            while (System.nanoTime() < next) {
                Thread.onSpinWait();
            }
            payload.putLong(0, System.nanoTime());
            while (writer.offer(payload, 0, PAYLOAD_BYTES) == QueueWriter.REJECTED) {
                checkHelpers(failure);
                Thread.onSpinWait();
            }
        }
    }

    private static void runReader(final QueueReader reader,
                                  final Histogram histogram,
                                  final AtomicBoolean running,
                                  final AtomicReference<Throwable> failure) {
        final int[] received = {0};
        final MessageHandler handler = (seq, buffer, offset, length) -> {
            final long latency = System.nanoTime() - buffer.getLong(offset);
            if (++received[0] > WARMUP_MESSAGES) {
                histogram.recordValue(Math.min(latency, HISTOGRAM_HIGHEST_NS));
            }
            return true;
        };
        while (received[0] < MESSAGES && running.get() && failure.get() == null) {
            if (reader.poll(handler, 256) == 0) {
                Thread.onSpinWait();
            }
        }
    }

    private static void printResults(final Histogram histogram,
                                     final long wallNanos) {
        System.out.println(PERFORMANCE_RESULT_TITLE);
        System.out.printf("%-28s: %13d%n", "Messages", MESSAGES);
        System.out.printf("%-28s: %13d%n", "Payload bytes", PAYLOAD_BYTES);
        System.out.printf("%-28s: %13d%n", "Pause between messages, ns", PAUSE_NANOS);
        System.out.printf("%-28s: %13.1f%n", "Wall time, s", wallNanos / 1e9);
        System.out.println(BR);
        System.out.printf("%s%s%s%n", "-".repeat(9), "[ Latency Histogram (ns) ]", "-".repeat(10));
        System.out.printf("%-28s: %13d%n", "Recorded samples", histogram.getTotalCount());
        System.out.printf("%-28s: %13.0f%n", "Mean", histogram.getMean());
        System.out.printf("%-28s: %13d%n", "Min", histogram.getMinValue());
        System.out.printf("%-28s: %13d%n", "p50", histogram.getValueAtPercentile(50.0));
        System.out.printf("%-28s: %13d%n", "p90", histogram.getValueAtPercentile(90.0));
        System.out.printf("%-28s: %13d%n", "p99", histogram.getValueAtPercentile(99.0));
        System.out.printf("%-28s: %13d%n", "p99.9", histogram.getValueAtPercentile(99.9));
        System.out.printf("%-28s: %13d%n", "p99.99", histogram.getValueAtPercentile(99.99));
        System.out.printf("%-28s: %13d%n", "Max", histogram.getMaxValue());
        System.out.println(BR);
    }
}
