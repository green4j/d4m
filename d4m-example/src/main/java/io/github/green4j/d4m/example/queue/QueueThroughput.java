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

import io.github.green4j.d4m.common.AtomicBuffer;
import io.github.green4j.d4m.common.UnsafeBuffer;
import io.github.green4j.d4m.queue.DiskMode;
import io.github.green4j.d4m.queue.MessageHandler;
import io.github.green4j.d4m.queue.QueueReader;
import io.github.green4j.d4m.queue.QueueWriter;
import io.github.green4j.d4m.queue.ReadMode;
import io.github.green4j.d4m.queue.SegmentManager;
import io.github.green4j.d4m.queue.SegmentedQueue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.green4j.d4m.example.ExampleSupport.BR;
import static io.github.green4j.d4m.example.ExampleSupport.PERFORMANCE_RESULT_TITLE;
import static io.github.green4j.d4m.example.ExampleSupport.formatBytesToHumanReadable;
import static io.github.green4j.d4m.example.ExampleSupport.getInt;
import static io.github.green4j.d4m.example.ExampleSupport.printPerformanceMetric;
import static io.github.green4j.d4m.example.queue.QueueExampleSupport.MAX_MEMORY_BYTES;
import static io.github.green4j.d4m.example.queue.QueueExampleSupport.PAYLOAD_BYTES;
import static io.github.green4j.d4m.example.queue.QueueExampleSupport.SEGMENT_SIZE;
import static io.github.green4j.d4m.example.queue.QueueExampleSupport.checkHelpers;
import static io.github.green4j.d4m.example.queue.QueueExampleSupport.deleteDirectory;
import static io.github.green4j.d4m.example.queue.QueueExampleSupport.startHelper;
import static io.github.green4j.d4m.example.queue.QueueExampleSupport.startManager;

/**
 * Writer-to-reader throughput: the writer offers 200-byte messages as fast as it
 * can (spinning on {@link QueueWriter#REJECTED}), the reader drains them in
 * batches and checks every message.
 *
 * <pre>
 *   -Dd4m.queue.messages=20000000   messages to send (default 20 000 000)
 *   -Dd4m.queue.diskMode=JOURNAL    disk mode (default OVERFLOW)
 *   -Dd4m.queue.claim=true          encode in place with claim/commit instead of offer
 *   -Dd4m.queue.maxMemory=...       memory pool, small values force overflow to disk
 * </pre>
 */
public final class QueueThroughput {
    private static final int MESSAGES = getInt("d4m.queue.messages", 20_000_000);
    private static final DiskMode DISK_MODE =
            DiskMode.valueOf(System.getProperty("d4m.queue.diskMode", DiskMode.OVERFLOW.name()));

    private static final boolean CLAIM = Boolean.getBoolean("d4m.queue.claim");

    private QueueThroughput() {
    }

    public static void main(final String[] args) throws Exception {
        final Path dir = Files.createTempDirectory("d4m-queue-throughput-");
        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        try (SegmentManager manager = SegmentManager.builder()
                .withSegmentSize(SEGMENT_SIZE)
                .withMaxMemoryBytes(MAX_MEMORY_BYTES)
                .withDiskMode(DISK_MODE)
                .withSpareSegments(2)
                .build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            final Thread managerThread = startManager(manager, running, failure);
            final long[] rejected = {0};

            final long start = System.nanoTime();
            final Thread reader = startHelper("queue-reader", failure,
                    () -> runReader(queue.reader(), running, failure));
            final long writerNanos;
            try {
                writerNanos = runWriter(queue.writer(), rejected, failure);
                reader.join();
            } finally {
                // the reader and the manager stop before the manager closes the queue
                running.set(false);
                reader.join();
                managerThread.join();
            }
            final long totalNanos = System.nanoTime() - start;
            checkHelpers(failure);

            System.out.println(PERFORMANCE_RESULT_TITLE);
            System.out.printf("%-28s: %13d%n", "Messages", MESSAGES);
            System.out.printf("%-28s: %13d%n", "Payload bytes", PAYLOAD_BYTES);
            System.out.printf("%-28s: %13s%n", "Disk mode", DISK_MODE);
            System.out.printf("%-28s: %13s%n", "Write path", CLAIM ? "claim/commit" : "offer");
            System.out.printf("%-28s: %13s%n", "Memory pool", formatBytesToHumanReadable(MAX_MEMORY_BYTES));
            System.out.printf("%-28s: %13d%n", CLAIM ? "Rejected claims" : "Rejected offers", rejected[0]);
            printPerformanceMetric(CLAIM ? "CLAIMs" : "OFFERs", MESSAGES, writerNanos);
            printPerformanceMetric("W->R", MESSAGES, totalNanos);
            System.out.println(BR);
        } finally {
            deleteDirectory(dir);
        }
    }

    private static long runWriter(final QueueWriter writer,
                                  final long[] rejected,
                                  final AtomicReference<Throwable> failure) {
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[PAYLOAD_BYTES]);
        final long start = System.nanoTime();
        for (long i = 0; i < MESSAGES; i++) {
            if (CLAIM) {
                int at;
                while ((at = writer.claim(PAYLOAD_BYTES)) == (int) QueueWriter.REJECTED) {
                    rejected[0]++;
                    checkHelpers(failure);
                    Thread.onSpinWait();
                }
                final AtomicBuffer buffer = writer.claimBuffer();
                buffer.putLong(at, i);
                buffer.putLong(at + PAYLOAD_BYTES - 8, ~i);
                writer.commit(PAYLOAD_BYTES);
                continue;
            }
            payload.putLong(0, i);
            payload.putLong(PAYLOAD_BYTES - 8, ~i);
            while (writer.offer(payload, 0, PAYLOAD_BYTES) == QueueWriter.REJECTED) {
                rejected[0]++;
                checkHelpers(failure);
                Thread.onSpinWait();
            }
        }
        return System.nanoTime() - start;
    }

    private static void runReader(final QueueReader reader,
                                  final AtomicBoolean running,
                                  final AtomicReference<Throwable> failure) {
        final long[] expected = {0};
        final MessageHandler handler = (seq, buffer, offset, length) -> {
            final long value = buffer.getLong(offset);
            if (value != expected[0] || seq != value || buffer.getLong(offset + length - 8) != ~value) {
                throw new IllegalStateException("Unexpected message " + value + " at seq " + seq);
            }
            expected[0]++;
            return true;
        };
        while (expected[0] < MESSAGES && running.get() && failure.get() == null) {
            if (reader.poll(handler, 1024) == 0) {
                Thread.onSpinWait();
            }
        }
    }
}
