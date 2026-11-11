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
package io.github.green4j.d4m.benchmark.jmh.queue;

import io.github.green4j.d4m.common.AtomicBuffer;
import io.github.green4j.d4m.common.UnsafeBuffer;
import io.github.green4j.d4m.queue.DiskMode;
import io.github.green4j.d4m.queue.MessageHandler;
import io.github.green4j.d4m.queue.QueueReader;
import io.github.green4j.d4m.queue.QueueWriter;
import io.github.green4j.d4m.queue.SegmentManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * Shared utilities and constants for JMH benchmarks in the d4m-queue module.
 */
public final class BenchmarkSupport {
    public static final int PAYLOAD_SIZE = 200;
    public static final int READ_BATCH = 256;

    static final int SEGMENT_SIZE = 32 * 1024 * 1024;
    static final long MAX_MEMORY_BYTES = 512L * 1024 * 1024;
    static final long MAX_LAG = 1_000_000; // about 216 MB of frames, fits the memory pool

    static final MessageHandler NO_OP = (seq, buffer, offset, length) -> true;

    private BenchmarkSupport() {
    }

    /**
     * Defines how the writer puts a message into the queue.
     */
    public enum WritePath {
        /**
         * {@code offer}: copies the payload into the queue.
         */
        OFFER,
        /**
         * {@code claim}, a copy of the payload into the claimed region, {@code commit}.
         */
        CLAIM
    }

    /**
     * Creates a segment manager with 32 MB segments and a 512 MB memory pool.
     *
     * @param diskMode the disk mode
     * @return a new manager
     */
    static SegmentManager createManager(final DiskMode diskMode) {
        return createManager(diskMode, SEGMENT_SIZE, MAX_MEMORY_BYTES);
    }

    /**
     * Creates a segment manager with 2 spare segments per queue.
     *
     * @param diskMode       the disk mode
     * @param segmentSize    the segment size in bytes
     * @param maxMemoryBytes the memory pool in bytes
     * @return a new manager
     */
    static SegmentManager createManager(final DiskMode diskMode,
                                        final int segmentSize,
                                        final long maxMemoryBytes) {
        return SegmentManager.builder()
                .withSegmentSize(segmentSize)
                .withMaxMemoryBytes(maxMemoryBytes)
                .withDiskMode(diskMode)
                .withSpareSegments(2)
                .build();
    }

    /**
     * Creates an empty temporary directory for a queue.
     *
     * @return the directory
     * @throws IOException if the directory cannot be created
     */
    static Path createDirectory() throws IOException {
        return Files.createTempDirectory("d4m-queue-jmh-");
    }

    /**
     * Deletes a directory with its content.
     *
     * @param directory the directory
     * @throws IOException if a file cannot be deleted
     */
    static void deleteDirectory(final Path directory) throws IOException {
        try (Stream<Path> paths = Files.walk(directory)) {
            for (final Path path : (Iterable<Path>) paths.sorted(Comparator.reverseOrder())::iterator) {
                Files.delete(path);
            }
        }
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
     * Writes one message of {@link #PAYLOAD_SIZE} bytes.
     *
     * @param writer  the queue writer
     * @param path    how to write
     * @param payload the message content
     * @return the sequence number, or {@link QueueWriter#REJECTED}
     */
    static long write(final QueueWriter writer,
                      final WritePath path,
                      final AtomicBuffer payload) {
        if (path == WritePath.OFFER) {
            return writer.offer(payload, 0, PAYLOAD_SIZE);
        }
        final int at = writer.claim(PAYLOAD_SIZE);
        if (at == (int) QueueWriter.REJECTED) {
            return QueueWriter.REJECTED;
        }
        writer.claimBuffer().putBytes(at, payload, 0, PAYLOAD_SIZE);
        return writer.commit(PAYLOAD_SIZE);
    }

    /**
     * Starts a thread that calls {@link SegmentManager#doWork()} in a busy loop
     * while {@code running} is set. A failure stops the thread and is recorded in
     * {@code failure}, see {@link #checkHelpers(AtomicReference)}.
     *
     * @param manager the manager
     * @param running cleared to stop the thread
     * @param failure receives the first failure of a helper thread
     * @return the started thread
     */
    static Thread startManager(final SegmentManager manager,
                               final AtomicBoolean running,
                               final AtomicReference<Throwable> failure) {
        return startHelper("queue-manager", failure, () -> {
            while (running.get()) {
                if (manager.doWork() == 0) {
                    Thread.onSpinWait();
                }
            }
        });
    }

    /**
     * Starts a thread that polls the queue in a busy loop while {@code running} is set,
     * spending about {@code workNanos} on every message. Each poll publishes the reader
     * position, {@link io.github.green4j.d4m.queue.BinaryQueue#readerSeq()}. A failure
     * stops the thread and is recorded in {@code failure}.
     *
     * @param reader    the queue reader
     * @param workNanos time spent on every message, {@code 0} for none
     * @param running   cleared to stop the thread
     * @param failure   receives the first failure of a helper thread
     * @return the started thread
     */
    static Thread startReader(final QueueReader reader,
                              final long workNanos,
                              final AtomicBoolean running,
                              final AtomicReference<Throwable> failure) {
        final MessageHandler handler = workNanos == 0 ? NO_OP : (seq, buffer, offset, length) -> {
            final long until = System.nanoTime() + workNanos;
            while (System.nanoTime() < until) {
                Thread.onSpinWait();
            }
            return true;
        };
        return startHelper("queue-reader", failure, () -> {
            while (running.get()) {
                if (reader.poll(handler, READ_BATCH) == 0) {
                    Thread.onSpinWait();
                }
            }
        });
    }

    /**
     * Throws if a helper thread has failed, so that a benchmark waiting for it fails
     * instead of spinning forever or reporting numbers without it.
     *
     * @param failure the failure recorded by the helper threads
     * @throws IllegalStateException if a helper thread has failed
     */
    static void checkHelpers(final AtomicReference<Throwable> failure) {
        final Throwable t = failure.get();
        if (t != null) {
            throw new IllegalStateException("A helper thread failed", t);
        }
    }

    private static Thread startHelper(final String name,
                                      final AtomicReference<Throwable> failure,
                                      final HelperLoop loop) {
        final Thread thread = new Thread(() -> {
            try {
                loop.run();
            } catch (final Throwable t) {
                failure.compareAndSet(null, t);
            }
        }, name);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    @FunctionalInterface
    private interface HelperLoop {
        void run() throws IOException;
    }
}
