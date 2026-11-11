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
package io.github.green4j.d4m.queue;

import io.github.green4j.d4m.common.AtomicBuffer;
import io.github.green4j.d4m.common.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Concurrency stress tests: writer, reader and manager run on their own threads
 * for a configurable time; the reader stalls now and then so that segments are
 * written to disk and loaded back. Every message carries its sequence number, a variable
 * length and a checksum.
 *
 * <pre>
 *   -Dstress.duration.ms=60000   longer run (default 5 000)
 * </pre>
 */
class SegmentedQueueStressTest {
    private static final long DURATION_MS = Long.parseLong(System.getProperty("stress.duration.ms", "5000"));
    private static final int SEGMENT = 64 * 1024;
    private static final long SEED = 0xDEAD_BEEF_CAFE_BABEL;
    private static final long JOIN_TIMEOUT_MS = 60_000;

    @TempDir
    Path dir;

    @Test
    void overflowWithSlowReader() throws Exception {
        run(SegmentManager.builder()
                .withSegmentSize(SEGMENT)
                .withMaxMemoryBytes(8L * SEGMENT)
                .withDiskMode(DiskMode.OVERFLOW)
                .build(), ReadMode.TAILING);
    }

    @Test
    void journalWithPersistedOnlyReader() throws Exception {
        run(SegmentManager.builder()
                .withSegmentSize(SEGMENT)
                .withMaxMemoryBytes(8L * SEGMENT)
                .withDiskMode(DiskMode.JOURNAL)
                .withRetainSegments(1)
                .build(), ReadMode.PERSISTED_ONLY);
    }

    @Test
    void journalWithTailingReader() throws Exception {
        run(SegmentManager.builder()
                .withSegmentSize(SEGMENT)
                .withMaxMemoryBytes(8L * SEGMENT)
                .withDiskMode(DiskMode.JOURNAL)
                .withSpareSegments(2)
                .build(), ReadMode.TAILING);
    }

    private void run(final SegmentManager manager,
                     final ReadMode readMode) throws Exception {
        final AtomicBoolean stopped = new AtomicBoolean();
        try {
            runThreads(manager, readMode, stopped);
        } finally {
            if (stopped.get()) { // closing under running threads would free memory they use
                manager.close();
            }
        }
    }

    private void runThreads(final SegmentManager manager,
                            final ReadMode readMode,
                            final AtomicBoolean stopped) throws Exception {
        final SegmentedQueue queue = manager.open(dir, readMode);
        final AtomicBoolean writing = new AtomicBoolean(true);
        final AtomicBoolean reading = new AtomicBoolean(true);
        final AtomicBoolean managing = new AtomicBoolean(true);
        final AtomicBoolean writerDone = new AtomicBoolean();
        final AtomicLong written = new AtomicLong();
        final AtomicLong read = new AtomicLong();
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        final Thread managerThread = new Thread(() -> {
            try {
                while (managing.get()) {
                    if (manager.doWork() == 0) {
                        LockSupport.parkNanos(10_000);
                    }
                }
            } catch (final Throwable t) {
                failure.compareAndSet(null, t);
            }
        }, "manager");
        final Thread writerThread = new Thread(() -> write(queue.writer(), writing, writerDone, written, failure),
                "writer");
        final Thread readerThread = new Thread(() -> read(queue.reader(), reading, written, writerDone, read, failure),
                "reader");
        for (final Thread thread : new Thread[] {managerThread, writerThread, readerThread}) {
            thread.setDaemon(true);
            thread.start();
        }

        Thread.sleep(DURATION_MS);
        writing.set(false);
        writerThread.join(JOIN_TIMEOUT_MS);
        readerThread.join(JOIN_TIMEOUT_MS);
        final boolean readerKeptUp = !readerThread.isAlive();
        reading.set(false);
        readerThread.join(JOIN_TIMEOUT_MS);
        managing.set(false);
        managerThread.join(JOIN_TIMEOUT_MS);

        for (final Thread thread : new Thread[] {managerThread, writerThread, readerThread}) {
            assertFalse(thread.isAlive(), "The " + thread.getName() + " thread did not stop");
        }
        stopped.set(true);
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        assertTrue(readerKeptUp, "The reader did not catch up with the writer in time");
        assertTrue(written.get() > 0, "Nothing was written");
        assertEquals(written.get(), read.get(), "The reader did not receive every message");
    }

    private static void write(final QueueWriter writer,
                              final AtomicBoolean writing,
                              final AtomicBoolean writerDone,
                              final AtomicLong written,
                              final AtomicReference<Throwable> failure) {
        final UnsafeBuffer message = new UnsafeBuffer(new byte[1024]);
        long seq = 0;
        try {
            while (writing.get() && failure.get() == null) {
                final int length = 24 + (int) (seq % 1000);
                final long assigned;
                if ((seq & 1) == 0) {
                    message.putLong(0, seq);
                    message.putLong(8, length);
                    message.putLong(length - 8, seq ^ length ^ SEED);
                    assigned = writer.offer(message, 0, length);
                } else {
                    final int at = writer.claim(1024);
                    if (at != (int) QueueWriter.REJECTED) {
                        final AtomicBuffer claimed = writer.claimBuffer();
                        claimed.putLong(at, seq);
                        claimed.putLong(at + 8, length);
                        claimed.putLong(at + length - 8, seq ^ length ^ SEED);
                        assigned = writer.commit(length);
                    } else {
                        assigned = QueueWriter.REJECTED;
                    }
                }
                if (assigned == QueueWriter.REJECTED) {
                    Thread.onSpinWait();
                    continue;
                }
                if (assigned != seq) {
                    throw new AssertionError("Assigned " + assigned + " instead of " + seq);
                }
                seq++;
                written.lazySet(seq);
            }
        } catch (final Throwable t) {
            failure.compareAndSet(null, t);
        } finally {
            writerDone.set(true);
        }
    }

    private static void read(final QueueReader reader,
                             final AtomicBoolean reading,
                             final AtomicLong written,
                             final AtomicBoolean writerDone,
                             final AtomicLong read,
                             final AtomicReference<Throwable> failure) {
        final long[] expected = {0};
        final MessageHandler handler = (seq, buffer, offset, length) -> {
            if (seq != expected[0] || buffer.getLong(offset) != seq) {
                throw new AssertionError("Expected " + expected[0] + ", got seq " + seq
                        + " with payload " + buffer.getLong(offset));
            }
            if (buffer.getLong(offset + 8) != length
                    || buffer.getLong(offset + length - 8) != (seq ^ length ^ SEED)) {
                throw new AssertionError("Corrupt message " + seq);
            }
            expected[0]++;
            return true;
        };
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        try {
            while (reading.get() && failure.get() == null && (!writerDone.get() || expected[0] < written.get())) {
                if (reader.poll(handler, 1 + random.nextInt(500)) == 0) {
                    Thread.onSpinWait();
                } else if (random.nextInt(2000) == 0) {
                    LockSupport.parkNanos(random.nextInt(20_000_000)); // a stall lets the queue grow
                }
            }
        } catch (final Throwable t) {
            failure.compareAndSet(null, t);
        }
        read.set(expected[0]);
    }
}
