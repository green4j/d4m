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

import io.github.green4j.d4m.example.ExampleSupport;
import io.github.green4j.d4m.queue.SegmentManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

/**
 * Shared settings and helpers of the queue examples.
 */
final class QueueExampleSupport extends ExampleSupport {
    static final int PAYLOAD_BYTES = 200;
    static final int SEGMENT_SIZE = getInt("d4m.queue.segmentSize", 32 * (int) MEGABYTE);
    static final long MAX_MEMORY_BYTES = getLong("d4m.queue.maxMemory", 512 * MEGABYTE);
    static final long MANAGER_IDLE_NANOS = getLong("d4m.queue.managerIdleNs", 10_000L);

    private QueueExampleSupport() {
    }

    /**
     * Runs {@link SegmentManager#doWork()} on a dedicated thread until {@code running} is cleared.
     * A failure stops the thread and is recorded in {@code failure}.
     *
     * @param manager the manager
     * @param running stop flag
     * @param failure receives the first failure of a helper thread
     * @return the started thread
     */
    static Thread startManager(final SegmentManager manager,
                               final AtomicBoolean running,
                               final AtomicReference<Throwable> failure) {
        return startHelper("queue-manager", failure, () -> {
            while (running.get()) {
                if (manager.doWork() == 0) {
                    LockSupport.parkNanos(MANAGER_IDLE_NANOS);
                }
            }
        });
    }

    /**
     * Starts a daemon thread that runs {@code body}; a failure stops the thread and is
     * recorded in {@code failure}.
     *
     * @param name    the thread name
     * @param failure receives the first failure of a helper thread
     * @param body    what the thread does
     * @return the started thread
     */
    static Thread startHelper(final String name,
                              final AtomicReference<Throwable> failure,
                              final HelperBody body) {
        final Thread thread = new Thread(() -> {
            try {
                body.run();
            } catch (final Throwable t) {
                failure.compareAndSet(null, t);
            }
        }, name);
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    /**
     * Throws if a helper thread has failed, so that the example fails instead of waiting
     * forever or printing numbers without it.
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

    /**
     * The body of a helper thread.
     */
    @FunctionalInterface
    interface HelperBody {
        void run() throws IOException;
    }

    static void deleteDirectory(final Path dir) throws IOException {
        try (Stream<Path> paths = Files.walk(dir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }
}
