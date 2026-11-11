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
import io.github.green4j.d4m.queue.DiskMode;
import io.github.green4j.d4m.queue.QueueWriter;
import io.github.green4j.d4m.queue.ReadMode;
import io.github.green4j.d4m.queue.SegmentManager;
import io.github.green4j.d4m.queue.SegmentedQueue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Measures the time a burst of {@code burst} messages takes to get through the queue: one
 * invocation writes the messages and waits until the reader, running on its own thread and
 * spending {@code readerNs} on every message, has consumed the last of them, i.e. until
 * {@code readerSeq()} reaches its sequence number. The reader and the manager are helper
 * threads outside JMH, so only the writer's thread is timed. The time runs from the write
 * of the first message, so it covers the writer's own work on the whole burst, the transfer
 * to the reader, the consumption and the reader's acknowledgement seen by the writer; while
 * the reader is slower than the writer, it is the wait of the last message behind the
 * backlog. A failure of a helper thread fails the benchmark.
 *
 * <ul>
 *   <li>{@code burst = 1}: the reader keeps up; the time is one round trip plus
 *   {@code readerNs}.</li>
 *   <li>{@code burst = 1000}: a short burst; the last message waits behind the others.</li>
 *   <li>{@code burst = 1000000}: about 216 MB of messages against a 64 MB memory pool.
 *   With {@code readerNs = 100} the reader is slower than the writer, the backlog fills
 *   the pool and overflows to disk, and is loaded back ahead of the reader.</li>
 * </ul>
 *
 * <p>{@link Mode#SampleTime} reports the distribution (percentiles and maximum), not just
 * the mean. The loop is closed: the next burst starts only after the previous one is
 * consumed, so the numbers show each backlog on an otherwise idle queue.</p>
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :d4m-benchmark:jmh:jmh -PjmhArgs="QueueLatencyBenchmark"
 * </pre>
 */
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 10)
@Fork(value = 1, jvmArgs = {
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/java.nio=ALL-UNNAMED",
        "-Xmx1g", "-Xms1g", "-XX:MaxDirectMemorySize=1g"
})
@Threads(1)
@State(Scope.Thread)
public class QueueLatencyBenchmark {
    static final int SEGMENT_SIZE = 8 * 1024 * 1024;
    static final long MAX_MEMORY_BYTES = 64L * 1024 * 1024;

    @Param({"1", "1000", "1000000"})
    int burst;

    @Param({"0", "100"})
    long readerNs;

    private Path directory;
    private SegmentManager manager;
    private SegmentedQueue queue;
    private QueueWriter writer;
    private AtomicBuffer payload;
    private AtomicBoolean running;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private Thread managerThread;
    private Thread readerThread;

    /**
     * Opens a queue in a fresh directory and starts the manager and reader threads.
     *
     * @throws IOException if the queue cannot be opened
     */
    @Setup(Level.Trial)
    public void setup() throws IOException {
        directory = BenchmarkSupport.createDirectory();
        manager = BenchmarkSupport.createManager(DiskMode.OVERFLOW, SEGMENT_SIZE, MAX_MEMORY_BYTES);
        queue = manager.open(directory, ReadMode.TAILING);
        writer = queue.writer();
        payload = BenchmarkSupport.createPayload();
        running = new AtomicBoolean(true);
        managerThread = BenchmarkSupport.startManager(manager, running, failure);
        readerThread = BenchmarkSupport.startReader(queue.reader(), readerNs, running, failure);
    }

    /**
     * Stops the helper threads, closes the queue and deletes its directory.
     *
     * @throws IOException if a file operation fails
     * @throws InterruptedException if interrupted while waiting for the helper threads
     */
    @TearDown(Level.Trial)
    public void tearDown() throws IOException, InterruptedException {
        running.set(false);
        readerThread.join();
        managerThread.join();
        manager.close();
        BenchmarkSupport.deleteDirectory(directory);
        BenchmarkSupport.checkHelpers(failure);
    }

    /**
     * Writes {@code burst} messages and waits until the reader has consumed the last one.
     *
     * @return the sequence number of the last message
     */
    @Benchmark
    public long burst() {
        long last = QueueWriter.REJECTED;
        for (int i = 0; i < burst; i++) {
            while ((last = writer.offer(payload, 0, BenchmarkSupport.PAYLOAD_SIZE)) == QueueWriter.REJECTED) {
                BenchmarkSupport.checkHelpers(failure);
                Thread.onSpinWait();
            }
        }
        while (queue.readerSeq() < last) {
            BenchmarkSupport.checkHelpers(failure);
            Thread.onSpinWait();
        }
        return last;
    }
}
