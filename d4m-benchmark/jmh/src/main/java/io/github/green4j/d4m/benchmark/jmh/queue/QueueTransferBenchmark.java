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
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Measures writer-to-reader throughput: one writer thread offers messages while
 * one reader thread polls them, and the manager runs on its own thread. The
 * writer stays at most {@link BenchmarkSupport#MAX_LAG} messages ahead of the
 * reader, so the backlog fits the memory pool and the disk takes only what the
 * {@code diskMode} asks for.
 *
 * <p>The auxiliary counters {@code offered} and {@code messages} report the
 * messages written and delivered per second; the primary score counts
 * invocations, including empty polls and writer steps held back by the lag.
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :d4m-benchmark:jmh:jmh -PjmhArgs="QueueTransferBenchmark"
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 10)
@Fork(value = 1, jvmArgs = {
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/java.nio=ALL-UNNAMED",
        "-Xmx1g", "-Xms1g", "-XX:MaxDirectMemorySize=1g"
})
@State(Scope.Group)
public class QueueTransferBenchmark {

    @Param
    DiskMode diskMode;

    @Param
    BenchmarkSupport.WritePath writePath;

    private Path directory;
    private SegmentManager manager;
    private SegmentedQueue queue;
    private AtomicBoolean running;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private Thread managerThread;

    /**
     * Opens a queue in a fresh directory and starts the manager thread.
     *
     * @throws IOException if the queue cannot be opened
     */
    @Setup(Level.Trial)
    public void setup() throws IOException {
        directory = BenchmarkSupport.createDirectory();
        manager = BenchmarkSupport.createManager(diskMode);
        queue = manager.open(directory, ReadMode.TAILING);
        running = new AtomicBoolean(true);
        managerThread = BenchmarkSupport.startManager(manager, running, failure);
    }

    /**
     * Fails the iteration if the manager thread has failed, since the writer would only
     * have its offers rejected.
     */
    @TearDown(Level.Iteration)
    public void checkManager() {
        BenchmarkSupport.checkHelpers(failure);
    }

    /**
     * Stops the manager thread, closes the queue and deletes its directory.
     *
     * @throws IOException if a file operation fails
     * @throws InterruptedException if interrupted while waiting for the manager thread
     */
    @TearDown(Level.Trial)
    public void tearDown() throws IOException, InterruptedException {
        running.set(false);
        managerThread.join();
        manager.close();
        BenchmarkSupport.deleteDirectory(directory);
        BenchmarkSupport.checkHelpers(failure);
    }

    /**
     * Thread-local state for the writer side of the benchmark.
     */
    @State(Scope.Thread)
    public static class WriterState {
        AtomicBuffer payload;
        long nextSeq;
        long limit;

        /**
         * Creates the payload buffer.
         */
        @Setup(Level.Trial)
        public void setup() {
            payload = BenchmarkSupport.createPayload();
        }
    }

    /**
     * Auxiliary JMH counter for messages written.
     */
    @AuxCounters(AuxCounters.Type.OPERATIONS)
    @State(Scope.Thread)
    public static class WriterCounters {
        public long offered;

        /**
         * Resets the counter before each iteration.
         */
        @Setup(Level.Iteration)
        public void reset() {
            offered = 0;
        }
    }

    /**
     * Auxiliary JMH counter for messages delivered to the reader.
     */
    @AuxCounters(AuxCounters.Type.OPERATIONS)
    @State(Scope.Thread)
    public static class ReaderCounters {
        public long messages;

        /**
         * Resets the counter before each iteration.
         */
        @Setup(Level.Iteration)
        public void reset() {
            messages = 0;
        }
    }

    /**
     * Writer side of the transfer group: writes one message per invocation unless it is
     * {@link BenchmarkSupport#MAX_LAG} messages ahead of the reader. The reader position is
     * read again only when the cached limit is reached.
     *
     * @param ws       the thread-local writer state
     * @param counters auxiliary counters tracking messages written
     */
    @Benchmark
    @Group("transfer")
    @GroupThreads(1)
    public void writer(final WriterState ws, final WriterCounters counters) {
        if (ws.nextSeq >= ws.limit) {
            ws.limit = queue.readerSeq() + 1 + BenchmarkSupport.MAX_LAG;
            if (ws.nextSeq >= ws.limit) {
                return;
            }
        }
        if (BenchmarkSupport.write(queue.writer(), writePath, ws.payload) != QueueWriter.REJECTED) {
            ws.nextSeq++;
            counters.offered++;
        }
    }

    /**
     * Reader side of the transfer group: polls a batch of messages.
     *
     * @param counters auxiliary counters tracking messages delivered
     */
    @Benchmark
    @Group("transfer")
    @GroupThreads(1)
    public void reader(final ReaderCounters counters) {
        counters.messages += queue.reader().poll(BenchmarkSupport.NO_OP, BenchmarkSupport.READ_BATCH);
    }
}
