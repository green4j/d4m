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
import io.github.green4j.d4m.queue.QueueReader;
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

/**
 * Measures the single-thread cost of a message (messages per second): one thread
 * writes a message and reads it back, and drives the manager whenever the writer
 * needs a new segment. No data crosses cores, so this is the CPU cost of the
 * queue operations themselves.
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :d4m-benchmark:jmh:jmh -PjmhArgs="QueueWriteReadBenchmark"
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
@Threads(1)
@State(Scope.Thread)
public class QueueWriteReadBenchmark {

    @Param
    DiskMode diskMode;

    @Param
    BenchmarkSupport.WritePath writePath;

    private Path directory;
    private SegmentManager manager;
    private QueueWriter writer;
    private QueueReader reader;
    private AtomicBuffer payload;

    /**
     * Opens a queue in a fresh directory.
     *
     * @throws IOException if the queue cannot be opened
     */
    @Setup(Level.Trial)
    public void setup() throws IOException {
        directory = BenchmarkSupport.createDirectory();
        manager = BenchmarkSupport.createManager(diskMode);
        final SegmentedQueue queue = manager.open(directory, ReadMode.TAILING);
        writer = queue.writer();
        reader = queue.reader();
        payload = BenchmarkSupport.createPayload();
    }

    /**
     * Closes the queue and deletes its directory.
     *
     * @throws IOException if a file operation fails
     */
    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        manager.close();
        BenchmarkSupport.deleteDirectory(directory);
    }

    /**
     * Writes one message and reads it back.
     *
     * @return the number of messages read, to keep the read alive
     * @throws IOException if the manager fails
     */
    @Benchmark
    public int writeRead() throws IOException {
        while (BenchmarkSupport.write(writer, writePath, payload) == QueueWriter.REJECTED) {
            manager.doWork();
        }
        return reader.poll(BenchmarkSupport.NO_OP, 1);
    }
}
