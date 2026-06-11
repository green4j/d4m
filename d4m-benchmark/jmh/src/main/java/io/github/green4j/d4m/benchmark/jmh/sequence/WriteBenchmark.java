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
package io.github.green4j.d4m.benchmark.jmh.sequence;

import io.github.green4j.d4m.common.AtomicBuffer;
import io.github.green4j.d4m.sequence.Sequence;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * Measures single-thread write throughput (entries written per second)
 * for one or 1000 sequences with varying write profiles and chunk sizes.
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :d4m-benchmark:jmh -PjmhArgs="WriteBenchmark"
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 10)
@Fork(value = 1, jvmArgs = {
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/java.nio=ALL-UNNAMED",
        "-Xmx8g", "-Xms8g"
})
@Threads(1)
@State(Scope.Thread)
public class WriteBenchmark {

    @Param({"65536", "131072", "524288"})
    int chunkSize;

    @Param({"1", "1000"})
    int sequenceCount;

    @Param
    BenchmarkSupport.WriteProfile writeProfile;

    private Sequence[] sequences;
    private long[] orderCounters;
    private AtomicBuffer payload;
    private long opCount;

    /**
     * Initializes sequences, order counters, and the payload buffer.
     */
    @Setup(Level.Trial)
    public void setup() {
        sequences = BenchmarkSupport.createSequences(
                sequenceCount, chunkSize,
                BenchmarkSupport.defaultMaxHeap(sequenceCount, chunkSize));
        orderCounters = new long[sequenceCount];
        payload = BenchmarkSupport.createPayload();
        opCount = 0;
    }

    /**
     * Writes one entry to the next sequence in round-robin order.
     */
    @Benchmark
    public void write() {
        final int si = (int) (opCount % sequenceCount);
        BenchmarkSupport.writeEntry(
                sequences[si], orderCounters, si, opCount, writeProfile, payload);
        opCount++;
    }
}
