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
package io.github.green4j.d4m.benchmark.jmh.kv;

import io.github.green4j.d4m.common.UnsafeBuffer;
import io.github.green4j.d4m.kv.ByteArrayValueConsumer;
import io.github.green4j.d4m.kv.KeyValueRing;
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
 * Measures single-thread read (get) throughput for key-value storage.
 *
 * <p>Two eviction profiles:
 * <ul>
 *   <li>{@code noEviction} — all pre-populated data resides in the hot tier.</li>
 *   <li>{@code evict30} — the hot tier is sized so that approximately 30%
 *       of the pre-populated data spills to mmap tiers. Reads hit both
 *       hot and mmap storage.</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :d4m-benchmark:jmh -PjmhArgs="ReadBenchmark"
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 10)
@Fork(value = 1, jvmArgs = {
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/java.nio=ALL-UNNAMED",
        "-Xmx4g", "-Xms4g"
})
@Threads(1)
@State(Scope.Thread)
public class ReadBenchmark {

    @Param({"noEviction", "evict30"})
    String eviction;

    @Param({"100000"})
    int populationSize;

    private KeyValueRing ring;
    private UnsafeBuffer keyBuf;
    private ByteArrayValueConsumer consumer;
    private long seq;

    /**
     * Creates and pre-populates the ring. In {@code noEviction} mode
     * a large hot tier holds all data. In {@code evict30} mode the hot
     * tier is sized to hold approximately 70% of the population.
     */
    @Setup(Level.Trial)
    public void setup() {
        if ("noEviction".equals(eviction)) {
            ring = BenchmarkSupport.createRingNoEviction();
        } else {
            ring = BenchmarkSupport.createRingEvict30(populationSize);
        }
        BenchmarkSupport.populate(ring, populationSize);
        keyBuf = BenchmarkSupport.createKeyBuffer();
        consumer = new ByteArrayValueConsumer();
        seq = 0;
    }

    /**
     * Gets one value by key. Keys cycle through the pre-populated range.
     *
     * @return whether a value was found for the key
     */
    @Benchmark
    public boolean get() {
        final long keyId = seq % populationSize;
        BenchmarkSupport.writeKeyInPlace(keyBuf, keyId);
        final boolean found = ring.get(keyBuf, 0, BenchmarkSupport.KEY_SIZE, consumer);
        seq++;
        return found;
    }
}
