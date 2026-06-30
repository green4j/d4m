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
 *   <li>{@code noEviction} - all pre-populated data resides in the hot tier.</li>
 *   <li>{@code evict30} - uses the larger pre-population
 *       ({@link BenchmarkSupport#EVICT_KEY_ARRAY_SIZE}) so the working set
 *       per segment exceeds CPU cache, with a 32 MB hot tier that still
 *       spills ~30 % of data to mmap. The reader therefore takes real
 *       CPU-cache misses on top of mmap accesses - the realistic shape
 *       of a workload that has spilled past the hot tier.</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :d4m-benchmark:jmh -PjmhArgs="KeyValuesReadBenchmark"
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
public class KeyValuesReadBenchmark {

    @Param({"noEviction", "evict30"})
    String eviction;

    private KeyValueRing ring;
    private UnsafeBuffer[] keys;
    private int keyIndexMask;
    private ByteArrayValueConsumer consumer;
    private long seq;

    /**
     * Creates and pre-populates the ring. {@code noEviction} pre-populates
     * {@link BenchmarkSupport#KEY_ARRAY_SIZE} keys into the large no-eviction
     * hot tier. {@code evict30} pre-populates
     * {@link BenchmarkSupport#EVICT_KEY_ARRAY_SIZE} keys so the working
     * set per segment exceeds CPU cache and the 32 MB hot tier spills
     * ~30 % of entries to mmap.
     */
    @Setup(Level.Trial)
    public void setup() {
        final UnsafeBuffer value = BenchmarkSupport.createValueBuffer();
        final int population;
        if ("noEviction".equals(eviction)) {
            ring = BenchmarkSupport.createRingNoEviction();
            keys = BenchmarkSupport.createKeyArray();
            population = BenchmarkSupport.KEY_ARRAY_SIZE;
            keyIndexMask = BenchmarkSupport.KEY_INDEX_MASK;
        } else { // evict30
            ring = BenchmarkSupport.createRingEvict30();
            keys = BenchmarkSupport.createEvictKeyArray();
            population = BenchmarkSupport.EVICT_KEY_ARRAY_SIZE;
            keyIndexMask = BenchmarkSupport.EVICT_KEY_INDEX_MASK;
        }
        KeyValuesBenchmarkSupport.populate(ring, keys, value, population);
        consumer = new ByteArrayValueConsumer();
        seq = 0;
    }

    /**
     * Gets one value by key - hot loop is array index plus the storage call.
     *
     * @return whether a value was found for the key
     */
    @Benchmark
    public boolean get() {
        final UnsafeBuffer k = keys[(int) (seq & keyIndexMask)];
        final boolean found = ring.get(k, 0, BenchmarkSupport.KEY_SIZE, consumer);
        seq++;
        return found;
    }
}
