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
 * Measures single-thread write (put) throughput for key-value storage.
 *
 * <p>Two eviction profiles:
 * <ul>
 *   <li>{@code noEviction} — large hot tier; keys cycle within a pre-populated
 *       range so no eviction ever occurs (measures pure in-memory write speed).</li>
 *   <li>{@code evict30} — small hot tier; unique keys are written continuously,
 *       causing constant eviction to mmap tiers.</li>
 * </ul>
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
        "-Xmx4g", "-Xms4g"
})
@Threads(1)
@State(Scope.Thread)
public class WriteBenchmark {

    @Param({"noEviction", "evict30"})
    String eviction;

    private KeyValueRing ring;
    private UnsafeBuffer keyBuf;
    private UnsafeBuffer valueBuf;
    private long seq;
    private long keyMod;

    /**
     * Creates the ring and pre-allocates reusable key and value buffers.
     * In {@code noEviction} mode the ring is pre-populated and keys cycle.
     * In {@code evict30} mode a small hot tier causes continuous eviction.
     */
    @Setup(Level.Trial)
    public void setup() {
        if ("noEviction".equals(eviction)) {
            ring = BenchmarkSupport.createRingNoEviction();
            BenchmarkSupport.populate(ring, BenchmarkSupport.WRITE_CYCLE_SIZE);
            keyMod = BenchmarkSupport.WRITE_CYCLE_SIZE;
        } else {
            ring = BenchmarkSupport.createRingEvictWrite();
            keyMod = Long.MAX_VALUE;
        }
        keyBuf = BenchmarkSupport.createKeyBuffer();
        valueBuf = BenchmarkSupport.createValueBuffer();
        seq = 0;
    }

    /**
     * Puts one key-value pair per invocation. In {@code noEviction} mode the key
     * cycles within the pre-populated range; in {@code evict30} mode a new unique
     * key is used each time.
     */
    @Benchmark
    public void put() {
        final long keyId = seq % keyMod;
        BenchmarkSupport.writeKeyInPlace(keyBuf, keyId);
        valueBuf.putLong(0, seq);
        ring.put(keyBuf, 0, BenchmarkSupport.KEY_SIZE,
                valueBuf, 0, BenchmarkSupport.VALUE_SIZE);
        seq++;
    }
}
