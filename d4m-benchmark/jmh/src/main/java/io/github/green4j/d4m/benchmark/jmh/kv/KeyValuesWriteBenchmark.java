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
 * Single-thread write benchmark for {@link KeyValueRing}, measured in
 * {@link Mode#SingleShotTime}. Each invocation does a fixed number of
 * puts in a tight loop and reports the elapsed time; per-op
 * throughput = {@code range / time}.
 *
 * <p>Both eviction profiles share the same {@link
 * BenchmarkSupport#HOT_TIER} hot tier. Profile differentiation comes
 * from the op count:
 * <ul>
 *   <li>{@code noEviction} writes {@link BenchmarkSupport#KV_KEYS_NO_EVICTION}
 *       entries -- all fit in the hot tier, 0 % mmap.</li>
 *   <li>{@code evict30} writes {@link BenchmarkSupport#KV_KEYS_EVICT_30}
 *       entries -- 30 % more than fit, 30 % spill to mmap.</li>
 * </ul>
 *
 * <p>Two write modes:
 * <ul>
 *   <li>{@code insert} -- {@code @Setup(Iteration)} builds an empty
 *       ring; the timed loop puts {@code range} fresh keys.</li>
 *   <li>{@code update} -- {@code @Setup(Iteration)} pre-populates
 *       the ring with {@code range} entries, so the timed loop's
 *       puts are in-place updates of existing keys (for
 *       {@code noEviction} all updates hit tier[0]; for
 *       {@code evict30} ~30 % of updates touch mmap-resident
 *       keys and cascade).</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :d4m-benchmark:jmh -PjmhArgs="KeyValuesWriteBenchmark"
 * </pre>
 */
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@Fork(value = 1, jvmArgs = {
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/java.nio=ALL-UNNAMED",
        "-Xmx4g", "-Xms4g"
})
@Threads(1)
@State(Scope.Thread)
public class KeyValuesWriteBenchmark {

    @Param({"noEviction", "evict30"})
    String eviction;

    @Param({"insert", "update"})
    String mode;

    private KeyValueRing ring;
    private UnsafeBuffer keyBuf;
    private UnsafeBuffer value;
    private int range;

    @Setup(Level.Trial)
    public void setupTrial() {
        keyBuf = BenchmarkSupport.createKeyBuffer();
        value = BenchmarkSupport.createValueBuffer();
        range = "noEviction".equals(eviction)
                ? BenchmarkSupport.KV_KEYS_NO_EVICTION
                : BenchmarkSupport.KV_KEYS_EVICT_30;
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
        ring = BenchmarkSupport.createRing();
        if ("update".equals(mode)) {
            KeyValuesBenchmarkSupport.populate(ring, keyBuf, value, range);
        }
    }

    @Benchmark
    public void put() {
        for (int seq = 0; seq < range; seq++) {
            keyBuf.putLong(BenchmarkSupport.KEY_SIZE - Long.BYTES, seq);
            ring.put(keyBuf, 0, BenchmarkSupport.KEY_SIZE,
                    value, 0, BenchmarkSupport.VALUE_SIZE);
        }
    }
}
