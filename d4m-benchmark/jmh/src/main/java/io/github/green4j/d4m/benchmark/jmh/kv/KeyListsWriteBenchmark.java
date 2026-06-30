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
import io.github.green4j.d4m.kv.KeyListStorage;
import io.github.green4j.d4m.kv.KeyListsWriter;
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
 * Single-thread append benchmark for {@link KeyListStorage},
 * measured in {@link Mode#SingleShotTime}. Each invocation appends a
 * fixed number of entries (one append per cycled list id) and
 * reports the elapsed time; per-op throughput = {@code range / time}.
 *
 * <p>Both eviction profiles share the same {@link
 * BenchmarkSupport#HOT_TIER} hot tier. Profile differentiation comes
 * from the op count:
 * <ul>
 *   <li>{@code noEviction} appends to {@link
 *       BenchmarkSupport#KL_LISTS_NO_EVICTION} lists -- the
 *       resulting working set fits in the hot tier, 0 % mmap.</li>
 *   <li>{@code evict30} appends to {@link
 *       BenchmarkSupport#KL_LISTS_EVICT_30} lists -- 30 % more
 *       than fit, 30 % spill to mmap.</li>
 * </ul>
 *
 * <p>Two write modes:
 * <ul>
 *   <li>{@code insert} -- {@code @Setup(Iteration)} builds an empty
 *       store; the timed loop appends one entry to each of {@code
 *       range} fresh list ids.</li>
 *   <li>{@code update} -- {@code @Setup(Iteration)} pre-populates
 *       the store with {@code range} lists, each holding one entry,
 *       so the timed loop's appends extend existing lists. For
 *       {@code noEviction} all metadata is in tier[0]; for
 *       {@code evict30} ~30 % of metadata reads come from
 *       mmap.</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :d4m-benchmark:jmh -PjmhArgs="KeyListsWriteBenchmark"
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
public class KeyListsWriteBenchmark {

    @Param({"noEviction", "evict30"})
    String eviction;

    @Param({"insert", "update"})
    String mode;

    private KeyListStorage lists;
    private KeyListsWriter writer;
    private UnsafeBuffer keyBuf;
    private UnsafeBuffer value;
    private int range;

    @Setup(Level.Trial)
    public void setupTrial() {
        keyBuf = BenchmarkSupport.createKeyBuffer();
        value = BenchmarkSupport.createValueBuffer();
        range = "noEviction".equals(eviction)
                ? BenchmarkSupport.KL_LISTS_NO_EVICTION
                : BenchmarkSupport.KL_LISTS_EVICT_30;
    }

    @Setup(Level.Iteration)
    public void setupIteration() {
        lists = KeyListsBenchmarkSupport.createKeyLists();
        if ("update".equals(mode)) {
            // One entry per list so the timed loop appends a second entry.
            KeyListsBenchmarkSupport.populate(lists, keyBuf, value, range, 1);
        }
        writer = lists.newWriter();
    }

    @Benchmark
    public void append() {
        for (int seq = 0; seq < range; seq++) {
            keyBuf.putLong(BenchmarkSupport.KEY_SIZE - Long.BYTES, seq);
            writer.append(keyBuf, 0, BenchmarkSupport.KEY_SIZE,
                    value, 0, BenchmarkSupport.VALUE_SIZE);
        }
    }
}
