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
import io.github.green4j.d4m.kv.KeyListStorage;
import io.github.green4j.d4m.kv.ListAccessor;
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
 * Measures single-thread list-load throughput for the
 * {@link KeyListStorage} API: each invocation binds the cursor to a
 * key and iterates every entry of that list via
 * {@link ListAccessor#forEach}.
 *
 * <p>Two eviction profiles, mirroring {@link KeyValuesReadBenchmark}:
 * <ul>
 *   <li>{@code noEviction} - all pre-populated data resides in the hot
 *       tier. Working set per segment (~ 39 MB) is already cache-cold.</li>
 *   <li>{@code evict30} - 32 MB hot tier per segment (&gt; M1 Pro SLC). The
 *       hot working set is cache-cold (same scale as {@code noEviction}),
 *       with the spillover above the hot tier landing in mmap. The reader
 *       therefore takes real CPU-cache misses on top of mmap accesses.</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :d4m-benchmark:jmh -PjmhArgs="KeyListsReadBenchmark"
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
public class KeyListsReadBenchmark {

    @Param({"noEviction", "evict30"})
    String eviction;

    @Param({"10"})
    int entriesPerList;

    private KeyListStorage lists;
    private UnsafeBuffer[] keys;
    private ListAccessor accessor;
    private ByteArrayValueConsumer consumer;
    private long seq;

    /**
     * Creates and pre-populates the store with {@link
     * BenchmarkSupport#KEY_ARRAY_SIZE} lists * {@code entriesPerList}
     * entries. The hot loop indexes the pre-built key array so the
     * measured cost is just {@code lists.list(...)} + {@code forEach}.
     *
     * <p>In {@code noEviction} a large hot tier holds all data. In
     * {@code evict30} the hot tier is fixed at {@link
     * BenchmarkSupport#HOT_TIER_EVICT} per segment - large enough to be
     * cache-cold yet smaller than the pre-populated working set so some
     * entries spill to mmap.
     */
    @Setup(Level.Trial)
    public void setup() {
        keys = BenchmarkSupport.createKeyArray();
        final UnsafeBuffer value = BenchmarkSupport.createValueBuffer();
        if ("noEviction".equals(eviction)) {
            lists = KeyListsBenchmarkSupport.createKeyListsNoEviction();
        } else { // evict30
            lists = KeyListsBenchmarkSupport.createKeyListsEvict30();
        }
        KeyListsBenchmarkSupport.populate(lists, keys, value,
                BenchmarkSupport.KEY_ARRAY_SIZE, entriesPerList);
        accessor = new ListAccessor();
        consumer = new ByteArrayValueConsumer();
        seq = 0;
    }

    /**
     * Loads one list and iterates every entry - hot loop is array index
     * plus the storage call (which then delivers {@code entriesPerList}
     * entries to the consumer).
     *
     * @return the number of entries delivered to the consumer
     */
    @Benchmark
    public int list() {
        final UnsafeBuffer k = keys[(int) (seq & BenchmarkSupport.KEY_INDEX_MASK)];
        lists.list(accessor, k, 0, BenchmarkSupport.KEY_SIZE);
        final int delivered = accessor.forEach(consumer);
        seq++;
        return delivered;
    }
}
