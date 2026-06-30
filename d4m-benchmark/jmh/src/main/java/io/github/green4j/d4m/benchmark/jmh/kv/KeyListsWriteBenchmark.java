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
 * Measures single-thread append throughput for the
 * {@link KeyListStorage} API.
 *
 * <p>Two eviction profiles, mirroring {@link KeyValuesWriteBenchmark}:
 * <ul>
 *   <li>{@code noEviction} - large hot tier; the writer cycles keys
 *       within a pre-populated range so no eviction ever occurs.
 *       On Apple M1 Pro the pre-populated working set (~ 39 MB per
 *       segment) is already cache-cold.</li>
 *   <li>{@code evict30} - 32 MB hot tier per segment (&gt; M1 Pro SLC);
 *       the writer cycles keys and grows lists, so the hot tier
 *       overflows and entries cascade to mmap. The hot working set is
 *       cache-cold, so the writer takes real CPU-cache misses on top of
 *       mmap eviction.</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :d4m-benchmark:jmh -PjmhArgs="KeyListsWriteBenchmark"
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
public class KeyListsWriteBenchmark {

    @Param({"noEviction", "evict30"})
    String eviction;

    private KeyListsWriter writer;
    private UnsafeBuffer[] keys;
    private UnsafeBuffer value;
    private long seq;

    /**
     * Creates the store and the pre-built key array. The hot loop is then
     * just an array index plus the {@code writer.append} call.
     *
     * <p>In {@code noEviction} the store is pre-populated with
     * {@link BenchmarkSupport#KEY_ARRAY_SIZE} lists * {@link
     * KeyListsBenchmarkSupport#ENTRIES_PER_LIST} entries; subsequent appends
     * cycle within that key range (each list grows over the run).
     * In {@code evict30} the store starts empty; appends cycle the same
     * key array and each append grows its list, so the 32 MB hot tier
     * fills up (taking the writer's working set cache-cold) and entries
     * cascade to mmap.
     */
    @Setup(Level.Trial)
    public void setup() {
        keys = BenchmarkSupport.createKeyArray();
        value = BenchmarkSupport.createValueBuffer();
        final KeyListStorage lists;
        if ("noEviction".equals(eviction)) {
            lists = KeyListsBenchmarkSupport.createKeyListsNoEviction();
            KeyListsBenchmarkSupport.populate(lists, keys, value,
                    BenchmarkSupport.KEY_ARRAY_SIZE,
                    KeyListsBenchmarkSupport.ENTRIES_PER_LIST);
        } else { // evict30
            lists = KeyListsBenchmarkSupport.createKeyListsEvict30();
        }
        writer = lists.newWriter();
        seq = 0;
    }

    /**
     * Appends one entry per invocation - hot loop is array index plus
     * the storage call.
     */
    @Benchmark
    public void append() {
        final UnsafeBuffer k = keys[(int) (seq & BenchmarkSupport.KEY_INDEX_MASK)];
        writer.append(k, 0, BenchmarkSupport.KEY_SIZE,
                value, 0, BenchmarkSupport.VALUE_SIZE);
        seq++;
    }
}
