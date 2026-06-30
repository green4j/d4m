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
import io.github.green4j.d4m.kv.KeyListsWriter;
import io.github.green4j.d4m.kv.ListAccessor;
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
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Measures concurrent append/list throughput for the
 * {@link KeyListStorage} API with a single writer thread and
 * 1 or 10 reader threads.
 *
 * <p>Two eviction profiles, mirroring {@link KeyValuesConcurrentReadWriteBenchmark}:
 * <ul>
 *   <li>{@code noEviction} - large hot tier; the writer cycles keys
 *       within a pre-populated range so no eviction occurs.</li>
 *   <li>{@code evict30} - 32 MB hot tier per segment (&gt; M1 Pro SLC);
 *       the pre-population partly fits in heap and partly in mmap, and
 *       the writer keeps extending lists on top of a cache-cold hot
 *       working set, so writers/readers take real CPU-cache misses on
 *       top of mmap traffic.</li>
 * </ul>
 *
 * <p>Two benchmark groups:
 * <ul>
 *   <li>{@code rw1} - 1 writer + 1 reader</li>
 *   <li>{@code rw10} - 1 writer + 10 readers</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :d4m-benchmark:jmh -PjmhArgs="KeyListsConcurrentReadWriteBenchmark"
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
@State(Scope.Group)
public class KeyListsConcurrentReadWriteBenchmark {

    private static final int PREPOPULATE_ENTRIES_PER_LIST = 10;

    @Param({"noEviction", "evict30"})
    String eviction;

    @Param({"8", "16"})
    int segments;

    KeyListStorage lists;
    UnsafeBuffer[] keys;            // shared, immutable
    UnsafeBuffer value;             // shared, immutable
    final AtomicLong written = new AtomicLong();

    /**
     * Creates and pre-populates the shared store with
     * {@link BenchmarkSupport#KEY_ARRAY_SIZE} lists * {@link
     * #PREPOPULATE_ENTRIES_PER_LIST} entries. In {@code noEviction} that
     * sits comfortably in the large hot tier; in {@code evict30} the
     * 32 MB hot tier holds part of the working set and the spillover
     * lands in mmap, with the hot footprint kept cache-cold. Writers and
     * readers cycle through the shared key array.
     */
    @Setup(Level.Trial)
    public void setup() {
        keys = BenchmarkSupport.createKeyArray();
        value = BenchmarkSupport.createValueBuffer();
        if ("noEviction".equals(eviction)) {
            lists = new KeyListStorage(
                    BenchmarkSupport.createRingNoEviction(segments));
        } else { // evict30
            lists = KeyListsBenchmarkSupport.createKeyListsEvict30(segments);
        }
        // Pre-populate in both modes so readers always have lists to load.
        // For evict30 the populate itself spills to mmap (working set >
        // hot tier), establishing the eviction-heavy state before the
        // benchmark even starts.
        KeyListsBenchmarkSupport.populate(lists, keys, value,
                BenchmarkSupport.KEY_ARRAY_SIZE, PREPOPULATE_ENTRIES_PER_LIST);
        written.set(BenchmarkSupport.KEY_ARRAY_SIZE);
    }

    /**
     * Thread-local writer state. Each writer thread owns its own
     * {@link KeyListsWriter}, honouring the "one writer per thread"
     * contract from {@code KeyListsConcurrencyTest}.
     */
    @State(Scope.Thread)
    public static class WriterState {
        KeyListsWriter writer;
        long seq;

        @Setup(Level.Trial)
        public void setup(final KeyListsConcurrentReadWriteBenchmark parent) {
            writer = parent.lists.newWriter();
            seq = parent.written.get();
        }
    }

    /**
     * Thread-local reader state. Each reader owns its own
     * {@link ListAccessor} and consumer per the API's threading rule.
     */
    @State(Scope.Thread)
    public static class ReaderState {
        ListAccessor accessor;
        ByteArrayValueConsumer consumer;
        long readSeq;

        @Setup(Level.Trial)
        public void setup() {
            accessor = new ListAccessor();
            consumer = new ByteArrayValueConsumer();
            readSeq = 0;
        }
    }

    private void doWrite(final WriterState ws) {
        final UnsafeBuffer k = keys[(int) (ws.seq & BenchmarkSupport.KEY_INDEX_MASK)];
        ws.writer.append(k, 0, BenchmarkSupport.KEY_SIZE,
                value, 0, BenchmarkSupport.VALUE_SIZE);
        written.lazySet(ws.seq);
        ws.seq++;
    }

    private int doRead(final ReaderState rs) {
        final UnsafeBuffer k = keys[(int) (rs.readSeq & BenchmarkSupport.KEY_INDEX_MASK)];
        lists.list(rs.accessor, k, 0, BenchmarkSupport.KEY_SIZE);
        final int delivered = rs.accessor.forEach(rs.consumer);
        rs.readSeq++;
        return delivered;
    }

    /**
     * Writer side for the 1W+1R group: appends one entry.
     */
    @Benchmark
    @Group("rw1")
    @GroupThreads(1)
    public void rw1Write(final WriterState ws) {
        doWrite(ws);
    }

    /**
     * Reader side for the 1W+1R group: loads one list and iterates every entry.
     */
    @Benchmark
    @Group("rw1")
    @GroupThreads(1)
    public int rw1Read(final ReaderState rs) {
        return doRead(rs);
    }

    /**
     * Writer side for the 1W+10R group: appends one entry.
     */
    @Benchmark
    @Group("rw10")
    @GroupThreads(1)
    public void rw10Write(final WriterState ws) {
        doWrite(ws);
    }

    /**
     * Reader side for the 1W+10R group: loads one list and iterates every entry.
     */
    @Benchmark
    @Group("rw10")
    @GroupThreads(10)
    public int rw10Read(final ReaderState rs) {
        return doRead(rs);
    }
}
