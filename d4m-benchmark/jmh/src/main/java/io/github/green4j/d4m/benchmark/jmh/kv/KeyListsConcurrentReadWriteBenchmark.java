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

/**
 * Concurrent append/list-load throughput for {@link KeyListStorage},
 * with a single writer thread plus 1 or 10 reader threads (group mode).
 *
 * <p>Both eviction profiles share the same {@link
 * BenchmarkSupport#HOT_TIER} hot tier. {@code @Setup(Level.Trial)}
 * pre-populates the profile-specific list count ({@link
 * BenchmarkSupport#KL_LISTS_NO_EVICTION} or {@link
 * BenchmarkSupport#KL_LISTS_EVICT_30}), each list holding one entry;
 * the writer cycles {@code seq % range} appending another entry per
 * list id, and the readers cycle the same range loading lists. For
 * {@code noEviction} every access is in tier[0]; for {@code evict30}
 * ~30 % of accesses touch mmap-resident metadata or entries.
 *
 * <p>Two benchmark groups:
 * <ul>
 *   <li>{@code rw1} -- 1 writer + 1 reader</li>
 *   <li>{@code rw10} -- 1 writer + 10 readers</li>
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

    @Param({"noEviction", "evict30"})
    String eviction;

    @Param({"8", "16"})
    int segments;

    KeyListStorage lists;
    UnsafeBuffer value;
    int range;

    @Setup(Level.Trial)
    public void setup() {
        value = BenchmarkSupport.createValueBuffer();
        range = "noEviction".equals(eviction)
                ? BenchmarkSupport.KL_LISTS_NO_EVICTION
                : BenchmarkSupport.KL_LISTS_EVICT_30;
        lists = KeyListsBenchmarkSupport.createKeyLists(segments);
        final UnsafeBuffer populateKey = BenchmarkSupport.createKeyBuffer();
        // One entry per list so the writer's cyclic appends extend
        // existing lists rather than allocating new ones.
        KeyListsBenchmarkSupport.populate(lists, populateKey, value, range, 1);
    }

    @State(Scope.Thread)
    public static class WriterState {
        KeyListsWriter writer;
        UnsafeBuffer keyBuf;
        long seq;

        @Setup(Level.Trial)
        public void setup(final KeyListsConcurrentReadWriteBenchmark parent) {
            writer = parent.lists.newWriter();
            keyBuf = BenchmarkSupport.createKeyBuffer();
            seq = 0;
        }
    }

    @State(Scope.Thread)
    public static class ReaderState {
        ListAccessor accessor;
        ByteArrayValueConsumer consumer;
        UnsafeBuffer keyBuf;
        long readSeq;

        @Setup(Level.Trial)
        public void setup() {
            accessor = new ListAccessor();
            consumer = new ByteArrayValueConsumer();
            keyBuf = BenchmarkSupport.createKeyBuffer();
            readSeq = 0;
        }
    }

    private void doWrite(final WriterState ws) {
        ws.keyBuf.putLong(BenchmarkSupport.KEY_SIZE - Long.BYTES, ws.seq % range);
        ws.writer.append(ws.keyBuf, 0, BenchmarkSupport.KEY_SIZE,
                value, 0, BenchmarkSupport.VALUE_SIZE);
        ws.seq++;
    }

    private int doRead(final ReaderState rs) {
        rs.keyBuf.putLong(BenchmarkSupport.KEY_SIZE - Long.BYTES, rs.readSeq % range);
        lists.list(rs.accessor, rs.keyBuf, 0, BenchmarkSupport.KEY_SIZE);
        final int delivered = rs.accessor.forEach(rs.consumer);
        rs.readSeq++;
        return delivered;
    }

    @Benchmark
    @Group("rw1")
    @GroupThreads(1)
    public void rw1Write(final WriterState ws) {
        doWrite(ws);
    }

    @Benchmark
    @Group("rw1")
    @GroupThreads(1)
    public int rw1Read(final ReaderState rs) {
        return doRead(rs);
    }

    @Benchmark
    @Group("rw10")
    @GroupThreads(1)
    public void rw10Write(final WriterState ws) {
        doWrite(ws);
    }

    @Benchmark
    @Group("rw10")
    @GroupThreads(10)
    public int rw10Read(final ReaderState rs) {
        return doRead(rs);
    }
}
