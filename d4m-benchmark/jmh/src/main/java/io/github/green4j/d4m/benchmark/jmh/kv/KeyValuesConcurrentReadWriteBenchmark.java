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
 * Concurrent read/write throughput for {@link KeyValueRing}, with a
 * single writer thread plus 1 or 10 reader threads (group mode).
 *
 * <p>Both eviction profiles share the same {@link
 * BenchmarkSupport#HOT_TIER} hot tier. {@code @Setup(Level.Trial)}
 * pre-populates the profile-specific entry count
 * ({@link BenchmarkSupport#KV_KEYS_NO_EVICTION} or {@link
 * BenchmarkSupport#KV_KEYS_EVICT_30}); both the writer and the
 * readers then cycle their op sequence over that range, so each
 * writer put is an in-place update and each reader get hits a
 * pre-populated key. For {@code noEviction} every access is in
 * tier[0]; for {@code evict30} ~30 % of accesses touch
 * mmap-resident entries.
 *
 * <p>Two benchmark groups:
 * <ul>
 *   <li>{@code rw1} -- 1 writer + 1 reader</li>
 *   <li>{@code rw10} -- 1 writer + 10 readers</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :d4m-benchmark:jmh -PjmhArgs="KeyValuesConcurrentReadWriteBenchmark"
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
public class KeyValuesConcurrentReadWriteBenchmark {

    @Param({"noEviction", "evict30"})
    String eviction;

    @Param({"8", "16"})
    int segments;

    KeyValueRing ring;
    UnsafeBuffer value;
    int range;

    @Setup(Level.Trial)
    public void setup() {
        value = BenchmarkSupport.createValueBuffer();
        range = "noEviction".equals(eviction)
                ? BenchmarkSupport.KV_KEYS_NO_EVICTION
                : BenchmarkSupport.KV_KEYS_EVICT_30;
        ring = BenchmarkSupport.createRing(segments);
        final UnsafeBuffer populateKey = BenchmarkSupport.createKeyBuffer();
        KeyValuesBenchmarkSupport.populate(ring, populateKey, value, range);
    }

    @State(Scope.Thread)
    public static class WriterState {
        UnsafeBuffer keyBuf;
        long seq;

        @Setup(Level.Trial)
        public void setup() {
            keyBuf = BenchmarkSupport.createKeyBuffer();
            seq = 0;
        }
    }

    @State(Scope.Thread)
    public static class ReaderState {
        ByteArrayValueConsumer consumer;
        UnsafeBuffer keyBuf;
        long readSeq;

        @Setup(Level.Trial)
        public void setup() {
            consumer = new ByteArrayValueConsumer();
            keyBuf = BenchmarkSupport.createKeyBuffer();
            readSeq = 0;
        }
    }

    private void doWrite(final WriterState ws) {
        ws.keyBuf.putLong(BenchmarkSupport.KEY_SIZE - Long.BYTES, ws.seq % range);
        ring.put(ws.keyBuf, 0, BenchmarkSupport.KEY_SIZE,
                value, 0, BenchmarkSupport.VALUE_SIZE);
        ws.seq++;
    }

    private boolean doRead(final ReaderState rs) {
        rs.keyBuf.putLong(BenchmarkSupport.KEY_SIZE - Long.BYTES, rs.readSeq % range);
        final boolean found = ring.get(rs.keyBuf, 0, BenchmarkSupport.KEY_SIZE, rs.consumer);
        rs.readSeq++;
        return found;
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
    public boolean rw1Read(final ReaderState rs) {
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
    public boolean rw10Read(final ReaderState rs) {
        return doRead(rs);
    }
}
