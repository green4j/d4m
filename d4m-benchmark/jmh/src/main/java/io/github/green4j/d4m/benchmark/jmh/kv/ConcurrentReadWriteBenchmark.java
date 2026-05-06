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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Measures concurrent write/read throughput for key-value storage with
 * a single writer thread and 1 or 10 reader threads.
 *
 * <p>Two eviction profiles:
 * <ul>
 *   <li>{@code noEviction} - large hot tier; the writer cycles keys within
 *       a pre-populated range so no eviction occurs.</li>
 *   <li>{@code evict30} - small hot tier; the writer inserts unique keys
 *       continuously, causing constant eviction to mmap tiers.</li>
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
 *   ./gradlew :d4m-benchmark:jmh -PjmhArgs="ConcurrentReadWriteBenchmark"
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
public class ConcurrentReadWriteBenchmark {

    private static final int PREPOPULATE = 10_000;

    @Param({"noEviction", "evict30"})
    String eviction;

    @Param({"8", "16"})
    int segments;

    KeyValueRing ring;
    final AtomicLong written = new AtomicLong();
    long keyMod;

    /**
     * Creates and pre-populates the shared ring.
     * In {@code noEviction} mode the ring is pre-populated with
     * {@link BenchmarkSupport#WRITE_CYCLE_SIZE} entries and the writer
     * cycles within that range. In {@code evict30} mode a small
     * hot tier is used and only a baseline set is pre-populated.
     */
    @Setup(Level.Trial)
    public void setup() {
        if ("noEviction".equals(eviction)) {
            ring = BenchmarkSupport.createRingNoEviction(segments);
            BenchmarkSupport.populate(ring, BenchmarkSupport.WRITE_CYCLE_SIZE);
            written.set(BenchmarkSupport.WRITE_CYCLE_SIZE);
            keyMod = BenchmarkSupport.WRITE_CYCLE_SIZE;
        } else {
            ring = BenchmarkSupport.createRingEvictWrite(segments);
            BenchmarkSupport.populate(ring, PREPOPULATE);
            written.set(PREPOPULATE);
            keyMod = Long.MAX_VALUE;
        }
    }

    /**
     * Thread-local writer state holding reusable key/value buffers and a sequence counter.
     */
    @State(Scope.Thread)
    public static class WriterState {
        UnsafeBuffer keyBuf;
        UnsafeBuffer valueBuf;
        long seq;

        /**
         * Initializes buffers and starts the sequence counter
         * above the pre-populated range.
         *
         * @param parent the enclosing benchmark providing the ring
         */
        @Setup(Level.Trial)
        public void setup(final ConcurrentReadWriteBenchmark parent) {
            keyBuf = BenchmarkSupport.createKeyBuffer();
            valueBuf = BenchmarkSupport.createValueBuffer();
            seq = parent.written.get();
        }
    }

    /**
     * Thread-local reader state holding a reusable key buffer, value consumer,
     * and read counter.
     */
    @State(Scope.Thread)
    public static class ReaderState {
        UnsafeBuffer keyBuf;
        ByteArrayValueConsumer consumer;
        long readSeq;

        /**
         * Initializes the key buffer and consumer.
         */
        @Setup(Level.Trial)
        public void setup() {
            keyBuf = BenchmarkSupport.createKeyBuffer();
            consumer = new ByteArrayValueConsumer();
            readSeq = 0;
        }
    }

    /**
     * Writer side for the 1W+1R group: puts one key-value pair.
     * In {@code noEviction} mode the key cycles; in {@code evict30}
     * mode a new unique key is used.
     *
     * @param ws thread-local writer state
     */
    @Benchmark
    @Group("rw1")
    @GroupThreads(1)
    public void rw1Write(final WriterState ws) {
        final long keyId = ws.seq % keyMod;
        BenchmarkSupport.writeKeyInPlace(ws.keyBuf, keyId);
        ws.valueBuf.putLong(0, ws.seq);
        ring.put(ws.keyBuf, 0, BenchmarkSupport.KEY_SIZE,
                ws.valueBuf, 0, BenchmarkSupport.VALUE_SIZE);
        written.lazySet(ws.seq);
        ws.seq++;
    }

    /**
     * Reader side for the 1W+1R group: gets one value by key from the
     * readable range.
     *
     * @param rs thread-local reader state
     * @return whether a value was found for the key
     */
    @Benchmark
    @Group("rw1")
    @GroupThreads(1)
    public boolean rw1Read(final ReaderState rs) {
        final long range = Math.min(written.get(), keyMod);
        final long keyId = rs.readSeq % range;
        BenchmarkSupport.writeKeyInPlace(rs.keyBuf, keyId);
        final boolean found = ring.get(
                rs.keyBuf, 0, BenchmarkSupport.KEY_SIZE, rs.consumer);
        rs.readSeq++;
        return found;
    }

    /**
     * Writer side for the 1W+10R group: puts one key-value pair.
     * In {@code noEviction} mode the key cycles; in {@code evict30}
     * mode a new unique key is used.
     *
     * @param ws thread-local writer state
     */
    @Benchmark
    @Group("rw10")
    @GroupThreads(1)
    public void rw10Write(final WriterState ws) {
        final long keyId = ws.seq % keyMod;
        BenchmarkSupport.writeKeyInPlace(ws.keyBuf, keyId);
        ws.valueBuf.putLong(0, ws.seq);
        ring.put(ws.keyBuf, 0, BenchmarkSupport.KEY_SIZE,
                ws.valueBuf, 0, BenchmarkSupport.VALUE_SIZE);
        written.lazySet(ws.seq);
        ws.seq++;
    }

    /**
     * Reader side for the 1W+10R group: gets one value by key from the
     * readable range.
     *
     * @param rs thread-local reader state
     * @return whether a value was found for the key
     */
    @Benchmark
    @Group("rw10")
    @GroupThreads(10)
    public boolean rw10Read(final ReaderState rs) {
        final long range = Math.min(written.get(), keyMod);
        final long keyId = rs.readSeq % range;
        BenchmarkSupport.writeKeyInPlace(rs.keyBuf, keyId);
        final boolean found = ring.get(
                rs.keyBuf, 0, BenchmarkSupport.KEY_SIZE, rs.consumer);
        rs.readSeq++;
        return found;
    }
}
