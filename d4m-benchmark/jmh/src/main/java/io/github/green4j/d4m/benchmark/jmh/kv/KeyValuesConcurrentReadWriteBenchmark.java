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
 *   <li>{@code evict30} - 32 MB hot tier per segment (&gt; M1 Pro SLC); the
 *       writer inserts unique keys continuously, so the hot tier fills
 *       and entries cascade to mmap. The hot working set is cache-cold,
 *       so writers and readers take real CPU-cache misses on top of mmap
 *       traffic - the realistic shape of a workload that has outgrown
 *       the hot tier.</li>
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
    UnsafeBuffer[] keys;            // populated for noEviction (shared, immutable)
    UnsafeBuffer value;             // shared, immutable
    boolean usingPreBuiltKeys;
    final AtomicLong written = new AtomicLong();

    /**
     * Creates and pre-populates the shared ring.
     * In {@code noEviction} mode the ring is pre-populated with
     * {@link BenchmarkSupport#KEY_ARRAY_SIZE} entries and reader/writer
     * cycle within that range. In {@code evict30} mode the writer keeps
     * producing fresh unique keys; the reader gates on {@link #written}
     * so it never indexes ahead of the writer.
     */
    @Setup(Level.Trial)
    public void setup() {
        value = BenchmarkSupport.createValueBuffer();
        usingPreBuiltKeys = "noEviction".equals(eviction);
        if (usingPreBuiltKeys) {
            ring = BenchmarkSupport.createRingNoEviction(segments);
            keys = BenchmarkSupport.createKeyArray();
            KeyValuesBenchmarkSupport.populate(
                    ring, keys, value, BenchmarkSupport.KEY_ARRAY_SIZE);
            written.set(BenchmarkSupport.KEY_ARRAY_SIZE);
        } else { // evict30
            ring = BenchmarkSupport.createRingEvict30(segments);
        }
    }

    /**
     * Thread-local writer state. For the eviction profiles each writer
     * holds its own {@code uniqueKeyBuf} so {@code writeKeyTail}
     * doesn't race with other writers.
     */
    @State(Scope.Thread)
    public static class WriterState {
        UnsafeBuffer uniqueKeyBuf;
        long seq;

        @Setup(Level.Trial)
        public void setup(final KeyValuesConcurrentReadWriteBenchmark parent) {
            if (!parent.usingPreBuiltKeys) {
                uniqueKeyBuf = BenchmarkSupport.createKeyBuffer();
            }
            seq = parent.written.get();
        }
    }

    /**
     * Thread-local reader state. The {@code lookupKeyBuf} is allocated only
     * for the eviction-profile paths so the reader can encode the writer's
     * trailing-digit key to look it up.
     */
    @State(Scope.Thread)
    public static class ReaderState {
        ByteArrayValueConsumer consumer;
        UnsafeBuffer lookupKeyBuf;
        long readSeq;

        @Setup(Level.Trial)
        public void setup(final KeyValuesConcurrentReadWriteBenchmark parent) {
            consumer = new ByteArrayValueConsumer();
            if (!parent.usingPreBuiltKeys) {
                lookupKeyBuf = BenchmarkSupport.createKeyBuffer();
            }
            readSeq = 0;
        }
    }

    private void doWrite(final WriterState ws) {
        final UnsafeBuffer k;
        if (usingPreBuiltKeys) {
            k = keys[(int) (ws.seq & BenchmarkSupport.KEY_INDEX_MASK)];
        } else {
            BenchmarkSupport.writeKeyTail(ws.uniqueKeyBuf, ws.seq);
            k = ws.uniqueKeyBuf;
        }
        ring.put(k, 0, BenchmarkSupport.KEY_SIZE,
                value, 0, BenchmarkSupport.VALUE_SIZE);
        written.lazySet(ws.seq);
        ws.seq++;
    }

    private boolean doRead(final ReaderState rs) {
        final UnsafeBuffer k;
        if (usingPreBuiltKeys) {
            k = keys[(int) (rs.readSeq & BenchmarkSupport.KEY_INDEX_MASK)];
        } else {
            // Same trailing-digit encoding as the writer; bounded by what's
            // been published so the reader never looks past the writer.
            final long range = Math.max(1, written.get());
            BenchmarkSupport.writeKeyTail(rs.lookupKeyBuf, rs.readSeq % range);
            k = rs.lookupKeyBuf;
        }
        final boolean found = ring.get(k, 0, BenchmarkSupport.KEY_SIZE, rs.consumer);
        rs.readSeq++;
        return found;
    }

    /**
     * Writer side for the 1W+1R group.
     */
    @Benchmark
    @Group("rw1")
    @GroupThreads(1)
    public void rw1Write(final WriterState ws) {
        doWrite(ws);
    }

    /**
     * Reader side for the 1W+1R group.
     */
    @Benchmark
    @Group("rw1")
    @GroupThreads(1)
    public boolean rw1Read(final ReaderState rs) {
        return doRead(rs);
    }

    /**
     * Writer side for the 1W+10R group.
     */
    @Benchmark
    @Group("rw10")
    @GroupThreads(1)
    public void rw10Write(final WriterState ws) {
        doWrite(ws);
    }

    /**
     * Reader side for the 1W+10R group.
     */
    @Benchmark
    @Group("rw10")
    @GroupThreads(10)
    public boolean rw10Read(final ReaderState rs) {
        return doRead(rs);
    }
}
