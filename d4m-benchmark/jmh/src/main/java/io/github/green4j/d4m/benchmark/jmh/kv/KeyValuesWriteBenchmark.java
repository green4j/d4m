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
 *   <li>{@code noEviction} - large hot tier; keys cycle within a pre-populated
 *       range so no eviction ever occurs (measures pure in-memory write speed).</li>
 *   <li>{@code evict30} - 32 MB hot tier per segment (&gt; M1 Pro SLC); unique
 *       keys are written continuously so the hot tier fills and entries
 *       cascade to mmap. The hot working set is cache-cold, so the writer
 *       takes real CPU-cache misses on top of mmap eviction - the realistic
 *       shape of a workload that has outgrown the hot tier.</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :d4m-benchmark:jmh -PjmhArgs="KeyValuesWriteBenchmark"
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
public class KeyValuesWriteBenchmark {

    @Param({"noEviction", "evict30"})
    String eviction;

    private KeyValueRing ring;
    private UnsafeBuffer[] keys;       // populated for noEviction
    private UnsafeBuffer uniqueKeyBuf; // populated for evict30
    private UnsafeBuffer value;
    private long seq;
    private boolean usingPreBuiltKeys;

    /**
     * Creates the ring and the appropriate key buffers.
     *
     * <p>In {@code noEviction} the ring is pre-populated with
     * {@link BenchmarkSupport#KEY_ARRAY_SIZE} keys. Subsequent puts are
     * in-place updates so no eviction fires; the hot loop is just an
     * array index plus the {@code ring.put}.
     *
     * <p>In {@code evict30} the writer must keep producing fresh unique
     * keys; pre-built cycling would degenerate into in-place updates and
     * miss the eviction path entirely. The hot loop encodes the sequence
     * number into one fixed buffer with a trailing-digit ASCII writer -
     * much cheaper than full {@link BenchmarkSupport#writeKeyInPlace} but
     * still produces a unique 32-byte key per call.
     */
    @Setup(Level.Trial)
    public void setup() {
        value = BenchmarkSupport.createValueBuffer();
        usingPreBuiltKeys = "noEviction".equals(eviction);
        if (usingPreBuiltKeys) {
            ring = BenchmarkSupport.createRingNoEviction();
            keys = BenchmarkSupport.createKeyArray();
            KeyValuesBenchmarkSupport.populate(
                    ring, keys, value, BenchmarkSupport.KEY_ARRAY_SIZE);
        } else { // evict30
            ring = BenchmarkSupport.createRingEvict30();
            uniqueKeyBuf = BenchmarkSupport.createKeyBuffer();
        }
        seq = 0;
    }

    /**
     * Puts one key-value pair per invocation. In the eviction profile the
     * key is encoded as the trailing ASCII digits of {@code seq} (matching
     * the original benchmark's key shape so the hash distribution and
     * eviction profile are unchanged), but without the leading-zero
     * padding rewrite that {@link BenchmarkSupport#writeKeyInPlace} does
     * every call.
     */
    @Benchmark
    public void put() {
        final UnsafeBuffer k;
        if (usingPreBuiltKeys) {
            k = keys[(int) (seq & BenchmarkSupport.KEY_INDEX_MASK)];
        } else {
            BenchmarkSupport.writeKeyTail(uniqueKeyBuf, seq);
            k = uniqueKeyBuf;
        }
        ring.put(k, 0, BenchmarkSupport.KEY_SIZE,
                value, 0, BenchmarkSupport.VALUE_SIZE);
        seq++;
    }
}
