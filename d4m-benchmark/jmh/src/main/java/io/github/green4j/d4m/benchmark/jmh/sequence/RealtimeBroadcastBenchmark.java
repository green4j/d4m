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
package io.github.green4j.d4m.benchmark.jmh.sequence;

import io.github.green4j.d4m.common.AtomicBuffer;
import io.github.green4j.d4m.sequence.EntryConsumer;
import io.github.green4j.d4m.sequence.ForwardCursor;
import io.github.green4j.d4m.sequence.MergedForwardCursor;
import io.github.green4j.d4m.sequence.Sequence;
import org.openjdk.jmh.annotations.AuxCounters;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * Measures realtime broadcast throughput with a single cursor:
 * one writer thread continuously appending entries while one reader
 * thread tails with a {@link ForwardCursor} or {@link MergedForwardCursor}.
 *
 * <p>The auxiliary counter {@code entries} reports the actual number
 * of entries consumed by the reader per second.
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :d4m-benchmark:jmh -PjmhArgs="RealtimeBroadcastBenchmark"
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 10)
@Fork(value = 1, jvmArgs = {
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/java.nio=ALL-UNNAMED",
        "-Xmx8g", "-Xms8g"
})
@State(Scope.Group)
public class RealtimeBroadcastBenchmark {

    @Param({"65536", "131072", "524288"})
    int chunkSize;

    @Param({"1", "1000"})
    int seriesCount;

    @Param
    BenchmarkSupport.WriteProfile writeProfile;

    @Param({"FORWARD", "MERGED_FORWARD"})
    BenchmarkSupport.CursorType cursorType;

    Sequence[] sequences;

    /**
     * Creates the shared sequences for the benchmark group.
     */
    @Setup(Level.Trial)
    public void setup() {
        sequences = BenchmarkSupport.createSequences(
                seriesCount, chunkSize,
                BenchmarkSupport.defaultMaxHeap(seriesCount, chunkSize));
    }

    /**
     * Thread-local state for the writer side of the benchmark.
     */
    @State(Scope.Thread)
    public static class WriterState {
        long[] orderCounters;
        AtomicBuffer payload;
        long opCount;

        /**
         * Initializes order counters and the payload buffer.
         *
         * @param parent the enclosing benchmark providing series count
         */
        @Setup(Level.Trial)
        public void setup(final RealtimeBroadcastBenchmark parent) {
            orderCounters = new long[parent.seriesCount];
            payload = BenchmarkSupport.createPayload();
        }
    }

    /**
     * Thread-local state for the reader side, managing a single cursor.
     */
    @State(Scope.Thread)
    public static class ReaderState {
        private static final EntryConsumer NO_OP = (owner, order, buffer, offset, size) -> {
        };

        private ForwardCursor fwd;
        private MergedForwardCursor mfwd;
        private boolean isMerged;

        /**
         * Creates and positions the cursor based on the configured cursor type.
         *
         * @param parent the enclosing benchmark providing sequences and cursor type
         */
        @Setup(Level.Trial)
        public void setup(final RealtimeBroadcastBenchmark parent) {
            switch (parent.cursorType) {
                case MERGED_FORWARD:
                    mfwd = MergedForwardCursor.create(parent.sequences);
                    mfwd.seekTo(0);
                    isMerged = true;
                    break;
                default:
                    fwd = new ForwardCursor(parent.sequences[0]);
                    fwd.seekTo(0);
                    break;
            }
        }

        /**
         * Reads the next batch of entries from the active cursor.
         *
         * @return the number of entries read in this batch
         */
        int read() {
            if (isMerged) {
                final int n = mfwd.next(BenchmarkSupport.READ_BATCH, NO_OP);
                if (n == 0) {
                    mfwd.refreshPeeks();
                }
                return n;
            }
            return fwd.next(BenchmarkSupport.READ_BATCH, NO_OP);
        }

        /**
         * Closes the allocated cursor.
         */
        @TearDown(Level.Trial)
        public void tearDown() {
            if (fwd != null) {
                fwd.close();
            }
            if (mfwd != null) {
                mfwd.close();
            }
        }
    }

    /**
     * Auxiliary JMH counters for tracking entries consumed by the reader.
     */
    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class ReaderCounters {
        public long entries;
    }

    /**
     * Writer side of the broadcast group: appends one entry per invocation.
     *
     * @param ws the thread-local writer state
     */
    @Benchmark
    @Group("broadcast")
    @GroupThreads(1)
    public void writer(final WriterState ws) {
        final int si = (int) (ws.opCount % seriesCount);
        BenchmarkSupport.writeEntry(
                sequences[si], ws.orderCounters, si, ws.opCount, writeProfile, ws.payload);
        ws.opCount++;
    }

    /**
     * Reader side of the broadcast group: tails the cursor and accumulates entry counts.
     *
     * @param rs       the thread-local reader state
     * @param counters auxiliary counters tracking entries consumed
     */
    @Benchmark
    @Group("broadcast")
    @GroupThreads(1)
    public void reader(final ReaderState rs, final ReaderCounters counters) {
        counters.entries += rs.read();
    }
}
