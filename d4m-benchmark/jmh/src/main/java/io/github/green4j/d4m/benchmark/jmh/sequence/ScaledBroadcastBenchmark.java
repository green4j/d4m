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
 * Measures realtime broadcast throughput with 100 cursors per reader thread.
 * Two group variants:
 * <ul>
 *   <li>{@code oneReader} - 1 writer thread + 1 reader thread (100 cursors)</li>
 *   <li>{@code twoReaders} - 1 writer thread + 2 reader threads (100 cursors each)</li>
 * </ul>
 *
 * <p>Each reader round-robins across its cursors, calling
 * {@code next(READ_BATCH, ...)} on one cursor per invocation.
 * The auxiliary counter {@code entries} reports total entries consumed.
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :d4m-benchmark:jmh -PjmhArgs="ScaledBroadcastBenchmark"
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
public class ScaledBroadcastBenchmark {

    static final int CURSOR_COUNT = 100;

    @Param({"65536", "131072", "524288"})
    int chunkSize;

    @Param({"1", "1024"})
    int sequenceCount;

    @Param
    BenchmarkSupport.WriteProfile writeProfile;

    @Param({"FORWARD", "MERGED_FORWARD"})
    BenchmarkSupport.CursorType cursorType;

    Sequence[] sequences;
    boolean appendOnly;
    int sequenceMask;

    /**
     * Creates the shared sequences for the benchmark group.
     */
    @Setup(Level.Trial)
    public void setup() {
        sequences = BenchmarkSupport.createSequences(
                sequenceCount, chunkSize,
                BenchmarkSupport.defaultMaxHeap(sequenceCount, chunkSize));
        appendOnly = writeProfile == BenchmarkSupport.WriteProfile.APPEND_100;
        sequenceMask = sequenceCount - 1;
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
         * @param parent the enclosing benchmark providing sequence count
         */
        @Setup(Level.Trial)
        public void setup(final ScaledBroadcastBenchmark parent) {
            orderCounters = new long[parent.sequenceCount];
            payload = BenchmarkSupport.createPayload();
        }
    }

    /**
     * Thread-local state for the reader side, managing {@value CURSOR_COUNT} cursors.
     */
    @State(Scope.Thread)
    public static class ReaderState {
        private static final EntryConsumer NO_OP = (owner,
                                                    order,
                                                    buffer,
                                                    offset,
                                                    size) -> {
        };

        private ForwardCursor[] fwdCursors;
        private MergedForwardCursor[] mfwdCursors;
        private boolean isMerged;
        private int nextCursor;

        /**
         * Allocates and positions all cursors based on the configured cursor type.
         *
         * @param parent the enclosing benchmark providing sequences and cursor type
         */
        @Setup(Level.Trial)
        public void setup(final ScaledBroadcastBenchmark parent) {
            switch (parent.cursorType) {
                case MERGED_FORWARD:
                    mfwdCursors = new MergedForwardCursor[CURSOR_COUNT];
                    for (int i = 0; i < CURSOR_COUNT; i++) {
                        mfwdCursors[i] = MergedForwardCursor.create(parent.sequences);
                        mfwdCursors[i].seekTo(0);
                    }
                    isMerged = true;
                    break;
                default:
                    fwdCursors = new ForwardCursor[CURSOR_COUNT];
                    for (int i = 0; i < CURSOR_COUNT; i++) {
                        final int si = i % parent.sequenceCount;
                        fwdCursors[i] = new ForwardCursor(parent.sequences[si]);
                        fwdCursors[i].seekTo(0);
                    }
                    break;
            }
        }

        /**
         * Reads the next batch from the current cursor and advances the round-robin index.
         *
         * @return the number of entries read in this batch
         */
        int readNext() {
            final int ci = nextCursor;
            nextCursor = (ci + 1) % CURSOR_COUNT;
            if (isMerged) {
                final int n = mfwdCursors[ci].next(BenchmarkSupport.READ_BATCH, NO_OP);
                if (n == 0) {
                    mfwdCursors[ci].refreshPeeks();
                }
                return n;
            }
            return fwdCursors[ci].next(BenchmarkSupport.READ_BATCH, NO_OP);
        }

        /**
         * Closes all allocated cursors.
         */
        @TearDown(Level.Trial)
        public void tearDown() {
            if (fwdCursors != null) {
                for (final ForwardCursor c : fwdCursors) {
                    if (c != null) {
                        c.close();
                    }
                }
            }
            if (mfwdCursors != null) {
                for (final MergedForwardCursor c : mfwdCursors) {
                    if (c != null) {
                        c.close();
                    }
                }
            }
        }
    }

    /**
     * Auxiliary JMH counters for tracking entries consumed by readers.
     */
    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class ReaderCounters {
        public long entries;
    }

    /**
     * Writer side of the one-reader group: appends one entry per invocation.
     *
     * @param ws the thread-local writer state
     */
    @Benchmark
    @Group("oneReader")
    @GroupThreads(1)
    public void writerOneReader(final WriterState ws) {
        writeOne(ws);
    }

    /**
     * Reader side of the one-reader group: reads from the next cursor in round-robin order.
     *
     * @param rs       the thread-local reader state managing {@value CURSOR_COUNT} cursors
     * @param counters auxiliary counters tracking entries consumed
     */
    @Benchmark
    @Group("oneReader")
    @GroupThreads(1)
    public void readerOneReader(final ReaderState rs,
                                final ReaderCounters counters) {
        counters.entries += rs.readNext();
    }

    /**
     * Writer side of the two-readers group: appends one entry per invocation.
     *
     * @param ws the thread-local writer state
     */
    @Benchmark
    @Group("twoReaders")
    @GroupThreads(1)
    public void writerTwoReaders(final WriterState ws) {
        writeOne(ws);
    }

    /**
     * Reader side of the two-readers group: each thread reads from its own set of cursors.
     *
     * @param rs       the thread-local reader state managing {@value CURSOR_COUNT} cursors
     * @param counters auxiliary counters tracking entries consumed
     */
    @Benchmark
    @Group("twoReaders")
    @GroupThreads(2)
    public void readerTwoReaders(final ReaderState rs,
                                 final ReaderCounters counters) {
        counters.entries += rs.readNext();
    }

    private void writeOne(final WriterState ws) {
        final int si = (int) (ws.opCount & sequenceMask);
        if (appendOnly) {
            BenchmarkSupport.appendOnlyEntry(sequences[si], ws.orderCounters, si, ws.payload);
        } else {
            BenchmarkSupport.writeEntry(
                    sequences[si], ws.orderCounters, si, ws.opCount, writeProfile, ws.payload);
        }
        ws.opCount++;
    }
}
