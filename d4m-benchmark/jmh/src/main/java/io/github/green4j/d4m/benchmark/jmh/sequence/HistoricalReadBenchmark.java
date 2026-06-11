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

import io.github.green4j.d4m.sequence.BackwardCursor;
import io.github.green4j.d4m.sequence.EntryConsumer;
import io.github.green4j.d4m.sequence.ForwardCursor;
import io.github.green4j.d4m.sequence.MergedBackwardCursor;
import io.github.green4j.d4m.sequence.MergedForwardCursor;
import io.github.green4j.d4m.sequence.Sequence;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * Measures historical (non-realtime) read throughput (entries read per second)
 * using pre-populated data. Supports forward and backward cursors as well as
 * merged variants, over 1 or 1000 sequences.
 *
 * <p>Thread count is controlled by the JMH {@code -t} flag:
 * <pre>
 *   ./gradlew :d4m-benchmark:jmh -PjmhArgs="HistoricalReadBenchmark -t 1"
 *   ./gradlew :d4m-benchmark:jmh -PjmhArgs="HistoricalReadBenchmark -t 2"
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
@State(Scope.Benchmark)
public class HistoricalReadBenchmark {

    private static final int BATCH = BenchmarkSupport.READ_BATCH;

    @Param({"65536", "131072", "524288"})
    int chunkSize;

    @Param({"1", "1000"})
    int sequenceCount;

    @Param
    BenchmarkSupport.WriteProfile writeProfile;

    @Param
    BenchmarkSupport.CursorType cursorType;

    Sequence[] sequences;

    /**
     * Creates sequences and pre-populates them with test data.
     */
    @Setup(Level.Trial)
    public void setup() {
        sequences = BenchmarkSupport.createSequences(
                sequenceCount, chunkSize,
                BenchmarkSupport.defaultMaxHeap(sequenceCount, chunkSize));
        BenchmarkSupport.populateSequences(
                sequences,
                BenchmarkSupport.entriesPerSeries(sequenceCount),
                writeProfile);
    }

    /**
     * Thread-local cursor state supporting all four cursor variants.
     */
    @State(Scope.Thread)
    public static class CursorState {
        private static final EntryConsumer NO_OP = (owner,
                                                    order,
                                                    buffer,
                                                    offset,
                                                    size) -> {
        };

        private ForwardCursor fwd;
        private BackwardCursor bwd;
        private MergedForwardCursor mfwd;
        private MergedBackwardCursor mbwd;
        private int type;

        /**
         * Creates and positions a cursor matching the configured cursor type.
         *
         * @param parent the enclosing benchmark providing sequences and cursor type
         */
        @Setup(Level.Trial)
        public void setup(final HistoricalReadBenchmark parent) {
            switch (parent.cursorType) {
                case FORWARD:
                    fwd = new ForwardCursor(parent.sequences[0]);
                    fwd.seekTo(0);
                    type = 0;
                    break;
                case BACKWARD:
                    bwd = new BackwardCursor(parent.sequences[0]);
                    bwd.seekToEnd();
                    type = 1;
                    break;
                case MERGED_FORWARD:
                    mfwd = MergedForwardCursor.create(parent.sequences);
                    mfwd.seekTo(0);
                    type = 2;
                    break;
                case MERGED_BACKWARD:
                    mbwd = MergedBackwardCursor.create(parent.sequences);
                    mbwd.seekToEnd();
                    type = 3;
                    break;
                default:
                    throw new IllegalStateException("Unknown cursor type: " + parent.cursorType);
            }
        }

        /**
         * Reads up to {@code max} entries from the active cursor.
         *
         * @param max the maximum number of entries to read
         * @return the number of entries actually read
         */
        int readBatch(final int max) {
            switch (type) {
                case 0:
                    return fwd.next(max, NO_OP);
                case 1:
                    return bwd.next(max, NO_OP);
                case 2:
                    return mfwd.next(max, NO_OP);
                case 3:
                    return mbwd.next(max, NO_OP);
                default:
                    return 0;
            }
        }

        /**
         * Repositions the cursor to its starting point.
         */
        void restart() {
            switch (type) {
                case 0:
                    fwd.seekTo(0);
                    break;
                case 1:
                    bwd.seekToEnd();
                    break;
                case 2:
                    mfwd.seekTo(0);
                    break;
                case 3:
                    mbwd.seekToEnd();
                    break;
                default:
                    break;
            }
        }

        /**
         * Closes the allocated cursor.
         */
        @TearDown(Level.Trial)
        public void tearDown() {
            if (fwd != null) {
                fwd.close();
            }
            if (bwd != null) {
                bwd.close();
            }
            if (mfwd != null) {
                mfwd.close();
            }
            if (mbwd != null) {
                mbwd.close();
            }
        }
    }

    /**
     * Reads exactly {@link #BATCH} entries, restarting the cursor when exhausted.
     *
     * @param state the thread-local cursor state
     */
    @Benchmark
    @OperationsPerInvocation(BATCH)
    public void read(final CursorState state) {
        int remaining = BATCH;
        while (remaining > 0) {
            final int n = state.readBatch(remaining);
            if (n == 0) {
                state.restart();
            } else {
                remaining -= n;
            }
        }
    }
}
