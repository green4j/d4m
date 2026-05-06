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
package io.github.green4j.d4m.sequence;

import io.github.green4j.d4m.common.AtomicBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link MergedBackwardCursor}.
 */
class MergedBackwardCursorTest {
    private static final int CHUNK_SIZE = 1024;
    private static final int MSG_PAYLOAD = 8;

    private TestHarness harness;

    @BeforeEach
    void setUp() {
        harness = new TestHarness(CHUNK_SIZE);
    }

    private void appendWithId(final Sequence ts, final long order, final int id) {
        final AtomicBuffer pl = TestHarness.payloadWithId(id);
        ts.append(order, pl, 0, MSG_PAYLOAD);
    }

    private List<int[]> drainMerged(final MergedBackwardCursor cursor, final int max) {
        final List<int[]> collected = new ArrayList<>();
        cursor.next(max, (sourceIndex, owner, order, buf, off, size) -> {
            final byte[] copy = new byte[size];
            buf.getBytes(off, copy, 0, size);
            collected.add(new int[]{sourceIndex, TestHarness.idFromPayload(copy)});
        });
        return collected;
    }

    private List<byte[]> drainPlain(final MergedBackwardCursor cursor, final int max) {
        final List<byte[]> collected = new ArrayList<>();
        cursor.next(max, (owner, order, buf, off, size) -> {
            final byte[] copy = new byte[size];
            buf.getBytes(off, copy, 0, size);
            collected.add(copy);
        });
        return collected;
    }

    @Nested
    class TwoSeriesMerge {
        @Test
        void interleavedOrdersAreMergedInReverseOrder() {
            final Sequence tsA = harness.createTimeSeries("A");
            final Sequence tsB = harness.createTimeSeries("B");

            appendWithId(tsA, 10, 1);
            appendWithId(tsA, 30, 3);
            appendWithId(tsA, 50, 5);
            appendWithId(tsB, 20, 2);
            appendWithId(tsB, 40, 4);
            appendWithId(tsB, 60, 6);

            final MergedBackwardCursor cursor = MergedBackwardCursor.create(tsA, tsB);
            final List<int[]> result = drainMerged(cursor, 100);

            assertEquals(6, result.size());
            // Reverse order: 6, 5, 4, 3, 2, 1
            for (int i = 0; i < 6; i++) {
                assertEquals(6 - i, result.get(i)[1]);
            }
            cursor.close();
        }

        @Test
        void sourceIndexIsCorrectInReverse() {
            final Sequence tsA = harness.createTimeSeries("A");
            final Sequence tsB = harness.createTimeSeries("B");

            appendWithId(tsA, 10, 1);
            appendWithId(tsB, 20, 2);
            appendWithId(tsA, 30, 3);

            final MergedBackwardCursor cursor = MergedBackwardCursor.create(tsA, tsB);
            final List<int[]> result = drainMerged(cursor, 100);

            // Reverse: ts=30(A), ts=20(B), ts=10(A)
            assertEquals(0, result.get(0)[0]); // a
            assertEquals(1, result.get(1)[0]); // b
            assertEquals(0, result.get(2)[0]); // a
            cursor.close();
        }
    }

    @Nested
    class EmptySeries {
        @Test
        void allEmptyReturnsNothing() {
            final Sequence tsA = harness.createTimeSeries("A");
            final Sequence tsB = harness.createTimeSeries("B");

            final MergedBackwardCursor cursor = MergedBackwardCursor.create(tsA, tsB);
            final List<byte[]> result = drainPlain(cursor, 100);

            assertTrue(result.isEmpty());
            cursor.close();
        }
    }

    @Nested
    class SeekTo {
        @Test
        void seekLimitsAllSeries() {
            final Sequence tsA = harness.createTimeSeries("A");
            final Sequence tsB = harness.createTimeSeries("B");
            appendWithId(tsA, 10, 1);
            appendWithId(tsA, 30, 3);
            appendWithId(tsB, 20, 2);
            appendWithId(tsB, 40, 4);

            final MergedBackwardCursor cursor = MergedBackwardCursor.create(tsA, tsB);
            cursor.seekTo(25L);
            final List<int[]> result = drainMerged(cursor, 100);

            // Entries <= 25: ts=20(id=2), ts=10(id=1) in reverse
            assertEquals(2, result.size());
            assertEquals(2, result.get(0)[1]);
            assertEquals(1, result.get(1)[1]);
            cursor.close();
        }
    }

    @Nested
    class SeekToEnd {
        @Test
        void seekToEndResetsToTail() {
            final Sequence tsA = harness.createTimeSeries("A");
            appendWithId(tsA, 10, 1);
            appendWithId(tsA, 20, 2);

            final MergedBackwardCursor cursor = MergedBackwardCursor.create(tsA);
            drainPlain(cursor, 1); // consume ts=20
            cursor.seekToEnd();

            final List<byte[]> result = drainPlain(cursor, 100);
            assertEquals(2, result.size()); // reads both again
            cursor.close();
        }
    }

    @Nested
    class PeekOrder {
        @Test
        void peekReturnsLargestOrderAcrossSeries() {
            final Sequence tsA = harness.createTimeSeries("A");
            final Sequence tsB = harness.createTimeSeries("B");
            appendWithId(tsA, 50, 1);
            appendWithId(tsB, 80, 2);

            final MergedBackwardCursor cursor = MergedBackwardCursor.create(tsA, tsB);

            assertEquals(80L, cursor.peekNextOrder());
            cursor.close();
        }

        @Test
        void peekReturnsExhaustedWhenAllDrained() {
            final Sequence sequence = harness.createTimeSeries("x");
            appendWithId(sequence, 10, 1);

            final MergedBackwardCursor cursor = MergedBackwardCursor.create(sequence);
            drainPlain(cursor, 100);

            assertEquals(Long.MIN_VALUE, cursor.peekNextOrder());
            cursor.close();
        }
    }

    @Nested
    class RefreshPeeks {
        @Test
        void refreshPeeksRecomputesCachedValueAtCurrentPosition() {
            final Sequence tsA = harness.createTimeSeries("A");
            appendWithId(tsA, 10, 1);
            appendWithId(tsA, 20, 2);

            final MergedBackwardCursor cursor = MergedBackwardCursor.create(tsA);

            // Peek caches ts=20
            assertEquals(20L, cursor.peekNextOrder());

            // Consume one entry (ts=20)
            drainPlain(cursor, 1);

            // Cached peek is now stale; refresh forces recomputation
            cursor.refreshPeeks();
            assertEquals(10L, cursor.peekNextOrder());

            cursor.close();
        }

        @Test
        void seekToEndDetectsNewDataAfterExhaustion() {
            final Sequence tsA = harness.createTimeSeries("A");
            appendWithId(tsA, 10, 1);

            final MergedBackwardCursor cursor = MergedBackwardCursor.create(tsA);
            drainPlain(cursor, 100);
            assertEquals(Long.MIN_VALUE, cursor.peekNextOrder());

            // Append new data; refreshPeeks alone cannot reposition an exhausted cursor
            appendWithId(tsA, 20, 2);
            cursor.seekToEnd();

            assertEquals(20L, cursor.peekNextOrder());
            cursor.close();
        }
    }

    @Nested
    class WidthAndAccessor {
        @Test
        void widthReturnsNumberOfSeries() {
            final Sequence tsA = harness.createTimeSeries("A");
            final Sequence tsB = harness.createTimeSeries("B");

            final MergedBackwardCursor cursor = MergedBackwardCursor.create(tsA, tsB);

            assertEquals(2, cursor.width());
            assertNotNull(cursor.cursor(0));
            assertNotNull(cursor.cursor(1));
            cursor.close();
        }
    }
}