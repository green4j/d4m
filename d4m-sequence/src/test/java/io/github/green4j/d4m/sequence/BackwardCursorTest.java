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
import io.github.green4j.d4m.common.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link BackwardCursor}.
 */
class BackwardCursorTest {
    private static final int CHUNK_SIZE = 1024;
    private static final int DATA_CAP = CHUNK_SIZE - Chunk.HEADER_SIZE;
    private static final int MSG_PAYLOAD = 8;
    private static final int MSG_TOTAL = Chunk.entrySize(MSG_PAYLOAD);

    private TestHarness harness;
    private Sequence sequence;

    @BeforeEach
    void setUp() {
        harness = new TestHarness(CHUNK_SIZE);
        sequence = harness.createTimeSeries("bwd");
    }

    private List<byte[]> drain(final BackwardCursor cursor,
                               final int max) {
        final List<byte[]> collected = new ArrayList<>();
        final int n = cursor.next(max, (owner, order, buf, off, size) -> {
            final byte[] copy = new byte[size];
            buf.getBytes(off, copy, 0, size);
            collected.add(copy);
        });
        assertEquals(n, collected.size());
        return collected;
    }

    private void appendN(final int count) {
        final AtomicBuffer payload = new UnsafeBuffer(new byte[MSG_PAYLOAD]);
        for (int i = 0; i < count; i++) {
            payload.putInt(0, i);
            sequence.append(i * 10L, payload, 0, MSG_PAYLOAD);
        }
    }

    @Nested
    class EmptySeries {
        @Test
        void nextReturnsZeroOnEmpty() {
            final BackwardCursor cursor = new BackwardCursor(sequence);
            final List<byte[]> result = drain(cursor, 100);

            assertTrue(result.isEmpty());
            cursor.close();
        }

        @Test
        void peekReturnsMinValueOnEmpty() {
            final BackwardCursor cursor = new BackwardCursor(sequence);

            assertEquals(Long.MIN_VALUE, cursor.peekNextOrder());
            cursor.close();
        }
    }

    @Nested
    class ReverseRead {
        @Test
        void readsAllEntriesInReverseOrder() {
            appendN(5); // ids 0..4, orders 0,10,20,30,40
            final BackwardCursor cursor = new BackwardCursor(sequence);
            final List<byte[]> result = drain(cursor, 100);

            assertEquals(5, result.size());
            // Should come back 4, 3, 2, 1, 0
            for (int i = 0; i < 5; i++) {
                final int id = result.get(i)[0] & 0xFF;
                assertEquals(4 - i, id);
            }
            cursor.close();
        }

        @Test
        void readInBatches() {
            appendN(5);
            final BackwardCursor cursor = new BackwardCursor(sequence);

            final List<byte[]> batch1 = drain(cursor, 2);
            assertEquals(2, batch1.size());
            assertEquals(4, batch1.get(0)[0] & 0xFF);
            assertEquals(3, batch1.get(1)[0] & 0xFF);

            final List<byte[]> batch2 = drain(cursor, 2);
            assertEquals(2, batch2.size());
            assertEquals(2, batch2.get(0)[0] & 0xFF);
            assertEquals(1, batch2.get(1)[0] & 0xFF);

            final List<byte[]> batch3 = drain(cursor, 2);
            assertEquals(1, batch3.size());
            assertEquals(0, batch3.get(0)[0] & 0xFF);

            cursor.close();
        }

        @Test
        void readAcrossMultipleChunks() {
            final int entriesPerChunk = DATA_CAP / MSG_TOTAL;
            final int totalEntries = entriesPerChunk * 2 + 3;
            appendN(totalEntries);

            assertTrue(sequence.snapshot().size() >= 2, "must span multiple chunks");

            final BackwardCursor cursor = new BackwardCursor(sequence);
            final List<byte[]> result = drain(cursor, totalEntries + 10);

            assertEquals(totalEntries, result.size());
            // Verify descending ids
            for (int i = 0; i < totalEntries; i++) {
                final int id = result.get(i)[0] & 0xFF;
                assertEquals((totalEntries - 1 - i) & 0xFF, id);
            }
            cursor.close();
        }
    }

    @Nested
    class SeekToOrder {
        @Test
        void seekSkipsEntriesAfterOrder() {
            appendN(10); // orders 0, 10, ..., 90
            final BackwardCursor cursor = new BackwardCursor(sequence);
            cursor.seekTo(50L);
            final List<byte[]> result = drain(cursor, 100);

            // Entries with ts <= 50: 0, 10, 20, 30, 40, 50 -> in reverse: 50, 40, 30, 20, 10, 0
            assertEquals(6, result.size());
            assertEquals(5, result.get(0)[0] & 0xFF); // ts=50, id=5
            assertEquals(0, result.get(5)[0] & 0xFF); // ts=0, id=0
            cursor.close();
        }

        @Test
        void seekToZeroReadsOnlyFirstEntry() {
            appendN(5);
            final BackwardCursor cursor = new BackwardCursor(sequence);
            cursor.seekTo(0L);
            final List<byte[]> result = drain(cursor, 100);

            assertEquals(1, result.size());
            assertEquals(0, result.get(0)[0] & 0xFF);
            cursor.close();
        }

        @Test
        void seekBeforeAllEntriesReturnsNothing() {
            appendN(3); // orders 0, 10, 20
            final BackwardCursor cursor = new BackwardCursor(sequence);
            cursor.seekTo(-1L);
            final List<byte[]> result = drain(cursor, 100);

            assertEquals(0, result.size());
            cursor.close();
        }
    }

    @Nested
    class SeekToEnd {
        @Test
        void seekToEndRestartsFromTail() {
            appendN(3);
            final BackwardCursor cursor = new BackwardCursor(sequence);
            drain(cursor, 1); // read one (id=2, ts=20)
            cursor.seekToEnd();

            final List<byte[]> result = drain(cursor, 100);
            assertEquals(3, result.size());
            assertEquals(2, result.get(0)[0] & 0xFF); // back to latest
            cursor.close();
        }
    }

    @Nested
    class PeekOrder {
        @Test
        void peekReturnsLatestOrder() {
            appendN(3); // 0, 10, 20
            final BackwardCursor cursor = new BackwardCursor(sequence);

            assertEquals(20L, cursor.peekNextOrder());
            cursor.close();
        }

        @Test
        void peekAdvancesAfterConsumption() {
            appendN(3); // 0, 10, 20
            final BackwardCursor cursor = new BackwardCursor(sequence);

            assertEquals(20L, cursor.peekNextOrder());
            drain(cursor, 1); // consume ts=20
            cursor.invalidatePeek();
            assertEquals(10L, cursor.peekNextOrder());
            cursor.close();
        }

        @Test
        void peekReturnsMinValueWhenExhausted() {
            appendN(1);
            final BackwardCursor cursor = new BackwardCursor(sequence);
            drain(cursor, 1);
            cursor.invalidatePeek();

            assertEquals(Long.MIN_VALUE, cursor.peekNextOrder());
            cursor.close();
        }
    }

    @Nested
    class NextUntil {
        @Test
        void stopsAtLowerBound() {
            appendN(10); // 0, 10, ..., 90
            final BackwardCursor cursor = new BackwardCursor(sequence);

            final List<byte[]> result = new ArrayList<>();
            final int n = cursor.nextUntil(100, 50L, (owner, order, buf, off, size) -> {
                final byte[] copy = new byte[size];
                buf.getBytes(off, copy, 0, size);
                result.add(copy);
            });

            assertEquals(n, result.size());
            // Entries with ts >= 50: 90, 80, 70, 60, 50
            assertEquals(5, n);
            assertEquals(9, result.get(0)[0] & 0xFF); // ts=90
            cursor.close();
        }
    }

    @Nested
    class CloseReleasesPin {
        @Test
        void closeIsIdempotent() {
            final BackwardCursor cursor = new BackwardCursor(sequence);
            appendN(1);
            drain(cursor, 1);

            assertDoesNotThrow(cursor::close);
            assertDoesNotThrow(cursor::close);
        }
    }
}