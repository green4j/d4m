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
 * Tests for {@link ForwardCursor}.
 */
class ForwardCursorTest {
    private static final int CHUNK_SIZE = 1024;
    private static final int DATA_CAP = CHUNK_SIZE - Chunk.HEADER_SIZE;
    private static final int MSG_PAYLOAD = 8;
    private static final int MSG_TOTAL = Chunk.entrySize(MSG_PAYLOAD);

    private Sequence sequence;

    @BeforeEach
    void setUp() {
        final TestHarness harness = new TestHarness(CHUNK_SIZE);
        sequence = harness.createSequence("fwd");
    }

    private List<byte[]> drain(final ForwardCursor cursor, final int max) {
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
            final ForwardCursor cursor = new ForwardCursor(sequence);
            final List<byte[]> result = drain(cursor, 100);

            assertTrue(result.isEmpty());
            cursor.close();
        }

        @Test
        void peekReturnsMaxValueOnEmpty() {
            final ForwardCursor cursor = new ForwardCursor(sequence);

            assertEquals(Long.MAX_VALUE, cursor.peekNextOrder());
            cursor.close();
        }
    }

    @Nested
    class SequentialRead {
        @Test
        void readsAllEntriesInOrder() {
            appendN(5);
            final ForwardCursor cursor = new ForwardCursor(sequence);
            final List<byte[]> result = drain(cursor, 100);

            assertEquals(5, result.size());
            for (int i = 0; i < 5; i++) {
                final int id = result.get(i)[0] & 0xFF;
                assertEquals(i, id);
            }
            cursor.close();
        }

        @Test
        void readInBatches() {
            appendN(5);
            final ForwardCursor cursor = new ForwardCursor(sequence);

            final List<byte[]> batch1 = drain(cursor, 2);
            assertEquals(2, batch1.size());

            final List<byte[]> batch2 = drain(cursor, 2);
            assertEquals(2, batch2.size());

            final List<byte[]> batch3 = drain(cursor, 2);
            assertEquals(1, batch3.size());

            cursor.close();
        }

        @Test
        void readAcrossMultipleChunks() {
            final int entriesPerChunk = DATA_CAP / MSG_TOTAL;
            final int totalEntries = entriesPerChunk * 2 + 3;
            appendN(totalEntries);

            assertTrue(sequence.snapshot().size() >= 2, "must span multiple chunks");

            final ForwardCursor cursor = new ForwardCursor(sequence);
            final List<byte[]> result = drain(cursor, totalEntries + 10);

            assertEquals(totalEntries, result.size());
            cursor.close();
        }
    }

    @Nested
    class SeekTo {
        @Test
        void seekSkipsEntriesBeforeOrder() {
            appendN(10); // orders 0, 10, 20, ..., 90
            final ForwardCursor cursor = new ForwardCursor(sequence);

            cursor.seekTo(50L);
            final List<byte[]> result = drain(cursor, 100);

            // Entries with order >= 50: 50, 60, 70, 80, 90
            assertEquals(5, result.size());
            assertEquals(5, result.get(0)[0] & 0xFF);
            cursor.close();
        }

        @Test
        void seekToZeroReadsFromBeginning() {
            appendN(3);
            final ForwardCursor cursor = new ForwardCursor(sequence);
            cursor.seekTo(0L);
            final List<byte[]> result = drain(cursor, 100);

            assertEquals(3, result.size());
            cursor.close();
        }

        @Test
        void seekBeyondAllEntriesReturnsNothing() {
            appendN(3); // orders 0, 10, 20
            final ForwardCursor cursor = new ForwardCursor(sequence);
            cursor.seekTo(1000L);
            final List<byte[]> result = drain(cursor, 100);

            assertEquals(0, result.size());
            cursor.close();
        }
    }

    @Nested
    class PeekOrder {
        @Test
        void peekReturnsNextEntryOrder() {
            appendN(3); // 0, 10, 20
            final ForwardCursor cursor = new ForwardCursor(sequence);

            assertEquals(0L, cursor.peekNextOrder());

            drain(cursor, 1);
            cursor.invalidatePeek();
            assertEquals(10L, cursor.peekNextOrder());

            cursor.close();
        }

        @Test
        void peekReturnsMaxValueWhenExhausted() {
            appendN(1);
            final ForwardCursor cursor = new ForwardCursor(sequence);
            drain(cursor, 1);
            cursor.invalidatePeek();

            assertEquals(Long.MAX_VALUE, cursor.peekNextOrder());
            cursor.close();
        }

        @Test
        void peekIsCachedUntilInvalidated() {
            appendN(2);
            final ForwardCursor cursor = new ForwardCursor(sequence);
            final long first = cursor.peekNextOrder();
            // Not invalidated - should return cached value
            assertEquals(first, cursor.peekNextOrder());
            cursor.close();
        }
    }

    @Nested
    class NextUntil {
        @Test
        void stopsAtUpperBound() {
            appendN(10); // 0, 10, ..., 90
            final ForwardCursor cursor = new ForwardCursor(sequence);

            final List<byte[]> result = new ArrayList<>();
            final int n = cursor.nextUntil(100, 30L, (owner, order, buf, off, size) -> {
                final byte[] copy = new byte[size];
                buf.getBytes(off, copy, 0, size);
                result.add(copy);
            });

            // Entries with order <= 30: 0, 10, 20, 30
            assertEquals(n, result.size());
            for (final byte[] msg : result) {
                final int id = msg[0] & 0xFF;
                assertTrue(id * 10L <= 30L);
            }
            cursor.close();
        }
    }

    @Nested
    class LiveTailing {
        @Test
        void detectsNewEntriesAfterInitialRead() {
            appendN(2);
            final ForwardCursor cursor = new ForwardCursor(sequence);
            final List<byte[]> batch1 = drain(cursor, 100);
            assertEquals(2, batch1.size());

            // Append more after cursor read
            final AtomicBuffer pl = new UnsafeBuffer(new byte[MSG_PAYLOAD]);
            pl.putInt(0, 99);
            sequence.append(100L, pl, 0, MSG_PAYLOAD);

            final List<byte[]> batch2 = drain(cursor, 100);
            assertEquals(1, batch2.size());
            assertEquals(99, batch2.get(0)[0] & 0xFF);
            cursor.close();
        }
    }

    @Nested
    class CloseReleasesPin {
        @Test
        void closeIsIdempotent() {
            final ForwardCursor cursor = new ForwardCursor(sequence);
            appendN(1);
            drain(cursor, 1);

            assertDoesNotThrow(cursor::close);
            assertDoesNotThrow(cursor::close);
        }
    }
}