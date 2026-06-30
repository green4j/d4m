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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for cursor repositioning after COW inserts and snapshot changes.
 */
class CursorRepositionTest {
    private static final int CHUNK_SIZE = 1024;
    private static final int MSG_PAYLOAD = 8;

    private Sequence sequence;

    @BeforeEach
    void setUp() {
        final TestHarness harness = new TestHarness(CHUNK_SIZE);
        sequence = harness.createSequence("repos");
    }

    private void appendWithId(final long order,
                              final int id) {
        final AtomicBuffer payload = TestHarness.payloadWithId(id);
        sequence.append(order, payload, 0, MSG_PAYLOAD);
    }

    private List<byte[]> drainForward(final ForwardCursor cursor,
                                      final int max) {
        final List<byte[]> collected = new ArrayList<>();
        cursor.next(max, (owner, order, buf, off, size) -> {
            final byte[] c = new byte[size];
            buf.getBytes(off, c, 0, size);
            collected.add(c);
        });
        return collected;
    }

    private List<byte[]> drainBackward(final BackwardCursor cursor,
                                       final int max) {
        final List<byte[]> collected = new ArrayList<>();
        cursor.next(max, (owner, order, buf, off, size) -> {
            final byte[] c = new byte[size];
            buf.getBytes(off, c, 0, size);
            collected.add(c);
        });
        return collected;
    }

    @Nested
    class ForwardCursorReposition {
        @Test
        void repositionsAfterCowInsert() {
            appendWithId(100L, 1);
            appendWithId(300L, 3);

            final ForwardCursor cursor = new ForwardCursor(sequence);
            final List<byte[]> batch1 = drainForward(cursor, 1);
            assertEquals(1, batch1.size());
            assertEquals(1, TestHarness.idFromPayload(batch1.get(0)));

            // COW insert between already-read and unread
            sequence.insert(200L, TestHarness.payloadWithId(2), 0, MSG_PAYLOAD);

            // Cursor should reposition and continue from where it left off
            final List<byte[]> batch2 = drainForward(cursor, 100);
            assertFalse(batch2.isEmpty(), "should read remaining entries");

            // Verify order=300 (id=3) is eventually delivered
            boolean foundId3 = false;
            for (final byte[] msg : batch2) {
                if (TestHarness.idFromPayload(msg) == 3) {
                    foundId3 = true;
                    break;
                }
            }
            assertTrue(foundId3, "id=3 must be delivered after reposition");
            cursor.close();
        }

        @Test
        void continuesReadingAfterNewAppendChangesSnapshot() {
            appendWithId(10L, 1);
            appendWithId(20L, 2);

            final ForwardCursor cursor = new ForwardCursor(sequence);
            drainForward(cursor, 2); // consume both

            appendWithId(30L, 3); // triggers new snapshot

            final List<byte[]> batch = drainForward(cursor, 100);
            assertEquals(1, batch.size());
            assertEquals(3, TestHarness.idFromPayload(batch.get(0)));
            cursor.close();
        }
    }

    @Nested
    class BackwardCursorReposition {
        @Test
        void repositionsAfterCowInsert() {
            appendWithId(100L, 1);
            appendWithId(300L, 3);

            final BackwardCursor cursor = new BackwardCursor(sequence);
            final List<byte[]> batch1 = drainBackward(cursor, 1);
            assertEquals(1, batch1.size());
            assertEquals(3, TestHarness.idFromPayload(batch1.get(0)));

            // COW insert
            sequence.insert(200L, TestHarness.payloadWithId(2), 0, MSG_PAYLOAD);

            // Should continue from before order=300
            final List<byte[]> batch2 = drainBackward(cursor, 100);
            assertFalse(batch2.isEmpty());

            boolean foundId1 = false;
            for (final byte[] msg : batch2) {
                if (TestHarness.idFromPayload(msg) == 1) {
                    foundId1 = true;
                    break;
                }
            }
            assertTrue(foundId1, "id=1 must be delivered after reposition");
            cursor.close();
        }

        @Test
        void continuesAfterNewAppendChangesSnapshot() {
            appendWithId(10L, 1);
            appendWithId(20L, 2);
            appendWithId(30L, 3);

            final BackwardCursor cursor = new BackwardCursor(sequence);
            drainBackward(cursor, 1); // consume order=30

            appendWithId(40L, 4); // new snapshot

            // Should NOT re-deliver order=30 but continue to order=20
            final List<byte[]> batch = drainBackward(cursor, 100);
            // Ts=20 (id=2) and order=10 (id=1) expected
            boolean foundId3 = false;
            for (final byte[] msg : batch) {
                if (TestHarness.idFromPayload(msg) == 3) {
                    foundId3 = true;
                    break;
                }
            }
            assertFalse(foundId3, "Order=30 must not be re-delivered");
            cursor.close();
        }
    }
}