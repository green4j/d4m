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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link Sequence#insertOrUpdateEqual}.
 */
class InsertOrUpdateEqualTest {
    private static final int CHUNK_SIZE = 1024;
    private static final int MSG_PAYLOAD = 8;

    private TestHarness harness;
    private Sequence sequence;

    @BeforeEach
    void setUp() {
        harness = new TestHarness(CHUNK_SIZE);
        sequence = harness.createTimeSeries("eq");
    }

    private static final PayloadEquals PAYLOAD_EQUALS = (payloadA,
                                                                  payloadOffsetA,
                                                                  payloadSizeA,
                                                                  payloadB,
                                                                  payloadOffsetB,
                                                                  payloadSizeB) -> {
        if (payloadSizeA != payloadSizeB) {
            return false;
        }
        for (int i = 0; i < payloadSizeA; i++) {
            if (payloadA.getByte(payloadOffsetA + i)
                    != payloadB.getByte(payloadOffsetB + i)) {
                return false;
            }
        }
        return true;
    };

    private int totalEntries() {
        final ChunkSnapshot snapshot = sequence.snapshot();
        int total = 0;
        for (int chunkIndex = 0; chunkIndex < snapshot.size(); chunkIndex++) {
            total += snapshot.chunk(chunkIndex).getEntryCount();
        }
        return total;
    }

    @Nested
    class WhenPayloadMatches {
        @Test
        void updatesExistingEntry() {
            final io.github.green4j.d4m.common.AtomicBuffer payload = TestHarness.payloadWithId(1);
            sequence.append(100L, payload, 0, MSG_PAYLOAD);

            // Same payload -> update (replace)
            final io.github.green4j.d4m.common.AtomicBuffer samePayload = TestHarness.payloadWithId(1);
            sequence.insertOrUpdateEqual(100L, samePayload, 0, MSG_PAYLOAD, PAYLOAD_EQUALS);

            assertEquals(1, totalEntries());
        }
    }

    @Nested
    class WhenPayloadDiffers {
        @Test
        void insertsNewEntryInSameOrderGroup() {
            final io.github.green4j.d4m.common.AtomicBuffer payload1 = TestHarness.payloadWithId(1);
            sequence.append(100L, payload1, 0, MSG_PAYLOAD);

            final io.github.green4j.d4m.common.AtomicBuffer payload2 = TestHarness.payloadWithId(2);
            sequence.insertOrUpdateEqual(100L, payload2, 0, MSG_PAYLOAD, PAYLOAD_EQUALS);

            assertEquals(2, totalEntries());
        }
    }

    @Nested
    class WhenOrderNotPresent {
        @Test
        void insertsAtCorrectPosition() {
            final io.github.green4j.d4m.common.AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            sequence.append(100L, payload, 0, MSG_PAYLOAD);
            sequence.append(300L, payload, 0, MSG_PAYLOAD);

            sequence.insertOrUpdateEqual(200L, TestHarness.payloadWithId(99),
                    0, MSG_PAYLOAD, PAYLOAD_EQUALS);

            assertEquals(3, totalEntries());

            // Verify ordering
            final ChunkSnapshot snapshot = sequence.snapshot();
            final List<Long> orders = new ArrayList<>();
            for (int chunkIndex = 0; chunkIndex < snapshot.size(); chunkIndex++) {
                final Chunk chunk = snapshot.chunk(chunkIndex);
                final int entryCount = chunk.getEntryCount();
                int entryOffset = Chunk.HEADER_SIZE;
                for (int entryIndex = 0; entryIndex < entryCount; entryIndex++) {
                    orders.add(chunk.entryOrder(entryOffset));
                    entryOffset += chunk.entryTotalSize(entryOffset);
                }
            }
            assertEquals(100L, orders.get(0));
            assertEquals(200L, orders.get(1));
            assertEquals(300L, orders.get(2));
        }
    }

    @Nested
    class WhenEmpty {
        @Test
        void insertsAsFirstEntry() {
            sequence.insertOrUpdateEqual(100L,
                    TestHarness.payloadWithId(1),
                    0,
                    MSG_PAYLOAD,
                    PAYLOAD_EQUALS
            );

            assertEquals(1, totalEntries());
            assertEquals(100L, sequence.snapshot().chunk(0).getMinOrder());
        }
    }
}