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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link Sequence}.
 */
class SequenceTest {
    private static final int CHUNK_SIZE = 1024;
    private static final int DATA_CAP = CHUNK_SIZE - Chunk.HEADER_SIZE; // 768
    private static final int MSG_PAYLOAD = 8;
    // EntrySize(8) = align8(24+8) = 32
    private static final int MSG_TOTAL = Chunk.entrySize(MSG_PAYLOAD);

    private TestHarness harness;
    private Sequence sequence;

    @BeforeEach
    void setUp() {
        harness = new TestHarness(CHUNK_SIZE);
        sequence = harness.createTimeSeries("test-series");
    }

    @Nested
    class SingleAppend {
        @Test
        void appendToEmptySeriesSucceeds() {
            final AtomicBuffer payload = TestHarness.payloadWithId(1);
            assertTrue(sequence.append(100L, payload, 0, MSG_PAYLOAD));

            final ChunkSnapshot snapshot = sequence.snapshot();
            assertEquals(1, snapshot.size());
            assertEquals(1, snapshot.chunk(0).getEntryCount());
        }

        @Test
        void appendedEntryHasCorrectOrder() {
            final AtomicBuffer payload = TestHarness.payloadWithId(1);
            sequence.append(500L, payload, 0, MSG_PAYLOAD);

            final Chunk chunk = sequence.snapshot().chunk(0);
            assertEquals(500L, chunk.entryOrder(Chunk.HEADER_SIZE));
        }

        @Test
        void appendedEntryPreservesPayload() {
            final AtomicBuffer payload = TestHarness.payloadWithId(42);
            sequence.append(100L, payload, 0, MSG_PAYLOAD);

            final Chunk chunk = sequence.snapshot().chunk(0);
            final byte[] readBack = new byte[MSG_PAYLOAD];
            chunk.buffer().getBytes(
                    Chunk.HEADER_SIZE + Chunk.ENTRY_HEADER_SIZE, readBack, 0, MSG_PAYLOAD);
            assertEquals(42, TestHarness.idFromPayload(readBack));
        }

        @Test
        void appendUpdatesMinMaxOrder() {
            final AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            sequence.append(100L, payload, 0, MSG_PAYLOAD);
            sequence.append(200L, payload, 0, MSG_PAYLOAD);

            final Chunk chunk = sequence.snapshot().chunk(0);
            assertEquals(100L, chunk.getMinOrder());
            assertEquals(200L, chunk.getMaxOrder());
        }

        @Test
        void appendAssignsMonotonicallyIncreasingVersions() {
            final AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            sequence.append(100L, payload, 0, MSG_PAYLOAD);
            sequence.append(100L, payload, 0, MSG_PAYLOAD); // same order ok

            final Chunk chunk = sequence.snapshot().chunk(0);
            final long v1 = chunk.entryVersion(Chunk.HEADER_SIZE);
            final long v2 = chunk.entryVersion(Chunk.HEADER_SIZE + MSG_TOTAL);
            assertTrue(v2 > v1);
        }

        @Test
        void equalOrdersArePermitted() {
            final AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            assertTrue(sequence.append(100L, payload, 0, MSG_PAYLOAD));
            assertTrue(sequence.append(100L, payload, 0, MSG_PAYLOAD));
            assertEquals(2, sequence.snapshot().chunk(0).getEntryCount());
        }

        @Test
        void strictlyEarlierOrderIsRejected() {
            final AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            sequence.append(200L, payload, 0, MSG_PAYLOAD);

            assertFalse(sequence.append(100L, payload, 0, MSG_PAYLOAD));
        }
    }

    @Nested
    class ChunkSpilling {
        @Test
        void newChunkCreatedWhenTailFull() {
            final AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            final int entriesPerChunk = DATA_CAP / MSG_TOTAL;

            for (int i = 0; i < entriesPerChunk; i++) {
                assertTrue(sequence.append(i, payload, 0, MSG_PAYLOAD));
            }
            assertEquals(1, sequence.snapshot().size());

            // Next entry spills to new chunk
            assertTrue(sequence.append(entriesPerChunk, payload, 0, MSG_PAYLOAD));
            assertEquals(2, sequence.snapshot().size());
        }

        @Test
        void previousChunkIsSealedAfterSpill() {
            final AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            final int entriesPerChunk = DATA_CAP / MSG_TOTAL;

            for (int i = 0; i <= entriesPerChunk; i++) {
                sequence.append(i, payload, 0, MSG_PAYLOAD);
            }

            assertTrue(sequence.snapshot().chunk(0).isSealed());
            assertFalse(sequence.snapshot().chunk(1).isSealed());
        }

        @Test
        void snapshotVersionIncreasesOnNewChunk() {
            final AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            final long v0 = sequence.snapshot().version();

            sequence.append(1L, payload, 0, MSG_PAYLOAD);
            final long v1 = sequence.snapshot().version();
            assertTrue(v1 > v0);
        }
    }

    @Nested
    class BatchAppend {
        @Test
        void appendBatchWritesAll() {
            final int count = 5;
            final long[] orders = {100, 200, 300, 400, 500};
            final AtomicBuffer payloads = TestHarness.payload(count * MSG_PAYLOAD);
            final int[] offsets = new int[count];
            final int[] sizes = new int[count];
            for (int i = 0; i < count; i++) {
                offsets[i] = i * MSG_PAYLOAD;
                sizes[i] = MSG_PAYLOAD;
            }

            final int written = sequence.appendBatch(orders, payloads, offsets, sizes, count);

            assertEquals(count, written);
            assertEquals(count, sequence.snapshot().chunk(0).getEntryCount());
        }

        @Test
        void appendBatchRejectsOutOfOrder() {
            final AtomicBuffer pl = TestHarness.payload(MSG_PAYLOAD);
            sequence.append(500L, pl, 0, MSG_PAYLOAD);

            final long[] orders = {100}; // earlier than existing max
            final int[] offsets = {0};
            final int[] sizes = {MSG_PAYLOAD};

            assertEquals(0, sequence.appendBatch(orders, pl, offsets, sizes, 1));
        }

        @Test
        void appendBatchSpillsAcrossChunks() {
            final int entriesPerChunk = DATA_CAP / MSG_TOTAL;
            final int totalEntries = entriesPerChunk + 3;
            final long[] orders = new long[totalEntries];
            final AtomicBuffer payloads = TestHarness.payload(totalEntries * MSG_PAYLOAD);
            final int[] offsets = new int[totalEntries];
            final int[] sizes = new int[totalEntries];
            for (int i = 0; i < totalEntries; i++) {
                orders[i] = i * 10L;
                offsets[i] = i * MSG_PAYLOAD;
                sizes[i] = MSG_PAYLOAD;
            }

            final int written = sequence.appendBatch(orders, payloads, offsets, sizes, totalEntries);

            assertEquals(totalEntries, written);
            assertEquals(2, sequence.snapshot().size());
        }
    }

    @Nested
    class BatchPackedAppend {
        @Test
        void appendBatchPackedWritesAll() {
            // Packed format: long ts (8) + int pLen (4) + payload (pLen)
            final int count = 3;
            final int packedMsgSize = 8 + 4 + MSG_PAYLOAD; // 20 bytes per packed entry
            final AtomicBuffer packed =
                    new UnsafeBuffer(new byte[count * packedMsgSize]);

            for (int i = 0; i < count; i++) {
                final int off = i * packedMsgSize;
                packed.putLong(off, (i + 1) * 100L);
                packed.putInt(off + 8, MSG_PAYLOAD);
                packed.putByte(off + 12, (byte) i); // identifiable payload
            }

            final int written = sequence.appendBatchPacked(packed, 0, count);

            assertEquals(count, written);
            final Chunk chunk = sequence.snapshot().chunk(0);
            assertEquals(count, chunk.getEntryCount());
            assertEquals(100L, chunk.getMinOrder());
            assertEquals(300L, chunk.getMaxOrder());
        }
    }

    @Nested
    class Insert {
        @Test
        void insertIntoMiddleOfChunk() {
            final AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            sequence.append(100L, payload, 0, MSG_PAYLOAD);
            sequence.append(300L, payload, 0, MSG_PAYLOAD);

            final AtomicBuffer insertPayload = TestHarness.payloadWithId(99);
            sequence.insert(200L, insertPayload, 0, MSG_PAYLOAD);

            // Verify ordering via chunk scan
            final ChunkSnapshot snapshot = sequence.snapshot();
            long prevOrder = Long.MIN_VALUE;
            int totalEntries = 0;
            for (int chunkIdx = 0; chunkIdx < snapshot.size(); chunkIdx++) {
                final Chunk chunk = snapshot.chunk(chunkIdx);
                final int entryCount = chunk.getEntryCount();
                int entryOffset = Chunk.HEADER_SIZE;
                for (int entryIndex = 0; entryIndex < entryCount; entryIndex++) {
                    final long entryOrder = chunk.entryOrder(entryOffset);
                    assertTrue(entryOrder >= prevOrder, "entries must be ordered");
                    prevOrder = entryOrder;
                    entryOffset += chunk.entryTotalSize(entryOffset);
                    totalEntries++;
                }
            }
            assertEquals(3, totalEntries);
        }

        @Test
        void insertAtEndFallsBackToAppend() {
            final AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            sequence.append(100L, payload, 0, MSG_PAYLOAD);

            sequence.insert(200L, payload, 0, MSG_PAYLOAD);

            final Chunk chunk = sequence.snapshot().chunk(sequence.snapshot().size() - 1);
            assertEquals(200L, chunk.getMaxOrder());
        }

        @Test
        void insertIntoEmptySeriesBehavesAsAppend() {
            final AtomicBuffer payload = TestHarness.payloadWithId(1);
            sequence.insert(100L, payload, 0, MSG_PAYLOAD);

            assertEquals(1, sequence.snapshot().size());
            assertEquals(1, sequence.snapshot().chunk(0).getEntryCount());
        }
    }

    @Nested
    class InsertOrUpdateUnique {
        @Test
        void insertsWhenOrderNotPresent() {
            final AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            sequence.append(100L, payload, 0, MSG_PAYLOAD);
            sequence.append(300L, payload, 0, MSG_PAYLOAD);

            sequence.insertOrUpdateUnique(200L, payload, 0, MSG_PAYLOAD);

            // Total entries should be 3
            int total = 0;
            final ChunkSnapshot snapshot = sequence.snapshot();
            for (int chunkIdx = 0; chunkIdx < snapshot.size(); chunkIdx++) {
                total += snapshot.chunk(chunkIdx).getEntryCount();
            }
            assertEquals(3, total);
        }

        @Test
        void updatesWhenOrderAlreadyExists() {
            final AtomicBuffer payload1 = TestHarness.payloadWithId(1);
            final AtomicBuffer payload2 = TestHarness.payloadWithId(2);
            sequence.append(100L, payload1, 0, MSG_PAYLOAD);

            sequence.insertOrUpdateUnique(100L, payload2, 0, MSG_PAYLOAD);

            // Still only 1 entry, but payload changed
            final ChunkSnapshot snapshot = sequence.snapshot();
            int total = 0;
            for (int chunkIdx = 0; chunkIdx < snapshot.size(); chunkIdx++) {
                total += snapshot.chunk(chunkIdx).getEntryCount();
            }
            assertEquals(1, total);

            // Read payload of the single entry
            final Chunk chunk = snapshot.chunk(0);
            final byte[] readBack = new byte[MSG_PAYLOAD];
            chunk.buffer().getBytes(
                    Chunk.HEADER_SIZE + Chunk.ENTRY_HEADER_SIZE, readBack, 0, MSG_PAYLOAD);
            assertEquals(2, TestHarness.idFromPayload(readBack));
        }
    }

    @Nested
    class NameAndSnapshot {
        @Test
        void nameReturnsConstructionName() {
            assertEquals("test-series", sequence.name());
        }

        @Test
        void initialSnapshotIsEmpty() {
            final ChunkSnapshot snapshot = sequence.snapshot();
            assertEquals(0, snapshot.size());
        }
    }

    @Nested
    class InsertBatch {
        @Test
        void insertBatchIntoEmptySequence() {
            final int count = 5;
            final long[] orders = {100, 200, 300, 400, 500};
            final AtomicBuffer payloads = TestHarness.payload(count * MSG_PAYLOAD);
            final int[] offsets = new int[count];
            final int[] sizes = new int[count];
            for (int i = 0; i < count; i++) {
                offsets[i] = i * MSG_PAYLOAD;
                sizes[i] = MSG_PAYLOAD;
            }

            final int written = sequence.insertBatch(orders, payloads, offsets, sizes, count);

            assertEquals(count, written);
            assertEquals(count, totalEntries(sequence.snapshot()));
        }

        @Test
        void insertBatchIntoMiddleOfSingleChunk() {
            final AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            sequence.append(100L, payload, 0, MSG_PAYLOAD);
            sequence.append(300L, payload, 0, MSG_PAYLOAD);
            sequence.append(500L, payload, 0, MSG_PAYLOAD);

            final long[] orders = {200, 400};
            final AtomicBuffer payloads = TestHarness.payload(2 * MSG_PAYLOAD);
            final int[] offsets = {0, MSG_PAYLOAD};
            final int[] sizes = {MSG_PAYLOAD, MSG_PAYLOAD};

            final int written = sequence.insertBatch(orders, payloads, offsets, sizes, 2);

            assertEquals(2, written);
            assertEquals(5, totalEntries(sequence.snapshot()));
            assertOrdered(sequence.snapshot());
        }

        @Test
        void insertBatchSpansMultipleChunks() {
            final AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            final int entriesPerChunk = DATA_CAP / MSG_TOTAL;

            // Fill first chunk
            for (int i = 0; i < entriesPerChunk; i++) {
                sequence.append(i * 10L, payload, 0, MSG_PAYLOAD);
            }
            // Spill to second chunk
            sequence.append((entriesPerChunk) * 10L, payload, 0, MSG_PAYLOAD);
            sequence.append((entriesPerChunk + 1) * 10L, payload, 0, MSG_PAYLOAD);

            // Insert into both chunks
            final long[] orders = {5, (entriesPerChunk) * 10L + 5};
            final AtomicBuffer payloads = TestHarness.payload(2 * MSG_PAYLOAD);
            final int[] offsets = {0, MSG_PAYLOAD};
            final int[] sizes = {MSG_PAYLOAD, MSG_PAYLOAD};

            final int written = sequence.insertBatch(orders, payloads, offsets, sizes, 2);

            assertEquals(2, written);
            assertEquals(entriesPerChunk + 4, totalEntries(sequence.snapshot()));
            assertOrdered(sequence.snapshot());
        }

        @Test
        void insertBatchAllBeyondMaxOrder() {
            final AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            sequence.append(100L, payload, 0, MSG_PAYLOAD);

            final long[] orders = {200, 300, 400};
            final AtomicBuffer payloads = TestHarness.payload(3 * MSG_PAYLOAD);
            final int[] offsets = {0, MSG_PAYLOAD, 2 * MSG_PAYLOAD};
            final int[] sizes = {MSG_PAYLOAD, MSG_PAYLOAD, MSG_PAYLOAD};

            final int written = sequence.insertBatch(orders, payloads, offsets, sizes, 3);

            assertEquals(3, written);
            assertEquals(4, totalEntries(sequence.snapshot()));
            assertOrdered(sequence.snapshot());
        }

        @Test
        void insertBatchMixedCowAndAppend() {
            final AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            sequence.append(100L, payload, 0, MSG_PAYLOAD);
            sequence.append(300L, payload, 0, MSG_PAYLOAD);
            sequence.append(500L, payload, 0, MSG_PAYLOAD);

            // 200 -> COW, 500 and 600 -> append
            final long[] orders = {200, 500, 600};
            final AtomicBuffer payloads = TestHarness.payload(3 * MSG_PAYLOAD);
            final int[] offsets = {0, MSG_PAYLOAD, 2 * MSG_PAYLOAD};
            final int[] sizes = {MSG_PAYLOAD, MSG_PAYLOAD, MSG_PAYLOAD};

            final int written = sequence.insertBatch(orders, payloads, offsets, sizes, 3);

            assertEquals(3, written);
            assertEquals(6, totalEntries(sequence.snapshot()));
            assertOrdered(sequence.snapshot());
        }

        @Test
        void insertBatchCausesChunkSpill() {
            final AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            final int entriesPerChunk = DATA_CAP / MSG_TOTAL;

            // Fill chunk to near capacity (leave room for 1 more)
            for (int i = 0; i < entriesPerChunk - 1; i++) {
                sequence.append(i * 10L, payload, 0, MSG_PAYLOAD);
            }
            // Append one more entry beyond this chunk
            sequence.append((entriesPerChunk - 1) * 10L, payload, 0, MSG_PAYLOAD);

            final ChunkSnapshot before = sequence.snapshot();
            final int chunksBefore = before.size();

            // Insert enough entries to overflow the first chunk
            final int insertCount = 3;
            final long[] orders = new long[insertCount];
            final AtomicBuffer payloads = TestHarness.payload(insertCount * MSG_PAYLOAD);
            final int[] offsets = new int[insertCount];
            final int[] sizes = new int[insertCount];
            for (int i = 0; i < insertCount; i++) {
                orders[i] = i * 10L + 5; // interleave
                offsets[i] = i * MSG_PAYLOAD;
                sizes[i] = MSG_PAYLOAD;
            }

            sequence.insertBatch(orders, payloads, offsets, sizes, insertCount);

            assertTrue(sequence.snapshot().size() > chunksBefore);
            assertEquals(entriesPerChunk + insertCount, totalEntries(sequence.snapshot()));
            assertOrdered(sequence.snapshot());
        }

        @Test
        void insertBatchPreservesPayload() {
            final AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            sequence.append(100L, payload, 0, MSG_PAYLOAD);
            sequence.append(300L, payload, 0, MSG_PAYLOAD);

            final AtomicBuffer insertPayload = TestHarness.payloadWithId(42);
            final long[] orders = {200};
            final int[] offsets = {0};
            final int[] sizes = {MSG_PAYLOAD};

            sequence.insertBatch(orders, insertPayload, offsets, sizes, 1);

            // Find the entry with order 200 and check payload
            final ChunkSnapshot snap = sequence.snapshot();
            boolean found = false;
            for (int chunkIdx = 0; chunkIdx < snap.size(); chunkIdx++) {
                final Chunk chunk = snap.chunk(chunkIdx);
                int off = Chunk.HEADER_SIZE;
                for (int ei = 0; ei < chunk.getEntryCount(); ei++) {
                    if (chunk.entryOrder(off) == 200L) {
                        final byte[] readBack = new byte[MSG_PAYLOAD];
                        chunk.buffer().getBytes(off + Chunk.ENTRY_HEADER_SIZE, readBack, 0, MSG_PAYLOAD);
                        assertEquals(42, TestHarness.idFromPayload(readBack));
                        found = true;
                        break;
                    }
                    off += chunk.entryTotalSize(off);
                }
                if (found) {
                    break;
                }
            }
            assertTrue(found, "entry with order 200 must exist");
        }

        @Test
        void insertBatchRejectsOversizedPayload() {
            final AtomicBuffer payload = TestHarness.payload(DATA_CAP + 1);
            final long[] orders = {100};
            final int[] offsets = {0};
            final int[] sizes = {DATA_CAP + 1};

            assertThrows(IllegalArgumentException.class,
                    () -> sequence.insertBatch(orders, payload, offsets, sizes, 1));
        }

        @Test
        void insertBatchSingleSnapshotPublish() {
            final AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            final int entriesPerChunk = DATA_CAP / MSG_TOTAL;

            // Fill two chunks
            for (int i = 0; i < entriesPerChunk; i++) {
                sequence.append(i * 10L, payload, 0, MSG_PAYLOAD);
            }
            sequence.append(entriesPerChunk * 10L, payload, 0, MSG_PAYLOAD);
            sequence.append((entriesPerChunk + 1) * 10L, payload, 0, MSG_PAYLOAD);

            final long versionBefore = sequence.snapshot().version();

            // Insert into both chunks
            final long[] orders = {5, entriesPerChunk * 10L + 5};
            final AtomicBuffer payloads = TestHarness.payload(2 * MSG_PAYLOAD);
            final int[] offsets = {0, MSG_PAYLOAD};
            final int[] sizes = {MSG_PAYLOAD, MSG_PAYLOAD};

            sequence.insertBatch(orders, payloads, offsets, sizes, 2);

            final long versionAfter = sequence.snapshot().version();
            assertEquals(versionBefore + 1, versionAfter);
        }

        @Test
        void insertBatchWithDuplicateOrders() {
            final AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            sequence.append(100L, payload, 0, MSG_PAYLOAD);
            sequence.append(200L, payload, 0, MSG_PAYLOAD);
            sequence.append(300L, payload, 0, MSG_PAYLOAD);

            // Insert entries with same order as existing
            final long[] orders = {100, 200};
            final AtomicBuffer payloads = TestHarness.payload(2 * MSG_PAYLOAD);
            final int[] offsets = {0, MSG_PAYLOAD};
            final int[] sizes = {MSG_PAYLOAD, MSG_PAYLOAD};

            final int written = sequence.insertBatch(orders, payloads, offsets, sizes, 2);

            assertEquals(2, written);
            assertEquals(5, totalEntries(sequence.snapshot()));
            assertOrdered(sequence.snapshot());
        }

        private int totalEntries(final ChunkSnapshot snapshot) {
            int total = 0;
            for (int chunkIdx = 0; chunkIdx < snapshot.size(); chunkIdx++) {
                total += snapshot.chunk(chunkIdx).getEntryCount();
            }
            return total;
        }

        private void assertOrdered(final ChunkSnapshot snapshot) {
            long prevOrder = Long.MIN_VALUE;
            for (int chunkIdx = 0; chunkIdx < snapshot.size(); chunkIdx++) {
                final Chunk chunk = snapshot.chunk(chunkIdx);
                int off = Chunk.HEADER_SIZE;
                for (int ei = 0; ei < chunk.getEntryCount(); ei++) {
                    final long order = chunk.entryOrder(off);
                    assertTrue(order >= prevOrder,
                            "entries must be ordered: " + prevOrder + " -> " + order);
                    prevOrder = order;
                    off += chunk.entryTotalSize(off);
                }
            }
        }
    }

    @Nested
    class InsertOrUpdateEqual {
        private final PayloadEquals payloadEquals = (payloadA,
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

        @Test
        void updatesWhenPayloadMatches() {
            final AtomicBuffer payload = TestHarness.payloadWithId(1);
            sequence.append(100L, payload, 0, MSG_PAYLOAD);

            final AtomicBuffer samePayload = TestHarness.payloadWithId(1);
            sequence.insertOrUpdateEqual(100L, samePayload, 0, MSG_PAYLOAD, payloadEquals);

            assertEquals(1, totalEntries(sequence.snapshot()));
        }

        @Test
        void insertsNewEntryWhenPayloadDiffers() {
            final AtomicBuffer payload1 = TestHarness.payloadWithId(1);
            sequence.append(100L, payload1, 0, MSG_PAYLOAD);

            final AtomicBuffer payload2 = TestHarness.payloadWithId(2);
            sequence.insertOrUpdateEqual(100L, payload2, 0, MSG_PAYLOAD, payloadEquals);

            assertEquals(2, totalEntries(sequence.snapshot()));
        }

        @Test
        void insertsAtCorrectPositionWhenOrderNotPresent() {
            final AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            sequence.append(100L, payload, 0, MSG_PAYLOAD);
            sequence.append(300L, payload, 0, MSG_PAYLOAD);

            sequence.insertOrUpdateEqual(200L, TestHarness.payloadWithId(99),
                    0, MSG_PAYLOAD, payloadEquals);

            assertEquals(3, totalEntries(sequence.snapshot()));

            final ChunkSnapshot snapshot = sequence.snapshot();
            final List<Long> orders = new ArrayList<>();
            for (int chunkIdx = 0; chunkIdx < snapshot.size(); chunkIdx++) {
                final Chunk chunk = snapshot.chunk(chunkIdx);
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

        @Test
        void insertsAsFirstEntryWhenEmpty() {
            sequence.insertOrUpdateEqual(100L,
                    TestHarness.payloadWithId(1),
                    0,
                    MSG_PAYLOAD,
                    payloadEquals
            );

            assertEquals(1, totalEntries(sequence.snapshot()));
            assertEquals(100L, sequence.snapshot().chunk(0).getMinOrder());
        }

        private int totalEntries(final ChunkSnapshot snapshot) {
            int total = 0;
            for (int chunkIdx = 0; chunkIdx < snapshot.size(); chunkIdx++) {
                total += snapshot.chunk(chunkIdx).getEntryCount();
            }
            return total;
        }
    }

    @Nested
    class OversizedPayload {
        @Test
        void appendRejectsOversizedPayload() {
            final AtomicBuffer payload = TestHarness.payload(DATA_CAP + 1);
            assertThrows(IllegalArgumentException.class,
                    () -> sequence.append(1L, payload, 0, DATA_CAP + 1));
        }

        @Test
        void insertRejectsOversizedPayload() {
            final AtomicBuffer payload = TestHarness.payload(DATA_CAP + 1);
            assertThrows(IllegalArgumentException.class,
                    () -> sequence.insert(1L, payload, 0, DATA_CAP + 1));
        }
    }

}