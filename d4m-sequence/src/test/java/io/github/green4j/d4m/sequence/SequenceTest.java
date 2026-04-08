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
            final io.github.green4j.d4m.common.AtomicBuffer payload = TestHarness.payloadWithId(1);
            assertTrue(sequence.append(100L, payload, 0, MSG_PAYLOAD));

            final ChunkSnapshot snapshot = sequence.snapshot();
            assertEquals(1, snapshot.size());
            assertEquals(1, snapshot.chunk(0).getEntryCount());
        }

        @Test
        void appendedEntryHasCorrectOrder() {
            final io.github.green4j.d4m.common.AtomicBuffer payload = TestHarness.payloadWithId(1);
            sequence.append(500L, payload, 0, MSG_PAYLOAD);

            final Chunk chunk = sequence.snapshot().chunk(0);
            assertEquals(500L, chunk.entryOrder(Chunk.HEADER_SIZE));
        }

        @Test
        void appendedEntryPreservesPayload() {
            final io.github.green4j.d4m.common.AtomicBuffer payload = TestHarness.payloadWithId(42);
            sequence.append(100L, payload, 0, MSG_PAYLOAD);

            final Chunk chunk = sequence.snapshot().chunk(0);
            final byte[] readBack = new byte[MSG_PAYLOAD];
            chunk.buffer().getBytes(
                    Chunk.HEADER_SIZE + Chunk.ENTRY_HEADER_SIZE, readBack, 0, MSG_PAYLOAD);
            assertEquals(42, TestHarness.idFromPayload(readBack));
        }

        @Test
        void appendUpdatesMinMaxOrder() {
            final io.github.green4j.d4m.common.AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            sequence.append(100L, payload, 0, MSG_PAYLOAD);
            sequence.append(200L, payload, 0, MSG_PAYLOAD);

            final Chunk chunk = sequence.snapshot().chunk(0);
            assertEquals(100L, chunk.getMinOrder());
            assertEquals(200L, chunk.getMaxOrder());
        }

        @Test
        void appendAssignsMonotonicallyIncreasingVersions() {
            final io.github.green4j.d4m.common.AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            sequence.append(100L, payload, 0, MSG_PAYLOAD);
            sequence.append(100L, payload, 0, MSG_PAYLOAD); // same order ok

            final Chunk chunk = sequence.snapshot().chunk(0);
            final long v1 = chunk.entryVersion(Chunk.HEADER_SIZE);
            final long v2 = chunk.entryVersion(Chunk.HEADER_SIZE + MSG_TOTAL);
            assertTrue(v2 > v1);
        }

        @Test
        void equalOrdersArePermitted() {
            final io.github.green4j.d4m.common.AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            assertTrue(sequence.append(100L, payload, 0, MSG_PAYLOAD));
            assertTrue(sequence.append(100L, payload, 0, MSG_PAYLOAD));
            assertEquals(2, sequence.snapshot().chunk(0).getEntryCount());
        }

        @Test
        void strictlyEarlierOrderIsRejected() {
            final io.github.green4j.d4m.common.AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            sequence.append(200L, payload, 0, MSG_PAYLOAD);

            assertFalse(sequence.append(100L, payload, 0, MSG_PAYLOAD));
        }
    }

    @Nested
    class ChunkSpilling {
        @Test
        void newChunkCreatedWhenTailFull() {
            final io.github.green4j.d4m.common.AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
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
            final io.github.green4j.d4m.common.AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            final int entriesPerChunk = DATA_CAP / MSG_TOTAL;

            for (int i = 0; i <= entriesPerChunk; i++) {
                sequence.append(i, payload, 0, MSG_PAYLOAD);
            }

            assertTrue(sequence.snapshot().chunk(0).isSealed());
            assertFalse(sequence.snapshot().chunk(1).isSealed());
        }

        @Test
        void snapshotVersionIncreasesOnNewChunk() {
            final io.github.green4j.d4m.common.AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
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
            final io.github.green4j.d4m.common.AtomicBuffer payloads = TestHarness.payload(count * MSG_PAYLOAD);
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
            final io.github.green4j.d4m.common.AtomicBuffer pl = TestHarness.payload(MSG_PAYLOAD);
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
            final io.github.green4j.d4m.common.AtomicBuffer payloads = TestHarness.payload(totalEntries * MSG_PAYLOAD);
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
            final io.github.green4j.d4m.common.AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            sequence.append(100L, payload, 0, MSG_PAYLOAD);
            sequence.append(300L, payload, 0, MSG_PAYLOAD);

            final io.github.green4j.d4m.common.AtomicBuffer insertPayload = TestHarness.payloadWithId(99);
            sequence.insert(200L, insertPayload, 0, MSG_PAYLOAD);

            // Verify ordering via chunk scan
            final ChunkSnapshot snapshot = sequence.snapshot();
            long prevOrder = Long.MIN_VALUE;
            int totalEntries = 0;
            for (int chunkIndex = 0; chunkIndex < snapshot.size(); chunkIndex++) {
                final Chunk chunk = snapshot.chunk(chunkIndex);
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
            final io.github.green4j.d4m.common.AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            sequence.append(100L, payload, 0, MSG_PAYLOAD);

            sequence.insert(200L, payload, 0, MSG_PAYLOAD);

            final Chunk chunk = sequence.snapshot().chunk(sequence.snapshot().size() - 1);
            assertEquals(200L, chunk.getMaxOrder());
        }

        @Test
        void insertIntoEmptySeriesBehavesAsAppend() {
            final io.github.green4j.d4m.common.AtomicBuffer payload = TestHarness.payloadWithId(1);
            sequence.insert(100L, payload, 0, MSG_PAYLOAD);

            assertEquals(1, sequence.snapshot().size());
            assertEquals(1, sequence.snapshot().chunk(0).getEntryCount());
        }
    }

    @Nested
    class InsertOrUpdateUnique {
        @Test
        void insertsWhenOrderNotPresent() {
            final io.github.green4j.d4m.common.AtomicBuffer payload = TestHarness.payload(MSG_PAYLOAD);
            sequence.append(100L, payload, 0, MSG_PAYLOAD);
            sequence.append(300L, payload, 0, MSG_PAYLOAD);

            sequence.insertOrUpdateUnique(200L, payload, 0, MSG_PAYLOAD);

            // Total entries should be 3
            int total = 0;
            final ChunkSnapshot snapshot = sequence.snapshot();
            for (int chunkIndex = 0; chunkIndex < snapshot.size(); chunkIndex++) {
                total += snapshot.chunk(chunkIndex).getEntryCount();
            }
            assertEquals(3, total);
        }

        @Test
        void updatesWhenOrderAlreadyExists() {
            final io.github.green4j.d4m.common.AtomicBuffer payload1 = TestHarness.payloadWithId(1);
            final io.github.green4j.d4m.common.AtomicBuffer payload2 = TestHarness.payloadWithId(2);
            sequence.append(100L, payload1, 0, MSG_PAYLOAD);

            sequence.insertOrUpdateUnique(100L, payload2, 0, MSG_PAYLOAD);

            // Still only 1 entry, but payload changed
            final ChunkSnapshot snapshot = sequence.snapshot();
            int total = 0;
            for (int chunkIndex = 0; chunkIndex < snapshot.size(); chunkIndex++) {
                total += snapshot.chunk(chunkIndex).getEntryCount();
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
    class OversizedPayload {
        @Test
        void appendRejectsOversizedPayload() {
            final io.github.green4j.d4m.common.AtomicBuffer payload = TestHarness.payload(DATA_CAP + 1);
            assertThrows(IllegalArgumentException.class,
                    () -> sequence.append(1L, payload, 0, DATA_CAP + 1));
        }

        @Test
        void insertRejectsOversizedPayload() {
            final io.github.green4j.d4m.common.AtomicBuffer payload = TestHarness.payload(DATA_CAP + 1);
            assertThrows(IllegalArgumentException.class,
                    () -> sequence.insert(1L, payload, 0, DATA_CAP + 1));
        }
    }

}