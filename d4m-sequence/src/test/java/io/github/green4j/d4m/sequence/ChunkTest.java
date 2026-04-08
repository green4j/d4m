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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link Chunk}.
 */
class ChunkTest {
    private static final int CHUNK_SIZE = 1024;

    private static Chunk freshChunk() {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(CHUNK_SIZE);
        final AtomicBuffer buffer = new UnsafeBuffer(byteBuffer);
        return new Chunk(buffer);
    }

    @Nested
    class IdentityFields {
        @Test
        void refCountCasLifecycle() {
            final Chunk chunk = freshChunk();
            chunk.putRefCountOrdered(0);

            assertTrue(chunk.casRefCount(0, 1));
            assertEquals(1, chunk.getRefCount());

            assertTrue(chunk.casRefCount(1, 2));
            assertEquals(2, chunk.getRefCount());

            assertFalse(chunk.casRefCount(0, 3)); // mismatch
            assertEquals(2, chunk.getRefCount());
        }

        @Test
        void evictionStateCasTransitions() {
            final Chunk chunk = freshChunk();
            chunk.putEvictionStateOrdered(Chunk.EVICTION_NONE);

            assertEquals(Chunk.EVICTION_NONE, chunk.getEvictionState());

            assertTrue(chunk.casEvictionState(Chunk.EVICTION_NONE, Chunk.EVICTION_CANDIDATE));
            assertEquals(Chunk.EVICTION_CANDIDATE, chunk.getEvictionState());

            assertTrue(chunk.casEvictionState(Chunk.EVICTION_CANDIDATE, Chunk.EVICTION_IN_PROGRESS));
            assertEquals(Chunk.EVICTION_IN_PROGRESS, chunk.getEvictionState());
        }

        @Test
        void chunkEpochVolatileRoundTrip() {
            final Chunk chunk = freshChunk();
            chunk.putChunkEpoch(0xCAFE_BABEL);

            assertEquals(0xCAFE_BABEL, chunk.getChunkEpoch());
        }
    }

    @Nested
    class WriterMetadata {
        @Test
        void entryCountOrderedStoreVolatileLoad() {
            final Chunk chunk = freshChunk();
            chunk.putEntryCountOrdered(7);

            assertEquals(7, chunk.getEntryCount());
        }

        @Test
        void entryCountPlainStore() {
            final Chunk chunk = freshChunk();
            chunk.putEntryCountPlain(5);

            assertEquals(5, chunk.getEntryCount());
        }

        @Test
        void sealTransition() {
            final Chunk chunk = freshChunk();
            chunk.putEntryCountPlain(0);
            assertFalse(chunk.isSealed());

            chunk.seal();
            assertTrue(chunk.isSealed());
        }

        @Test
        void ordersPlainAccess() {
            final Chunk chunk = freshChunk();
            chunk.putMinOrder(100L);
            chunk.putMaxOrder(999L);

            assertEquals(100L, chunk.getMinOrder());
            assertEquals(999L, chunk.getMaxOrder());
        }

        @Test
        void ordersVolatileAccess() {
            final Chunk chunk = freshChunk();
            chunk.putMinOrder(50L);
            chunk.putMaxOrder(500L);

            assertEquals(50L, chunk.getMinOrderVolatile());
            assertEquals(500L, chunk.getMaxOrderVolatile());
        }

        @Test
        void dataWriteOffset() {
            final Chunk chunk = freshChunk();
            chunk.putDataWriteOffset(Chunk.HEADER_SIZE + 128);

            assertEquals(Chunk.HEADER_SIZE + 128, chunk.getDataWriteOffset());
        }
    }

    @Nested
    class EntryOperations {
        @Test
        void writeAndReadSingleEntry() {
            final Chunk chunk = freshChunk();
            final byte[] payload = {10, 20, 30, 40};
            final AtomicBuffer payloadBuf = new UnsafeBuffer(payload);

            final int offset = Chunk.HEADER_SIZE;
            chunk.writeEntry(offset, 1000L, 1L, payloadBuf, 0, 4);

            assertEquals(1000L, chunk.entryOrder(offset));
            assertEquals(1L, chunk.entryVersion(offset));
            assertEquals(4, chunk.entryPayloadSize(offset));
        }

        @Test
        void entryTotalSizeAlignedTo8() {
            final Chunk chunk = freshChunk();
            final AtomicBuffer payloadBuf = new UnsafeBuffer(new byte[5]);
            chunk.writeEntry(Chunk.HEADER_SIZE, 1L, 1L, payloadBuf, 0, 5);

            // Header=24, payload=5, total=29, aligned to 32
            assertEquals(32, chunk.entryTotalSize(Chunk.HEADER_SIZE));
        }

        @Test
        void entrySizeStaticComputation() {
            assertEquals(24, Chunk.entrySize(0)); // header only, aligned
            assertEquals(32, Chunk.entrySize(1)); // 24+1=25, aligned to 32
            assertEquals(32, Chunk.entrySize(8)); // 24+8=32, already aligned
            assertEquals(40, Chunk.entrySize(9)); // 24+9=33, aligned to 40
        }

        @Test
        void writeMultipleEntriesSequentially() {
            final Chunk chunk = freshChunk();
            final AtomicBuffer pl = new UnsafeBuffer(new byte[8]);

            int offset = Chunk.HEADER_SIZE;
            for (int i = 0; i < 3; i++) {
                pl.putLong(0, i * 100L);
                chunk.writeEntry(offset, (i + 1) * 1000L, i + 1L, pl, 0, 8);
                offset += Chunk.entrySize(8);
            }
            chunk.putEntryCountOrdered(3);
            chunk.putDataWriteOffset(offset);

            assertEquals(3, chunk.getEntryCount());

            // Verify second entry
            final int secondOffset = Chunk.HEADER_SIZE + Chunk.entrySize(8);
            assertEquals(2000L, chunk.entryOrder(secondOffset));
            assertEquals(2L, chunk.entryVersion(secondOffset));
        }

        @Test
        void payloadBytesAreCopiedCorrectly() {
            final Chunk chunk = freshChunk();
            final byte[] payload = {0xA, 0xB, 0xC, 0xD, 0xE, 0xF, 0x1, 0x2};
            final AtomicBuffer plBuf = new UnsafeBuffer(payload);
            chunk.writeEntry(Chunk.HEADER_SIZE, 1L, 1L, plBuf, 0, 8);

            final byte[] readBack = new byte[8];
            chunk.buffer().getBytes(Chunk.HEADER_SIZE + Chunk.ENTRY_HEADER_SIZE, readBack, 0, 8);
            assertArrayEquals(payload, readBack);
        }
    }

    @Nested
    class CopyAndZero {
        @Test
        void copyChunkDataFromPreservesEntries() {
            final Chunk src = freshChunk();
            final AtomicBuffer pl =
                    new UnsafeBuffer(new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
            src.putDataWriteOffset(Chunk.HEADER_SIZE);
            src.writeEntry(Chunk.HEADER_SIZE, 500L, 1L, pl, 0, 8);
            src.putDataWriteOffset(Chunk.HEADER_SIZE + Chunk.entrySize(8));
            src.putMinOrder(500L);
            src.putMaxOrder(500L);
            src.putEntryCountOrdered(1);

            final Chunk dst = freshChunk();
            dst.putChunkEpoch(99L); // identity must be untouched
            dst.copyChunkDataFrom(src);

            assertEquals(1, dst.getEntryCount());
            assertEquals(500L, dst.entryOrder(Chunk.HEADER_SIZE));
            assertEquals(99L, dst.getChunkEpoch()); // identity preserved
        }

        @Test
        void zeroWriterMetaClearsOnlyCacheLine1() {
            final Chunk chunk = freshChunk();
            chunk.putChunkEpoch(77L);
            chunk.putRefCountOrdered(3);
            chunk.putMinOrder(100L);
            chunk.putMaxOrder(200L);
            chunk.putEntryCountOrdered(5);

            chunk.zeroWriterMeta();

            // Identity intact
            assertEquals(77L, chunk.getChunkEpoch());
            assertEquals(3, chunk.getRefCount());
            // Writer metadata zeroed
            assertEquals(0, chunk.getEntryCount());
            assertEquals(0L, chunk.getMinOrder());
            assertEquals(0L, chunk.getMaxOrder());
        }
    }

    @Nested
    class HeapVsMmapDetection {
        @Test
        void heapByteBufferIsHeapBased() {
            final Chunk chunk =
                    new Chunk(new UnsafeBuffer(ByteBuffer.allocate(CHUNK_SIZE)));

            assertTrue(chunk.isHeapBased());
            assertFalse(chunk.isMmapBased());
        }

        @Test
        void directByteBufferIsMmapBased() {
            final Chunk chunk = new Chunk(
                    new UnsafeBuffer(ByteBuffer.allocateDirect(CHUNK_SIZE)));

            assertFalse(chunk.isHeapBased());
            assertTrue(chunk.isMmapBased());
        }
    }
}