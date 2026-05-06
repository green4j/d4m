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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link MmapChunkAllocator}.
 */
class MmapChunkAllocatorTest {
    private static final int CHUNK_SIZE = 1024;

    @Nested
    class Allocation {
        @Test
        void allocatesDistinctDirectChunks(@TempDir final File dir) {
            final MmapChunkAllocator alloc = new MmapChunkAllocator(
                    CHUNK_SIZE,
                    dir,
                    false,
                    new AtomicLong()
            );
            final Chunk chunk1 = alloc.allocate();
            final Chunk chunk2 = alloc.allocate();

            assertNotNull(chunk1);
            assertNotNull(chunk2);
            assertTrue(chunk1.isMmapBased());
            assertTrue(chunk2.isMmapBased());
            assertNotSame(chunk1.buffer(), chunk2.buffer());
        }

        @Test
        void allocatedChunksHaveDistinctEpochs(@TempDir final File dir) {
            final MmapChunkAllocator alloc = new MmapChunkAllocator(
                    CHUNK_SIZE,
                    dir,
                    false,
                    new AtomicLong()
            );
            final Chunk chunk1 = alloc.allocate();
            final Chunk chunk2 = alloc.allocate();

            assertNotEquals(chunk1.getChunkEpoch(), chunk2.getChunkEpoch());
        }

        @Test
        void allocatedChunkHasCleanState(@TempDir final File dir) {
            final MmapChunkAllocator alloc = new MmapChunkAllocator(
                    CHUNK_SIZE,
                    dir,
                    false,
                    new AtomicLong()
            );
            final Chunk chunk = alloc.allocate();

            assertEquals(0, chunk.getRefCount());
            assertEquals(Chunk.EVICTION_NONE, chunk.getEvictionState());
            assertEquals(0, chunk.getEntryCount());
        }

        @Test
        void allocatedChunkHasCorrectCapacity(@TempDir final File dir) {
            final MmapChunkAllocator alloc = new MmapChunkAllocator(
                    CHUNK_SIZE,
                    dir,
                    false,
                    new AtomicLong()
            );
            final Chunk chunk = alloc.allocate();

            assertEquals(CHUNK_SIZE, chunk.buffer().capacity());
        }

        @Test
        void preAllocCreatesMappingImmediately(@TempDir final File dir) {
            final MmapChunkAllocator alloc = new MmapChunkAllocator(
                    CHUNK_SIZE,
                    dir,
                    true,
                    new AtomicLong()
            );
            final Chunk chunk = alloc.allocate();

            assertNotNull(chunk);
        }
    }

    @Nested
    class FreedChunkReuse {
        @Test
        void freedChunkIsReusedOnNextAllocate(@TempDir final File dir) {
            final MmapChunkAllocator alloc = new MmapChunkAllocator(
                    CHUNK_SIZE,
                    dir,
                    false,
                    new AtomicLong()
            );
            final Chunk chunk1 = alloc.allocate();
            final AtomicBuffer firstBuffer = chunk1.buffer();
            final long firstEpoch = chunk1.getChunkEpoch();

            alloc.free(chunk1);
            final Chunk chunk2 = alloc.allocate();

            assertSame(firstBuffer, chunk2.buffer());
            assertNotEquals(firstEpoch, chunk2.getChunkEpoch());
        }
    }

    @Nested
    class Reclamation {
        @Test
        void reclaimedChunkBecomesAvailable(@TempDir final File dir) {
            final MmapChunkAllocator alloc = new MmapChunkAllocator(
                    CHUNK_SIZE,
                    dir,
                    false,
                    new AtomicLong()
            );
            final Chunk chunk1 = alloc.allocate();
            assertEquals(0, chunk1.getRefCount());

            alloc.submitPendingReclamation(chunk1);
            final Chunk chunk2 = alloc.allocate();

            assertNotNull(chunk2);
        }

        @Test
        void pinnedChunkNotImmediatelyReclaimed(@TempDir final File dir) {
            final MmapChunkAllocator alloc = new MmapChunkAllocator(
                    CHUNK_SIZE,
                    dir,
                    false,
                    new AtomicLong()
            );
            final Chunk chunk = alloc.allocate();
            chunk.casRefCount(0, 1);

            final Set<AtomicBuffer> allocated = new HashSet<>();
            allocated.add(chunk.buffer());

            alloc.submitPendingReclamation(chunk);

            for (int i = 0; i < 5; i++) {
                final Chunk nextChunk = alloc.allocate();
                allocated.add(nextChunk.buffer());
            }

            chunk.casRefCount(1, 0);
        }
    }
}