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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HeapChunkAllocator}.
 */
class HeapChunkAllocatorTest {
    private static final int CHUNK_SIZE = 1024;
    private static final int SLAB_SIZE = 4096;
    private static final int CHUNKS_PER_SLAB = SLAB_SIZE / CHUNK_SIZE;

    private HeapChunkAllocator makeAllocator(final long maxHeap) {
        return new HeapChunkAllocator(CHUNK_SIZE, CHUNKS_PER_SLAB, maxHeap, new AtomicLong());
    }

    @Nested
    class Allocation {
        @Test
        void allocatesDistinctChunks() {
            final HeapChunkAllocator alloc = makeAllocator(8 * 1024L);
            final Chunk chunk1 = alloc.tryAllocate();
            final Chunk chunk2 = alloc.tryAllocate();

            assertNotNull(chunk1);
            assertNotNull(chunk2);
            assertNotSame(chunk1.buffer(), chunk2.buffer());
        }

        @Test
        void allocatedChunkHasCorrectCapacity() {
            final HeapChunkAllocator alloc = makeAllocator(4096);
            final Chunk chunk = alloc.tryAllocate();

            assertNotNull(chunk);
            assertEquals(CHUNK_SIZE, chunk.buffer().capacity());
        }

        @Test
        void allocatedChunksHaveDistinctEpochs() {
            final HeapChunkAllocator alloc = makeAllocator(8 * 1024L);
            final Chunk chunk1 = alloc.tryAllocate();
            final Chunk chunk2 = alloc.tryAllocate();

            assertTrue(chunk1.getChunkEpoch() < chunk2.getChunkEpoch());
        }

        @Test
        void allocatedChunkHasCleanMetadata() {
            final HeapChunkAllocator alloc = makeAllocator(4096);
            final Chunk chunk = alloc.tryAllocate();

            assertEquals(0, chunk.getRefCount());
            assertEquals(Chunk.EVICTION_NONE, chunk.getEvictionState());
            assertEquals(0, chunk.getEntryCount());
        }

        @Test
        void returnsNullWhenExhausted() {
            // 1 Slab = 4 chunks
            final HeapChunkAllocator alloc = makeAllocator(SLAB_SIZE);
            for (int i = 0; i < 4; i++) {
                assertNotNull(alloc.tryAllocate(), "chunk " + i);
            }
            assertNull(alloc.tryAllocate());
        }

        @Test
        void chunkSizeAccessor() {
            final HeapChunkAllocator alloc = makeAllocator(4096);
            assertEquals(CHUNK_SIZE, alloc.chunkSize());
        }
    }

    @Nested
    class Listeners {
        private final class RecordingListener implements HeapChunkAllocator.Listener {
            int slabCount;
            int lastChunksInSlab;
            int poolExhaustedCount;
            int reclaimedCount;
            long lastReclaimedEpoch;

            @Override
            public void onSlabAllocated(final HeapChunkAllocator notifier,
                                        final int slabIndex,
                                        final int slabBytes,
                                        final int chunksInSlab) {
                slabCount++;
                lastChunksInSlab = chunksInSlab;
            }

            @Override
            public void onPoolExhausted(final HeapChunkAllocator notifier) {
                poolExhaustedCount++;
            }

            @Override
            public void onChunkReclaimed(final HeapChunkAllocator notifier,
                                         final long chunkEpoch) {
                reclaimedCount++;
                lastReclaimedEpoch = chunkEpoch;
            }
        }

        @Test
        void onSlabAllocatedFiresPerSlab() {
            final RecordingListener listener = new RecordingListener();
            // 2 slabs of 4 chunks each
            new HeapChunkAllocator(CHUNK_SIZE, CHUNKS_PER_SLAB, 2L * SLAB_SIZE,
                    new AtomicLong(), listener);

            assertEquals(2, listener.slabCount);
            assertEquals(CHUNKS_PER_SLAB, listener.lastChunksInSlab);
        }

        @Test
        void onPoolExhaustedFiresWhenEmpty() {
            final RecordingListener listener = new RecordingListener();
            final HeapChunkAllocator alloc = new HeapChunkAllocator(
                    CHUNK_SIZE, CHUNKS_PER_SLAB, SLAB_SIZE, new AtomicLong(), listener);
            for (int i = 0; i < 4; i++) {
                assertNotNull(alloc.tryAllocate());
            }
            assertEquals(0, listener.poolExhaustedCount);

            assertNull(alloc.tryAllocate());
            assertEquals(1, listener.poolExhaustedCount);
        }

        @Test
        void onChunkReclaimedFiresOnReclamation() {
            final RecordingListener listener = new RecordingListener();
            final HeapChunkAllocator alloc = new HeapChunkAllocator(
                    CHUNK_SIZE, CHUNKS_PER_SLAB, SLAB_SIZE, new AtomicLong(), listener);
            final Chunk chunk = alloc.tryAllocate();
            final long epoch = chunk.getChunkEpoch();

            alloc.submitPendingReclamation(chunk);
            alloc.drainPendingReclamation();

            assertEquals(1, listener.reclaimedCount);
            assertEquals(epoch, listener.lastReclaimedEpoch);
        }
    }

    @Nested
    class Reclamation {
        @Test
        void reclaimedChunkBecomesAllocatableAgain() {
            final HeapChunkAllocator alloc = makeAllocator(SLAB_SIZE);
            // Exhaust all 4 chunks
            final Chunk[] allocated = new Chunk[4];
            for (int i = 0; i < 4; i++) {
                allocated[i] = alloc.tryAllocate();
            }
            assertNull(alloc.tryAllocate());

            // Submit one for reclamation (refCount must be 0)
            alloc.submitPendingReclamation(allocated[0]);
            alloc.drainPendingReclamation();

            final Chunk recycled = alloc.tryAllocate();
            assertNotNull(recycled);
        }

        @Test
        void reclaimedChunkGetsNewEpoch() {
            final HeapChunkAllocator alloc = makeAllocator(SLAB_SIZE);
            final Chunk first = alloc.tryAllocate();
            final long originalEpoch = first.getChunkEpoch();

            alloc.submitPendingReclamation(first);
            alloc.drainPendingReclamation();

            final Chunk recycled = alloc.tryAllocate();
            assertTrue(recycled.getChunkEpoch() > originalEpoch);
        }

        @Test
        void pinnedChunkIsNotImmediatelyReclaimed() {
            final HeapChunkAllocator alloc = makeAllocator(SLAB_SIZE);
            final Chunk chunk = alloc.tryAllocate();
            // Exhaust remaining
            for (int i = 0; i < 3; i++) {
                alloc.tryAllocate();
            }

            // Simulate pin
            chunk.casRefCount(0, 1);
            alloc.submitPendingReclamation(chunk);
            alloc.drainPendingReclamation();

            // Still pinned so not reclaimable
            assertNull(alloc.tryAllocate());

            // Unpin
            chunk.casRefCount(1, 0);
            alloc.drainPendingReclamation();

            assertNotNull(alloc.tryAllocate());
        }
    }
}