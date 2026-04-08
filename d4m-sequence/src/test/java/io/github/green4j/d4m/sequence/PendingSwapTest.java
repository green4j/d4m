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

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests for {@link PendingSwap}.
 */
class PendingSwapTest {
    @Nested
    class Construction {
        @Test
        void capturesEpochsAtCreationTime() {
            final Chunk heapChunk =
                    new Chunk(new io.github.green4j.d4m.common.UnsafeBuffer(ByteBuffer.allocate(512)));
            heapChunk.putChunkEpoch(10L);
            final Chunk mmapChunk =
                    new Chunk(new io.github.green4j.d4m.common.UnsafeBuffer(ByteBuffer.allocateDirect(512)));
            mmapChunk.putChunkEpoch(20L);

            final PendingSwap swap = new PendingSwap(heapChunk, mmapChunk);

            assertSame(heapChunk, swap.oldHeapChunk);
            assertEquals(10L, swap.oldHeapEpoch);
            assertSame(mmapChunk, swap.newMmapChunk);
            assertEquals(20L, swap.newMmapEpoch);
        }

        @Test
        void survivesSubsequentEpochChange() {
            final Chunk heapChunk =
                    new Chunk(new io.github.green4j.d4m.common.UnsafeBuffer(ByteBuffer.allocate(512)));
            heapChunk.putChunkEpoch(10L);
            final Chunk mmapChunk =
                    new Chunk(new io.github.green4j.d4m.common.UnsafeBuffer(ByteBuffer.allocateDirect(512)));
            mmapChunk.putChunkEpoch(20L);

            final PendingSwap swap = new PendingSwap(heapChunk, mmapChunk);

            // Mutate epochs after creation
            heapChunk.putChunkEpoch(999L);
            mmapChunk.putChunkEpoch(888L);

            // Swap retains original captured values
            assertEquals(10L, swap.oldHeapEpoch);
            assertEquals(20L, swap.newMmapEpoch);
        }
    }
}