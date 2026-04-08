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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests for {@link ChunkSnapshot}.
 */
class ChunkSnapshotTest {
    private static Chunk dummyChunk(final long epoch) {
        final AtomicBuffer buffer =
                new UnsafeBuffer(ByteBuffer.allocate(512));
        final Chunk chunk = new Chunk(buffer);
        chunk.putChunkEpoch(epoch);
        return chunk;
    }

    private static ChunkSnapshot snapshotOf(final Chunk[] chunks,
                                            final long[] epochs,
                                            final int size,
                                            final long version) {
        if (size == 0) {
            return new ChunkSnapshot(new Chunk[0][], new long[0][], 0, 0, version);
        }
        final int segCount = ((size - 1) >> ChunkSnapshot.SEG_SHIFT) + 1;
        final Chunk[][] cs = new Chunk[segCount][];
        final long[][] es = new long[segCount][];
        for (int s = 0; s < segCount; s++) {
            cs[s] = new Chunk[ChunkSnapshot.SEG_SIZE];
            es[s] = new long[ChunkSnapshot.SEG_SIZE];
            final int from = s << ChunkSnapshot.SEG_SHIFT;
            final int count = Math.min(ChunkSnapshot.SEG_SIZE, size - from);
            System.arraycopy(chunks, from, cs[s], 0, count);
            System.arraycopy(epochs, from, es[s], 0, count);
        }
        return new ChunkSnapshot(cs, es, size, 0, version);
    }

    @Nested
    class EmptySnapshot {
        @Test
        void emptyHasSizeZero() {
            assertEquals(0, ChunkSnapshot.EMPTY.size());
        }

        @Test
        void emptyHasVersionZero() {
            assertEquals(0, ChunkSnapshot.EMPTY.version());
        }
    }

    @Nested
    class PopulatedSnapshot {
        @Test
        void sizeReflectsLogicalLength() {
            final Chunk chunk1 = dummyChunk(1);
            final Chunk chunk2 = dummyChunk(2);
            // Backing array over-allocated by 2 slots
            final Chunk[] chunks = {chunk1, chunk2, null, null};
            final long[] epochs = {1L, 2L, 0L, 0L};

            final ChunkSnapshot snapshot = snapshotOf(chunks, epochs, 2, 5);

            assertEquals(2, snapshot.size());
            assertEquals(5, snapshot.version());
        }

        @Test
        void chunkAndEpochAccessByIndex() {
            final Chunk chunk1 = dummyChunk(10);
            final Chunk chunk2 = dummyChunk(20);
            final Chunk[] chunks = {chunk1, chunk2};
            final long[] epochs = {10L, 20L};

            final ChunkSnapshot snapshot = snapshotOf(chunks, epochs, 2, 1);

            assertSame(chunk1, snapshot.chunk(0));
            assertSame(chunk2, snapshot.chunk(1));
            assertEquals(10L, snapshot.epoch(0));
            assertEquals(20L, snapshot.epoch(1));
        }

        @Test
        void versionIsStamped() {
            final ChunkSnapshot snapshot = snapshotOf(
                    new Chunk[0], new long[0], 0, 42);

            assertEquals(42, snapshot.version());
        }
    }
}