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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link CursorSupport}.
 */
class CursorSupportTest {
    private static final int CHUNK_SIZE = 512;

    private static Chunk makeChunk(final long epoch) {
        final AtomicBuffer buffer =
                new UnsafeBuffer(ByteBuffer.allocate(CHUNK_SIZE));
        final Chunk chunk = new Chunk(buffer);
        chunk.putChunkEpoch(epoch);
        chunk.putRefCountOrdered(0);
        chunk.putEvictionStateOrdered(Chunk.EVICTION_NONE);
        return chunk;
    }

    private static ChunkSnapshot snapshotOf(final Chunk... chunks) {
        final int len = chunks.length;
        if (len == 0) {
            return new ChunkSnapshot(new Chunk[0][], new long[0][], 0, 0, 1);
        }
        final int segCount = ((len - 1) >> ChunkSnapshot.SEG_SHIFT) + 1;
        final Chunk[][] cs = new Chunk[segCount][];
        final long[][] es = new long[segCount][];
        for (int s = 0; s < segCount; s++) {
            cs[s] = new Chunk[ChunkSnapshot.SEG_SIZE];
            es[s] = new long[ChunkSnapshot.SEG_SIZE];
        }
        for (int i = 0; i < len; i++) {
            cs[i >> ChunkSnapshot.SEG_SHIFT][i & ChunkSnapshot.SEG_MASK] = chunks[i];
            es[i >> ChunkSnapshot.SEG_SHIFT][i & ChunkSnapshot.SEG_MASK] = chunks[i].getChunkEpoch();
        }
        return new ChunkSnapshot(cs, es, len, 0, 1);
    }

    @Nested
    class PinLifecycle {
        @Test
        void acquirePinIncrementsRefCount() {
            final Chunk chunk = makeChunk(1L);
            final ChunkSnapshot snapshot = snapshotOf(chunk);
            final CursorSupport.PinState pin = new CursorSupport.PinState();

            assertTrue(CursorSupport.acquirePin(snapshot, 0, pin));
            assertEquals(1, chunk.getRefCount());
        }

        @Test
        void releasePinDecrementsRefCount() {
            final Chunk chunk = makeChunk(1L);
            final ChunkSnapshot snapshot = snapshotOf(chunk);
            final CursorSupport.PinState pin = new CursorSupport.PinState();

            CursorSupport.acquirePin(snapshot, 0, pin);
            CursorSupport.releasePin(pin);

            assertEquals(0, chunk.getRefCount());
            assertNull(pin.pinned);
        }

        @Test
        void reacquireSameChunkDoesNotDoubleIncrement() {
            final Chunk chunk = makeChunk(1L);
            final ChunkSnapshot snapshot = snapshotOf(chunk);
            final CursorSupport.PinState pin = new CursorSupport.PinState();

            CursorSupport.acquirePin(snapshot, 0, pin);
            // Second acquire on same chunk/epoch should reuse
            assertTrue(CursorSupport.acquirePin(snapshot, 0, pin));
            assertEquals(1, chunk.getRefCount());
        }

        @Test
        void acquireFailsWhenEpochChangedBeforeCas() {
            final Chunk chunk = makeChunk(1L);
            final ChunkSnapshot snapshot = snapshotOf(chunk);
            final CursorSupport.PinState pin = new CursorSupport.PinState();

            chunk.putChunkEpoch(999L); // epoch drifted

            assertFalse(CursorSupport.acquirePin(snapshot, 0, pin));
        }

        @Test
        void acquireFailsWhenRefCountNegative() {
            final Chunk chunk = makeChunk(1L);
            chunk.putRefCountOrdered(-1); // poisoned
            final ChunkSnapshot snapshot = snapshotOf(chunk);
            final CursorSupport.PinState pin = new CursorSupport.PinState();

            assertFalse(CursorSupport.acquirePin(snapshot, 0, pin));
        }

        @Test
        void releaseOnClearPinIsNoOp() {
            final CursorSupport.PinState pin = new CursorSupport.PinState();
            assertDoesNotThrow(() -> CursorSupport.releasePin(pin));
        }
    }

    @Nested
    class TransferPin {
        @Test
        void transferMovesOwnershipWithoutRefCountChange() {
            final Chunk chunk = makeChunk(1L);
            final ChunkSnapshot snapshot = snapshotOf(chunk);
            final CursorSupport.PinState from = new CursorSupport.PinState();
            final CursorSupport.PinState to = new CursorSupport.PinState();

            CursorSupport.acquirePin(snapshot, 0, from);
            assertEquals(1, chunk.getRefCount());

            CursorSupport.transferPin(from, to);

            assertEquals(1, chunk.getRefCount()); // no change
            assertNull(from.pinned);
            assertSame(chunk, to.pinned);
        }

        @Test
        void transferReleasesTargetExistingPin() {
            final Chunk chunk1 = makeChunk(1L);
            final Chunk chunk2 = makeChunk(2L);
            final ChunkSnapshot snapshot = snapshotOf(chunk1, chunk2);
            final CursorSupport.PinState from = new CursorSupport.PinState();
            final CursorSupport.PinState to = new CursorSupport.PinState();

            CursorSupport.acquirePin(snapshot, 0, to);
            CursorSupport.acquirePin(snapshot, 1, from);

            CursorSupport.transferPin(from, to);

            assertEquals(0, chunk1.getRefCount()); // old target released
            assertEquals(1, chunk2.getRefCount()); // transferred pin held
        }
    }

    @Nested
    class DecRef {
        @Test
        void decRefFromOneReachesZero() {
            final Chunk chunk = makeChunk(1L);
            chunk.putRefCountOrdered(1);

            CursorSupport.decRef(chunk);

            assertEquals(0, chunk.getRefCount());
        }
    }
}