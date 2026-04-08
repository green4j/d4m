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

/**
 * Low-level pin/unpin helpers shared by all cursor implementations.
 * <p>
 * All snapshot access uses the parallel-array API
 * ({@code snap.chunk(chunkIndex)}/{@code snap.epoch(chunkIndex)}).
 */
abstract class CursorSupport {
    private CursorSupport() {
    }

    private static final int PIN_SPINS = 64;
    private static final int SPIN_WAIT_THRESHOLD = 8;

    /**
     * Mutable holder for the state of a single pinned chunk reference.
     * Tracks the pinned chunk, its epoch, chunk index, and the snapshot
     * version at the time of pinning.
     */
    static final class PinState {
        Chunk pinned;
        long pinnedEpoch = -1;
        int pinnedChunkIndex = -1;
        long pinnedSnapshotVersion = -1;

        /**
         * Returns {@code true} if this pin is still valid for the given
         * snapshot and chunk index, accounting for buffer identity and
         * epoch consistency.
         *
         * @param snap the current snapshot to validate against
         * @param ci   the chunk index to validate
         * @return whether the existing pin is still usable
         */
        boolean isValidFor(final ChunkSnapshot snap, final int ci) {
            if (pinned == null) {
                return false;
            }
            if (pinnedChunkIndex != ci || pinnedSnapshotVersion != snap.version()) {
                if (pinned.buffer() != snap.chunk(ci).buffer()) {
                    return false;
                }
                if (pinnedEpoch != snap.epoch(ci)) {
                    return false;
                }
            }
            return pinned.getChunkEpoch() == pinnedEpoch;
        }

        /**
         * Records the pinned chunk, its epoch, chunk index, and snapshot version.
         *
         * @param chunk           the pinned chunk
         * @param epoch           the chunk's epoch at pin time
         * @param chunkIndex      the chunk's index within the snapshot
         * @param snapshotVersion the snapshot version at pin time
         */
        void set(final Chunk chunk,
                 final long epoch,
                 final int chunkIndex,
                 final long snapshotVersion) {
            pinned = chunk;
            pinnedEpoch = epoch;
            pinnedChunkIndex = chunkIndex;
            pinnedSnapshotVersion = snapshotVersion;
        }

        /**
         * Clears all pin state without decrementing the reference count.
         */
        void clear() {
            pinned = null;
            pinnedEpoch = -1;
            pinnedChunkIndex = -1;
            pinnedSnapshotVersion = -1;
        }
    }

    /**
     * Attempts to pin a chunk by atomically incrementing its reference count,
     * verifying epoch consistency. Reuses an existing pin if it is still valid
     * for the requested snapshot and chunk index.
     *
     * @param forSnapshot the snapshot containing the chunk to pin
     * @param chunkIndex  the index of the chunk within the snapshot
     * @param state       mutable pin state to populate on success
     * @return {@code true} if the chunk was successfully pinned
     */
    static boolean acquirePin(final ChunkSnapshot forSnapshot,
                              final int chunkIndex,
                              final PinState state) {
        if (state.isValidFor(forSnapshot, chunkIndex)) {
            state.pinnedChunkIndex = chunkIndex;
            state.pinnedSnapshotVersion = forSnapshot.version();
            return true;
        }
        releasePin(state);

        final Chunk chunk = forSnapshot.chunk(chunkIndex);
        final long expectedEpoch = forSnapshot.epoch(chunkIndex);
        if (chunk.getChunkEpoch() != expectedEpoch) {
            return false;
        }

        for (int s = 0; s < PIN_SPINS; s++) {
            final int refCount = chunk.getRefCount();
            if (refCount < 0) {
                return false;
            }
            if (chunk.casRefCount(refCount, refCount + 1)) {
                if (chunk.getChunkEpoch() != expectedEpoch) {
                    decRef(chunk);
                    return false;
                }
                state.set(chunk, expectedEpoch, chunkIndex, forSnapshot.version());
                return true;
            }
            if (s >= SPIN_WAIT_THRESHOLD) {
                Thread.onSpinWait();
            }
        }
        return false;
    }

    /**
     * Releases a previously acquired pin by decrementing the chunk's
     * reference count and clearing the pin state.
     *
     * @param state the pin state to release
     */
    static void releasePin(final PinState state) {
        if (state.pinned == null) {
            return;
        }
        decRef(state.pinned);
        state.clear();
    }

    /**
     * Transfers pin ownership from {@code from} to {@code to} without
     * touching the ref-count.  The target's existing pin (if any) is
     * released first.  After the call, {@code from} is cleared (no
     * decRef) and {@code to} holds the live pin.
     *
     * <p>This eliminates the unpin/re-pin window that would otherwise
     * exist between {@code reposition()} releasing its working pin and
     * the next {@code acquirePin()} in the delivery loop.</p>
     *
     * @param from the source pin state whose ownership is being transferred
     * @param to   the target pin state that will receive ownership
     */
    static void transferPin(final PinState from, final PinState to) {
        releasePin(to);
        to.pinned = from.pinned;
        to.pinnedEpoch = from.pinnedEpoch;
        to.pinnedChunkIndex = from.pinnedChunkIndex;
        to.pinnedSnapshotVersion = from.pinnedSnapshotVersion;
        from.clear();   // clear without decRef — ownership moved
    }

    /**
     * Atomically decrements the chunk's reference count using a CAS spin
     * loop with {@link Thread#onSpinWait()} back-off. No-ops if the
     * ref-count is already zero or negative.
     *
     * @param chunk the chunk whose reference count to decrement
     */
    static void decRef(final Chunk chunk) {
        while (true) {
            final int refCount = chunk.getRefCount();
            if (refCount <= 0) {
                return;
            }
            if (chunk.casRefCount(refCount, refCount - 1)) {
                return;
            }
            Thread.onSpinWait();
        }
    }
}
