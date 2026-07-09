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
 * A cursor that iterates over sequence entries in forward (ascending)
 * order, supporting real-time tailing of the active tail chunk. The
 * cursor maintains its position across snapshot changes, automatically
 * repositioning when chunks are evicted or swapped.
 *
 * <p>Not thread-safe - intended for single-reader use.</p>
 */
public final class ForwardCursor implements AutoCloseable {
    private final Sequence sequence;

    // Pre-allocated pin states (no allocation on hot path)
    private final CursorSupport.PinState pinState = new CursorSupport.PinState();
    private final CursorSupport.PinState peekPin = new CursorSupport.PinState();
    private final CursorSupport.PinState reposPin = new CursorSupport.PinState();

    private long lastEntryOrder = Long.MIN_VALUE;
    private long lastEntryVersion = -1;

    private int currentChunkIndex;
    private int currentEntryIndex;
    private int currentEntryOffset;

    private ChunkSnapshot snapshot;
    private long snapshotVersion = -1;

    private long seekOrder;
    private boolean seekActive;

    private long cachedPeekOrder = Long.MAX_VALUE;
    private boolean peekDirty = true;

    /**
     * Creates a forward cursor over the given sequence.
     *
     * @param sequence the sequence to iterate
     */
    public ForwardCursor(final Sequence sequence) {
        this.sequence = sequence;
        resetPos();
    }

    /**
     * Returns the sequence the cursor is wrapping.
     *
     * @return the sequence
     */
    public Sequence sequence() {
        return sequence;
    }

    private void resetPos() {
        currentChunkIndex = 0;
        currentEntryIndex = 0;
        currentEntryOffset = Chunk.HEADER_SIZE;
    }

    /**
     * Positions the cursor to start iterating forward from the entry
     * with the given order (inclusive). Entries with orders less than
     * {@code order} are skipped.
     *
     * @param order the order to seek to
     */
    public void seekTo(final long order) {
        CursorSupport.releasePin(pinState);
        lastEntryOrder = Long.MIN_VALUE;
        lastEntryVersion = -1;
        resetPos();
        snapshot = null;
        snapshotVersion = -1;
        seekOrder = order;
        seekActive = true;
        peekDirty = true;
    }

    /**
     * Returns the order of the next entry that would be delivered by
     * {@link #next}, without advancing the cursor. Returns
     * {@link Long#MAX_VALUE} if no more entries are currently available.
     *
     * @return the next entry's order, or {@link Long#MAX_VALUE}
     */
    long peekNextOrder() {
        if (!peekDirty) {
            return cachedPeekOrder;
        }
        cachedPeekOrder = computePeekOrder();
        peekDirty = false;
        return cachedPeekOrder;
    }

    /**
     * Marks the cached peek value as stale, forcing the next call to
     * {@link #peekNextOrder()} to recompute it.
     */
    void invalidatePeek() {
        peekDirty = true;
    }

    private long computePeekOrder() {
        final ChunkSnapshot currentSnapshot = refresh();
        if (currentSnapshot.isEmpty()) {
            return Long.MAX_VALUE;
        }

        final int savedChunkIndex = currentChunkIndex;
        final int savedEntryIndex = currentEntryIndex;
        final int savedEntryOffset = currentEntryOffset;

        try {
            while (currentChunkIndex < currentSnapshot.size()) {
                if (!CursorSupport.acquirePin(currentSnapshot, currentChunkIndex, peekPin)) {
                    forceRefresh();
                    return Long.MAX_VALUE;
                }
                // See nextInternal: prefer the explicitly pinned chunk over a
                // second plain read of the spine, which could be stale.
                final Chunk chunk = peekPin.pinned;
                final int entryCount = chunk.getEntryCount();
                final int dataWriteOffset = chunk.getDataWriteOffset();

                while (currentEntryIndex < entryCount) {
                    if (currentEntryOffset >= dataWriteOffset) {
                        break;
                    }
                    final long entryOrder = chunk.entryOrder(currentEntryOffset);
                    final long entryVersion = chunk.entryVersion(currentEntryOffset);
                    if (after(entryOrder, entryVersion) && (!seekActive || entryOrder >= seekOrder)) {
                        return entryOrder;
                    }
                    currentEntryOffset += chunk.entryTotalSize(currentEntryOffset);
                    currentEntryIndex++;
                }

                final boolean sealed = chunk.isSealed();
                if (currentEntryIndex >= entryCount && (sealed || currentChunkIndex < currentSnapshot.size() - 1)) {
                    currentChunkIndex++;
                    currentEntryIndex = 0;
                    currentEntryOffset = Chunk.HEADER_SIZE;
                } else {
                    break;
                }
            }
            return Long.MAX_VALUE;
        } finally {
            CursorSupport.releasePin(peekPin);

            currentChunkIndex = savedChunkIndex;
            currentEntryIndex = savedEntryIndex;
            currentEntryOffset = savedEntryOffset;
        }
    }

    /**
     * Delivers up to {@code maxEntryCount} entries in forward order to
     * the given consumer.
     *
     * @param maxEntryCount maximum number of entries to deliver
     * @param consumer      callback to receive each entry
     * @return the number of entries actually delivered
     */
    public int next(final int maxEntryCount,
                    final EntryConsumer consumer) {
        return nextInternal(maxEntryCount, Long.MAX_VALUE, consumer);
    }

    /**
     * Delivers up to {@code maxEntryCount} entries in forward order,
     * stopping when the entry order exceeds {@code orderUpperBound}.
     *
     * @param maxEntryCount   maximum number of entries to deliver
     * @param orderUpperBound exclusive upper bound on entry order
     * @param consumer        callback to receive each entry
     * @return the number of entries actually delivered
     */
    public int nextUntil(final int maxEntryCount,
                         final long orderUpperBound,
                         final EntryConsumer consumer) {
        return nextInternal(maxEntryCount, orderUpperBound, consumer);
    }

    private int nextInternal(final int maxEntryCount,
                             final long orderUpperBound,
                             final EntryConsumer consumer) {
        final ChunkSnapshot currentSnapshot = refresh();
        if (currentSnapshot.isEmpty()) {
            return 0;
        }
        int delivered = 0;

        outer:
        while (delivered < maxEntryCount && currentChunkIndex < currentSnapshot.size()) {
            if (!CursorSupport.acquirePin(currentSnapshot, currentChunkIndex, pinState)) {
                forceRefresh();
                if (delivered > 0) {
                    peekDirty = true;
                }
                return delivered;
            }

            // Use the pinned chunk rather than re-reading from the spine: a
            // concurrent drainSwaps may have swapped spine[currentChunkIndex]
            // between acquirePin and now, returning a chunk reference that
            // differs from the one we explicitly verified-and-pinned. The pin
            // holds the underlying chunk alive and synchronizes its state via
            // its volatile epoch read inside acquirePin.
            final Chunk chunk = pinState.pinned;
            int entryCount = chunk.getEntryCount();
            int dataWriteOffset = chunk.getDataWriteOffset();

            boolean sealed = false;
            while (true) { // re-read loop
                while (currentEntryIndex < entryCount && delivered < maxEntryCount) {
                    if (currentEntryOffset >= dataWriteOffset) {
                        break;
                    }

                    final long entryOrder = chunk.entryOrder(currentEntryOffset);
                    final long entryVersion = chunk.entryVersion(currentEntryOffset);
                    final int entryPayloadLen = chunk.entryPayloadSize(currentEntryOffset);
                    final int entrySize = Chunk.entrySize(entryPayloadLen);

                    if (!after(entryOrder, entryVersion) || (seekActive && entryOrder < seekOrder)) {
                        currentEntryOffset += entrySize;
                        currentEntryIndex++;
                        continue;
                    }
                    if (entryOrder > orderUpperBound) {
                        break outer;
                    }
                    seekActive = false;

                    lastEntryOrder = entryOrder;
                    lastEntryVersion = entryVersion;
                    consumer.onEntry(
                            sequence,
                            entryOrder,
                            chunk.buffer(),
                            currentEntryOffset + Chunk.ENTRY_HEADER_SIZE,
                            entryPayloadLen
                    );
                    delivered++;
                    currentEntryOffset += entrySize;
                    currentEntryIndex++;
                }

                if (currentEntryIndex < entryCount) {
                    // Inner exit came from currentEntryOffset >= dataWriteOffset
                    // (or batch full). Don't re-read; the caller will retry.
                    break;
                }

                // Exhausted by count. Read isSealed BEFORE the final
                // getEntryCount: the acquire on a true seal observation
                // makes the writer's last putEntryCountOrdered (released
                // before the seal store) visible to the subsequent
                // entryCount read, so we never advance past a chunk whose
                // final batch of entries was committed between our previous
                // entryCount read and the seal.
                if (!sealed) {
                    sealed = chunk.isSealed();
                }
                final int mc2 = chunk.getEntryCount();
                if (mc2 > entryCount) {
                    entryCount = mc2;
                    dataWriteOffset = chunk.getDataWriteOffset();
                    continue; // re-enter inner
                }
                break; // done with chunk
            }

            if (currentEntryIndex >= entryCount
                    && (sealed || currentChunkIndex < currentSnapshot.size() - 1)) {
                currentChunkIndex++;
                currentEntryIndex = 0;
                currentEntryOffset = Chunk.HEADER_SIZE;
            } else {
                break;
            }
        }
        if (delivered > 0) {
            peekDirty = true;
        }
        return delivered;
    }

    private boolean after(final long order,
                          final long version) {
        return order > lastEntryOrder
                || (order == lastEntryOrder && version > lastEntryVersion);
    }

    private ChunkSnapshot refresh() {
        final ChunkSnapshot newSnapshot = sequence.snapshot();
        if (newSnapshot.version() != snapshotVersion) {
            if (lastEntryOrder != Long.MIN_VALUE) {
                if (!tryAdvance(newSnapshot)) {
                    if (!reposition(newSnapshot)) {
                        resetPos();
                        return ChunkSnapshot.EMPTY;
                    }
                }
            } else {
                CursorSupport.releasePin(pinState);
                resetPos();
            }
            snapshot = newSnapshot;
            snapshotVersion = newSnapshot.version();
        }
        return snapshot != null ? snapshot : ChunkSnapshot.EMPTY;
    }

    /**
     * Fast-path snapshot advancement for the common case where the cursor's
     * current chunk is present at the same index in the new snapshot
     * (e.g. append-only growth or heap-to-mmap swap of other chunks).
     * Compares epochs to detect structural changes; if the epoch at the
     * cursor's index is unchanged, the position is valid and the
     * expensive O(N) reposition scan can be skipped.
     *
     * @param newSnapshot freshly published snapshot from {@link Sequence#snapshot()}
     * @return {@code true} if the cursor position is valid in {@code newSnapshot}
     */
    private boolean tryAdvance(final ChunkSnapshot newSnapshot) {
        if (snapshot == null) {
            return false;
        }
        if (snapshot.isEmpty()) {
            return true;
        }
        final int checkIndex = Math.min(currentChunkIndex, snapshot.size() - 1);
        return checkIndex < newSnapshot.size()
                && snapshot.epoch(checkIndex) == newSnapshot.epoch(checkIndex);
    }

    private void forceRefresh() {
        CursorSupport.releasePin(pinState);
        snapshot = null;
        snapshotVersion = -1;
        peekDirty = true;
    }

    private boolean reposition(final ChunkSnapshot forSnapshot) {
        CursorSupport.releasePin(pinState);
        CursorSupport.releasePin(reposPin);

        try {
            for (int chunkIdx = 0; chunkIdx < forSnapshot.size(); chunkIdx++) {
                if (!CursorSupport.acquirePin(forSnapshot, chunkIdx, reposPin)) {
                    forceRefresh();
                    return false;
                }
                // See nextInternal: prefer the explicitly pinned chunk over a
                // second plain read of the spine.
                final Chunk chunk = reposPin.pinned;
                final int entryCount = chunk.getEntryCount();
                if (entryCount == 0) {
                    continue;
                }
                final long maxOrder = chunk.getMaxOrder();
                if (maxOrder < lastEntryOrder) {
                    continue;
                }

                int entryOff = Chunk.HEADER_SIZE;
                for (int entryIdx = 0; entryIdx < entryCount; entryIdx++) {
                    final long entryOrder = chunk.entryOrder(entryOff);
                    final long entryVersion = chunk.entryVersion(entryOff);
                    if (after(entryOrder, entryVersion)) {
                        currentChunkIndex = chunkIdx;
                        currentEntryIndex = entryIdx;
                        currentEntryOffset = entryOff;
                        CursorSupport.transferPin(reposPin, pinState);
                        return true;
                    }
                    entryOff += chunk.entryTotalSize(entryOff);
                }
            }
            currentChunkIndex = forSnapshot.size();
            currentEntryIndex = 0;
            currentEntryOffset = Chunk.HEADER_SIZE;
        } finally {
            CursorSupport.releasePin(reposPin);
        }
        return true;
    }

    /**
     * Releases all pinned resources held by this cursor. The cursor
     * should not be used after calling this method.
     */
    @Override
    public void close() {
        CursorSupport.releasePin(pinState);
        CursorSupport.releasePin(peekPin);
        CursorSupport.releasePin(reposPin);
    }
}
