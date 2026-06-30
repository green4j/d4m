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
 * A cursor that iterates over sequence entries in reverse (descending)
 * order. The cursor maintains its position across snapshot changes,
 * automatically repositioning when chunks are evicted or swapped.
 *
 * <p>Not thread-safe - intended for single-reader use.</p>
 */
public final class BackwardCursor {
    private final Sequence sequence;

    // Pre-allocated pin states (no allocation on hot path)
    private final CursorSupport.PinState pinState = new CursorSupport.PinState();
    private final CursorSupport.PinState peekPin = new CursorSupport.PinState();
    private final CursorSupport.PinState reposPin = new CursorSupport.PinState();

    private long[] peekTraverseIndex = new long[256];

    private long lastEntryOrder = Long.MAX_VALUE;
    private long lastEntryVersion = Long.MAX_VALUE;

    private int currentChunkIndex = -1;
    private int currentEntryIndex = -1;

    private ChunkSnapshot snapshot;
    private long snapshotVersion = -1;

    private long[] traverseIndex = new long[1024];
    private Chunk traverseIndexChunk;
    private int traverseIndexSize;

    private long seekOrder = Long.MAX_VALUE;
    private boolean seekActive;
    private boolean entryIndexNeedsResolve;

    private long cachedPeekOrder = Long.MIN_VALUE;
    private boolean peekDirty = true;

    /**
     * Creates a backward cursor over the given sequence.
     *
     * @param sequence the sequence to iterate
     */
    public BackwardCursor(final Sequence sequence) {
        this.sequence = sequence;
    }

    /**
     * Returns the sequence the cursor is wrapping.
     *
     * @return the sequence
     */
    public Sequence sequence() {
        return sequence;
    }

    /**
     * Resets the cursor to start iterating from the end of the sequence
     * (the most recent entry).
     */
    public void seekToEnd() {
        CursorSupport.releasePin(pinState);
        lastEntryOrder = Long.MAX_VALUE;
        lastEntryVersion = Long.MAX_VALUE;
        currentChunkIndex = -1;
        currentEntryIndex = -1;
        seekActive = false;
        entryIndexNeedsResolve = false;
        snapshot = null;
        snapshotVersion = -1;
        traverseIndexChunk = null;
        traverseIndexSize = 0;
        peekDirty = true;
    }

    /**
     * Positions the cursor to start iterating backward from the entry
     * with the given order (inclusive). Entries with orders greater than
     * {@code order} are skipped.
     *
     * @param order the order to seek to
     */
    public void seekTo(final long order) {
        CursorSupport.releasePin(pinState);
        lastEntryOrder = Long.MAX_VALUE;
        lastEntryVersion = Long.MAX_VALUE;
        currentChunkIndex = -1;
        currentEntryIndex = -1;
        seekOrder = order;
        seekActive = true;
        entryIndexNeedsResolve = false;
        snapshot = null;
        snapshotVersion = -1;
        traverseIndexChunk = null;
        traverseIndexSize = 0;
        peekDirty = true;
    }

    /**
     * Returns the order of the next entry that would be delivered by
     * {@link #next}, without advancing the cursor. Returns
     * {@link Long#MIN_VALUE} if no more entries are available.
     *
     * @return the next entry's order, or {@link Long#MIN_VALUE}
     */
    public long peekNextOrder() {
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
    public void invalidatePeek() {
        peekDirty = true;
    }

    private long computePeekOrder() {
        final ChunkSnapshot currentSnapshot = refresh();
        if (currentSnapshot.isEmpty() || currentChunkIndex < 0) {
            return Long.MIN_VALUE;
        }

        final int savedChunkIndex = currentChunkIndex;
        final int savedEntryIndex = currentEntryIndex;
        final boolean savedEntryIndexNeedsResolve = entryIndexNeedsResolve;

        int peekChunkIndex = currentChunkIndex;
        int peekEntryIndex = currentEntryIndex;
        boolean peekEntryIndexNeedsResolve = entryIndexNeedsResolve;

        try {
            while (peekChunkIndex >= 0) {
                if (!CursorSupport.acquirePin(currentSnapshot, peekChunkIndex, peekPin)) {
                    forceRefresh();
                    return Long.MIN_VALUE;
                }
                // Prefer the explicitly pinned chunk over a second plain read
                // of the spine, which a concurrent drainSwaps may have swapped.
                final Chunk chunk = peekPin.pinned;
                final int entryCount = chunk.getEntryCount();

                peekTraverseIndex = buildRawIndex(chunk, entryCount, peekTraverseIndex);

                if (peekEntryIndexNeedsResolve) {
                    peekEntryIndex = entryCount - 1;
                }

                while (peekEntryIndex >= 0) {
                    final int entryOff = (int) (peekTraverseIndex[peekEntryIndex] >>> 32);
                    final long entryOrder = chunk.entryOrder(entryOff);
                    final long entryVersion = chunk.entryVersion(entryOff);
                    if (seekActive && entryOrder > seekOrder) {
                        peekEntryIndex--;
                        continue;
                    }
                    if (!before(entryOrder, entryVersion)) {
                        peekEntryIndex--;
                        continue;
                    }
                    return entryOrder;
                }
                peekChunkIndex--;
                peekEntryIndexNeedsResolve = true;
            }
            return Long.MIN_VALUE;
        } finally {
            CursorSupport.releasePin(peekPin);
            currentChunkIndex = savedChunkIndex;
            currentEntryIndex = savedEntryIndex;
            entryIndexNeedsResolve = savedEntryIndexNeedsResolve;
        }
    }

    /**
     * Delivers up to {@code maxEntryCount} entries in reverse order to
     * the given consumer.
     *
     * @param maxEntryCount maximum number of entries to deliver
     * @param consumer      callback to receive each entry
     * @return the number of entries actually delivered
     */
    public int next(final int maxEntryCount,
                    final EntryConsumer consumer) {
        return nextInternal(maxEntryCount, Long.MIN_VALUE, consumer);
    }

    /**
     * Delivers up to {@code maxEntryCount} entries in reverse order,
     * stopping when the entry order drops below {@code orderLowerBound}.
     *
     * @param maxEntryCount   maximum number of entries to deliver
     * @param orderLowerBound inclusive lower bound on entry order
     * @param consumer        callback to receive each entry
     * @return the number of entries actually delivered
     */
    public int nextUntil(final int maxEntryCount,
                         final long orderLowerBound,
                         final EntryConsumer consumer) {
        return nextInternal(maxEntryCount, orderLowerBound, consumer);
    }

    private int nextInternal(final int maxEntryCount,
                             final long orderLowerBound,
                             final EntryConsumer consumer) {
        final ChunkSnapshot currentSnapshot = refresh();
        if (currentSnapshot.isEmpty() || currentChunkIndex < 0) {
            return 0;
        }
        int delivered = 0;

        while (delivered < maxEntryCount && currentChunkIndex >= 0) {
            if (!CursorSupport.acquirePin(currentSnapshot, currentChunkIndex, pinState)) {
                forceRefresh();
                if (delivered > 0) {
                    peekDirty = true;
                }
                return delivered;
            }
            // Prefer the explicitly pinned chunk over a second plain read of
            // the spine, which a concurrent drainSwaps may have swapped.
            final Chunk chunk = pinState.pinned;
            final int entryCount = chunk.getEntryCount();

            if (traverseIndexChunk == null || traverseIndexChunk.buffer() != chunk.buffer()) {
                buildIndex(chunk, entryCount);
            }
            if (entryIndexNeedsResolve) {
                currentEntryIndex = traverseIndexSize - 1;
                entryIndexNeedsResolve = false;
            }

            boolean hitBound = false;

            while (currentEntryIndex >= 0 && delivered < maxEntryCount) {
                final int entryOff = (int) (traverseIndex[currentEntryIndex] >>> 32);
                final long entryOrder = chunk.entryOrder(entryOff);
                final long entryVersion = chunk.entryVersion(entryOff);
                final int entryPayloadLen = chunk.entryPayloadSize(entryOff);

                if (seekActive && entryOrder > seekOrder) {
                    currentEntryIndex--;
                    continue;
                }
                seekActive = false;
                if (!before(entryOrder, entryVersion)) {
                    currentEntryIndex--;
                    continue;
                }
                if (entryOrder < orderLowerBound) {
                    hitBound = true;
                    break;
                }

                consumer.onEntry(
                        sequence,
                        entryOrder,
                        chunk.buffer(),
                        entryOff + Chunk.ENTRY_HEADER_SIZE,
                        entryPayloadLen
                );
                lastEntryOrder = entryOrder;
                lastEntryVersion = entryVersion;
                delivered++;
                currentEntryIndex--;
            }

            if (hitBound) {
                break;
            }

            if (currentEntryIndex < 0) {
                traverseIndexChunk = null;
                traverseIndexSize = 0;
                currentChunkIndex--;
                entryIndexNeedsResolve = true;
            } else {
                break;
            }
        }
        if (delivered > 0) {
            peekDirty = true;
        }
        return delivered;
    }

    private boolean before(final long order,
                           final long version) {
        return order < lastEntryOrder
                || (order == lastEntryOrder && version < lastEntryVersion);
    }

    private ChunkSnapshot refresh() {
        final ChunkSnapshot newSnapshot = sequence.snapshot();
        if (newSnapshot.version() != snapshotVersion) {
            if (lastEntryOrder != Long.MAX_VALUE) {
                if (!tryAdvance(newSnapshot)) {
                    reposition(newSnapshot);
                    if (snapshotVersion == -1) {
                        return snapshot != null ? snapshot : ChunkSnapshot.EMPTY;
                    }
                }
            } else {
                posEnd(newSnapshot);
            }
            snapshot = newSnapshot;
            snapshotVersion = newSnapshot.version();
        }
        return snapshot != null ? snapshot : ChunkSnapshot.EMPTY;
    }

    /**
     * Fast-path snapshot advancement for the common case where the cursor's
     * current chunk is present at the same index in the new snapshot.
     * Mirrors.
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
        final int checkIndex;
        if (currentChunkIndex >= 0 && currentChunkIndex < snapshot.size()) {
            checkIndex = currentChunkIndex;
        } else if (currentChunkIndex < 0) {
            checkIndex = 0;
        } else {
            checkIndex = snapshot.size() - 1;
        }
        return checkIndex < newSnapshot.size()
                && snapshot.epoch(checkIndex) == newSnapshot.epoch(checkIndex);
    }

    private void forceRefresh() {
        CursorSupport.releasePin(pinState);
        snapshot = null;
        snapshotVersion = -1;
        traverseIndexChunk = null;
        traverseIndexSize = 0;
        entryIndexNeedsResolve = false;
        peekDirty = true;
    }

    private void posEnd(final ChunkSnapshot forSnapshot) {
        if (!forSnapshot.isEmpty()) {
            currentChunkIndex = forSnapshot.size() - 1;
            entryIndexNeedsResolve = true;
        } else {
            currentChunkIndex = -1;
            currentEntryIndex = -1;
            entryIndexNeedsResolve = false;
        }
    }

    private void reposition(final ChunkSnapshot forSnapshot) {
        CursorSupport.releasePin(pinState);
        CursorSupport.releasePin(reposPin);

        traverseIndexChunk = null;
        traverseIndexSize = 0;
        entryIndexNeedsResolve = false;

        try {
            for (int chunkIdx = forSnapshot.size() - 1; chunkIdx >= 0; chunkIdx--) {
                if (!CursorSupport.acquirePin(forSnapshot, chunkIdx, reposPin)) {
                    forceRefresh();
                    return;
                }
                // Prefer the explicitly pinned chunk over a second plain read
                // of the spine.
                final Chunk chunk = reposPin.pinned;
                final int entryCount = chunk.getEntryCount();
                if (entryCount == 0) {
                    continue;
                }
                final long minOrder = chunk.getMinOrder();
                if (minOrder > lastEntryOrder) {
                    continue;
                }

                int entryOffset = Chunk.HEADER_SIZE;
                int last = -1;
                for (int entryIdx = 0; entryIdx < entryCount; entryIdx++) {
                    final long entryOrder = chunk.entryOrder(entryOffset);
                    final long entryVersion = chunk.entryVersion(entryOffset);
                    if (before(entryOrder, entryVersion)) {
                        last = entryIdx;
                    }
                    entryOffset += chunk.entryTotalSize(entryOffset);
                }
                if (last >= 0) {
                    currentChunkIndex = chunkIdx;
                    currentEntryIndex = last;
                    CursorSupport.transferPin(reposPin, pinState);
                    return;
                }
            }
            currentChunkIndex = -1;
            currentEntryIndex = -1;
        } finally {
            CursorSupport.releasePin(reposPin);
        }
    }

    private void buildIndex(final Chunk chunk,
                            final int entryCount) {
        if (entryCount > traverseIndex.length) {
            traverseIndex = new long[entryCount + (entryCount >> 1)];
        }
        int entryOff = Chunk.HEADER_SIZE;
        for (int entryIdx = 0; entryIdx < entryCount; entryIdx++) {
            final int entryTotalSize = chunk.entryTotalSize(entryOff);
            traverseIndex[entryIdx] = ((long) entryOff << 32) | (entryTotalSize & 0xFFFF_FFFFL);
            entryOff += entryTotalSize;
        }
        traverseIndexChunk = chunk;
        traverseIndexSize = entryCount;
    }

    private static long[] buildRawIndex(final Chunk chunk,
                                        final int entryCount,
                                        final long[] scratchIn) {
        long[] scratch = scratchIn;
        if (entryCount > scratch.length) {
            scratch = new long[entryCount + (entryCount >> 1)];
        }
        int entryOffset = Chunk.HEADER_SIZE;
        for (int entryIdx = 0; entryIdx < entryCount; entryIdx++) {
            final int entryTotalSize = chunk.entryTotalSize(entryOffset);
            scratch[entryIdx] = ((long) entryOffset << 32) | (entryTotalSize & 0xFFFF_FFFFL);
            entryOffset += entryTotalSize;
        }
        return scratch;
    }

    /**
     * Releases all pinned resources held by this cursor. The cursor
     * should not be used after calling this method.
     */
    public void close() {
        CursorSupport.releasePin(pinState);
        CursorSupport.releasePin(peekPin);
        CursorSupport.releasePin(reposPin);
    }
}
