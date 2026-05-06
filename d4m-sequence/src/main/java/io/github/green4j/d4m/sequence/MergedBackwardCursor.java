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
 * Merges multiple {@link BackwardCursor} instances into a single
 * reverse-ordered stream using a max-heap. At each step, the entry
 * with the largest order among all sources is delivered first.
 *
 * <p>Not thread-safe - intended for single-reader use.</p>
 */
public final class MergedBackwardCursor {
    private static final long EXHAUSTED = Long.MIN_VALUE;

    private final BackwardCursor[] cursors;
    private final long[] peekTs;
    private final int[] heap;
    private int heapSize;

    private MergedEntryConsumer activeConsumer;
    private final EntryConsumer[] wrappers;

    /**
     * Creates a merged backward cursor over the given individual cursors.
     *
     * @param cursors the backward cursors to merge
     */
    public MergedBackwardCursor(final BackwardCursor... cursors) {
        this.cursors = cursors.clone();
        peekTs = new long[cursors.length];
        heap = new int[cursors.length];
        wrappers = new EntryConsumer[cursors.length];
        for (int i = 0; i < cursors.length; i++) {
            final int idx = i;
            wrappers[i] = (owner,
                           order,
                           buffer,
                           offset,
                           size) ->
                    activeConsumer.onEntry(idx,
                            owner,
                            order,
                            buffer,
                            offset,
                            size);
        }
        buildHeap();
    }

    /**
     * Convenience factory that creates backward cursors for each sequence
     * and wraps them in a merged cursor.
     *
     * @param series the sequences to merge
     * @return a new merged backward cursor
     */
    public static MergedBackwardCursor create(final Sequence... series) {
        final BackwardCursor[] c = new BackwardCursor[series.length];
        for (int i = 0; i < series.length; i++) {
            c[i] = new BackwardCursor(series[i]);
        }
        return new MergedBackwardCursor(c);
    }

    /**
     * Positions all underlying cursors to start iterating backward from
     * the given order and rebuilds the merge heap.
     *
     * @param ts the order to seek to
     */
    public void seekTo(final long ts) {
        for (final BackwardCursor c : cursors) {
            c.seekTo(ts);
        }
        buildHeap();
    }

    /**
     * Resets all underlying cursors to the end of their sequences and
     * rebuilds the merge heap.
     */
    public void seekToEnd() {
        for (final BackwardCursor c : cursors) {
            c.seekToEnd();
        }
        buildHeap();
    }

    /**
     * Returns the order of the next entry that would be delivered,
     * or {@link Long#MIN_VALUE} if all sources are exhausted.
     *
     * @return the next entry's order, or {@link Long#MIN_VALUE}
     */
    public long peekNextOrder() {
        return heapSize > 0 ? peekTs[heap[0]] : EXHAUSTED;
    }

    /**
     * Delivers up to {@code max} entries in merged reverse order to
     * a plain consumer (without source index information).
     *
     * @param max      maximum number of entries to deliver
     * @param consumer callback to receive each entry
     * @return the number of entries actually delivered
     */
    public int next(final int max, final EntryConsumer consumer) {
        return deliverLoop(max, consumer, false);
    }

    /**
     * Delivers up to {@code max} entries in merged reverse order to
     * a merged consumer that receives the source index of each entry.
     *
     * @param max      maximum number of entries to deliver
     * @param consumer callback to receive each entry with its source index
     * @return the number of entries actually delivered
     */
    public int next(final int max, final MergedEntryConsumer consumer) {
        activeConsumer = consumer;
        final int n = deliverLoop(max, null, true);
        activeConsumer = null;
        return n;
    }

    /**
     * Invalidates and recomputes peek values for any exhausted sources,
     * allowing them to rejoin the merge if new data has arrived.
     */
    public void refreshPeeks() {
        boolean changed = false;
        for (int i = 0; i < cursors.length; i++) {
            if (peekTs[i] == EXHAUSTED) {
                cursors[i].invalidatePeek();
                final long order = cursors[i].peekNextOrder();
                if (order != EXHAUSTED) {
                    peekTs[i] = order;
                    changed = true;
                }
            }
        }
        if (changed) {
            rebuildHeap();
        }
    }

    /**
     * Returns the number of source cursors in this merged cursor.
     *
     * @return the number of underlying cursors
     */
    public int width() {
        return cursors.length;
    }

    /**
     * Returns the underlying backward cursor at the given index.
     *
     * @param i index of the cursor
     * @return the backward cursor at that index
     */
    public BackwardCursor cursor(final int i) {
        return cursors[i];
    }

    /**
     * Closes all underlying cursors, releasing their pinned resources.
     */
    public void close() {
        for (final BackwardCursor c : cursors) {
            c.close();
        }
    }

    private int deliverLoop(final int maxEntryCount,
                            final EntryConsumer plain,
                            final boolean useMerged) {
        int delivered = 0;
        while (delivered < maxEntryCount && heapSize > 0) {
            final int chunkIndex = heap[0];
            if (peekTs[chunkIndex] == EXHAUSTED) {
                break;
            }

            final EntryConsumer target = useMerged ? wrappers[chunkIndex] : plain;

            long nextPeerTs = EXHAUSTED;
            if (heapSize > 1) {
                nextPeerTs = peekTs[heap[1]];
            }
            if (heapSize > 2) {
                nextPeerTs = Math.max(nextPeerTs, peekTs[heap[2]]);
            }

            final int n;
            if (peekTs[chunkIndex] > nextPeerTs) {
                n = cursors[chunkIndex].nextUntil(maxEntryCount - delivered, nextPeerTs, target);
            } else {
                n = cursors[chunkIndex].next(1, target);
            }

            final long newPeek = cursors[chunkIndex].peekNextOrder();
            peekTs[chunkIndex] = newPeek;

            if (n == 0) {
                if (newPeek == EXHAUSTED) {
                    removeTop();
                } else {
                    break;
                }
            } else {
                delivered += n;
                siftDown(0);
            }
        }
        return delivered;
    }

    private void buildHeap() {
        heapSize = 0;
        for (int i = 0; i < cursors.length; i++) {
            peekTs[i] = cursors[i].peekNextOrder();
            if (peekTs[i] != EXHAUSTED) {
                heap[heapSize++] = i;
            }
        }
        for (int i = (heapSize >>> 1) - 1; i >= 0; i--) {
            siftDown(i);
        }
    }

    private void rebuildHeap() {
        heapSize = 0;
        for (int i = 0; i < cursors.length; i++) {
            if (peekTs[i] != EXHAUSTED) {
                heap[heapSize++] = i;
            }
        }
        for (int i = (heapSize >>> 1) - 1; i >= 0; i--) {
            siftDown(i);
        }
    }

    private void removeTop() {
        if (heapSize <= 1) {
            heapSize = 0;
            return;
        }
        heap[0] = heap[--heapSize];
        siftDown(0);
    }

    private void siftDown(final int startPos) {
        int pos = startPos;
        while (true) {
            int best = pos;
            final int l = (pos << 1) + 1, r = (pos << 1) + 2;
            if (l < heapSize && greater(heap[l], heap[best])) {
                best = l;
            }
            if (r < heapSize && greater(heap[r], heap[best])) {
                best = r;
            }
            if (best == pos) {
                break;
            }
            final int tmp = heap[pos];
            heap[pos] = heap[best];
            heap[best] = tmp;
            pos = best;
        }
    }

    private boolean greater(final int a,
                            final int b) {
        final long ta = peekTs[a], tb = peekTs[b];
        return ta > tb || (ta == tb && a > b);
    }
}
