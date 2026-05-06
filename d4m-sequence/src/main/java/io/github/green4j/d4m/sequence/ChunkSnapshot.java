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
 * Immutable-once-published snapshot of the chunk list.
 *
 * <p>Uses a <b>segmented spine</b> - an array of fixed-size arrays
 * ({@code Chunk[][]} + {@code long[][]}) - instead of flat arrays.
 * This eliminates bulk copies when the writer appends new chunks:
 * only the small spine pointer array is ever copied (when it needs
 * to grow), while existing segment arrays are shared as-is.</p>
 *
 * <p>A {@code headOffset} translates logical indices (0-based, as seen
 * by cursors) to physical positions within the spine.  When the writer
 * compacts dead leading chunks, it advances the head offset so that
 * readers automatically skip retired entries.  Old snapshots keep their
 * own head offset, so readers holding stale references remain safe.</p>
 *
 * <p>Safety: the writer only ever appends beyond the previous snapshot's
 * logical length, so readers of an older snapshot never observe a torn
 * write within their valid index range.  COW paths allocate new segments
 * for the affected region.  Swap (heap-to-mmap) paths modify segment
 * elements in-place - see {@code Sequence.drainSwaps()} for the safety
 * argument.</p>
 *
 * <p><b>JMM note:</b> all instance fields are {@code final}, which
 * guarantees that any thread that obtains a reference to a
 * {@code ChunkSnapshot} (even via a data race) will see the fully
 * constructed object.  In practice, snapshots are always published
 * through the {@code volatile} field {@code Sequence.snapshot},
 * providing an additional happens-before edge.</p>
 */
public final class ChunkSnapshot {
    static final int SEG_SHIFT = 7;
    static final int SEG_SIZE = 1 << SEG_SHIFT; // 128
    static final int SEG_MASK = SEG_SIZE - 1;

    public static final ChunkSnapshot EMPTY =
            new ChunkSnapshot(new Chunk[0][], new long[0][], 0, 0, 0);

    private final Chunk[][] chunkSpine;
    private final long[][] epochSpine;
    private final int size;
    private final int headOffset;
    private final long version;

    /**
     * Primary constructor - stores spine arrays directly (zero copy).
     * Package-private; used by {@link Sequence} on the writer path.
     *
     * @param chunkSpine segmented chunk array
     * @param epochSpine segmented epoch array
     * @param size       number of <em>live</em> chunks (logical size)
     * @param headOffset number of dead/compacted elements at the front
     *                   of the spine that logical index 0 must skip
     * @param version    monotonically increasing snapshot version
     */
    ChunkSnapshot(final Chunk[][] chunkSpine,
                  final long[][] epochSpine,
                  final int size,
                  final int headOffset,
                  final long version) {
        this.chunkSpine = chunkSpine;
        this.epochSpine = epochSpine;
        this.size = size;
        this.headOffset = headOffset;
        this.version = version;
    }

    /**
     * Returns the number of live chunks in this snapshot.
     *
     * @return logical chunk count
     */
    public int size() {
        return size;
    }

    /**
     * Returns {@code true} if this snapshot contains no chunks.
     *
     * @return whether the snapshot is empty
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Returns the chunk at the given logical index.
     *
     * @param i logical index (0-based)
     * @return the chunk at that index
     */
    public Chunk chunk(final int i) {
        final int p = i + headOffset;
        return chunkSpine[p >> SEG_SHIFT][p & SEG_MASK];
    }

    /**
     * Returns the epoch of the chunk at the given logical index.
     *
     * @param i logical index (0-based)
     * @return the epoch recorded for that chunk
     */
    public long epoch(final int i) {
        final int p = i + headOffset;
        return epochSpine[p >> SEG_SHIFT][p & SEG_MASK];
    }

    /**
     * Returns the monotonically increasing version of this snapshot.
     *
     * @return snapshot version
     */
    public long version() {
        return version;
    }
}
