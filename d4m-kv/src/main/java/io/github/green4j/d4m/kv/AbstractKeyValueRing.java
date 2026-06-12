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
package io.github.green4j.d4m.kv;

import io.github.green4j.d4m.common.AtomicBuffer;
import io.github.green4j.d4m.common.BitSupport;

/**
 * Common state and structural behavior for a {@link KeyValues} that distributes
 * key-value pairs across a ring of {@link KeyValueSegment}s using key hashing.
 * Segments are shuffled in the internal array to spread adjacent hash values
 * across distinct segment instances.
 *
 * <p>Subclasses implement {@link #put} and {@link #get} and choose their own
 * synchronization policy: {@link KeyValueRing} acquires a per-segment lock,
 * while {@link SingleThreadedKeyValueRing} relies on the caller for single-thread
 * access.
 */
public abstract class AbstractKeyValueRing implements KeyValueSegments, KeyValues {
    protected final KeyValueSegment[] segments;
    protected final int numberOfSegments;

    /**
     * Builds the shuffled segment array. Both {@code size} and {@code shuffleMultiplier}
     * are rounded up to the next power of two; the internal array has length
     * {@code normSize * normShuffleMultiplier}, with each distinct segment instance
     * repeated at positions {@code i, i + normSize, i + 2*normSize, ...}.
     *
     * @param size              the desired number of distinct segments
     * @param shuffleMultiplier the multiplier for the internal segment array
     * @param factory           the factory used to create each segment
     */
    protected AbstractKeyValueRing(final int size,
                                   final int shuffleMultiplier,
                                   final SegmentFactory factory) {
        final int normSize = BitSupport.nextPowerOfTwo(size);
        final int normShuffleMultiplier = BitSupport.nextPowerOfTwo(shuffleMultiplier);

        segments = new KeyValueSegment[normSize * normShuffleMultiplier];
        for (int i = 0; i < normSize; i++) {
            final KeyValueSegment segment = factory.next(i);
            for (int j = 0; j < normShuffleMultiplier; j++) {
                segments[i + (j * normSize)] = segment;
            }
        }

        numberOfSegments = normSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int numberOfSegments() {
        return numberOfSegments;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final KeyValueSegment getSegment(final int index) {
        return segments[index];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final KeyValueSegment[] segments() {
        return segments;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int size() {
        return segments.length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract void put(AtomicBuffer key,
                             int keyOffset,
                             int keySize,
                             AtomicBuffer value,
                             int valueOffset,
                             int valueSize);

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract boolean get(AtomicBuffer key,
                                int keyOffset,
                                int keySize,
                                KeyValueConsuming.ValueConsumer<KeyValueConsuming.Value> consumer);

    /**
     * {@inheritDoc}
     */
    @Override
    public final void compute(final AtomicBuffer key,
                              final int keyOffset,
                              final int keySize,
                              final ComputeAction action) {
        final int hash = KeyValueSupport.hash(key, keyOffset, keySize);
        final int segmentIndex = hash & (segments.length - 1);
        action.context().bind(segments[segmentIndex], hash, key, keyOffset, keySize);
        runUnderSegmentLock(segmentIndex, action);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void compute(final AtomicBuffer key1,
                              final int key1Offset,
                              final int key1Size,
                              final AtomicBuffer key2,
                              final int key2Offset,
                              final int key2Size,
                              final TwoKeyComputeAction action) {
        final int hash1 = KeyValueSupport.hash(key1, key1Offset, key1Size);
        final int hash2 = KeyValueSupport.hash(key2, key2Offset, key2Size);
        final int seg1 = hash1 & (segments.length - 1);
        final int seg2 = hash2 & (segments.length - 1);
        action.key1Context().bind(segments[seg1], hash1, key1, key1Offset, key1Size);
        action.key2Context().bind(segments[seg2], hash2, key2, key2Offset, key2Size);
        // Canonical segment indices are 0..numberOfSegments-1; two shuffled
        // indices that map to the same underlying segment share their
        // canonical index (and lock). Sorting canonical indices keeps the
        // lock-acquisition order globally consistent and avoids re-acquiring
        // the same non-reentrant lock.
        final int canon1 = seg1 % numberOfSegments;
        final int canon2 = seg2 % numberOfSegments;
        final int firstIdx = Math.min(canon1, canon2);
        final int secondIdx = Math.max(canon1, canon2);
        runUnderTwoSegmentLocks(firstIdx, secondIdx, action);
    }

    /**
     * Runs the given action's {@link ComputeAction#execute()} under any
     * locking the subclass associates with the segment at {@code segmentIndex}.
     *
     * @param segmentIndex the index of the distinct segment whose lock applies
     * @param action       the action whose {@code execute()} to invoke
     */
    protected abstract void runUnderSegmentLock(int segmentIndex, ComputeAction action);

    /**
     * Runs the given action's {@link TwoKeyComputeAction#execute()} under any
     * locking the subclass associates with the two segments at the given
     * indices. The indices are guaranteed to satisfy {@code firstIdx &lt;= secondIdx}
     * - subclasses must acquire the {@code firstIdx} lock first to keep the
     * order canonical and deadlock-free. When {@code firstIdx == secondIdx},
     * only one lock is needed.
     *
     * @param firstIdx  the lower segment index (acquire first)
     * @param secondIdx the higher segment index
     * @param action    the action whose {@code execute()} to invoke
     */
    protected abstract void runUnderTwoSegmentLocks(int firstIdx,
                                                    int secondIdx,
                                                    TwoKeyComputeAction action);
}
