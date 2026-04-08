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

import java.util.concurrent.locks.StampedLock;

/**
 * A thread-safe {@link KeyValues} that distributes key-value pairs across
 * a ring of {@link KeyValueSegment}s using key hashing. Segments are shuffled
 * in the internal array to reduce contention for adjacent hash values.
 */
public class KeyValueRing implements KeyValues {
    private final KeyValueSegment[] segments;
    private final int numberOfSegments;

    /**
     * Creates a ring with the specified number of segments and a default shuffle multiplier of 16.
     *
     * @param size the desired number of distinct segments (rounded up to power of two)
     * @param factory the factory used to create each segment
     */
    public KeyValueRing(final int size,
                        final SegmentFactory factory) {
        this(
                size,
                16,
                factory
        );
    }

    /**
     * Creates a ring with the specified number of segments and shuffle multiplier.
     * The shuffle multiplier controls the internal array size to distribute hash
     * collisions across segments more evenly.
     *
     * @param size the desired number of distinct segments (rounded up to power of two)
     * @param shuffleMultiplier the multiplier for the internal segment array (rounded up to power of two)
     * @param factory the factory used to create each segment
     */
    public KeyValueRing(final int size,
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
    public void put(final AtomicBuffer key,
                    final int keyOffset,
                    final int keySize,
                    final AtomicBuffer value,
                    final int valueOffset,
                    final int valueSize) {
        final int hash = KeyValueSupport.hash(
                key,
                keyOffset,
                keySize
        );

        assert hash > 0;

        final int segmentIndex = hash & (segments.length - 1);
        final KeyValueSegment segment = segments[segmentIndex];

        final StampedLock segmentLock = segment.lock();
        final long stamp = segmentLock.writeLock();
        try {
            segment.put(
                    hash,
                    key,
                    keyOffset,
                    keySize,
                    value,
                    valueOffset,
                    valueSize
            );
        } finally {
            segmentLock.unlockWrite(stamp);
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean get(final AtomicBuffer key,
                       final int keyOffset,
                       final int keySize,
                       final KeyValueConsuming.ValueConsumer<KeyValueConsuming.Value> consumer) {
        final int hash = KeyValueSupport.hash(
                key,
                keyOffset,
                keySize
        );

        assert hash > 0;

        final int segmentIndex = hash & (segments.length - 1);
        final KeyValueSegment segment = this.segments[segmentIndex];

        final StampedLock segmentLock = segment.lock();
        final long stamp = segmentLock.readLock();
        try {
            return segment.get(
                    hash,
                    key,
                    keyOffset,
                    keySize,
                    consumer
            );
        } finally {
            segmentLock.unlockRead(stamp);
        }
    }

    /**
     * Returns the number of distinct segments in this ring.
     *
     * @return the number of segments
     */
    public int numberOfSegments() {
        return numberOfSegments;
    }

    /**
     * Returns the segment at the given index in the internal shuffled array.
     *
     * @param index the index into the shuffled segment array
     * @return the segment at the given index
     */
    public KeyValueSegment getSegment(final int index) {
        return segments[index];
    }

    /**
     * Returns the internal shuffled segment array.
     *
     * @return the segment array
     */
    public KeyValueSegment[] segments() {
        return segments;
    }

    /**
     * Returns the total size of the internal shuffled segment array,
     * which equals {@code numberOfSegments * shuffleMultiplier}.
     *
     * @return the total array size
     */
    public int size() {
        return segments.length;
    }
}
