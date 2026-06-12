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

/**
 * A lock-free {@link AbstractKeyValueRing} intended for single-threaded use.
 * All {@link #put} and {@link #get} calls must be made by the same thread;
 * concurrent access has undefined behavior. Avoids the per-segment lock
 * overhead of {@link KeyValueRing}.
 */
public class SingleThreadedKeyValueRing extends AbstractKeyValueRing {

    /**
     * Creates a ring with the specified number of segments and a default shuffle multiplier of 16.
     *
     * @param size    the desired number of distinct segments (rounded up to power of two)
     * @param factory the factory used to create each segment
     */
    public SingleThreadedKeyValueRing(final int size,
                                      final SegmentFactory factory) {
        this(
                size,
                16,
                factory
        );
    }

    /**
     * Creates a ring with the specified number of segments and shuffle multiplier.
     *
     * @param size              the desired number of distinct segments (rounded up to power of two)
     * @param shuffleMultiplier the multiplier for the internal segment array (rounded up to power of two)
     * @param factory           the factory used to create each segment
     */
    public SingleThreadedKeyValueRing(final int size,
                                      final int shuffleMultiplier,
                                      final SegmentFactory factory) {
        super(size, shuffleMultiplier, factory);
    }

    /**
     * {@inheritDoc}
     */
    @Override
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
        segments[segmentIndex].put(
                hash,
                key,
                keyOffset,
                keySize,
                value,
                valueOffset,
                valueSize
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
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
        return segments[segmentIndex].get(
                hash,
                key,
                keyOffset,
                keySize,
                consumer
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void runUnderSegmentLock(final int segmentIndex,
                                       final ComputeAction action) {
        action.execute();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void runUnderTwoSegmentLocks(final int firstIdx,
                                           final int secondIdx,
                                           final TwoKeyComputeAction action) {
        action.execute();
    }
}
