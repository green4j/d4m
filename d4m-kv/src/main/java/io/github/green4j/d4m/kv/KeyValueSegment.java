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

import java.util.concurrent.locks.StampedLock;

/**
 * A segment within a {@link KeyValueRing} that manages a chain of {@link Tier}s.
 * When the hottest tier fills up, evicted entries cascade into cooler tiers, which
 * are created on demand by the configured {@link TierFactory}.
 */
public class KeyValueSegment {
    private final StampedLock lock = new StampedLock();

    private final TierFactory factory;
    private final EvictionListener evictionListener;
    private final EvictionListener tierLinker;

    private Tier[] tiers;
    private int size;

    /**
     * Creates a segment that pre-allocates the given number of tiers on construction.
     *
     * @param startSize        the number of tiers to create immediately
     * @param factory          the factory used to create new tiers
     * @param evictionListener listener notified when data is evicted from the coldest tier,
     *                         or {@code null} if evicted data can be silently dropped
     */
    public KeyValueSegment(final int startSize,
                           final TierFactory factory,
                           final EvictionListener evictionListener) {
        this.factory = factory;
        this.evictionListener = evictionListener;

        tierLinker = (
                notifier,
                hash,
                keyValue,
                keyOffset,
                keySize,
                valueOffset,
                valueSize
        ) -> {
            assert size > 0;

            switch (size) {
                case 1: {
                    tryToPutToNewTier(
                            hash,
                            keyValue,
                            keyOffset,
                            keySize,
                            valueOffset,
                            valueSize
                    );
                    break;
                }
                case 2: {
                    if (notifier == tiers[0]) {
                        tiers[1].put(
                                hash,
                                keyValue,
                                keyOffset,
                                keySize,
                                valueOffset,
                                valueSize
                        );
                        return;
                    }
                    if (notifier == tiers[1]) {
                        tryToPutToNewTier(
                                hash,
                                keyValue,
                                keyOffset,
                                keySize,
                                valueOffset,
                                valueSize
                        );
                    }
                    break;
                }
                default:
                    for (int i = 0; i < size - 1; i++) {
                        final Tier tier = tiers[i];
                        if (notifier == tier) {
                            final Tier nextTier = tiers[i + 1];
                            nextTier.put(
                                    hash,
                                    keyValue,
                                    keyOffset,
                                    keySize,
                                    valueOffset,
                                    valueSize
                            );
                            return;
                        }
                    }
                    tryToPutToNewTier(
                            hash,
                            keyValue,
                            keyOffset,
                            keySize,
                            valueOffset,
                            valueSize
                    );
                    break;
            }
        };

        tiers = new Tier[startSize];

        size = 0;

        for (int i = 0; i < startSize; i++) {
            try {
                tiers[i] = factory.next(tiers, size, tierLinker);
            } catch (final RuntimeException e) {
                throw e;
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
            size++;
        }
    }

    /**
     * Returns the stamp-based lock used to synchronize access to this segment.
     * This lock is not reentrant; do not acquire it again on the same thread
     * while already holding a read or write stamp.
     *
     * @return the lock
     */
    public StampedLock lock() {
        return lock;
    }

    /**
     * Inserts or updates a key-value pair in the hottest (first) tier.
     * If no tiers exist yet, one is created on demand.
     *
     * @param hash        the pre-computed hash of the key
     * @param key         the buffer containing the key
     * @param keyOffset   the offset of the key within the buffer
     * @param keySize     the size of the key in bytes
     * @param value       the buffer containing the value
     * @param valueOffset the offset of the value within the buffer
     * @param valueSize   the size of the value in bytes
     */
    public void put(final int hash,
                    final AtomicBuffer key,
                    final int keyOffset,
                    final int keySize,
                    final AtomicBuffer value,
                    final int valueOffset,
                    final int valueSize) {
        if (size == 0) {
            final Tier firstTier = addTier();
            if (firstTier == null) { // no more Tiers, we loose the data
                return;
            }
            firstTier.put(
                    hash,
                    key,
                    keyOffset,
                    keySize,
                    value,
                    valueOffset,
                    valueSize
            );
            return;
        }
        // Put to the head (hottest tier)
        tiers[0].put(
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
     * Searches all tiers (from hottest to coldest) for a value matching the given key.
     *
     * @param hash      the pre-computed hash of the key
     * @param key       the buffer containing the key
     * @param keyOffset the offset of the key within the buffer
     * @param keySize   the size of the key in bytes
     * @param consumer  the consumer that will receive the value if found
     * @return {@code true} if the key was found in any tier
     */
    public boolean get(final int hash,
                       final AtomicBuffer key,
                       final int keyOffset,
                       final int keySize,
                       final KeyValueConsuming.ValueConsumer<KeyValueConsuming.Value> consumer) {
        switch (size) {
            case 0:
                return false;
            case 1: {
                return tiers[0].get(
                        hash,
                        key,
                        keyOffset,
                        keySize,
                        consumer
                );
            }
            case 2: {
                if (tiers[0].get(
                        hash,
                        key,
                        keyOffset,
                        keySize,
                        consumer
                )) {
                    return true;
                }
                if (tiers[1].get(
                        hash,
                        key,
                        keyOffset,
                        keySize,
                        consumer
                )) {
                    return true;
                }
                return false;
            }
            default: {
                for (int i = 0; i < size; i++) {
                    if (tiers[i].get(
                            hash,
                            key,
                            keyOffset,
                            keySize,
                            consumer
                    )) {
                        return true;
                    }
                }
                break;
            }
        }
        return false;
    }

    /**
     * Returns the number of tiers currently allocated in this segment.
     *
     * @return the tier count
     */
    public int size() {
        return size;
    }

    /**
     * Returns the tier at the given index, where index 0 is the hottest tier.
     *
     * @param index the tier index
     * @return the tier
     */
    public Tier getTier(final int index) {
        return tiers[index];
    }

    private void tryToPutToNewTier(final int hash,
                                   final AtomicBuffer keyValue,
                                   final int keyOffset,
                                   final int keySize,
                                   final int valueOffset,
                                   final int valueSize) {
        final Tier nextTier = addTier();
        if (nextTier == null) { // no more Tiers, we loose the data
            if (evictionListener != null) {
                evictionListener.onEviction(
                        this,
                        hash,
                        keyValue,
                        keyOffset,
                        keySize,
                        valueOffset,
                        valueSize
                );
            }
            return;
        }
        nextTier.put(
                hash,
                keyValue,
                keyOffset,
                keySize,
                valueOffset,
                valueSize
        );
    }

    private Tier addTier() {
        final Tier result;
        try {
            result = factory.next(
                    tiers,
                    size,
                    tierLinker
            );
        } catch (final RuntimeException e) {
            throw e;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        if (result == null) { // there is a decision to don't save evicted data
            return null;
        }

        if (tiers.length == size) {
            final int newLength = Math.max(1, tiers.length << 1);
            final Tier[] newTiers = new Tier[newLength];
            System.arraycopy(tiers, 0, newTiers, 0, size);
            tiers = newTiers;
        }

        tiers[size++] = result;

        return result;
    }
}
