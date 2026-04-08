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

import static io.github.green4j.d4m.common.BitSupport.INT_BITS;

/**
 * An open-addressing hash map tier that uses linear probing for collision resolution.
 * Keys and values are stored in a {@link KeyValueBuffer}; metadata slots track
 * occupancy, hash, and buffer index. When the load factor is exceeded, the metadata
 * array is expanded.
 */
public class Tier {
    private static final double LOAD_FACTOR = 0.75;

    private final KeyValueBuffer kvBuffer;
    // Metadata array: each long contains [occupied(1) | hash(31) | index(32)]
    private long[] metadata;

    private int capacity;
    private int size;

    private static final long OCCUPIED_MASK = 1L << 63; // just the sign bit
    private static final long HASH_MASK = 0x7FFFFFFFL;
    private static final long INDEX_MASK = 0xFFFFFFFFL;

    /**
     * Creates a tier with the specified initial metadata capacity and binary buffer,
     * without an eviction listener.
     *
     * @param initialCapacity the initial number of metadata slots (rounded up to power of two)
     * @param kvBuffer the buffer used to store key-value binary data
     */
    public Tier(final int initialCapacity,
                final AtomicBuffer kvBuffer) {
        this(
                initialCapacity,
                kvBuffer,
                null
        );
    }

    /**
     * Creates a tier with the specified initial metadata capacity, binary buffer,
     * and an optional eviction listener.
     *
     * @param initialCapacity the initial number of metadata slots (rounded up to power of two)
     * @param kvBuffer the buffer used to store key-value binary data
     * @param evictionListener listener notified when entries are evicted, or {@code null}
     */
    public Tier(final int initialCapacity,
                final AtomicBuffer kvBuffer,
                final EvictionListener evictionListener) {
        this.kvBuffer = new KeyValueBuffer(
                kvBuffer,
                new KeyValueBuffer.Listener() {
                    @Override
                    public void onAfterInserted(final int index,
                                                final int slotIndex,
                                                final int keySize,
                                                final int valueSize) {
                    }

                    @Override
                    public void onAfterUpdated(final int index,
                                               final int slotIndex,
                                               final int keySize,
                                               final int valueSize) {
                    }

                    @Override
                    public void onBeforeEvicted(final int index,
                                                final int slotIndex,
                                                final int keySize,
                                                final int valueSize) {
                        final long m = metadata[slotIndex];

                        final int hash = unpackHash(m);

                        final KeyValueBuffer buffer = Tier.this.kvBuffer;
                        final int keyOffset = buffer.keyOffset(index);

                        if (evictionListener != null) {
                            evictionListener.onEviction(
                                    Tier.this,
                                    hash,
                                    buffer.buffer(),
                                    keyOffset,
                                    keySize,
                                    keyOffset + keySize,
                                    valueSize
                            );
                        }

                        // Free slot then...
                        if (!unpackOccupied(m)) {
                            throw new IllegalStateException("Not occupied slot: " + slotIndex);
                        }

                        metadata[slotIndex] = 0;

                        compactCluster(slotIndex);

                        size--;
                    }
                }
        );

        capacity = BitSupport.nextPowerOfTwo(initialCapacity);
        size = 0;

        metadata = new long[capacity];
    }

    /**
     * Returns the number of key-value entries currently stored in this tier.
     *
     * @return the entry count
     */
    public int size() {
        return size;
    }

    /**
     * Returns whether this tier contains no entries.
     *
     * @return {@code true} if the tier is empty
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Returns whether the underlying binary buffer has wrapped around,
     * indicating that earlier data has been overwritten.
     *
     * @return {@code true} if the buffer has overlapped
     */
    public boolean isFull() {
        return kvBuffer.isOverlapped();
    }

    /**
     * Returns the capacity of the underlying binary key-value buffer in bytes.
     *
     * @return the buffer capacity
     */
    public int binaryCapacity() {
        return kvBuffer.capacity();
    }

    /**
     * Returns the number of bytes currently used in the binary buffer.
     *
     * @return the used space in bytes
     */
    public int binaryUsedSpace() {
        return kvBuffer.usedSpace();
    }

    /**
     * Returns the current capacity of the metadata slot array.
     *
     * @return the metadata capacity
     */
    public int metadataCapacity() {
        return metadata.length;
    }

    /**
     * Inserts or updates a key-value pair where the key and value are
     * stored contiguously in a single buffer.
     *
     * @param hash the pre-computed positive hash of the key
     * @param keyValue the buffer containing key followed by value
     * @param keyOffset the offset of the key within the buffer
     * @param keySize the size of the key in bytes
     * @param valueOffset the offset of the value within the buffer
     * @param valueSize the size of the value in bytes
     */
    public void put(final int hash,
                    final AtomicBuffer keyValue,
                    final int keyOffset,
                    final int keySize,
                    final int valueOffset,
                    final int valueSize) {
        expandIfLoadFactor();

        int slot = findSlot(keyValue, keyOffset, keySize, hash, false);

        if (slot == -1) {
            // Table full, but we must not have such
            // Because previously called expandIfLoadFactor() always produces not full table
            throw new IllegalStateException("Cannot find slot");
        }

        final long md = metadata[slot];
        final boolean isUpdate = unpackOccupied(md);

        if (isUpdate) {
            final int index = unpackIndex(md);

            updateValue(index, keyValue, valueOffset, valueSize);
        } else {
            final int sizeBeforePut = size;

            final KeyValueConsuming.IndexedKeyValue kvi = kvBuffer.putKeyValue(
                    slot,
                    keySize,
                    valueSize
            );

            final int index = kvi.index();

            final BinaryContent content = kvi.content();
            content.buffer().putBytes(content.offset(), keyValue, keyOffset, keySize + valueSize);
            kvi.apply();

            if (size < sizeBeforePut) {
                // Evictions during putKeyValue may have rearranged metadata
                // Via compactCluster, invalidating the original slot
                slot = findSlot(keyValue, keyOffset, keySize, hash, false);
                if (slot == -1) {
                    throw new IllegalStateException("Cannot find slot after eviction");
                }
                kvBuffer.updateSlotIndex(index, slot);
            }

            metadata[slot] = packMetadata(true, hash, index);
            size++;
        }
    }

    /**
     * Inserts or updates a key-value pair where the key and value reside
     * in separate buffers. On update the new value must be the same size
     * as the existing one.
     *
     * @param hash the pre-computed positive hash of the key
     * @param key the buffer containing the key
     * @param keyOffset the offset of the key within the key buffer
     * @param keySize the size of the key in bytes
     * @param value the buffer containing the value
     * @param valueOffset the offset of the value within the value buffer
     * @param valueSize the size of the value in bytes
     */
    public void put(final int hash,
                    final AtomicBuffer key,
                    final int keyOffset,
                    final int keySize,
                    final AtomicBuffer value,
                    final int valueOffset,
                    final int valueSize) {
        expandIfLoadFactor();

        int slot = findSlot(key, keyOffset, keySize, hash, false);

        if (slot == -1) {
            // Table full, but we must not have such
            // Because previously called expandIfLoadFactor() always produces not full table
            throw new IllegalStateException("Cannot find slot");
        }

        final long md = metadata[slot];
        final boolean isUpdate = unpackOccupied(md);

        if (isUpdate) {
            final int index = unpackIndex(md);

            updateValue(index, value, valueOffset, valueSize);
        } else {
            final int sizeBeforePut = size;

            final KeyValueConsuming.IndexedKeyValue kvi = kvBuffer.putKeyValue(
                    slot,
                    keySize,
                    valueSize
            );

            final int index = kvi.index();

            final BinaryContent content = kvi.content();
            content.buffer().putBytes(content.offset(), key, keyOffset, keySize);
            content.buffer().putBytes(content.offset() + keySize, value, valueOffset, valueSize);
            kvi.apply();

            if (size < sizeBeforePut) {
                // Evictions during putKeyValue may have rearranged metadata
                // Via compactCluster, invalidating the original slot
                slot = findSlot(key, keyOffset, keySize, hash, false);
                if (slot == -1) {
                    throw new IllegalStateException("Cannot find slot after eviction");
                }
                kvBuffer.updateSlotIndex(index, slot);
            }

            metadata[slot] = packMetadata(true, hash, index);
            size++;
        }
    }

    /**
     * Looks up the value associated with a key and delivers it to the consumer.
     *
     * @param hash the pre-computed positive hash of the key
     * @param key the buffer containing the key
     * @param offset the offset of the key within the buffer
     * @param size the size of the key in bytes
     * @param consumer the consumer that receives the value if found
     * @param <V> the value type
     * @param <C> the consumer type
     * @return {@code true} if the key was found and the value delivered
     */
    public <V extends KeyValueConsuming.Value, C extends KeyValueConsuming.ValueConsumer<V>> boolean get(
            final int hash,
            final AtomicBuffer key,
            final int offset,
            final int size,
            final C consumer) {

        final int slot = findSlot(key, offset, size, hash, true);

        if (slot == -1) {
            return false;
        }

        final long md = metadata[slot];
        final int index = unpackIndex(md);
        kvBuffer.readValue(index, consumer);

        return true;
    }

    /**
     * Checks whether a key exists in this tier.
     *
     * @param hash the pre-computed positive hash of the key
     * @param key the buffer containing the key
     * @param offset the offset of the key within the buffer
     * @param size the size of the key in bytes
     * @return {@code true} if the key is present
     */
    public boolean containsKey(
            final int hash,
            final AtomicBuffer key,
            final int offset,
            final int size) {
        final int slot = findSlot(key, offset, size, hash, true);
        return slot != -1;
    }

    private void expandIfLoadFactor() {
        if (size >= capacity * LOAD_FACTOR) {
            expand();
        }
    }

    private void expand() {
        final long[] newMetadata = new long[metadata.length << 1]; // also power of 2

        for (int i = 0; i < metadata.length; i++) {
            final long md = metadata[i];
            if (!unpackOccupied(md)) {
                continue;
            }

            final int index = unpackIndex(md);
            final int hash = unpackHash(md);

            final int newSlotIndex = findEmptySlotInMetadata(newMetadata, hash);

            if (newSlotIndex == -1) {
                throw new IllegalStateException("Couldn't find an empty slot in expanded metadata.");
            }

            if (newMetadata[newSlotIndex] != 0) {
                throw new IllegalStateException();
            }

            newMetadata[newSlotIndex] = packMetadata(true, hash, index);
            kvBuffer.updateSlotIndex(index, newSlotIndex);
        }

        metadata = newMetadata;
        capacity = newMetadata.length;
    }

    private int findSlot(final AtomicBuffer key,
                         final int offset,
                         final int size,
                         final int hash,
                         final boolean mustExist) {
        assert hash > 0;

        final int capacityMask = capacity - 1;

        int slot = hash & capacityMask;

        final int originalSlot = slot;

        do {
            final long meta = metadata[slot];
            if (!unpackOccupied(meta)) {
                if (mustExist) {
                    break;
                }
                return slot; // empty slot found
            }

            // Check the same key
            if (unpackHash(meta) == hash) {
                final int index = unpackIndex(meta);
                if (kvBuffer.keyEquals(
                        index,
                        key,
                        offset,
                        size
                )) {
                    return slot; // key found
                }
            }

            slot = (slot + 1) & capacityMask;
        } while (slot != originalSlot); // slot == originalSlot only if table is full

        return -1; // not found
    }

    private static int findEmptySlotInMetadata(final long[] metadata,
                                               final int hash) {
        assert hash > 0;

        final int capacityMask = metadata.length - 1;

        int slot = hash & capacityMask;

        final int originalSlot = slot;

        do {
            final long meta = metadata[slot];
            if (!unpackOccupied(meta)) {
                return slot; // empty slot found
            }

            slot = (slot + 1) & capacityMask;
        } while (slot != originalSlot); // slot == originalSlot only if table is full

        return -1; // table full
    }

    private void updateValue(final int index,
                             final AtomicBuffer value,
                             final int valueOffset,
                             final int valueSize) {
        // Update existing entry - value must be same size
        final int existingValueSize = kvBuffer.valueSize(index);

        if (existingValueSize != valueSize) {
            throw new IllegalArgumentException("Update value must be same size as existing value");
        }

        // Update value in-place (slot index and kv index stay the same)
        final KeyValueConsuming.Value vi = kvBuffer.updateValue(index);
        final BinaryContent content = vi.valueContent();
        content.buffer().putBytes(content.offset(), value, valueOffset, valueSize);
        vi.apply();
    }

    private void compactCluster(final int originalEmptySlot) {
        int emptySlot = originalEmptySlot;

        final int capacityMask = capacity - 1;
        int slot = (emptySlot + 1) & capacityMask;

        while (slot != originalEmptySlot) { // slot == originalEmptySlot only if table is full
            final long m = metadata[slot];
            if (!unpackOccupied(m)) {
                break;
            }

            final int hash = unpackHash(m);
            final int idealSlot = hash & capacityMask;

            if (canMoveToEmptySlot(idealSlot, emptySlot, slot, capacityMask)) {
                // Move element to empty slot
                final int index = unpackIndex(m);
                metadata[emptySlot] = m;
                metadata[slot] = 0;

                kvBuffer.updateSlotIndex(index, emptySlot);

                emptySlot = slot;
            }

            slot = (slot + 1) & capacityMask;
        }
    }

    /**
     * Determines if an element can be moved to fill a gap during cluster compaction.
     * An element can move to fill a gap if the gap is in its probing path
     * from ideal to current index.
     *
     * @param idealSlot the ideal index (hash & capacityMask) of the element
     * @param emptySlot the slot index of the gap to fill
     * @param currentSlot the slot index of the candidate entry
     * @param capacityMask {@code capacity - 1} for the metadata table
     * @return true if the element can move to fill the gap
     */
    private boolean canMoveToEmptySlot(final int idealSlot,
                                       final int emptySlot,
                                       final int currentSlot,
                                       final int capacityMask) {
        final int idealToEmpty = (emptySlot - idealSlot) & capacityMask;
        final int idealToCurrent = (currentSlot - idealSlot) & capacityMask;

        return idealToEmpty <= idealToCurrent;
    }

    private static long packMetadata(final boolean occupied,
                                     final int hash,
                                     final int index) {
        // Pack metadata into long: [occupied(1) | hash(31) | index(32)]
        long result = 0;
        if (occupied) {
            result |= OCCUPIED_MASK;
        }
        result |= ((long) hash & HASH_MASK) << INT_BITS;
        result |= index & INDEX_MASK;
        return result;
    }

    private static boolean unpackOccupied(final long packed) {
        return (packed & OCCUPIED_MASK) != 0; // or just metadata < 0
    }

    private static int unpackHash(final long packed) {
        return (int) ((packed >>> INT_BITS) & HASH_MASK);
    }

    private static int unpackIndex(final long packed) {
        return (int) (packed & INDEX_MASK);
    }
}
