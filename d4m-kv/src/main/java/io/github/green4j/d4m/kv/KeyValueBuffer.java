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

import static io.github.green4j.d4m.common.BitSupport.INT_BITS;
import static io.github.green4j.d4m.common.BitSupport.MASK_32_BITS;
import static io.github.green4j.d4m.common.BitSupport.SIZE_OF_LONG;
import static io.github.green4j.d4m.common.BitSupport.alignToLong;
import static io.github.green4j.d4m.common.BitSupport.isAlignedToLong;
import static io.github.green4j.d4m.common.BitSupport.isPowerOfTwo;

/**
 * A ring buffer that stores key-value entries contiguously in an {@link AtomicBuffer}.
 * Each entry consists of a fixed-size header followed by key and value bytes.
 * When the buffer is full, the oldest entries are evicted to make room for new ones.
 */
public class KeyValueBuffer implements
        KeyValueConsuming.KeyValueConsumer<KeyValueConsuming.IndexedKeyValue> {

    /**
     * Listener for insert, update, and eviction events on the buffer.
     */
    public interface Listener {
        /**
         * Called after a new key-value entry has been inserted into the buffer.
         *
         * @param index     the buffer index of the inserted entry
         * @param slotIndex the metadata slot index assigned to the entry
         * @param keySize   the size of the key in bytes
         * @param valueSize the size of the value in bytes
         */
        void onAfterInserted(int index,
                             int slotIndex,
                             int keySize,
                             int valueSize);

        /**
         * Called after an existing key-value entry's value has been updated.
         *
         * @param index     the buffer index of the updated entry
         * @param slotIndex the metadata slot index of the entry
         * @param keySize   the size of the key in bytes
         * @param valueSize the size of the value in bytes
         */
        void onAfterUpdated(int index,
                            int slotIndex,
                            int keySize,
                            int valueSize);

        /**
         * Called before the oldest key-value entry is evicted to make room for new data.
         *
         * @param index     the buffer index of the entry being evicted
         * @param slotIndex the metadata slot index of the entry being evicted
         * @param keySize   the size of the key in bytes
         * @param valueSize the size of the value in bytes
         */
        void onBeforeEvicted(int index,
                             int slotIndex,
                             int keySize,
                             int valueSize);
    }

    static final int HEADER_SIZE = SIZE_OF_LONG * 2;

    private static final int NO_NEXT_ENTRY = -1;

    private final KeyValueWriter keyValueWriter = new KeyValueWriter();
    private final ValueWriter valueWriter = new ValueWriter();

    private final AtomicBuffer buffer;
    private final int capacity;

    private final Listener listener;

    private int usedSpace;

    private int nextIndex; // next index to write (starts from 0, goes forward)
    private int oldestIndex; // index of oldest header
    private int newestIndex; // index of newest header
    private boolean overlapped;
    private int size;

    /**
     * Creates a buffer backed by the given {@link AtomicBuffer} without a listener.
     *
     * @param buffer the backing buffer whose capacity must be a power of two and greater than the header size
     */
    public KeyValueBuffer(final AtomicBuffer buffer) {
        this(buffer, null);
    }

    /**
     * Creates a buffer backed by the given {@link AtomicBuffer} with an optional listener.
     *
     * @param buffer   the backing buffer whose capacity must be a power of two and greater than the header size
     * @param listener optional listener for insert/update/eviction events, or {@code null}
     */
    public KeyValueBuffer(final AtomicBuffer buffer,
                          final Listener listener) {
        final int capacity = buffer.capacity();
        if (capacity <= HEADER_SIZE) {
            throw new IllegalArgumentException("Capacity must be greater than " + HEADER_SIZE);
        }
        if (!isPowerOfTwo(capacity)) {
            throw new IllegalArgumentException("Capacity must be power of two");
        }

        this.buffer = buffer;
        this.listener = listener;

        this.capacity = capacity;

        reset();
    }

    /**
     * Returns the offset within the buffer where the key data begins for the given entry index.
     *
     * @param index the entry index
     * @return the key data offset
     */
    public int keyOffset(final int index) {
        return index + HEADER_SIZE;
    }

    /**
     * Returns the underlying {@link AtomicBuffer} used by this key-value buffer.
     *
     * @return the backing buffer
     */
    public AtomicBuffer buffer() {
        return buffer;
    }

    /**
     * Returns the number of key-value entries currently stored in this buffer.
     *
     * @return the number of entries
     */
    public int size() {
        return size;
    }

    /**
     * Returns the number of bytes currently occupied in the buffer,
     * including entry headers and any wrap-around padding.
     *
     * @return the used space in bytes
     */
    public int usedSpace() {
        return usedSpace;
    }

    /**
     * Returns the number of free bytes remaining in the buffer.
     *
     * @return the free space in bytes
     */
    public int freeSpace() {
        return capacity - usedSpace;
    }

    /**
     * Returns whether the buffer has wrapped around, meaning older entries
     * were overwritten by newer ones.
     *
     * @return {@code true} if the buffer has overlapped
     */
    public boolean isOverlapped() {
        return overlapped;
    }

    /**
     * Returns whether this buffer contains no entries.
     *
     * @return {@code true} if the buffer is empty
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Returns the index position where the next entry will be written.
     *
     * @return the current write index
     */
    public int nextIndex() {
        return nextIndex;
    }

    /**
     * Returns the total capacity of the buffer in bytes. This is always a power of two.
     *
     * @return the buffer capacity in bytes
     */
    public int capacity() {
        return capacity;
    }

    /**
     * Returns the buffer index of the oldest entry.
     *
     * @return the oldest entry index, or -1 if the buffer is empty
     */
    public int oldestIndex() {
        return oldestIndex;
    }

    /**
     * Returns the buffer index of the newest (most recently written) entry.
     *
     * @return the newest entry index, or -1 if the buffer is empty
     */
    public int newestIndex() {
        return newestIndex;
    }

    /**
     * Clears all key-values from the buffer.
     * No notification about eviction
     */
    public void clear() {
        reset();
    }

    /**
     * Allocates space for a new key-value entry in the ring buffer, evicting
     * the oldest entries if necessary to make room.
     *
     * @param slotIndex the metadata slot index to assign to the new entry
     * @param keySize   the size of the key in bytes
     * @param valueSize the size of the value in bytes
     * @return the writable {@link KeyValueConsuming.IndexedKeyValue}, or {@code null} if the
     * total entry size exceeds the buffer capacity
     */
    @Override
    public KeyValueConsuming.IndexedKeyValue putKeyValue(final int slotIndex,
                                                         final int keySize,
                                                         final int valueSize) {
        keyValueWriter.checkNotInUse();

        final int totalRequiredAligned = alignToLong(HEADER_SIZE + keySize + valueSize);

        if (totalRequiredAligned > capacity) {
            return null; // data total is too large for the buffer
        }

        // It must be already aligned by last .apply() in KeyValueWriter
        if (!isAlignedToLong(nextIndex)) {
            throw new IllegalStateException("Next index must be aligned");
        }


        int newNextIndex = nextIndex;
        // Handle wrap-around case
        if (newNextIndex + totalRequiredAligned > capacity) {
            overlapped = true;
            // Wrap to beginning - pad to end of buffer
            newNextIndex = 0; // start of buffer is already aligned
            freePaddingSpace();
        }

        nextIndex = newNextIndex;

        freeNonWrappedSpace(totalRequiredAligned);

        // Store current write index as newest key-value index
        final int previousNewest = newestIndex;
        newestIndex = nextIndex;

        // If this is the first key-value, it's also the oldest
        if (size == 0) {
            oldestIndex = nextIndex;
        } else {
            writeNextEntryIndex(previousNewest, nextIndex);
        }

        keyValueWriter.start(
                slotIndex,
                keySize,
                valueSize
        );

        return keyValueWriter;
    }

    /**
     * Returns the metadata slot index stored in the entry header at the given index.
     *
     * @param index the entry index
     * @return the slot index
     */
    public int slotIndex(final int index) {
        checkNotEmpty();

        final long firstLong = buffer.getLong(index);
        return unpackSlotIndex(firstLong);
    }

    /**
     * Returns the value size stored in the entry header at the given index.
     *
     * @param index the entry index
     * @return the value size in bytes
     */
    public int valueSize(final int index) {
        checkNotEmpty();

        final long secondLong = buffer.getLong(index + SIZE_OF_LONG);
        return unpackValueSize(secondLong);
    }

    /**
     * Updates the metadata slot index in the entry header at the given buffer index.
     *
     * @param index     the entry index
     * @param slotIndex the new slot index to store
     */
    public void updateSlotIndex(final int index,
                                final int slotIndex) {
        checkNotEmpty();

        writeSlotIndex(index, slotIndex);
    }

    /**
     * Returns a writable {@link KeyValueConsuming.Value} for updating the value
     * of an existing entry at the given index.
     *
     * @param index the entry index
     * @return a writable value handle
     */
    public KeyValueConsuming.Value updateValue(final int index) {
        checkNotEmpty();

        valueWriter.checkNotInUse();

        valueWriter.start(index);

        return valueWriter;
    }

    /**
     * Reads key-value content at the given buffer index and delivers it to the consumer.
     * Accessing a non-existing index leads to unpredictable behaviour, most likely
     * to {@link IndexOutOfBoundsException}. Callers must track valid indices via
     * {@link Listener} callbacks.
     *
     * @param index    the buffer index of the entry to read
     * @param consumer the consumer that receives the key-value data
     * @param <K>      the concrete {@link KeyValueConsuming.KeyValue} type
     * @param <C>      the concrete {@link KeyValueConsuming.KeyValueConsumer} type
     */
    public <K extends KeyValueConsuming.KeyValue, C extends KeyValueConsuming.KeyValueConsumer<K>> void read(
            final int index,
            final C consumer) {
        if (isEmpty()) {
            return;
        }

        final long firstLong = buffer.getLong(index);
        final long secondLong = buffer.getLong(index + SIZE_OF_LONG);

        final int slotIndex = unpackSlotIndex(firstLong);
        final int keySize = unpackKeySize(secondLong);
        final int valueSize = unpackValueSize(secondLong);

        final K kv = consumer.putKeyValue(slotIndex, keySize, valueSize);
        if (kv == null) {
            return;
        }

        final BinaryContent content = kv.content();
        if (content == null) {
            return;
        }

        final int dataStart = index + HEADER_SIZE;
        final int dataSize = keySize + valueSize;
        content.buffer().putBytes(content.offset(), buffer, dataStart, dataSize);
        kv.apply();
    }

    /**
     * Reads only the value content at the given buffer index and delivers it to the consumer.
     * Accessing a non-existing index leads to unpredictable behaviour, most likely
     * to {@link IndexOutOfBoundsException}. Callers must track valid indices via
     * {@link Listener} callbacks.
     *
     * @param index    the buffer index of the entry whose value to read
     * @param consumer the consumer that receives the value data
     * @param <V>      the concrete {@link KeyValueConsuming.Value} type
     * @param <C>      the concrete {@link KeyValueConsuming.ValueConsumer} type
     */
    public <V extends KeyValueConsuming.Value, C extends KeyValueConsuming.ValueConsumer<V>> void readValue(
            final int index,
            final C consumer) {
        if (isEmpty()) {
            return;
        }

        final long secondLong = buffer.getLong(index + SIZE_OF_LONG);
        final int keySize = unpackKeySize(secondLong);
        final int valueSize = unpackValueSize(secondLong);

        final V v = consumer.putValue(valueSize);
        if (v == null) {
            return;
        }

        final BinaryContent content = v.valueContent();
        if (content == null) {
            return;
        }

        final int dataStart = index + HEADER_SIZE;
        content.buffer().putBytes(content.offset(), buffer, dataStart + keySize, valueSize);
        v.apply();
    }

    /**
     * Evicts the specified number of oldest key-value entries from the buffer.
     *
     * @param numberOfKeyValues the maximum number of entries to evict
     * @return the actual number of entries evicted
     */
    public int evictOldestKeyValues(final int numberOfKeyValues) {
        int evicted = 0;
        while (evicted < numberOfKeyValues && !isEmpty()) {
            evictOldestKeyValue();
            evicted++;
        }
        return evicted;
    }

    /**
     * Compares the key stored at the given buffer index with the provided key data.
     *
     * @param index  the entry index
     * @param key    the buffer containing the key to compare against
     * @param offset the offset of the key within the buffer
     * @param size   the size of the key in bytes
     * @return {@code true} if the stored key equals the provided key
     */
    public boolean keyEquals(final int index,
                             final AtomicBuffer key,
                             final int offset,
                             final int size) {
        checkNotEmpty();

        final long secondLong = buffer.getLong(index + SIZE_OF_LONG);
        final int storedKeySize = unpackKeySize(secondLong);

        final int storedKeyOffset = index + HEADER_SIZE; // aligned

        return KeyValueSupport.equals(
                buffer,
                storedKeyOffset,
                storedKeySize,
                key,
                offset,
                size
        );
    }

    private void freePaddingSpace() {
        while (oldestIndex >= nextIndex) {
            evictOldestKeyValue();
        }
    }

    private void freeNonWrappedSpace(final int requiredSpace) {
        while ((oldestIndex >= nextIndex)
                && (oldestIndex - nextIndex) < requiredSpace) {
            evictOldestKeyValue();
        }
    }

    private void evictOldestKeyValue() {
        assert size > 0;

        final int index = oldestIndex;

        final long firstLong = buffer.getLong(index);
        final long secondLong = buffer.getLong(index + SIZE_OF_LONG);

        final int slotIndex = unpackSlotIndex(firstLong);
        final int nextEntryIndex = unpackNextEntryIndex(firstLong);
        final int keySize = unpackKeySize(secondLong);
        final int valueSize = unpackValueSize(secondLong);

        if (listener != null) {
            listener.onBeforeEvicted(index, slotIndex, keySize, valueSize);
        }

        final int spaceToFree;
        final int newOldestIndex;
        if (size == 1) {
            // Last entry - free all remaining space
            spaceToFree = usedSpace;
            newOldestIndex = -1;
        } else {
            spaceToFree = alignToLong(HEADER_SIZE + keySize + valueSize);
            newOldestIndex = nextEntryIndex;
        }

        usedSpace -= spaceToFree;
        size--;

        if (size == 0) {
            // No more entries
            oldestIndex = -1;
            newestIndex = -1;
            nextIndex = 0;
        } else {
            oldestIndex = newOldestIndex;
        }
    }

    private void checkNotEmpty() {
        if (size == 0) {
            throw new IllegalArgumentException("Buffer is empty");
        }
    }

    private void reset() {
        usedSpace = 0;
        size = 0;
        nextIndex = 0;
        oldestIndex = -1; // -1 indicates no entries
        newestIndex = -1;
    }

    private void writeHeader(final int index,
                             final int slotIndex,
                             final int keySize,
                             final int valueSize) {
        // 4 bytes nextEntryIndex + 4 bytes slotIndex
        final long firstLong = (((long) KeyValueBuffer.NO_NEXT_ENTRY) << INT_BITS)
                | (((long) slotIndex) & MASK_32_BITS);

        // 4 bytes keySize + 4 bytes valueSize
        final long secondLong = (((long) keySize) << INT_BITS) | (((long) valueSize) & MASK_32_BITS);

        buffer.putLong(index, firstLong);
        buffer.putLong(index + SIZE_OF_LONG, secondLong);
    }

    private void writeSlotIndex(final int index,
                                final int slotIndex) {
        final long firstLong = buffer.getLong(index);
        final long newFirstLong = (firstLong & (MASK_32_BITS << INT_BITS)) | (((long) slotIndex) & MASK_32_BITS);
        buffer.putLong(index, newFirstLong);
    }

    private void writeNextEntryIndex(final int index,
                                     final int nextEntryIndex) {
        final long firstLong = buffer.getLong(index);
        final long newFirstLong = (firstLong & MASK_32_BITS) | (((long) nextEntryIndex) << INT_BITS);
        buffer.putLong(index, newFirstLong);
    }

    private static int unpackNextEntryIndex(final long packed) {
        return (int) (packed >>> INT_BITS);
    }

    private static int unpackSlotIndex(final long packed) {
        return (int) (packed & MASK_32_BITS);
    }

    private static int unpackKeySize(final long packed) {
        return (int) (packed >>> INT_BITS);
    }

    private static int unpackValueSize(final long packed) {
        return (int) (packed & MASK_32_BITS);
    }

    private abstract class AbstractWriter implements BinaryContent {
        int required = -1; // is free
        int offset;

        int slotIndex;
        int keySize;
        int valueSize;

        void checkNotInUse() {
            if (this.required != -1) {
                throw new IllegalStateException("Concurrent modification");
            }
        }

        void start(final int offset,
                   final int required,
                   final int slotIndex,
                   final int keySize,
                   final int valueSize) {

            this.required = required;
            this.offset = offset;

            this.slotIndex = slotIndex;
            this.keySize = keySize;
            this.valueSize = valueSize;
        }

        @Override
        public AtomicBuffer buffer() {
            return buffer;
        }

        @Override
        public int offset() {
            return offset;
        }
    }

    private final class KeyValueWriter
            extends AbstractWriter implements KeyValueConsuming.IndexedKeyValue {

        private void start(final int slotIndex,
                           final int keySize,
                           final int valueSize) {
            super.start(
                    nextIndex + HEADER_SIZE,
                    keySize + valueSize,
                    slotIndex,
                    keySize,
                    valueSize
            );

            writeHeader(nextIndex, slotIndex, keySize, valueSize);
        }

        @Override
        public BinaryContent content() {
            return this;
        }

        @Override
        public int index() {
            return nextIndex;
        }

        @Override
        public void apply() {
            final int totalRequiredAligned = alignToLong(HEADER_SIZE + required);

            int newNextIndex = nextIndex + totalRequiredAligned;
            // Handle wrap-around case
            assert newNextIndex <= capacity;
            if (newNextIndex == capacity) {
                newNextIndex = 0;
            }

            nextIndex = newNextIndex;

            usedSpace += totalRequiredAligned;
            size++;

            try {
                if (listener != null) {
                    listener.onAfterInserted(
                            newestIndex,
                            slotIndex,
                            keySize,
                            valueSize
                    );
                }
            } finally {
                required = -1;
            }
        }
    }

    private final class ValueWriter
            extends AbstractWriter implements KeyValueConsuming.Value {
        int index;

        protected void start(final int index) {
            final long firstLong = buffer.getLong(index);
            final long secondLong = buffer.getLong(index + SIZE_OF_LONG);

            final int slotIndex = unpackSlotIndex(firstLong);
            final int keySize = unpackKeySize(secondLong);
            final int valueSize = unpackValueSize(secondLong);

            super.start(
                    index + HEADER_SIZE + keySize,
                    valueSize,
                    slotIndex,
                    keySize,
                    valueSize
            );

            this.index = index;
        }

        @Override
        public BinaryContent valueContent() {
            return this;
        }

        @Override
        public void apply() {
            try {
                if (listener != null) {
                    listener.onAfterUpdated(
                            index,
                            slotIndex,
                            keySize,
                            valueSize
                    );
                }
            } finally {
                required = -1;
            }
        }
    }
}
