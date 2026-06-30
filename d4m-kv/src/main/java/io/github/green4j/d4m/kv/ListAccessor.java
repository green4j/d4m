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
import io.github.green4j.d4m.common.UnsafeBuffer;

/**
 * A mutable, reusable cursor for reading entries from a list bound via
 * {@link KeyLists#list}.
 *
 * <p>Each accessor owns its own per-thread buffers and metadata consumer, so
 * any number of reader threads may use their own accessors concurrently
 * against the same {@link KeyLists} (provided the backing {@link KeyValues}
 * is thread-safe). Do not share a single accessor across threads.
 *
 * <pre>{@code
 *   ListAccessor acc = new ListAccessor();
 *
 *   storage.list(acc, key1, 0, key1.capacity());
 *   acc.forEach(consumer);
 *
 *   storage.list(acc, key2, 0, key2.capacity());
 *   for (int i = 0; i < acc.size(); i++) {
 *       acc.get(i, consumer);
 *   }
 *
 *   // Stop scanning as soon as the consumer is satisfied (e.g. a match):
 *   storage.list(acc, key3, 0, key3.capacity());
 *   acc.forEach(stoppableConsumer); // stops once stoppableConsumer.stopped() is true
 *
 *   ListAccessor acc2 = new ListAccessor();
 *   acc.copyTo(acc2);
 * }</pre>
 */
public final class ListAccessor {

    KeyValues kvStore;
    long userKeyIndex;
    int count;

    private byte[] prefixedKeyArray = new byte[256];
    final UnsafeBuffer prefixedKeyBuffer = new UnsafeBuffer(prefixedKeyArray);
    final KeyListStorage.MetadataValueConsumer metadataConsumer =
            new KeyListStorage.MetadataValueConsumer();

    private final UnsafeBuffer syntheticKeyBuffer =
            new UnsafeBuffer(new byte[KeyListStorage.SYNTHETIC_KEY_SIZE]);

    /**
     * Creates a new unbound accessor. Call {@link KeyLists#list} to populate
     * it before reading entries.
     */
    public ListAccessor() {
    }

    /**
     * Returns the number of entries in the loaded list.
     *
     * @return the entry count, or 0 if not loaded or list does not exist
     */
    public int size() {
        return count;
    }

    /**
     * Returns whether the loaded list exists (has at least one entry).
     *
     * @return {@code true} if the list has entries
     */
    public boolean exists() {
        return count > 0;
    }

    /**
     * Reads the value at the given zero-based index from the loaded list.
     *
     * @param index    the zero-based position (0 = first appended entry)
     * @param consumer the consumer that receives the value data
     * @return {@code true} if the entry was found and delivered to the consumer
     * @throws IllegalStateException     if no list has been bound
     * @throws IndexOutOfBoundsException if index &lt; 0 or index &gt;= size()
     */
    public boolean get(final int index,
                       final KeyValueConsuming.ValueConsumer<KeyValueConsuming.Value> consumer) {
        if (kvStore == null) {
            throw new IllegalStateException("No list bound. Call KeyLists.list() first.");
        }
        if (index < 0 || index >= count) {
            throw new IndexOutOfBoundsException(
                    "Index: " + index + ", Size: " + count);
        }

        KeyListStorage.writeSyntheticKey(syntheticKeyBuffer, 0, userKeyIndex, index);
        return kvStore.get(
                syntheticKeyBuffer, 0, KeyListStorage.SYNTHETIC_KEY_SIZE,
                consumer
        );
    }

    /**
     * Iterates all entries in the loaded list from index 0 to {@code size() - 1},
     * delivering each value to the consumer.
     *
     * @param consumer the consumer that receives each entry's value
     * @return the number of entries delivered; equal to {@link #size()} unless an
     *         entry is missing in the backing store (which would indicate corruption)
     * @throws IllegalStateException if no list has been bound
     */
    public int forEach(
            final KeyValueConsuming.ValueConsumer<KeyValueConsuming.Value> consumer) {
        if (kvStore == null) {
            throw new IllegalStateException("No list bound. Call KeyLists.list() first.");
        }

        int delivered = 0;
        for (int i = 0; i < count; i++) {
            KeyListStorage.writeSyntheticKey(syntheticKeyBuffer, 0, userKeyIndex, i);
            if (!kvStore.get(
                    syntheticKeyBuffer, 0, KeyListStorage.SYNTHETIC_KEY_SIZE,
                    consumer)) {
                break;
            }
            delivered++;
        }
        return delivered;
    }

    /**
     * Iterates entries in the loaded list from index 0 upward, delivering each
     * value to the consumer, and stops early as soon as
     * {@link KeyValueConsuming.StoppableValueConsumer#stopped()} returns
     * {@code true} after a delivery.
     *
     * <p>{@code stopped()} is polled <em>after</em> the current entry has been
     * delivered, so the entry that triggers the stop is included in the
     * returned count; entries after the stop point are neither fetched nor
     * delivered.
     *
     * @param consumer the stoppable consumer that receives each entry's value
     * @return the number of entries delivered, including the entry after which
     *         iteration was stopped
     * @throws IllegalStateException if no list has been bound
     */
    public int forEach(
            final KeyValueConsuming.StoppableValueConsumer<KeyValueConsuming.Value> consumer) {
        if (kvStore == null) {
            throw new IllegalStateException("No list bound. Call KeyLists.list() first.");
        }

        int delivered = 0;
        for (int i = 0; i < count; i++) {
            KeyListStorage.writeSyntheticKey(syntheticKeyBuffer, 0, userKeyIndex, i);
            if (!kvStore.get(
                    syntheticKeyBuffer, 0, KeyListStorage.SYNTHETIC_KEY_SIZE,
                    consumer)) {
                break;
            }
            delivered++;
            if (consumer.stopped()) {
                break;
            }
        }
        return delivered;
    }

    /**
     * Copies the loaded state (list identity, count, and store reference)
     * into the target accessor. After this call, both accessors can read the
     * same list independently.
     *
     * @param target the accessor to copy state into
     */
    public void copyTo(final ListAccessor target) {
        target.kvStore = this.kvStore;
        target.userKeyIndex = this.userKeyIndex;
        target.count = this.count;
    }

    /**
     * Builds the prefixed user-metadata key into this accessor's reusable
     * buffer. Used by {@link KeyListStorage#list} so that no buffers are
     * shared across reader threads.
     *
     * @param key       the buffer containing the user key
     * @param keyOffset the offset of the key within the buffer
     * @param keySize   the size of the user key in bytes
     * @return the size (in bytes) of the prefixed key
     */
    int writePrefixedUserKey(final AtomicBuffer key,
                             final int keyOffset,
                             final int keySize) {
        final int totalSize = 1 + keySize;
        if (prefixedKeyArray.length < totalSize) {
            prefixedKeyArray = new byte[Math.max(totalSize, prefixedKeyArray.length << 1)];
            prefixedKeyBuffer.wrap(prefixedKeyArray);
        }
        prefixedKeyBuffer.putByte(0, KeyListStorage.USER_KEY_PREFIX);
        if (keySize > 0) {
            prefixedKeyBuffer.putBytes(1, key, keyOffset, keySize);
        }
        return totalSize;
    }
}
