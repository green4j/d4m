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
import io.github.green4j.d4m.common.UnsafeBuffer;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Counter-based implementation of {@link KeyLists} over a {@link KeyValues} store.
 *
 * <h2>Threading</h2>
 * Inherits the threading contract of the backing {@link KeyValues}. With a
 * thread-safe backing store (e.g. {@link KeyValueRing}), any number of writer
 * threads may call {@code append} on their own {@link KeyListsWriter}
 * instances concurrently - including against the same user key - and any
 * number of reader threads may call {@link #list} and iterate via their own
 * {@link ListAccessor} instances. The implementation adds no locks of its
 * own; multi-writer atomicity comes from {@link KeyValues#compute} on the
 * backing store, which acquires segment locks in canonical order.
 *
 * <h2>Key namespaces</h2>
 * Two byte-distinguishable key forms share the underlying store:
 * <ul>
 *   <li><b>User metadata key</b> - {@code [0x01 | user_key_bytes]} (length
 *       {@code 1 + N}). Byte&nbsp;0 is the literal {@code 0x01}.</li>
 *   <li><b>Synthetic entry key</b> - 8 bytes, byte&nbsp;0 has bit&nbsp;7 set
 *       (value {@code >= 0x80}). Carries a 39-bit {@code userKeyIndex} and a
 *       24-bit {@code entryIndex}:
 *       <pre>
 *   byte 0: 1xxxxxxx  - marker bit + top 7 bits of userKeyIndex
 *   bytes 1..4:        middle 32 bits of userKeyIndex (big-endian)
 *   bytes 5..7:        24 bits of entryIndex (big-endian)
 *       </pre></li>
 * </ul>
 * Writes are byte-by-byte and therefore independent of native byte order.
 *
 * <h2>Metadata value layout (fixed 8 bytes)</h2>
 * <pre>
 *   [ userKeyIndex (40 bits) | entryCount (24 bits) ]   big-endian
 * </pre>
 *
 * <h2>Capacity</h2>
 * <ul>
 *   <li>Distinct user keys: {@code 2^39 - 1 ~ 549 billion}</li>
 *   <li>Entries per list:   {@code 2^24 - 1 = 16,777,215}</li>
 * </ul>
 *
 * <h2>Constraints</h2>
 * The backing {@link KeyValues} store must be empty at construction time;
 * reopening against a non-empty store is not supported.
 */
public class KeyListStorage implements KeyLists {

    static final byte USER_KEY_PREFIX = 0x01;
    static final byte SYNTHETIC_MARKER = (byte) 0x80;

    static final int SYNTHETIC_KEY_SIZE = 8;
    static final int METADATA_VALUE_SIZE = BitSupport.SIZE_OF_LONG;

    static final long MAX_USER_KEY_INDEX = (1L << 39) - 1L;
    static final int MAX_ENTRY_COUNT = (1 << 24) - 1;

    private static final long USER_KEY_INDEX_MASK = MAX_USER_KEY_INDEX;
    private static final int ENTRY_INDEX_MASK = MAX_ENTRY_COUNT;

    private final KeyValues kvStore;
    private final AtomicLong userKeyIndexSequence = new AtomicLong(1L);

    /**
     * Creates a new {@link KeyListStorage} backed by the given key-value
     * store. The backing store must be empty; reopening against an existing
     * store is not supported.
     *
     * @param kvStore the underlying key-value store
     */
    public KeyListStorage(final KeyValues kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    public KeyListsWriter newWriter() {
        return new StorageKeyListsWriter();
    }

    @Override
    public boolean list(final ListAccessor accessor,
                        final AtomicBuffer key, final int keyOffset, final int keySize) {
        final int prefixedKeySize = accessor.writePrefixedUserKey(key, keyOffset, keySize);

        accessor.metadataConsumer.reset();
        final boolean exists = kvStore.get(
                accessor.prefixedKeyBuffer, 0, prefixedKeySize,
                accessor.metadataConsumer
        );

        accessor.kvStore = this.kvStore;
        if (exists) {
            accessor.userKeyIndex = accessor.metadataConsumer.userKeyIndex();
            accessor.count = accessor.metadataConsumer.entryCount();
        } else {
            accessor.userKeyIndex = 0L;
            accessor.count = 0;
        }
        return exists;
    }

    /**
     * Writes the synthetic 8-byte entry key for a given list / position.
     * Per-byte writes keep the layout endianness-independent and make the
     * marker bit on byte&nbsp;0 explicit.
     *
     * @param buffer       the target buffer
     * @param offset       the offset to write at
     * @param userKeyIndex the list identifier (must fit in 39 bits)
     * @param entryIndex   the zero-based entry position (must fit in 24 bits)
     */
    static void writeSyntheticKey(final AtomicBuffer buffer, final int offset,
                                  final long userKeyIndex, final int entryIndex) {
        final long uki = userKeyIndex & USER_KEY_INDEX_MASK;
        final int ei = entryIndex & ENTRY_INDEX_MASK;

        buffer.putByte(offset, (byte) (SYNTHETIC_MARKER | ((uki >>> 32) & 0x7F)));
        buffer.putByte(offset + 1, (byte) (uki >>> 24));
        buffer.putByte(offset + 2, (byte) (uki >>> 16));
        buffer.putByte(offset + 3, (byte) (uki >>> 8));
        buffer.putByte(offset + 4, (byte) uki);
        buffer.putByte(offset + 5, (byte) (ei >>> 16));
        buffer.putByte(offset + 6, (byte) (ei >>> 8));
        buffer.putByte(offset + 7, (byte) ei);
    }

    /**
     * Writes the fixed 8-byte metadata value:
     * {@code [ userKeyIndex (40 bits BE) | entryCount (24 bits BE) ]}.
     *
     * @param buffer       the target buffer
     * @param offset       the offset to write at
     * @param userKeyIndex the list identifier
     * @param entryCount   the current entry count
     */
    static void writeMetadataValue(final AtomicBuffer buffer, final int offset,
                                   final long userKeyIndex, final int entryCount) {
        buffer.putByte(offset, (byte) (userKeyIndex >>> 32));
        buffer.putByte(offset + 1, (byte) (userKeyIndex >>> 24));
        buffer.putByte(offset + 2, (byte) (userKeyIndex >>> 16));
        buffer.putByte(offset + 3, (byte) (userKeyIndex >>> 8));
        buffer.putByte(offset + 4, (byte) userKeyIndex);
        buffer.putByte(offset + 5, (byte) (entryCount >>> 16));
        buffer.putByte(offset + 6, (byte) (entryCount >>> 8));
        buffer.putByte(offset + 7, (byte) entryCount);
    }

    static long readMetadataUserKeyIndex(final AtomicBuffer buffer, final int offset) {
        return ((long) (buffer.getByte(offset) & 0xFF) << 32)
                | ((long) (buffer.getByte(offset + 1) & 0xFF) << 24)
                | ((long) (buffer.getByte(offset + 2) & 0xFF) << 16)
                | ((long) (buffer.getByte(offset + 3) & 0xFF) << 8)
                | ((long) (buffer.getByte(offset + 4) & 0xFF));
    }

    static int readMetadataEntryCount(final AtomicBuffer buffer, final int offset) {
        return ((buffer.getByte(offset + 5) & 0xFF) << 16)
                | ((buffer.getByte(offset + 6) & 0xFF) << 8)
                | (buffer.getByte(offset + 7) & 0xFF);
    }

    /**
     * Per-thread writer. Owns all reusable buffers needed for append, plus
     * the two {@link ComputeContext}s consumed by the two-key compute call.
     * The writer also acts as the {@link TwoKeyComputeAction}, so a steady
     * {@code append} call allocates nothing.
     */
    private final class StorageKeyListsWriter
            implements KeyListsWriter, TwoKeyComputeAction {

        private byte[] prefixedKeyArray = new byte[256];
        private final UnsafeBuffer prefixedKeyBuffer = new UnsafeBuffer(prefixedKeyArray);
        private final UnsafeBuffer syntheticKeyBuffer =
                new UnsafeBuffer(new byte[SYNTHETIC_KEY_SIZE]);
        private final UnsafeBuffer metadataValueBuffer =
                new UnsafeBuffer(new byte[METADATA_VALUE_SIZE]);
        private final MetadataValueConsumer metadataConsumer = new MetadataValueConsumer();
        private final ComputeContext metaCtx = new ComputeContext();
        private final ComputeContext synCtx = new ComputeContext();

        private AtomicBuffer pendingValue;
        private int pendingValueOffset;
        private int pendingValueSize;
        private long ukiSpec;
        private int countSpec;
        private boolean committed;

        @Override
        public ComputeContext key1Context() {
            return metaCtx;
        }

        @Override
        public ComputeContext key2Context() {
            return synCtx;
        }

        @Override
        public void append(final AtomicBuffer key, final int keyOffset, final int keySize,
                           final AtomicBuffer value, final int valueOffset, final int valueSize) {
            final int prefixedKeySize = writePrefixedUserKey(key, keyOffset, keySize);

            pendingValue = value;
            pendingValueOffset = valueOffset;
            pendingValueSize = valueSize;

            try {
                while (true) {
                    metadataConsumer.reset();
                    final boolean exists = kvStore.get(
                            prefixedKeyBuffer, 0, prefixedKeySize,
                            metadataConsumer
                    );

                    if (exists) {
                        ukiSpec = metadataConsumer.userKeyIndex();
                        countSpec = metadataConsumer.entryCount();
                    } else {
                        final long next = userKeyIndexSequence.getAndIncrement();
                        if (next > MAX_USER_KEY_INDEX) {
                            throw new IllegalStateException(
                                    "Exhausted userKeyIndex range: " + MAX_USER_KEY_INDEX);
                        }
                        ukiSpec = next;
                        countSpec = 0;
                    }

                    if (countSpec >= MAX_ENTRY_COUNT) {
                        throw new IllegalStateException(
                                "List reached maximum capacity: "
                                        + MAX_ENTRY_COUNT + " entries");
                    }

                    writeSyntheticKey(syntheticKeyBuffer, 0, ukiSpec, countSpec);

                    committed = false;
                    kvStore.compute(
                            prefixedKeyBuffer, 0, prefixedKeySize,
                            syntheticKeyBuffer, 0, SYNTHETIC_KEY_SIZE,
                            this
                    );

                    if (committed) {
                        return;
                    }
                }
            } finally {
                pendingValue = null;
            }
        }

        @Override
        public void execute() {
            metadataConsumer.reset();
            final boolean exists = metaCtx.get(metadataConsumer);
            final long ukiNow = exists ? metadataConsumer.userKeyIndex() : ukiSpec;
            final int countNow = exists ? metadataConsumer.entryCount() : 0;
            if (ukiNow != ukiSpec || countNow != countSpec) {
                return;
            }
            if (countNow >= MAX_ENTRY_COUNT) {
                return;
            }

            synCtx.put(pendingValue, pendingValueOffset, pendingValueSize);
            writeMetadataValue(metadataValueBuffer, 0, ukiSpec, countSpec + 1);
            metaCtx.put(metadataValueBuffer, 0, METADATA_VALUE_SIZE);
            committed = true;
        }

        private int writePrefixedUserKey(final AtomicBuffer key,
                                         final int keyOffset,
                                         final int keySize) {
            final int totalSize = 1 + keySize;
            if (prefixedKeyArray.length < totalSize) {
                prefixedKeyArray = new byte[Math.max(totalSize, prefixedKeyArray.length << 1)];
                prefixedKeyBuffer.wrap(prefixedKeyArray);
            }
            prefixedKeyBuffer.putByte(0, USER_KEY_PREFIX);
            if (keySize > 0) {
                prefixedKeyBuffer.putBytes(1, key, keyOffset, keySize);
            }
            return totalSize;
        }
    }

    /**
     * Single-instance consumer that captures the 8-byte metadata value into
     * an internal buffer, then exposes {@code userKeyIndex} / {@code entryCount}
     * accessors. Combines the {@code ValueConsumer}, {@code Value}, and
     * {@code BinaryContent} roles into one class - same pattern as
     * {@link ByteArrayValueConsumer}.
     */
    static final class MetadataValueConsumer
            implements KeyValueConsuming.ValueConsumer<KeyValueConsuming.Value>,
                       KeyValueConsuming.Value,
                       BinaryContent {

        private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[METADATA_VALUE_SIZE]);
        private long userKeyIndex;
        private int entryCount;
        private boolean found;

        void reset() {
            found = false;
            userKeyIndex = 0L;
            entryCount = 0;
        }

        long userKeyIndex() {
            return userKeyIndex;
        }

        int entryCount() {
            return entryCount;
        }

        boolean found() {
            return found;
        }

        @Override
        public KeyValueConsuming.Value putValue(final int valueSize) {
            if (valueSize != METADATA_VALUE_SIZE) {
                return null;
            }
            return this;
        }

        @Override
        public BinaryContent valueContent() {
            return this;
        }

        @Override
        public AtomicBuffer buffer() {
            return buffer;
        }

        @Override
        public int offset() {
            return 0;
        }

        @Override
        public void apply() {
            userKeyIndex = readMetadataUserKeyIndex(buffer, 0);
            entryCount = readMetadataEntryCount(buffer, 0);
            found = true;
        }
    }
}
