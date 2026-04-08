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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link KeyValueBuffer}.
 */
class KeyValueBufferTest {
    static final int DEFAULT_BUFFER_SIZE = 128;

    private KeyValueBuffer buffer;
    private TestListener listener;

    private static final class TestListener implements KeyValueBuffer.Listener {
        final List<Event> insertEvents = new ArrayList<>();
        final List<Event> updateEvents = new ArrayList<>();
        final List<Event> evictEvents = new ArrayList<>();

        @Override
        public void onAfterInserted(final int index,
                                    final int slotIndex,
                                    final int keySize,
                                    final int valueSize) {
            insertEvents.add(new Event(index, slotIndex, keySize, valueSize));
        }

        @Override
        public void onAfterUpdated(final int index,
                                   final int slotIndex,
                                   final int keySize,
                                   final int valueSize) {
            updateEvents.add(new Event(index, slotIndex, keySize, valueSize));
        }

        @Override
        public void onBeforeEvicted(final int index,
                                    final int slotIndex,
                                    final int keySize,
                                    final int valueSize) {
            evictEvents.add(new Event(index, slotIndex, keySize, valueSize));
        }

        void clear() {
            insertEvents.clear();
            updateEvents.clear();
            evictEvents.clear();
        }

        static class Event {
            final int index;
            final int slotIndex;
            final int keySize;
            final int valueSize;

            Event(final int index,
                    final int slotIndex,
                    final int keySize,
                    final int valueSize) {
                this.index = index;
                this.slotIndex = slotIndex;
                this.keySize = keySize;
                this.valueSize = valueSize;
            }
        }
    }

    @BeforeEach
    void setUp() {
        final AtomicBuffer atomicBuffer = new UnsafeBuffer(new byte[DEFAULT_BUFFER_SIZE]);
        listener = new TestListener();
        buffer = new KeyValueBuffer(atomicBuffer, listener);
    }

    private int putKeyValue(final int slotIndex,
                            final String key,
                            final String value) {
        final KeyValueConsuming.IndexedKeyValue kv = buffer.putKeyValue(
                slotIndex, key.length(), value.length());
        if (kv != null) {
            final int index = kv.index();
            writeAndApply(kv, (key + value).getBytes());
            return index;
        }
        return -1;
    }

    private void writeAndApply(final KeyValueConsuming.KeyValue kv,
                               final byte[] data) {
        final BinaryContent content = kv.content();
        assertNotNull(content);
        content.buffer().putBytes(content.offset(), data, 0, data.length);
        kv.apply();
    }

    @Nested
    class Construction {
        @Test
        void rejectsCapacityBelowHeaderSize() {
            final AtomicBuffer small = new UnsafeBuffer(new byte[8]);
            assertThrows(IllegalArgumentException.class,
                    () -> new KeyValueBuffer(small, null));
        }

        @Test
        void rejectsNonPowerOfTwoCapacity() {
            final AtomicBuffer bad = new UnsafeBuffer(new byte[100]);
            assertThrows(IllegalArgumentException.class,
                    () -> new KeyValueBuffer(bad, null));
        }

        @Test
        void acceptsValidPowerOfTwoCapacity() {
            final AtomicBuffer valid = new UnsafeBuffer(new byte[32]);
            assertDoesNotThrow(() -> new KeyValueBuffer(valid, null));
        }

        @Test
        void nullListenerAccepted() {
            final AtomicBuffer buf = new UnsafeBuffer(new byte[64]);
            final KeyValueBuffer noListener = new KeyValueBuffer(buf, null);
            assertTrue(noListener.isEmpty());
        }
    }

    @Nested
    class EmptyState {
        @Test
        void initialFieldsAreConsistent() {
            assertTrue(buffer.isEmpty());
            assertEquals(0, buffer.size());
            assertEquals(0, buffer.usedSpace());
            assertEquals(DEFAULT_BUFFER_SIZE, buffer.freeSpace());
            assertEquals(DEFAULT_BUFFER_SIZE, buffer.capacity());
            assertEquals(0, buffer.nextIndex());
            assertEquals(-1, buffer.oldestIndex());
            assertEquals(-1, buffer.newestIndex());
        }
    }

    @Nested
    class SingleInsertion {
        @Test
        void insertSetsStateCorrectly() {
            final KeyValueConsuming.IndexedKeyValue kv = buffer.putKeyValue(1, 5, 10);

            assertNotNull(kv);
            assertEquals(0, kv.index());

            writeAndApply(kv, "helloworld12345".getBytes());

            assertEquals(1, buffer.size());
            assertEquals(32, buffer.usedSpace());
            assertEquals(128 - 32, buffer.freeSpace());
            assertFalse(buffer.isEmpty());
            assertEquals(0, buffer.oldestIndex());
            assertEquals(0, buffer.newestIndex());
        }

        @Test
        void listenerNotifiedOnInsert() {
            putKeyValue(1, "hello", "world12345");

            assertEquals(1, listener.insertEvents.size());
            final TestListener.Event evt = listener.insertEvents.get(0);
            assertEquals(0, evt.index);
            assertEquals(1, evt.slotIndex);
            assertEquals(5, evt.keySize);
            assertEquals(10, evt.valueSize);
        }

        @Test
        void indexReflectsWriteOffset() {
            putKeyValue(0, "key1", "value1");
            final int nextPos = buffer.nextIndex();

            final KeyValueConsuming.IndexedKeyValue kv2 = buffer.putKeyValue(1, 4, 6);
            assertNotNull(kv2);
            assertEquals(nextPos, kv2.index());
        }
    }

    @Nested
    class MultipleInsertions {
        @Test
        void threeEntriesTracked() {
            putKeyValue(1, "key1", "value1");
            putKeyValue(2, "key2", "value2");
            putKeyValue(3, "key3", "value3");

            final int entrySizeAligned = 32;

            assertEquals(3, buffer.size());
            assertEquals(entrySizeAligned * 3, buffer.usedSpace());
            assertEquals(0, buffer.oldestIndex());
            assertEquals(entrySizeAligned * 2, buffer.newestIndex());
        }

        @Test
        void slotIndicesReadable() {
            putKeyValue(1, "key1", "value1");
            putKeyValue(2, "key2", "value2");
            putKeyValue(3, "key3", "value3");

            final int entrySizeAligned = 32;

            assertEquals(1, buffer.slotIndex(0));
            assertEquals(2, buffer.slotIndex(entrySizeAligned));
            assertEquals(3, buffer.slotIndex(entrySizeAligned * 2));
        }

        @Test
        void newestAndOldestUpdatedOnEachInsert() {
            final int pos0 = putKeyValue(0, "a", "b");
            assertEquals(pos0, buffer.oldestIndex());
            assertEquals(pos0, buffer.newestIndex());

            final int pos1 = putKeyValue(1, "c", "d");
            assertEquals(pos0, buffer.oldestIndex());
            assertEquals(pos1, buffer.newestIndex());
        }
    }

    @Nested
    class ReadContent {
        @Test
        void readReturnsKeyAndValue() {
            final String key = "hello";
            final String value = "world";
            putKeyValue(1, key, value);

            final ByteArrayKeyValueConsumer consumer = new ByteArrayKeyValueConsumer();
            buffer.read(0, consumer);

            assertEquals(1, consumer.slotIndex());
            assertEquals(key.length(), consumer.keySize());
            assertEquals(value.length(), consumer.valueSize());

            final byte[] expected = (key + value).getBytes();
            final byte[] actual = Arrays.copyOf(
                    consumer.array(), consumer.keySize() + consumer.valueSize());
            assertArrayEquals(expected, actual);
        }

        @Test
        void readValueReturnsOnlyValuePortion() {
            final String key = "mykey";
            final String value = "myval";
            putKeyValue(0, key, value);

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            buffer.readValue(0, consumer);

            assertEquals(value.length(), consumer.valueSize());
            final byte[] actual = Arrays.copyOf(consumer.array(), consumer.valueSize());
            assertArrayEquals(value.getBytes(), actual);
        }

        @Test
        void readOnEmptyBufferIsNoOp() {
            final ByteArrayKeyValueConsumer consumer = new ByteArrayKeyValueConsumer();
            buffer.read(0, consumer);

            assertNull(consumer.array());
        }

        @Test
        void readValueOnEmptyBufferIsNoOp() {
            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            buffer.readValue(0, consumer);

            assertNull(consumer.array());
        }

        @Test
        void readMultipleIndices() {
            final String keyPrefix = "key";
            final String valuePrefix = "val";

            final int[] indices = new int[4];
            for (int i = 0; i < 4; i++) {
                indices[i] = putKeyValue(i, keyPrefix + i, valuePrefix + i);
            }

            final ByteArrayKeyValueConsumer consumer = new ByteArrayKeyValueConsumer();
            for (int i = 0; i < 4; i++) {
                buffer.read(indices[i], consumer);

                assertEquals(i, consumer.slotIndex());
                assertEquals((keyPrefix + i).length(), consumer.keySize());
                assertEquals((valuePrefix + i).length(), consumer.valueSize());

                final byte[] expected = (keyPrefix + i + valuePrefix + i).getBytes();
                final byte[] actual = Arrays.copyOf(
                        consumer.array(),
                        consumer.keySize() + consumer.valueSize()
                );
                assertArrayEquals(expected, actual);
            }
        }

        @Test
        void readAfterWrapAroundEviction() {
            final String keyPrefix = "key";
            final String valuePrefix = "value";
            final int entityLenAligned = 32;
            final int entriesToFill = DEFAULT_BUFFER_SIZE / entityLenAligned;

            final int[] indices = new int[entriesToFill + 3];
            for (int i = 0; i < indices.length; i++) {
                indices[i] = putKeyValue(i, keyPrefix + i, valuePrefix + i);
            }

            final ByteArrayKeyValueConsumer consumer = new ByteArrayKeyValueConsumer();
            final int evictedCount = listener.evictEvents.size();

            for (int i = evictedCount; i < indices.length; i++) {
                buffer.read(indices[i], consumer);
                assertEquals(i, consumer.slotIndex());
            }
        }
    }

    @Nested
    class KeyEquality {
        @Test
        void keyEqualsMatchesStoredKey() {
            putKeyValue(0, "mykey", "myval");

            final AtomicBuffer keyBuf = new UnsafeBuffer("mykey".getBytes());
            assertTrue(buffer.keyEquals(0, keyBuf, 0, 5));
        }

        @Test
        void keyEqualsMismatchReturnsFalse() {
            putKeyValue(0, "mykey", "myval");

            final AtomicBuffer keyBuf = new UnsafeBuffer("other".getBytes());
            assertFalse(buffer.keyEquals(0, keyBuf, 0, 5));
        }

        @Test
        void keyEqualsDifferentSizeReturnsFalse() {
            putKeyValue(0, "mykey", "myval");

            final AtomicBuffer keyBuf = new UnsafeBuffer("myke".getBytes());
            assertFalse(buffer.keyEquals(0, keyBuf, 0, 4));
        }

        @Test
        void keyEqualsOnEmptyBufferThrows() {
            final AtomicBuffer keyBuf = new UnsafeBuffer("key".getBytes());
            assertThrows(IllegalArgumentException.class,
                    () -> buffer.keyEquals(0, keyBuf, 0, 3));
        }
    }

    @Nested
    class ValueUpdate {
        @Test
        void updateValueInPlace() {
            putKeyValue(0, "key", "old");

            final KeyValueConsuming.Value valueWriter = buffer.updateValue(0);
            assertNotNull(valueWriter);
            final BinaryContent content = valueWriter.valueContent();
            content.buffer().putBytes(content.offset(), "new".getBytes(), 0, 3);
            valueWriter.apply();

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            buffer.readValue(0, consumer);
            final byte[] actual = Arrays.copyOf(consumer.array(), consumer.valueSize());
            assertArrayEquals("new".getBytes(), actual);
        }

        @Test
        void updateValueNotifiesListenerWithCorrectIndex() {
            final int pos0 = putKeyValue(0, "key", "old");
            putKeyValue(1, "ke2", "ne2");
            listener.clear();

            final KeyValueConsuming.Value valueWriter = buffer.updateValue(pos0);
            final BinaryContent content = valueWriter.valueContent();
            content.buffer().putBytes(content.offset(), "new".getBytes(), 0, 3);
            valueWriter.apply();

            assertEquals(1, listener.updateEvents.size());
            assertEquals(pos0, listener.updateEvents.get(0).index);
        }

        @Test
        void updateSlotIndexChangesMetadata() {
            putKeyValue(0, "key", "val");
            assertEquals(0, buffer.slotIndex(0));

            buffer.updateSlotIndex(0, 42);

            assertEquals(42, buffer.slotIndex(0));
        }

        @Test
        void updateSlotIndexOnEmptyBufferThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> buffer.updateSlotIndex(0, 5));
        }

        @Test
        void updateValueOnEmptyBufferThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> buffer.updateValue(0));
        }
    }

    @Nested
    class Eviction {
        @Test
        void evictsOldestWhenSpaceNeeded() {
            putKeyValue(1, "key1", "value_for_the_key_of_the_test_run_________1");
            putKeyValue(2, "key2", "value_for_the_key_of_the_test_run_________2");
            putKeyValue(3, "key2", "value_for_the_key_of_the_test_run_________3");

            assertEquals(2, buffer.size());
            assertEquals(1, listener.evictEvents.size());
            assertEquals(0, buffer.newestIndex());
            assertEquals(64, buffer.oldestIndex());

            final TestListener.Event evt = listener.evictEvents.get(0);
            assertEquals(0, evt.index);
            assertEquals(1, evt.slotIndex);
        }

        @Test
        void evictsMultipleWhenSingleEntryNeedsAllSpace() {
            putKeyValue(1, "key1", "value_for_the_key_of_the_test_run_____________1");
            putKeyValue(2, "key2", "value_for_the_key_of_the_test_run_____________2");
            putKeyValue(3, "key2", "value_for_the_key_of_the_test_run_____________3");

            assertEquals(1, buffer.size());
            assertEquals(2, listener.evictEvents.size());
            assertEquals(0, buffer.oldestIndex());
        }

        @Test
        void sizeStaysConsistentDuringEvictions() {
            for (int i = 0; i < 6; i++) {
                putKeyValue(i, "key" + i, "value_of_key_to_put_into_buffer" + i);
            }

            assertFalse(buffer.isEmpty());
            assertTrue(buffer.size() <= 2);
            assertEquals(buffer.size(),
                    listener.insertEvents.size() - listener.evictEvents.size());
        }

        @Test
        void sizeConsistentForPerfectFitEntries() {
            for (int i = 0; i < 6; i++) {
                putKeyValue(i, "key123456789",
                        "value_of_kvalue_of_kvalue_of_kvalue_of_kvalue_of_kvalue_of_k"
                                + "value_of_kvalue_of_kvalue_of_kvalue_of_k");
            }

            assertFalse(buffer.isEmpty());
            assertEquals(1, buffer.size());
            assertEquals(buffer.size(),
                    listener.insertEvents.size() - listener.evictEvents.size());
        }

        @Test
        void sizeConsistentForVaryingEntrySizes() {
            for (int i = 0; i < 7; i++) {
                putKeyValue(i, "key" + i, "value_of_kvalue" + i);
            }

            for (int i = 0; i < 6; i++) {
                putKeyValue(i, "key123456789",
                        "value_of_kvalue_of_kvalue_of_kvalue_of_kvalue_of_kvalue_of_k"
                                + "value_of_kvalue_of_kvalue_of_kvalue_of_k");
            }

            assertFalse(buffer.isEmpty());
            assertEquals(1, buffer.size());
            assertEquals(buffer.size(),
                    listener.insertEvents.size() - listener.evictEvents.size());
        }

        @Test
        void specificEvictionSequence() {
            putKeyValue(0, "123", "123456");
            putKeyValue(1, "123", "123456");
            putKeyValue(2, "123", "123456");
            putKeyValue(3, "123", "123456");
            putKeyValue(4, "12345", "123456789123");
            putKeyValue(4, "123456789", "123456789123456789");
            putKeyValue(4, "123456789", "123456789123456789");

            assertFalse(buffer.isEmpty());
            assertEquals(1, buffer.size());
            assertEquals(0, buffer.oldestIndex());
            assertEquals(0, buffer.newestIndex());
            assertEquals(48, buffer.nextIndex());
            assertEquals(7, listener.insertEvents.size());
            assertEquals(6, listener.evictEvents.size());
            assertEquals(buffer.size(),
                    listener.insertEvents.size() - listener.evictEvents.size());
        }

        @Test
        void evictOldestKeyValuesRemovesSpecifiedCount() {
            putKeyValue(0, "a", "b");
            putKeyValue(1, "c", "d");
            putKeyValue(2, "e", "f");

            assertEquals(3, buffer.size());

            final int evicted = buffer.evictOldestKeyValues(2);

            assertEquals(1, buffer.size());
            assertEquals(2, evicted);
        }

        @Test
        void evictOldestKeyValuesReturnsActualCount() {
            putKeyValue(0, "a", "b");

            final int evicted = buffer.evictOldestKeyValues(5);

            assertEquals(1, evicted);
            assertTrue(buffer.isEmpty());
        }

        @Test
        void evictOldestKeyValuesOnEmptyBufferReturnsZero() {
            final int evicted = buffer.evictOldestKeyValues(5);

            assertEquals(0, evicted);
            assertTrue(buffer.isEmpty());
        }
    }

    @Nested
    class Clear {
        @Test
        void clearResetsAllState() {
            putKeyValue(1, "key1", "value1");
            putKeyValue(2, "key2", "value2");
            assertFalse(buffer.isEmpty());

            buffer.clear();

            assertTrue(buffer.isEmpty());
            assertEquals(0, buffer.size());
            assertEquals(0, buffer.usedSpace());
            assertEquals(DEFAULT_BUFFER_SIZE, buffer.freeSpace());
            assertEquals(0, buffer.nextIndex());
            assertEquals(-1, buffer.oldestIndex());
            assertEquals(-1, buffer.newestIndex());
        }

        @Test
        void insertAfterClearWorksNormally() {
            putKeyValue(1, "key1", "value1");
            buffer.clear();

            putKeyValue(2, "key2", "value2");

            assertEquals(1, buffer.size());
            assertEquals(0, buffer.oldestIndex());
            assertEquals(0, buffer.newestIndex());
        }
    }

    @Nested
    class EdgeCases {
        @Test
        void concurrentModificationDetected() {
            buffer.putKeyValue(1, 5, 5);

            assertThrows(IllegalStateException.class,
                    () -> buffer.putKeyValue(2, 5, 5));
        }

        @Test
        void zeroSizeKeyAndValue() {
            final KeyValueConsuming.IndexedKeyValue kv1 = buffer.putKeyValue(1, 0, 0);
            assertNotNull(kv1);
            kv1.apply();

            assertEquals(1, buffer.size());
            assertEquals(KeyValueBuffer.HEADER_SIZE, buffer.usedSpace());
            assertEquals(
                    DEFAULT_BUFFER_SIZE - KeyValueBuffer.HEADER_SIZE,
                    buffer.freeSpace()
            );
            assertEquals(0, buffer.oldestIndex());
            assertEquals(0, buffer.newestIndex());
            assertEquals(KeyValueBuffer.HEADER_SIZE, buffer.nextIndex());

            final KeyValueConsuming.IndexedKeyValue kv2 = buffer.putKeyValue(2, 0, 0);
            assertNotNull(kv2);
            kv2.apply();

            final int twoHeaders = KeyValueBuffer.HEADER_SIZE * 2;
            assertEquals(2, buffer.size());
            assertEquals(twoHeaders, buffer.usedSpace());
            assertEquals(DEFAULT_BUFFER_SIZE - twoHeaders, buffer.freeSpace());
            assertEquals(0, buffer.oldestIndex());
            assertEquals(KeyValueBuffer.HEADER_SIZE, buffer.newestIndex());
            assertEquals(twoHeaders, buffer.nextIndex());
        }

        @Test
        void entryTooLargeForBufferReturnsNull() {
            final KeyValueConsuming.KeyValue kv = buffer.putKeyValue(1, 30, 90);

            assertNull(kv);
        }

        @Test
        void nullListenerDoesNotThrowOnInsert() {
            final AtomicBuffer buf = new UnsafeBuffer(new byte[64]);
            final KeyValueBuffer noListener = new KeyValueBuffer(buf, null);

            final KeyValueConsuming.IndexedKeyValue kv =
                    noListener.putKeyValue(0, 3, 3);
            assertNotNull(kv);
            writeAndApply(kv, "keyval".getBytes());

            assertEquals(1, noListener.size());
        }

        @Test
        void nullListenerDoesNotThrowOnEviction() {
            final AtomicBuffer buf = new UnsafeBuffer(new byte[64]);
            final KeyValueBuffer noListener = new KeyValueBuffer(buf, null);

            final KeyValueConsuming.IndexedKeyValue kv1 =
                    noListener.putKeyValue(0, 8, 16);
            assertNotNull(kv1);
            writeAndApply(kv1, new byte[24]);

            final KeyValueConsuming.IndexedKeyValue kv2 =
                    noListener.putKeyValue(1, 8, 16);
            assertNotNull(kv2);
            writeAndApply(kv2, new byte[24]);

            assertEquals(1, noListener.size());
        }

        @Test
        void valueSizeAccessor() {
            putKeyValue(0, "key", "value123");

            assertEquals(8, buffer.valueSize(0));
        }

        @Test
        void valueSizeOnEmptyBufferThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> buffer.valueSize(0));
        }

        @Test
        void slotIndexOnEmptyBufferThrows() {
            assertThrows(IllegalArgumentException.class,
                    () -> buffer.slotIndex(0));
        }
    }

    @Nested
    class WrapAround {
        @Test
        void overlapFlagSetOnWraparound() {
            putKeyValue(0, "key1", "value1");
            putKeyValue(1, "key2", "value2");
            putKeyValue(2, "key3", "value3");
            // 3 Entries of 32 bytes each = 96 used, nextIndex = 96
            // Entry with 40 aligned bytes would exceed capacity: 96+40 > 128
            putKeyValue(3, "key4", "value_larger_than_prev");

            assertTrue(buffer.isOverlapped());
        }

        @Test
        void notOverlappedWhenNoWraparound() {
            putKeyValue(0, "key1", "value1");

            assertFalse(buffer.isOverlapped());
        }
    }

    @Nested
    class WrapAroundDataIntegrity {
        // Each entry: HEADER(16) + 8-byte key + 8-byte value = 32 aligned bytes
        // 128-byte buffer fits exactly 4 such entries

        private String key(final int i) {
            return String.format("key_%04d", i);
        }

        private String val(final int i) {
            return String.format("val_%04d", i);
        }

        private void assertEntryData(final int index,
                                     final int expectedSlot,
                                     final String expectedKey,
                                     final String expectedValue) {
            final ByteArrayKeyValueConsumer consumer = new ByteArrayKeyValueConsumer();
            buffer.read(index, consumer);
            assertEquals(expectedSlot, consumer.slotIndex());
            assertEquals(expectedKey.length(), consumer.keySize());
            assertEquals(expectedValue.length(), consumer.valueSize());
            final byte[] expected = (expectedKey + expectedValue).getBytes();
            final byte[] actual = Arrays.copyOf(
                    consumer.array(), consumer.keySize() + consumer.valueSize());
            assertArrayEquals(expected, actual);
        }

        @Test
        void nextIndexResetsToZeroOnExactFill() {
            for (int i = 0; i < 4; i++) {
                putKeyValue(i, key(i), val(i));
            }
            assertEquals(0, buffer.nextIndex());
            assertEquals(4, buffer.size());
        }

        @Test
        void dataCorrectForEntryAtBufferTail() {
            for (int i = 0; i < 4; i++) {
                putKeyValue(i, key(i), val(i));
            }
            assertEntryData(96, 3, key(3), val(3));
            assertEquals(0, buffer.nextIndex());
        }

        @Test
        void dataIntactAfterSingleWrapAround() {
            for (int i = 0; i < 4; i++) {
                putKeyValue(i, key(i), val(i));
            }
            final int idx4 = putKeyValue(4, key(4), val(4));
            assertEquals(0, idx4);
            assertEquals(4, buffer.size());

            assertEntryData(32, 1, key(1), val(1));
            assertEntryData(64, 2, key(2), val(2));
            assertEntryData(96, 3, key(3), val(3));
            assertEntryData(0, 4, key(4), val(4));
        }

        @Test
        void keyEqualsOnEntryWrittenAfterWrap() {
            for (int i = 0; i < 5; i++) {
                putKeyValue(i, key(i), val(i));
            }
            final AtomicBuffer keyBuf = new UnsafeBuffer(key(4).getBytes());
            assertTrue(buffer.keyEquals(0, keyBuf, 0, 8));

            final AtomicBuffer wrongKey = new UnsafeBuffer(key(0).getBytes());
            assertFalse(buffer.keyEquals(0, wrongKey, 0, 8));
        }

        @Test
        void updateValueOnEntryWrittenAfterWrap() {
            for (int i = 0; i < 5; i++) {
                putKeyValue(i, key(i), val(i));
            }
            final String newVal = "NEWV_004";
            final KeyValueConsuming.Value vw = buffer.updateValue(0);
            assertNotNull(vw);

            final BinaryContent content = vw.valueContent();
            content.buffer().putBytes(content.offset(), newVal.getBytes(), 0, 8);
            vw.apply();

            final ByteArrayKeyValueConsumer consumer = new ByteArrayKeyValueConsumer();
            buffer.read(0, consumer);
            assertArrayEquals(key(4).getBytes(),
                    Arrays.copyOf(consumer.array(), 8));
            assertArrayEquals(newVal.getBytes(),
                    Arrays.copyOfRange(consumer.array(), 8, 16));
        }

        @Test
        void readValueAfterWrapAround() {
            for (int i = 0; i < 5; i++) {
                putKeyValue(i, key(i), val(i));
            }
            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            buffer.readValue(0, consumer);
            assertEquals(8, consumer.valueSize());
            assertArrayEquals(val(4).getBytes(),
                    Arrays.copyOf(consumer.array(), consumer.valueSize()));
        }

        @Test
        void readAllSurvivingEntriesAcrossWrapBoundary() {
            final int[] indices = new int[6];
            for (int i = 0; i < 6; i++) {
                indices[i] = putKeyValue(i, key(i), val(i));
            }
            assertEquals(4, buffer.size());
            assertEquals(64, buffer.oldestIndex());
            assertEquals(32, buffer.newestIndex());

            for (int i = 2; i < 6; i++) {
                assertEntryData(indices[i], i, key(i), val(i));
            }
        }

        @Test
        void multipleWrapAroundsPreserveData() {
            final int totalEntries = 12;
            final int[] indices = new int[totalEntries];
            for (int i = 0; i < totalEntries; i++) {
                indices[i] = putKeyValue(i, key(i), val(i));
            }
            assertEquals(4, buffer.size());
            for (int i = totalEntries - 4; i < totalEntries; i++) {
                assertEntryData(indices[i], i, key(i), val(i));
            }
        }

        @Test
        void mixedSizeEntriesAcrossWrap() {
            // 3 X 32-byte + 1 x 24-byte = 120 used, nextIndex=120
            putKeyValue(0, key(0), val(0));
            putKeyValue(1, key(1), val(1));
            putKeyValue(2, key(2), val(2));
            putKeyValue(3, "sm_3", "sv_3"); // h(16)+4+4=24 bytes

            assertEquals(120, buffer.nextIndex());

            // 32-byte entry wraps (120+32>128), evicts entry at index 0
            final int idx4 = putKeyValue(4, key(4), val(4));
            assertEquals(0, idx4);
            assertTrue(buffer.isOverlapped());

            assertEntryData(32, 1, key(1), val(1));
            assertEntryData(64, 2, key(2), val(2));
            assertEntryData(96, 3, "sm_3", "sv_3");
            assertEntryData(0, 4, key(4), val(4));
        }

        @Test
        void smallEntryAfterWrapDoesNotOverwriteNeighbor() {
            for (int i = 0; i < 4; i++) {
                putKeyValue(i, key(i), val(i));
            }
            assertEquals(0, buffer.nextIndex());

            // 24-byte entry (smaller than the 32-byte slots)
            putKeyValue(4, "sm_4", "sv_4");
            assertEquals(24, buffer.nextIndex());

            assertEntryData(0, 4, "sm_4", "sv_4");
            assertEntryData(32, 1, key(1), val(1));
        }

        @Test
        void evictionListenerReportsCorrectDataDuringWrap() {
            for (int i = 0; i < 4; i++) {
                putKeyValue(i, key(i), val(i));
            }
            listener.clear();

            putKeyValue(4, key(4), val(4));

            assertEquals(1, listener.evictEvents.size());
            final TestListener.Event evt = listener.evictEvents.get(0);
            assertEquals(0, evt.index);
            assertEquals(0, evt.slotIndex);
            assertEquals(8, evt.keySize);
            assertEquals(8, evt.valueSize);
        }

        @Test
        void tailPaddingEvictionDuringWrap() {
            // 48-byte entries: H(16) + 8-byte key + 24-byte value
            // 24-byte entry:  H(16) + 4-byte key + 4-byte value
            final String ka = "k" + "A".repeat(7);
            final String va = "v" + "A".repeat(23);
            final String kb = "k" + "B".repeat(7);
            final String vb = "v" + "B".repeat(23);
            final String kc = "kCCC";
            final String vc = "vCCC";
            final String kd = "k" + "D".repeat(7);
            final String vd = "v" + "D".repeat(23);
            final String ke = "k" + "E".repeat(7);
            final String ve = "v" + "E".repeat(23);
            final String kf = "k" + "F".repeat(7);
            final String vf = "v" + "F".repeat(23);

            putKeyValue(0, ka, va); // 48 bytes at idx 0,   next=48
            putKeyValue(1, kb, vb); // 48 bytes at idx 48,  next=96
            putKeyValue(2, kc, vc); // 24 bytes at idx 96,  next=120
            putKeyValue(3, kd, vd); // wraps, evicts A, idx 0,  next=48
            putKeyValue(4, ke, ve); // evicts B, idx 48,    next=96

            assertEquals(96, buffer.nextIndex());
            assertEquals(96, buffer.oldestIndex());

            listener.clear();

            // F: 96+48>128 -> wrap. freePaddingSpace evicts C (at 96),
            // Then freeNonWrappedSpace evicts D (at 0). F written at 0.
            putKeyValue(5, kf, vf);

            assertEquals(2, listener.evictEvents.size());
            final TestListener.Event evtC = listener.evictEvents.get(0);
            assertEquals(96, evtC.index);
            assertEquals(2, evtC.slotIndex);
            assertEquals(4, evtC.keySize);
            assertEquals(4, evtC.valueSize);

            final TestListener.Event evtD = listener.evictEvents.get(1);
            assertEquals(0, evtD.index);
            assertEquals(3, evtD.slotIndex);
            assertEquals(8, evtD.keySize);
            assertEquals(24, evtD.valueSize);

            assertEquals(2, buffer.size());
            assertEntryData(48, 4, ke, ve);
            assertEntryData(0, 5, kf, vf);
        }
    }
}
