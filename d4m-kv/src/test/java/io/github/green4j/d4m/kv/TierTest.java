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

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link Tier}.
 */
class TierTest {
    private static final int BUFFER_SIZE = 1024;

    private Tier tier;
    private AtomicBuffer keyBuffer;
    private AtomicBuffer valueBuffer;

    private static final class TestValueConsumer extends ByteArrayValueConsumer {
        private boolean wasCalled = false;
        private int callCount = 0;

        @Override
        public KeyValueConsuming.Value putValue(final int valueSize) {
            wasCalled = true;
            callCount++;
            return super.putValue(valueSize);
        }

        boolean wasCalled() {
            return wasCalled;
        }

        int callCount() {
            return callCount;
        }

        void reset() {
            wasCalled = false;
            callCount = 0;
        }
    }

    @BeforeEach
    void setUp() {
        keyBuffer = new UnsafeBuffer(new byte[64]);
        valueBuffer = new UnsafeBuffer(new byte[64]);
        tier = new Tier(8, new UnsafeBuffer(new byte[BUFFER_SIZE]));
    }

    private void putEntry(final String key, final String value) {
        keyBuffer.putBytes(0, key.getBytes());
        valueBuffer.putBytes(0, value.getBytes());
        final int hash = KeyValueSupport.hash(keyBuffer, 0, key.length());
        tier.put(hash, keyBuffer, 0, key.length(), valueBuffer, 0, value.length());
    }

    private int hashOf(final String key) {
        keyBuffer.putBytes(0, key.getBytes());
        return KeyValueSupport.hash(keyBuffer, 0, key.length());
    }

    @Nested
    class Construction {
        @Test
        void variousInitialCapacities() {
            final Tier tier1 =
                    new Tier(1, new UnsafeBuffer(new byte[BUFFER_SIZE]));
            assertNotNull(tier1);
            assertTrue(tier1.isEmpty());
            assertEquals(0, tier1.size());

            final Tier tier16 =
                    new Tier(16, new UnsafeBuffer(new byte[BUFFER_SIZE]));
            assertNotNull(tier16);
            assertTrue(tier16.isEmpty());
        }

        @Test
        void withEvictionListener() {
            final Tier tier = new Tier(
                    8,
                    new UnsafeBuffer(new byte[BUFFER_SIZE]),
                    (notifier, hash, kv, ko, kl, vo, vl) -> {
                    }
            );
            assertNotNull(tier);
            assertTrue(tier.isEmpty());
        }
    }

    @Nested
    class InitialState {
        @Test
        void emptyAfterCreation() {
            assertTrue(tier.isEmpty());
            assertEquals(0, tier.size());
            assertFalse(tier.isFull());
        }

        @Test
        void binaryCapacityMatchesBuffer() {
            assertEquals(BUFFER_SIZE, tier.binaryCapacity());
        }

        @Test
        void binaryUsedSpaceIsZero() {
            assertEquals(0, tier.binaryUsedSpace());
        }
    }

    @Nested
    class PutNewEntry {
        @Test
        void singleEntryIncreasesSize() {
            putEntry("testKey", "testValue");

            assertFalse(tier.isEmpty());
            assertEquals(1, tier.size());
        }

        @Test
        void entryIsRetrievableByContainsKey() {
            putEntry("testKey", "testValue");

            final int hash = hashOf("testKey");
            assertTrue(tier.containsKey(hash, keyBuffer, 0, "testKey".length()));
        }

        @Test
        void binaryUsedSpaceIncreases() {
            final int before = tier.binaryUsedSpace();

            putEntry("testKey", "testValue");

            assertTrue(tier.binaryUsedSpace() > before);
        }

        @Test
        void emptyKeyAndValue() {
            keyBuffer.putBytes(0, "".getBytes());
            valueBuffer.putBytes(0, "".getBytes());

            final int hash = KeyValueSupport.hash(keyBuffer, 0, 0);
            tier.put(hash, keyBuffer, 0, 0, valueBuffer, 0, 0);

            assertEquals(1, tier.size());
            assertTrue(tier.containsKey(hash, keyBuffer, 0, 0));
        }
    }

    @Nested
    class PutWithCombinedBuffer {
        @Test
        void singleBufferContainingKeyAndValue() {
            final byte[] combined = "testKeytestValue".getBytes();
            final AtomicBuffer kvBuf = new UnsafeBuffer(combined);

            final int hash = KeyValueSupport.hash(kvBuf, 0, 7);
            tier.put(hash, kvBuf, 0, 7, 7, 9);

            assertEquals(1, tier.size());
            assertTrue(tier.containsKey(hash, kvBuf, 0, 7));
        }

        @Test
        void retrieveValueFromCombinedBufferPut() {
            final byte[] combined = "myKeymyValue".getBytes();
            final AtomicBuffer kvBuf = new UnsafeBuffer(combined);

            final int hash = KeyValueSupport.hash(kvBuf, 0, 5);
            tier.put(hash, kvBuf, 0, 5, 5, 7);

            final TestValueConsumer consumer = new TestValueConsumer();
            assertTrue(tier.get(hash, kvBuf, 0, 5, consumer));
            assertTrue(consumer.wasCalled());
            assertEquals(7, consumer.valueSize());
        }
    }

    @Nested
    class UpdateExisting {
        @Test
        void sameSizeValueUpdatesInPlace() {
            putEntry("testKey", "value1__");
            putEntry("testKey", "value2__");

            assertEquals(1, tier.size());
            assertTrue(tier.containsKey(hashOf("testKey"), keyBuffer, 0, "testKey".length()));
        }

        @Test
        void differentSizeValueThrows() {
            putEntry("testKey", "value1");

            keyBuffer.putBytes(0, "testKey".getBytes());
            valueBuffer.putBytes(0, "differentSizeValue".getBytes());
            final int hash = hashOf("testKey");

            assertThrows(IllegalArgumentException.class, () ->
                    tier.put(hash, keyBuffer, 0, "testKey".length(),
                            valueBuffer, 0, "differentSizeValue".length()));
        }

        @Test
        void updatedValueReadBack() {
            putEntry("testKey", "oldVal__");

            keyBuffer.putBytes(0, "testKey".getBytes());
            valueBuffer.putBytes(0, "newVal__".getBytes());
            final int hash = hashOf("testKey");
            tier.put(hash, keyBuffer, 0, "testKey".length(),
                    valueBuffer, 0, "newVal__".length());

            final TestValueConsumer consumer = new TestValueConsumer();
            assertTrue(tier.get(hash, keyBuffer, 0, "testKey".length(), consumer));

            final byte[] actual = Arrays.copyOf(consumer.array(), consumer.valueSize());
            assertArrayEquals("newVal__".getBytes(), actual);
        }
    }

    @Nested
    class ContainsKey {
        @Test
        void existingKeyReturnsTrue() {
            putEntry("existingKey", "value");

            final int hash = hashOf("existingKey");
            assertTrue(tier.containsKey(hash, keyBuffer, 0, "existingKey".length()));
        }

        @Test
        void missingKeyReturnsFalse() {
            final int hash = hashOf("nonExistingKey");
            assertFalse(tier.containsKey(hash, keyBuffer, 0, "nonExistingKey".length()));
        }

        @Test
        void differentKeySameHashPrefix() {
            putEntry("key1", "val1");

            keyBuffer.putBytes(0, "key2".getBytes());
            final int hash = hashOf("key2");
            assertFalse(tier.containsKey(hash, keyBuffer, 0, "key2".length()));
        }
    }

    @Nested
    class GetOperations {
        @Test
        void existingKeyCallsConsumer() {
            putEntry("testKey", "testValue");

            final TestValueConsumer consumer = new TestValueConsumer();
            final int hash = hashOf("testKey");
            final boolean found = tier.get(hash, keyBuffer, 0, "testKey".length(), consumer);

            assertTrue(found);
            assertTrue(consumer.wasCalled());
            assertEquals(1, consumer.callCount());
        }

        @Test
        void missingKeyDoesNotCallConsumer() {
            final TestValueConsumer consumer = new TestValueConsumer();
            final int hash = hashOf("nonExistingKey");
            final boolean found = tier.get(hash, keyBuffer, 0, "nonExistingKey".length(), consumer);

            assertFalse(found);
            assertFalse(consumer.wasCalled());
        }

        @Test
        void retrievedValueMatchesInserted() {
            putEntry("key", "expected");

            final TestValueConsumer consumer = new TestValueConsumer();
            final int hash = hashOf("key");
            tier.get(hash, keyBuffer, 0, "key".length(), consumer);

            final byte[] actual = Arrays.copyOf(consumer.array(), consumer.valueSize());
            assertArrayEquals("expected".getBytes(), actual);
        }

        @Test
        void multipleConcurrentGets() {
            putEntry("key1", "value1");
            putEntry("key2", "value2");

            final TestValueConsumer consumer = new TestValueConsumer();

            keyBuffer.putBytes(0, "key1".getBytes());
            assertTrue(tier.get(hashOf("key1"), keyBuffer, 0, "key1".length(), consumer));

            keyBuffer.putBytes(0, "key2".getBytes());
            assertTrue(tier.get(hashOf("key2"), keyBuffer, 0, "key2".length(), consumer));

            assertEquals(2, consumer.callCount());
        }

        @Test
        void zeroSizeValueRetrievable() {
            keyBuffer.putBytes(0, "key".getBytes());
            final int hash = hashOf("key");
            tier.put(hash, keyBuffer, 0, "key".length(), valueBuffer, 0, 0);

            final TestValueConsumer consumer = new TestValueConsumer();
            assertTrue(tier.get(hash, keyBuffer, 0, "key".length(), consumer));
            assertTrue(consumer.wasCalled());
            assertEquals(0, consumer.valueSize());
        }
    }

    @Nested
    class MultipleEntries {
        @Test
        void fiveEntriesAllRetrievable() {
            for (int i = 0; i < 5; i++) {
                putEntry("key" + i, "value" + i);
            }

            assertEquals(5, tier.size());
            assertFalse(tier.isEmpty());

            for (int i = 0; i < 5; i++) {
                final String key = "key" + i;
                keyBuffer.putBytes(0, key.getBytes());
                final int hash = hashOf(key);
                assertTrue(tier.containsKey(hash, keyBuffer, 0, key.length()));
            }
        }

        @Test
        void consistencyAfterMidUpdate() {
            final String[] keys = {"key1", "key2", "key3"};
            final String[] values = {"value1", "value2", "value3"};

            for (int i = 0; i < keys.length; i++) {
                putEntry(keys[i], values[i]);
            }

            assertEquals(3, tier.size());

            putEntry("key2", "neval2");

            assertEquals(3, tier.size());
            for (final String key : keys) {
                keyBuffer.putBytes(0, key.getBytes());
                final int hash = hashOf(key);
                assertTrue(tier.containsKey(hash, keyBuffer, 0, key.length()));
            }
        }
    }

    @Nested
    class Expansion {
        @Test
        void expandsBeyondLoadFactor() {
            for (int i = 0; i < 10; i++) {
                putEntry("key" + i, "value" + i);
            }

            assertEquals(10, tier.size());

            for (int i = 0; i < 10; i++) {
                final String key = "key" + i;
                keyBuffer.putBytes(0, key.getBytes());
                final int hash = hashOf(key);
                assertTrue(tier.containsKey(hash, keyBuffer, 0, key.length()));
            }
        }

        @Test
        void metadataCapacityGrowsAfterExpansion() {
            final int initialCapacity = tier.metadataCapacity();

            for (int i = 0; i < 10; i++) {
                putEntry("key" + i, "value" + i);
            }

            assertTrue(tier.metadataCapacity() > initialCapacity);
        }
    }

    @Nested
    class EvictionAndCompaction {
        @Test
        void evictionListenerCalledOnOverflow() {
            final int[] evictionCount = {0};
            final Tier smallTier = new Tier(
                    4,
                    new UnsafeBuffer(new byte[128]),
                    (notifier, hash, kv, ko, kl, vo, vl) -> evictionCount[0]++
            );

            for (int i = 0; i < 30; i++) {
                final String key = "evict_key_" + i;
                final String value = "evict_value_" + i;
                keyBuffer.putBytes(0, key.getBytes());
                valueBuffer.putBytes(0, value.getBytes());
                final int hash = KeyValueSupport.hash(keyBuffer, 0, key.length());
                smallTier.put(hash, keyBuffer, 0, key.length(),
                        valueBuffer, 0, value.length());
            }

            assertTrue(evictionCount[0] > 0);
        }

        @Test
        void clusterCompactionPreservesExistingEntries() {
            final Tier compactTier = new Tier(
                    4,
                    new UnsafeBuffer(new byte[256]),
                    null
            );

            for (int i = 0; i < 5; i++) {
                final String key = "ck" + i;
                final String value = "cv" + i;
                keyBuffer.putBytes(0, key.getBytes());
                valueBuffer.putBytes(0, value.getBytes());
                final int hash = KeyValueSupport.hash(keyBuffer, 0, key.length());
                compactTier.put(hash, keyBuffer, 0, key.length(),
                        valueBuffer, 0, value.length());
            }

            for (int i = 0; i < 5; i++) {
                final String key = "ck" + i;
                keyBuffer.putBytes(0, key.getBytes());
                final int hash = KeyValueSupport.hash(keyBuffer, 0, key.length());
                assertTrue(compactTier.containsKey(hash, keyBuffer, 0, key.length()),
                        "key ck" + i + " should survive compaction");
            }
        }
    }

    @Nested
    class EdgeCases {
        @Test
        void largeKeyAndValue() {
            final StringBuilder largeKey = new StringBuilder();
            final StringBuilder largeValue = new StringBuilder();
            for (int i = 0; i < 50; i++) {
                largeKey.append("k");
                largeValue.append("v");
            }

            final AtomicBuffer bigKeyBuf = new UnsafeBuffer(new byte[128]);
            final AtomicBuffer bigValBuf = new UnsafeBuffer(new byte[128]);

            bigKeyBuf.putBytes(0, largeKey.toString().getBytes());
            bigValBuf.putBytes(0, largeValue.toString().getBytes());

            final int hash = KeyValueSupport.hash(bigKeyBuf, 0, largeKey.length());
            tier.put(hash, bigKeyBuf, 0, largeKey.length(),
                    bigValBuf, 0, largeValue.length());

            assertEquals(1, tier.size());
            assertTrue(tier.containsKey(hash, bigKeyBuf, 0, largeKey.length()));
        }

        @Test
        void boundaryCapacityOfOne() {
            final Tier smallTier = new Tier(
                    1, new UnsafeBuffer(new byte[256]));

            keyBuffer.putBytes(0, "k".getBytes());
            valueBuffer.putBytes(0, "v".getBytes());
            final int hash = KeyValueSupport.hash(keyBuffer, 0, 1);

            smallTier.put(hash, keyBuffer, 0, 1, valueBuffer, 0, 1);

            assertEquals(1, smallTier.size());
            assertTrue(smallTier.containsKey(hash, keyBuffer, 0, 1));
        }

        @Test
        void binaryCapacityAndUsedSpaceAfterPut() {
            final int initialCapacity = tier.binaryCapacity();
            final int initialUsed = tier.binaryUsedSpace();

            putEntry("testKey", "testValue");

            assertEquals(initialCapacity, tier.binaryCapacity());
            assertTrue(tier.binaryUsedSpace() > initialUsed);
        }
    }
}
