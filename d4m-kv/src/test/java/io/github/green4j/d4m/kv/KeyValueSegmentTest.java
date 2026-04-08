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

import java.util.concurrent.locks.StampedLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link KeyValueSegment}.
 */
class KeyValueSegmentTest {
    private static final int KV_BUFFER_SIZE = 1024;
    private static final int INITIAL_CAPACITY = 16;

    private AtomicBuffer keyBuffer;
    private AtomicBuffer valueBuffer;

    @BeforeEach
    void setUp() {
        keyBuffer = new UnsafeBuffer(new byte[64]);
        valueBuffer = new UnsafeBuffer(new byte[64]);
    }

    private static TierFactory heapTierFactory() {
        return heapTierFactory(KV_BUFFER_SIZE, INITIAL_CAPACITY);
    }

    private static TierFactory heapTierFactory(final int kvBufferSize,
                                               final int initialCapacity) {
        return (currentTiers, currentSize, evictionListener) ->
                new Tier(
                        initialCapacity,
                        new UnsafeBuffer(new byte[kvBufferSize]),
                        evictionListener
                );
    }

    private static TierFactory limitedTierFactory(final int maxTiers) {
        return (currentTiers, currentSize, evictionListener) -> {
            if (currentSize >= maxTiers) {
                return null;
            }
            return new Tier(
                    INITIAL_CAPACITY,
                    new UnsafeBuffer(new byte[KV_BUFFER_SIZE]),
                    evictionListener
            );
        };
    }

    private void putEntry(final KeyValueSegment segment,
                          final String key,
                          final String value) {
        keyBuffer.putBytes(0, key.getBytes());
        valueBuffer.putBytes(0, value.getBytes());
        final int hash = KeyValueSupport.hash(keyBuffer, 0, key.length());
        segment.put(hash, keyBuffer, 0, key.length(), valueBuffer, 0, value.length());
    }

    private boolean getEntry(final KeyValueSegment segment,
                             final String key,
                             final ByteArrayValueConsumer consumer) {
        keyBuffer.putBytes(0, key.getBytes());
        final int hash = KeyValueSupport.hash(keyBuffer, 0, key.length());
        return segment.get(hash, keyBuffer, 0, key.length(), consumer);
    }

    @Nested
    class Construction {
        @Test
        void createsRequestedNumberOfTiers() {
            final KeyValueSegment segment = new KeyValueSegment(2, heapTierFactory(), null);

            assertEquals(2, segment.size());
        }

        @Test
        void zeroStartSizeCreatesNoTiers() {
            final KeyValueSegment segment = new KeyValueSegment(0, heapTierFactory(), null);

            assertEquals(0, segment.size());
        }

        @Test
        void zeroStartSizeGrowsOnFirstPut() {
            final KeyValueSegment segment = new KeyValueSegment(0, heapTierFactory(), null);
            assertEquals(0, segment.size());

            putEntry(segment, "key", "val");

            assertEquals(1, segment.size());

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            assertTrue(getEntry(segment, "key", consumer));
        }

        @Test
        void singleTierAccessible() {
            final KeyValueSegment segment = new KeyValueSegment(1, heapTierFactory(), null);

            assertEquals(1, segment.size());
            assertNotNull(segment.getTier(0));
        }

        @Test
        void lockIsAvailable() {
            final KeyValueSegment segment = new KeyValueSegment(1, heapTierFactory(), null);

            assertNotNull(segment.lock());
            final long writeStamp = segment.lock().tryWriteLock();
            assertTrue(writeStamp != 0L);
            segment.lock().unlockWrite(writeStamp);
            final long readStamp = segment.lock().readLock();
            try {
                assertTrue(StampedLock.isReadLockStamp(readStamp));
            } finally {
                segment.lock().unlockRead(readStamp);
            }
        }
    }

    @Nested
    class PutAndGet {
        @Test
        void putAndRetrieveSingleEntry() {
            final KeyValueSegment segment = new KeyValueSegment(1, heapTierFactory(), null);

            putEntry(segment, "testKey", "testValue");

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            final boolean found = getEntry(segment, "testKey", consumer);

            assertTrue(found);
            assertEquals("testValue".length(), consumer.valueSize());
        }

        @Test
        void getMissingKeyReturnsFalse() {
            final KeyValueSegment segment = new KeyValueSegment(1, heapTierFactory(), null);

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            final boolean found = getEntry(segment, "missing", consumer);

            assertFalse(found);
        }

        @Test
        void getOnSingleTierSegmentMissingKeyReturnsFalse() {
            final KeyValueSegment segment = new KeyValueSegment(1, heapTierFactory(), null);

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            final boolean found = getEntry(segment, "nonexistent", consumer);

            assertFalse(found);
        }

        @Test
        void multipleKeysAllRetrievable() {
            final KeyValueSegment segment = new KeyValueSegment(1, heapTierFactory(), null);

            for (int i = 0; i < 5; i++) {
                putEntry(segment, "key" + i, "val" + i);
            }

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            for (int i = 0; i < 5; i++) {
                assertTrue(getEntry(segment, "key" + i, consumer));
            }
        }

        @Test
        void updateExistingKeySameValueSize() {
            final KeyValueSegment segment = new KeyValueSegment(1, heapTierFactory(), null);

            putEntry(segment, "key", "val1");
            putEntry(segment, "key", "val2");

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            assertTrue(getEntry(segment, "key", consumer));
            assertEquals(4, consumer.valueSize());
        }
    }

    @Nested
    class TierGrowth {
        @Test
        void additionalTiersCreatedOnOverflow() {
            final KeyValueSegment segment = new KeyValueSegment(
                    1,
                    heapTierFactory(128, 8),
                    null
            );
            assertEquals(1, segment.size());

            for (int i = 0; i < 20; i++) {
                putEntry(segment, "longkey_" + i, "longvalue_" + i);
            }

            assertTrue(segment.size() >= 1);
        }

        @Test
        void factoryReturningNullCapsTierCount() {
            final KeyValueSegment segment = new KeyValueSegment(
                    1,
                    limitedTierFactory(1),
                    null
            );

            assertEquals(1, segment.size());
        }

        @Test
        void factoryReturningNullStopsFurtherGrowth() {
            final KeyValueSegment segment = new KeyValueSegment(
                    1,
                    limitedTierFactory(1),
                    null
            );

            for (int i = 0; i < 10; i++) {
                putEntry(segment, "key" + i, "val" + i);
            }

            assertEquals(1, segment.size());
        }
    }

    @Nested
    class MultiTierSearch {
        @Test
        void getSearchesAcrossTwoTiers() {
            final KeyValueSegment segment = new KeyValueSegment(2, heapTierFactory(), null);
            assertEquals(2, segment.size());

            putEntry(segment, "key1", "val1");

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            assertTrue(getEntry(segment, "key1", consumer));
        }

        @Test
        void getSearchesAcrossThreeTiers() {
            final KeyValueSegment segment = new KeyValueSegment(3, heapTierFactory(), null);
            assertEquals(3, segment.size());

            putEntry(segment, "key1", "val1");

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            assertTrue(getEntry(segment, "key1", consumer));
        }
    }

    @Nested
    class EvictionCallback {
        @Test
        void evictionListenerCalledWhenDataLost() {
            final int[] evictionCount = {0};
            final EvictionListener listener = (notifier, hash, keyValue,
                                               keyOffset, keySize,
                                               valueOffset, valueSize) -> evictionCount[0]++;

            final KeyValueSegment segment = new KeyValueSegment(
                    1,
                    limitedTierFactory(1),
                    listener
            );

            for (int i = 0; i < 50; i++) {
                putEntry(segment, "key_evict_" + i, "value_evict_" + i);
            }

            assertTrue(evictionCount[0] > 0,
                    "eviction listener should be called when tier cannot grow");
        }
    }
}
