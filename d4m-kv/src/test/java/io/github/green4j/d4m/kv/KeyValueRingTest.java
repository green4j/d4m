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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link KeyValueRing}.
 */
class KeyValueRingTest {
    private static final int KV_BUFFER_SIZE = 1024;
    private static final int TIER_INITIAL_CAPACITY = 16;
    private static final int RING_SIZE = 4;

    private AtomicBuffer keyBuffer;
    private AtomicBuffer valueBuffer;

    @BeforeEach
    void setUp() {
        keyBuffer = new UnsafeBuffer(new byte[128]);
        valueBuffer = new UnsafeBuffer(new byte[128]);
    }

    private static SegmentFactory simpleSegmentFactory() {
        return index -> new KeyValueSegment(
                1,
                (currentTiers, currentSize, evictionListener) ->
                        new Tier(
                                TIER_INITIAL_CAPACITY,
                                new UnsafeBuffer(new byte[KV_BUFFER_SIZE]),
                                evictionListener
                        ),
                null
        );
    }

    private void putEntry(final KeyValueRing ring,
                          final String key,
                          final String value) {
        keyBuffer.putBytes(0, key.getBytes());
        valueBuffer.putBytes(0, value.getBytes());
        ring.put(keyBuffer, 0, key.length(), valueBuffer, 0, value.length());
    }

    private boolean getEntry(final KeyValueRing ring,
                             final String key,
                             final ByteArrayValueConsumer consumer) {
        keyBuffer.putBytes(0, key.getBytes());
        return ring.get(keyBuffer, 0, key.length(), consumer);
    }

    @Nested
    class Construction {
        @Test
        void numberOfSegmentsIsPowerOfTwo() {
            final KeyValueRing ring = new KeyValueRing(3, simpleSegmentFactory());

            assertTrue(BitSupport.isPowerOfTwo(ring.numberOfSegments()));
        }

        @Test
        void sizeReflectsSegmentsTimesShuffleMultiplier() {
            final KeyValueRing ring = new KeyValueRing(4, 16, simpleSegmentFactory());

            assertEquals(4 * 16, ring.size());
        }

        @Test
        void segmentsArrayPopulated() {
            final KeyValueRing ring = new KeyValueRing(RING_SIZE, simpleSegmentFactory());

            assertNotNull(ring.segments());
            assertTrue(ring.segments().length > 0);
        }

        @Test
        void eachSegmentAccessibleByIndex() {
            final KeyValueRing ring = new KeyValueRing(RING_SIZE, simpleSegmentFactory());

            for (int i = 0; i < ring.numberOfSegments(); i++) {
                assertNotNull(ring.getSegment(i));
            }
        }

        @Test
        void defaultShuffleMultiplierUsed() {
            final KeyValueRing ring = new KeyValueRing(RING_SIZE, simpleSegmentFactory());

            assertEquals(RING_SIZE * 16, ring.size());
        }
    }

    @Nested
    class PutAndGet {
        @Test
        void putAndRetrieveSingleKeyValue() {
            final KeyValueRing ring = new KeyValueRing(RING_SIZE, simpleSegmentFactory());

            putEntry(ring, "testKey", "testVal");

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            final boolean found = getEntry(ring, "testKey", consumer);

            assertTrue(found);
            assertEquals("testVal".length(), consumer.valueSize());
        }

        @Test
        void getMissingKeyReturnsFalse() {
            final KeyValueRing ring = new KeyValueRing(RING_SIZE, simpleSegmentFactory());

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            assertFalse(getEntry(ring, "missing", consumer));
        }

        @Test
        void multipleDistinctKeysAllRetrievable() {
            final KeyValueRing ring = new KeyValueRing(RING_SIZE, simpleSegmentFactory());

            for (int i = 0; i < 10; i++) {
                putEntry(ring, "key" + i, "value" + i);
            }

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            for (int i = 0; i < 10; i++) {
                assertTrue(getEntry(ring, "key" + i, consumer),
                        "key" + i + " should be found");
            }
        }

        @Test
        void updateExistingKeySameValueSize() {
            final KeyValueRing ring = new KeyValueRing(RING_SIZE, simpleSegmentFactory());

            putEntry(ring, "key", "val1");
            putEntry(ring, "key", "val2");

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            assertTrue(getEntry(ring, "key", consumer));
            assertEquals(4, consumer.valueSize());

            final byte[] actual = Arrays.copyOf(consumer.array(), consumer.valueSize());
            assertArrayEquals("val2".getBytes(), actual);
        }

        @Test
        void emptyKeyAndValueWorkCorrectly() {
            final KeyValueRing ring = new KeyValueRing(RING_SIZE, simpleSegmentFactory());

            ring.put(keyBuffer, 0, 0, valueBuffer, 0, 0);

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            assertTrue(ring.get(keyBuffer, 0, 0, consumer));
            assertEquals(0, consumer.valueSize());
        }
    }

    @Nested
    class SegmentDistribution {
        @Test
        void shuffledSegmentsShareReferences() {
            final KeyValueRing ring = new KeyValueRing(2, 4, simpleSegmentFactory());

            assertEquals(2, ring.numberOfSegments());
            assertEquals(8, ring.size());

            final KeyValueSegment seg0 = ring.getSegment(0);
            final KeyValueSegment seg1 = ring.getSegment(1);
            assertNotNull(seg0);
            assertNotNull(seg1);
        }

        @Test
        void manyKeysAreDistributedAcrossSegments() {
            final KeyValueRing ring = new KeyValueRing(RING_SIZE, simpleSegmentFactory());

            for (int i = 0; i < 100; i++) {
                putEntry(ring, "distributed-key-" + i, "v");
            }

            int nonEmptyTierCount = 0;
            for (int i = 0; i < ring.numberOfSegments(); i++) {
                final KeyValueSegment seg = ring.getSegment(i);
                if (seg.size() > 0 && seg.getTier(0).size() > 0) {
                    nonEmptyTierCount++;
                }
            }

            assertTrue(nonEmptyTierCount > 1,
                    "keys should distribute across multiple segments");
        }
    }
}
