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
 * Tests for {@link SingleThreadedKeyValueRing}.
 */
class SingleThreadedKeyValueRingTest {
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

    private void putEntry(final SingleThreadedKeyValueRing ring,
                          final String key,
                          final String value) {
        keyBuffer.putBytes(0, key.getBytes());
        valueBuffer.putBytes(0, value.getBytes());
        ring.put(keyBuffer, 0, key.length(), valueBuffer, 0, value.length());
    }

    private boolean getEntry(final SingleThreadedKeyValueRing ring,
                             final String key,
                             final ByteArrayValueConsumer consumer) {
        keyBuffer.putBytes(0, key.getBytes());
        return ring.get(keyBuffer, 0, key.length(), consumer);
    }

    @Nested
    class Construction {
        @Test
        void numberOfSegmentsIsPowerOfTwo() {
            final SingleThreadedKeyValueRing ring =
                    new SingleThreadedKeyValueRing(3, simpleSegmentFactory());

            assertTrue(BitSupport.isPowerOfTwo(ring.numberOfSegments()));
        }

        @Test
        void sizeReflectsSegmentsTimesShuffleMultiplier() {
            final SingleThreadedKeyValueRing ring =
                    new SingleThreadedKeyValueRing(4, 16, simpleSegmentFactory());

            assertEquals(4 * 16, ring.size());
        }

        @Test
        void segmentsArrayPopulated() {
            final SingleThreadedKeyValueRing ring =
                    new SingleThreadedKeyValueRing(RING_SIZE, simpleSegmentFactory());

            assertNotNull(ring.segments());
            assertTrue(ring.segments().length > 0);
        }

        @Test
        void eachSegmentAccessibleByIndex() {
            final SingleThreadedKeyValueRing ring =
                    new SingleThreadedKeyValueRing(RING_SIZE, simpleSegmentFactory());

            for (int i = 0; i < ring.numberOfSegments(); i++) {
                assertNotNull(ring.getSegment(i));
            }
        }

        @Test
        void defaultShuffleMultiplierUsed() {
            final SingleThreadedKeyValueRing ring =
                    new SingleThreadedKeyValueRing(RING_SIZE, simpleSegmentFactory());

            assertEquals(RING_SIZE * 16, ring.size());
        }
    }

    @Nested
    class PutAndGet {
        @Test
        void putAndRetrieveSingleKeyValue() {
            final SingleThreadedKeyValueRing ring =
                    new SingleThreadedKeyValueRing(RING_SIZE, simpleSegmentFactory());

            putEntry(ring, "testKey", "testVal");

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            final boolean found = getEntry(ring, "testKey", consumer);

            assertTrue(found);
            assertEquals("testVal".length(), consumer.valueSize());
        }

        @Test
        void getMissingKeyReturnsFalse() {
            final SingleThreadedKeyValueRing ring =
                    new SingleThreadedKeyValueRing(RING_SIZE, simpleSegmentFactory());

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            assertFalse(getEntry(ring, "missing", consumer));
        }

        @Test
        void multipleDistinctKeysAllRetrievable() {
            final SingleThreadedKeyValueRing ring =
                    new SingleThreadedKeyValueRing(RING_SIZE, simpleSegmentFactory());

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
            final SingleThreadedKeyValueRing ring =
                    new SingleThreadedKeyValueRing(RING_SIZE, simpleSegmentFactory());

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
            final SingleThreadedKeyValueRing ring =
                    new SingleThreadedKeyValueRing(RING_SIZE, simpleSegmentFactory());

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
            final SingleThreadedKeyValueRing ring =
                    new SingleThreadedKeyValueRing(2, 4, simpleSegmentFactory());

            assertEquals(2, ring.numberOfSegments());
            assertEquals(8, ring.size());

            final KeyValueSegment seg0 = ring.getSegment(0);
            final KeyValueSegment seg1 = ring.getSegment(1);
            assertNotNull(seg0);
            assertNotNull(seg1);

            final int numberOfSegments = ring.numberOfSegments();
            final int shuffleMultiplier = ring.size() / numberOfSegments;
            for (int i = 0; i < numberOfSegments; i++) {
                final KeyValueSegment base = ring.getSegment(i);
                for (int j = 1; j < shuffleMultiplier; j++) {
                    assertTrue(base == ring.getSegment(i + (j * numberOfSegments)),
                            "shuffled segment must equal base");
                }
            }
        }

        @Test
        void manyKeysAreDistributedAcrossSegments() {
            final SingleThreadedKeyValueRing ring =
                    new SingleThreadedKeyValueRing(RING_SIZE, simpleSegmentFactory());

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

    @Nested
    class Compute {
        @Test
        void singleKeyComputeRoundTrips() {
            final SingleThreadedKeyValueRing ring =
                    new SingleThreadedKeyValueRing(2, simpleSegmentFactory());

            final AtomicBuffer k = new UnsafeBuffer(new byte[4]);
            k.putBytes(0, "key".getBytes());

            final ValueAction action = new ValueAction();
            action.toWrite = 42L;
            ring.compute(k, 0, 3, action);
            assertEquals(0L, action.observedBefore);

            action.toWrite = 100L;
            ring.compute(k, 0, 3, action);
            assertEquals(42L, action.observedBefore);
        }

        @Test
        void twoKeyComputeRoundTrips() {
            final SingleThreadedKeyValueRing ring =
                    new SingleThreadedKeyValueRing(2, simpleSegmentFactory());

            final AtomicBuffer k1 = new UnsafeBuffer(new byte[3]);
            k1.putBytes(0, "k1\0".getBytes());
            final AtomicBuffer k2 = new UnsafeBuffer(new byte[3]);
            k2.putBytes(0, "k2\0".getBytes());

            final TwoKeyAction action = new TwoKeyAction();
            action.writeKey1 = 7L;
            action.writeKey2 = 9L;
            ring.compute(k1, 0, 3, k2, 0, 3, action);

            // Re-run; the action reads the previous values.
            action.writeKey1 = 70L;
            action.writeKey2 = 90L;
            ring.compute(k1, 0, 3, k2, 0, 3, action);
            assertEquals(7L, action.observedKey1);
            assertEquals(9L, action.observedKey2);
        }
    }

    private static final class ValueAction implements ComputeAction,
            KeyValueConsuming.ValueConsumer<KeyValueConsuming.Value>,
            KeyValueConsuming.Value, BinaryContent {

        private final ComputeContext ctx = new ComputeContext();
        private final UnsafeBuffer scratch = new UnsafeBuffer(new byte[8]);
        long toWrite;
        long observedBefore;

        @Override
        public ComputeContext context() {
            return ctx;
        }

        @Override
        public void execute() {
            observedBefore = 0L;
            ctx.get(this);
            scratch.putLong(0, toWrite);
            ctx.put(scratch, 0, 8);
        }

        @Override
        public KeyValueConsuming.Value putValue(final int valueSize) {
            return valueSize == 8 ? this : null;
        }

        @Override
        public BinaryContent valueContent() {
            return this;
        }

        @Override
        public AtomicBuffer buffer() {
            return scratch;
        }

        @Override
        public int offset() {
            return 0;
        }

        @Override
        public void apply() {
            observedBefore = scratch.getLong(0);
        }
    }

    private static final class TwoKeyAction implements TwoKeyComputeAction {

        private final ComputeContext c1 = new ComputeContext();
        private final ComputeContext c2 = new ComputeContext();
        private final LongConsumer reader1 = new LongConsumer();
        private final LongConsumer reader2 = new LongConsumer();
        private final UnsafeBuffer scratch1 = new UnsafeBuffer(new byte[8]);
        private final UnsafeBuffer scratch2 = new UnsafeBuffer(new byte[8]);

        long writeKey1;
        long writeKey2;
        long observedKey1;
        long observedKey2;

        @Override
        public ComputeContext key1Context() {
            return c1;
        }

        @Override
        public ComputeContext key2Context() {
            return c2;
        }

        @Override
        public void execute() {
            reader1.value = 0L;
            reader2.value = 0L;
            c1.get(reader1);
            c2.get(reader2);
            observedKey1 = reader1.value;
            observedKey2 = reader2.value;
            scratch1.putLong(0, writeKey1);
            scratch2.putLong(0, writeKey2);
            c1.put(scratch1, 0, 8);
            c2.put(scratch2, 0, 8);
        }
    }

    private static final class LongConsumer
            implements KeyValueConsuming.ValueConsumer<KeyValueConsuming.Value>,
                       KeyValueConsuming.Value, BinaryContent {
        private final UnsafeBuffer b = new UnsafeBuffer(new byte[8]);
        long value;

        @Override
        public KeyValueConsuming.Value putValue(final int valueSize) {
            return valueSize == 8 ? this : null;
        }

        @Override
        public BinaryContent valueContent() {
            return this;
        }

        @Override
        public AtomicBuffer buffer() {
            return b;
        }

        @Override
        public int offset() {
            return 0;
        }

        @Override
        public void apply() {
            value = b.getLong(0);
        }
    }
}
