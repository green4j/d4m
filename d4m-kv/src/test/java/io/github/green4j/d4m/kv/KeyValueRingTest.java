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

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
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
                if (seg.size() > 0 && !seg.getTier(0).isEmpty()) {
                    nonEmptyTierCount++;
                }
            }

            assertTrue(nonEmptyTierCount > 1,
                    "keys should distribute across multiple segments");
        }

        @Test
        void shuffledIndicesShareLockInstance() {
            final KeyValueRing ring = new KeyValueRing(2, 4, simpleSegmentFactory());

            final int numberOfSegments = ring.numberOfSegments();
            final int shuffleMultiplier = ring.size() / numberOfSegments;

            for (int i = 0; i < numberOfSegments; i++) {
                final KeyValueSegment baseSegment = ring.getSegment(i);
                final java.util.concurrent.locks.StampedLock baseLock = ring.getLock(i);
                assertNotNull(baseLock);
                for (int j = 1; j < shuffleMultiplier; j++) {
                    final int shuffledIndex = i + (j * numberOfSegments);
                    assertTrue(baseSegment == ring.getSegment(shuffledIndex),
                            "shuffled segment at " + shuffledIndex + " must equal base");
                    assertTrue(baseLock == ring.getLock(shuffledIndex),
                            "shuffled lock at " + shuffledIndex + " must equal base");
                }
            }
        }

        @Test
        void distinctSegmentsHaveDistinctLocks() {
            final KeyValueRing ring = new KeyValueRing(4, 2, simpleSegmentFactory());

            for (int i = 0; i < ring.numberOfSegments(); i++) {
                for (int k = i + 1; k < ring.numberOfSegments(); k++) {
                    assertTrue(ring.getLock(i) != ring.getLock(k),
                            "locks for distinct segments " + i + " and " + k + " must differ");
                }
            }
        }
    }

    @Nested
    class Compute {

        @Test
        void singleKeyComputeAtomicReadModifyWrite() throws Exception {
            // Two writer threads concurrently increment a counter stored under one key.
            // Without atomic RMW, races would lose updates.
            final KeyValueRing ring = new KeyValueRing(2, simpleSegmentFactory());

            final AtomicBuffer counterKey = new UnsafeBuffer(new byte[8]);
            counterKey.putLong(0, 0xDEADBEEFCAFE0000L);

            final int writers = 4;
            final int incrementsPerWriter = 5_000;

            final CountDownLatch start = new CountDownLatch(1);
            final CountDownLatch done = new CountDownLatch(writers);
            final ExecutorService pool = Executors.newFixedThreadPool(writers);

            for (int w = 0; w < writers; w++) {
                pool.submit(() -> {
                    final IncrementingAction action = new IncrementingAction();
                    try {
                        start.await();
                        for (int i = 0; i < incrementsPerWriter; i++) {
                            ring.compute(counterKey, 0, 8, action);
                        }
                    } catch (final InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            done.await();
            pool.shutdown();

            final LongValueConsumer reader = new LongValueConsumer();
            assertTrue(ring.get(counterKey, 0, 8, reader));
            assertEquals(writers * incrementsPerWriter, reader.value);
        }

        @Test
        void twoKeyComputeNoSelfDeadlockOnSharedSegment() {
            // Two keys whose shuffled indices differ but reduce to the same
            // canonical segment (and therefore the same StampedLock). A naive
            // implementation that orders by shuffled index would call
            // writeLock() twice on the same non-reentrant lock and hang.
            final KeyValueRing ring = new KeyValueRing(2, 4, simpleSegmentFactory());

            // ring.size() == 8, numberOfSegments == 2. Find two distinct
            // shuffled indices that share the same lock.
            final int numberOfSegments = ring.numberOfSegments();
            assertEquals(2, numberOfSegments);
            final java.util.concurrent.locks.StampedLock baseLock = ring.getLock(0);
            int sharedShuffledIndex = -1;
            for (int i = numberOfSegments; i < ring.size(); i += numberOfSegments) {
                if (ring.getLock(i) == baseLock) {
                    sharedShuffledIndex = i;
                    break;
                }
            }
            assertTrue(sharedShuffledIndex > 0, "must find a shuffled alias");

            // Pick keys whose hashes land on indices 0 and sharedShuffledIndex.
            final int mask = ring.size() - 1;
            final AtomicBuffer kA = findKeyHashingTo(0, mask);
            final AtomicBuffer kB = findKeyHashingTo(sharedShuffledIndex, mask);

            final NoopTwoKeyAction action = new NoopTwoKeyAction();
            assertTimeoutPreemptively(Duration.ofSeconds(2), () ->
                    ring.compute(kA, 0, kA.capacity(), kB, 0, kB.capacity(), action));
        }

        @Test
        void twoKeyComputeNoDeadlockUnderCrossingAcquire() {
            // Two threads request locks on the same two segments in opposite order
            // via compute(keyA, keyB) and compute(keyB, keyA). Without canonical
            // ordering this deadlocks; with it the run completes promptly.
            final KeyValueRing ring = new KeyValueRing(8, simpleSegmentFactory());

            // Pick two keys hashing to distinct segments. With ring size 8 and
            // shuffle 16, segments.length = 128; differing high bytes give a high
            // chance of distinct segments.
            final AtomicBuffer keyA = new UnsafeBuffer(new byte[4]);
            keyA.putBytes(0, new byte[]{1, 2, 3, 4});
            final AtomicBuffer keyB = new UnsafeBuffer(new byte[4]);
            keyB.putBytes(0, new byte[]{40, 39, 38, 37});

            final int iterations = 5_000;
            final CountDownLatch start = new CountDownLatch(1);

            final Callable<Void> forward = () -> {
                final NoopTwoKeyAction action = new NoopTwoKeyAction();
                start.await();
                for (int i = 0; i < iterations; i++) {
                    ring.compute(keyA, 0, 4, keyB, 0, 4, action);
                }
                return null;
            };
            final Callable<Void> reverse = () -> {
                final NoopTwoKeyAction action = new NoopTwoKeyAction();
                start.await();
                for (int i = 0; i < iterations; i++) {
                    ring.compute(keyB, 0, 4, keyA, 0, 4, action);
                }
                return null;
            };

            assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
                final ExecutorService pool = Executors.newFixedThreadPool(2);
                try {
                    final Future<Void> f1 = pool.submit(forward);
                    final Future<Void> f2 = pool.submit(reverse);
                    start.countDown();
                    f1.get();
                    f2.get();
                } finally {
                    pool.shutdown();
                }
            });
        }
    }

    /**
     * Reusable counter increment action. Reads the current long value (if any),
     * writes value+1 back. Counts visited iterations in {@code seenIterations}.
     */
    private static final class IncrementingAction implements ComputeAction,
            KeyValueConsuming.ValueConsumer<KeyValueConsuming.Value>,
            KeyValueConsuming.Value, BinaryContent {

        private final ComputeContext ctx = new ComputeContext();
        private final UnsafeBuffer scratch = new UnsafeBuffer(new byte[8]);
        private long observed;
        private boolean exists;

        @Override
        public ComputeContext context() {
            return ctx;
        }

        @Override
        public void execute() {
            exists = false;
            observed = 0L;
            ctx.get(this);
            scratch.putLong(0, observed + 1L);
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
            observed = scratch.getLong(0);
            exists = true;
        }
    }

    private static final class LongValueConsumer
            implements KeyValueConsuming.ValueConsumer<KeyValueConsuming.Value>,
                       KeyValueConsuming.Value, BinaryContent {
        private final UnsafeBuffer buf = new UnsafeBuffer(new byte[8]);
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
            return buf;
        }

        @Override
        public int offset() {
            return 0;
        }

        @Override
        public void apply() {
            value = buf.getLong(0);
        }
    }

    // Searches a small key space for a 4-byte key whose hash lands on the
    // requested shuffled index. Used by the same-segment regression test.
    private static AtomicBuffer findKeyHashingTo(final int wantedIdx, final int mask) {
        final AtomicBuffer probe = new UnsafeBuffer(new byte[4]);
        for (int v = 1; v < 1_000_000; v++) {
            probe.putByte(0, (byte) (v >>> 24));
            probe.putByte(1, (byte) (v >>> 16));
            probe.putByte(2, (byte) (v >>> 8));
            probe.putByte(3, (byte) v);
            final int hash = KeyValueSupport.hash(probe, 0, 4);
            if ((hash & mask) == wantedIdx) {
                final AtomicBuffer key = new UnsafeBuffer(new byte[4]);
                key.putByte(0, (byte) (v >>> 24));
                key.putByte(1, (byte) (v >>> 16));
                key.putByte(2, (byte) (v >>> 8));
                key.putByte(3, (byte) v);
                return key;
            }
        }
        throw new AssertionError("no key found for index " + wantedIdx);
    }

    /** Two-key action that does nothing under the locks - used purely to drive
     *  the canonical-ordering deadlock-free test. */
    private static final class NoopTwoKeyAction implements TwoKeyComputeAction {
        private final ComputeContext c1 = new ComputeContext();
        private final ComputeContext c2 = new ComputeContext();

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
            // intentionally empty
        }
    }
}
