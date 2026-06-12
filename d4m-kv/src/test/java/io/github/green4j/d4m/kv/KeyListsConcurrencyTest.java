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
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Concurrency stress tests for {@link KeyListStorage} against a thread-safe
 * {@link KeyValueRing}. Verifies that {@code KeyLists} inherits the threading
 * guarantees of the backing {@link KeyValues} without adding any locks of its
 * own.
 */
class KeyListsConcurrencyTest {
    private static final int KV_BUFFER_SIZE = 64 * 1024;
    private static final int TIER_INITIAL_CAPACITY = 1024;
    private static final int RING_SIZE = 8;

    private static SegmentFactory segmentFactory() {
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

    @Test
    void singleWriterManyReaders() throws Exception {
        final KeyValueRing ring = new KeyValueRing(RING_SIZE, segmentFactory());
        final KeyListStorage lists = new KeyListStorage(ring);

        final String[] keys = {"alpha", "beta", "gamma", "delta", "epsilon"};
        final int writesPerKey = 4_000;
        final int readerCount = 4;
        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicLong readerMismatches = new AtomicLong();
        final AtomicLong readerCalls = new AtomicLong();

        final ExecutorService pool = Executors.newFixedThreadPool(1 + readerCount);
        final CountDownLatch ready = new CountDownLatch(1);

        final Future<?> writerFuture = pool.submit(() -> {
            final AtomicBuffer keyBuffer = new UnsafeBuffer(new byte[64]);
            final AtomicBuffer valueBuffer = new UnsafeBuffer(new byte[64]);
            final KeyListsWriter w = lists.newWriter();
            try {
                ready.await();
                for (int round = 0; round < writesPerKey; round++) {
                    for (final String k : keys) {
                        final byte[] kb = k.getBytes();
                        keyBuffer.putBytes(0, kb);
                        final String value = k + "-" + round;
                        final byte[] vb = value.getBytes();
                        valueBuffer.putBytes(0, vb);
                        w.append(keyBuffer, 0, kb.length,
                                valueBuffer, 0, vb.length);
                    }
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                stop.set(true);
            }
        });

        for (int r = 0; r < readerCount; r++) {
            pool.submit(() -> {
                final ListAccessor acc = new ListAccessor();
                final AtomicBuffer keyBuffer = new UnsafeBuffer(new byte[64]);
                final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
                final int[] lastSeen = new int[keys.length];
                Arrays.fill(lastSeen, -1);
                try {
                    ready.await();
                    while (!stop.get()) {
                        for (int ki = 0; ki < keys.length; ki++) {
                            final String k = keys[ki];
                            final byte[] kb = k.getBytes();
                            keyBuffer.putBytes(0, kb);
                            lists.list(acc, keyBuffer, 0, kb.length);
                            final int size = acc.size();
                            if (size <= lastSeen[ki]) {
                                continue;
                            }
                            // Spot-check a handful of indices; verify reading
                            // size-1 returns the right payload.
                            if (size > 0) {
                                final boolean ok = acc.get(size - 1, consumer);
                                if (!ok) {
                                    readerMismatches.incrementAndGet();
                                    continue;
                                }
                                final String expected = k + "-" + (size - 1);
                                final String actual = new String(
                                        consumer.array(), 0, consumer.valueSize());
                                if (!expected.equals(actual)) {
                                    readerMismatches.incrementAndGet();
                                }
                            }
                            lastSeen[ki] = size;
                            readerCalls.incrementAndGet();
                        }
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        ready.countDown();
        writerFuture.get();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        // Verify the writer's final state from the main thread.
        final ListAccessor acc = new ListAccessor();
        final AtomicBuffer keyBuffer = new UnsafeBuffer(new byte[64]);
        for (final String k : keys) {
            final byte[] kb = k.getBytes();
            keyBuffer.putBytes(0, kb);
            assertTrue(lists.list(acc, keyBuffer, 0, kb.length));
            assertEquals(writesPerKey, acc.size(), "final size for " + k);
        }

        assertEquals(0L, readerMismatches.get(), "readers must see no torn payloads");
        assertTrue(readerCalls.get() > 0L, "readers must have observed updates");
    }

    @Test
    void multipleWritersDifferentKeys() throws Exception {
        final KeyValueRing ring = new KeyValueRing(RING_SIZE, segmentFactory());
        final KeyListStorage lists = new KeyListStorage(ring);

        final int writerCount = 4;
        final int writesPerWriter = 5_000;

        final CountDownLatch ready = new CountDownLatch(1);
        final ExecutorService pool = Executors.newFixedThreadPool(writerCount);

        for (int w = 0; w < writerCount; w++) {
            final int writerId = w;
            pool.submit(() -> {
                final KeyListsWriter writer = lists.newWriter();
                final AtomicBuffer keyBuffer = new UnsafeBuffer(new byte[16]);
                final AtomicBuffer valueBuffer = new UnsafeBuffer(new byte[32]);
                final byte[] kb = ("writer-" + writerId).getBytes();
                keyBuffer.putBytes(0, kb);
                try {
                    ready.await();
                    for (int i = 0; i < writesPerWriter; i++) {
                        final byte[] vb = ("v-" + writerId + "-" + i).getBytes();
                        valueBuffer.putBytes(0, vb);
                        writer.append(keyBuffer, 0, kb.length,
                                valueBuffer, 0, vb.length);
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        ready.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));

        final ListAccessor acc = new ListAccessor();
        final AtomicBuffer keyBuffer = new UnsafeBuffer(new byte[16]);
        final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
        for (int wid = 0; wid < writerCount; wid++) {
            final byte[] kb = ("writer-" + wid).getBytes();
            keyBuffer.putBytes(0, kb);
            assertTrue(lists.list(acc, keyBuffer, 0, kb.length));
            assertEquals(writesPerWriter, acc.size());
            // Entries must appear in the order each writer appended them.
            for (int i = 0; i < writesPerWriter; i++) {
                assertTrue(acc.get(i, consumer));
                final String expected = "v-" + wid + "-" + i;
                final String actual = new String(consumer.array(), 0, consumer.valueSize());
                assertEquals(expected, actual);
            }
        }
    }

    @Test
    void multipleWritersSameKey() throws Exception {
        final KeyValueRing ring = new KeyValueRing(RING_SIZE, segmentFactory());
        final KeyListStorage lists = new KeyListStorage(ring);

        final int writerCount = 4;
        final int writesPerWriter = 2_500;
        final int total = writerCount * writesPerWriter;

        final CountDownLatch ready = new CountDownLatch(1);
        final ExecutorService pool = Executors.newFixedThreadPool(writerCount);

        final String key = "hot-key";
        final byte[] kb = key.getBytes();

        for (int w = 0; w < writerCount; w++) {
            final int writerId = w;
            pool.submit(() -> {
                final KeyListsWriter writer = lists.newWriter();
                final AtomicBuffer keyBuffer = new UnsafeBuffer(new byte[16]);
                final AtomicBuffer valueBuffer = new UnsafeBuffer(new byte[16]);
                keyBuffer.putBytes(0, kb);
                try {
                    ready.await();
                    for (int i = 0; i < writesPerWriter; i++) {
                        // Encode (writerId, i) as two int32 BE so we can later
                        // recover the (writerId, i) pair from each entry.
                        valueBuffer.putByte(0, (byte) (writerId >>> 24));
                        valueBuffer.putByte(1, (byte) (writerId >>> 16));
                        valueBuffer.putByte(2, (byte) (writerId >>> 8));
                        valueBuffer.putByte(3, (byte) writerId);
                        valueBuffer.putByte(4, (byte) (i >>> 24));
                        valueBuffer.putByte(5, (byte) (i >>> 16));
                        valueBuffer.putByte(6, (byte) (i >>> 8));
                        valueBuffer.putByte(7, (byte) i);
                        writer.append(keyBuffer, 0, kb.length,
                                valueBuffer, 0, 8);
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        ready.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));

        // Every (writerId, i) pair must be present exactly once.
        final ListAccessor acc = new ListAccessor();
        final AtomicBuffer keyBuffer = new UnsafeBuffer(new byte[16]);
        keyBuffer.putBytes(0, kb);
        assertTrue(lists.list(acc, keyBuffer, 0, kb.length));
        assertEquals(total, acc.size());

        final BitSet seen = new BitSet(total);
        final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
        for (int i = 0; i < total; i++) {
            assertTrue(acc.get(i, consumer));
            assertEquals(8, consumer.valueSize());
            final byte[] vb = consumer.array();
            final int writerId = ((vb[0] & 0xFF) << 24) | ((vb[1] & 0xFF) << 16)
                    | ((vb[2] & 0xFF) << 8) | (vb[3] & 0xFF);
            final int seq = ((vb[4] & 0xFF) << 24) | ((vb[5] & 0xFF) << 16)
                    | ((vb[6] & 0xFF) << 8) | (vb[7] & 0xFF);
            assertTrue(writerId >= 0 && writerId < writerCount,
                    "writerId out of range: " + writerId);
            assertTrue(seq >= 0 && seq < writesPerWriter,
                    "seq out of range: " + seq);
            final int bit = writerId * writesPerWriter + seq;
            assertFalse(seen.get(bit), "duplicate (writerId, seq) = (" + writerId + "," + seq + ")");
            seen.set(bit);
        }
        assertEquals(total, seen.cardinality(), "every (writerId, seq) pair must appear");
    }
}
