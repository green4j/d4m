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
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Concurrency stress-tests for the key-value storage engine.
 *
 * <p>Each test runs a single writer thread alongside multiple reader threads
 * for a configurable duration (default 10 s). The goal is to shake out
 * visibility bugs, torn reads, lock contention issues, eviction hazards,
 * and tier-cascade consistency problems that only surface under sustained
 * multi-threaded pressure.</p>
 *
 * <pre>
 * Tuning knobs (system properties):
 *   -Dstress.duration.ms=30000   longer run  (default 10 000)
 *   -Dstress.readers=8           more reader threads (default 4)
 * </pre>
 */
class ConcurrencyStressTest {
    private static final long DURATION_MS =
            Long.parseLong(System.getProperty("stress.duration.ms", "10000"));
    private static final int READERS =
            Integer.parseInt(System.getProperty("stress.readers", "4"));

    // Value layout (24 bytes):
    // [0..7]   sequence number  (long)
    // [8..15]  Key hash         (long - widened from int)
    // [16..23] Checksum         (sequence ^ keyHash ^ SEED)
    private static final int VALUE_SIZE = 24;
    private static final long SEED = 0xDEAD_BEEF_CAFE_BABEL;

    // Hot tier: 32 KB - small enough to trigger eviction quickly
    private static final int SMALL_HOT_TIER = 32 * 1024;
    // Medium hot tier: 256 KB - enough room for several thousand entries
    private static final int MEDIUM_HOT_TIER = 256 * 1024;
    // Mmap tier: 1 MB
    private static final int MMAP_TIER = 1024 * 1024;

    @TempDir
    File dir;

    private static AtomicBuffer keyOf(final long seq) {
        final byte[] bytes = ("k" + seq).getBytes();
        return new UnsafeBuffer(bytes);
    }

    private static void encodeValue(final AtomicBuffer buf,
                                    final int offset,
                                    final long sequence,
                                    final int keyHash) {
        final long wideHash = keyHash & 0xFFFFFFFFL;
        buf.putLong(offset, sequence);
        buf.putLong(offset + 8, wideHash);
        buf.putLong(offset + 16, sequence ^ wideHash ^ SEED);
    }

    private static void checkValue(final byte[] value,
                                   final int valueSize,
                                   final String context) {
        if (valueSize < VALUE_SIZE) {
            throw new AssertionError(context + ": short value " + valueSize);
        }
        final AtomicBuffer buf = new UnsafeBuffer(value);
        final long s = buf.getLong(0);
        final long h = buf.getLong(8);
        final long c = buf.getLong(16);
        if (c != (s ^ h ^ SEED)) {
            throw new AssertionError(context
                    + ": value corrupt: seq=" + s
                    + " hash=0x" + Long.toHexString(h)
                    + " expect=0x" + Long.toHexString(s ^ h ^ SEED)
                    + " got=0x" + Long.toHexString(c));
        }
    }

    private KeyValueRing createRing(final int ringSize,
                                    final int hotTierSize,
                                    final int maxTiers) {
        return new KeyValueRing(
                ringSize,
                16,
                index -> new KeyValueSegment(
                        1,
                        new MmapTierFactory(
                                index,
                                hotTierSize,
                                false,
                                1024,
                                MMAP_TIER,
                                1024,
                                dir,
                                maxTiers,
                                null
                        ),
                        null
                )
        );
    }

    private static void join(final List<Future<?>> futures) throws Exception {
        final List<Throwable> errors = new ArrayList<>();
        for (final Future<?> f : futures) {
            try {
                f.get(60, TimeUnit.SECONDS);
            } catch (final ExecutionException e) {
                errors.add(e.getCause());
            } catch (final TimeoutException e) {
                errors.add(e);
            }
        }
        if (!errors.isEmpty()) {
            final AssertionError ae =
                    new AssertionError(errors.size() + " thread failure(s)");
            errors.forEach(ae::addSuppressed);
            throw ae;
        }
    }

    private Callable<Void> writer(final KeyValues storage,
                                  final AtomicBoolean run,
                                  final AtomicLong written,
                                  final CountDownLatch go) {
        return () -> {
            go.await();
            final UnsafeBuffer valueBuf = new UnsafeBuffer(new byte[VALUE_SIZE]);
            long seq = 0;
            while (run.get()) {
                final AtomicBuffer key = keyOf(seq);
                final int hash = KeyValueSupport.hash(key);
                encodeValue(valueBuf, 0, seq, hash);

                storage.put(key, 0, key.capacity(),
                        valueBuf, 0, VALUE_SIZE);
                seq++;
                if ((seq & 0x3F) == 0) {
                    written.lazySet(seq);
                }
            }
            written.set(seq);
            return null;
        };
    }

    private Callable<Void> reader(final KeyValues storage,
                                  final AtomicBoolean run,
                                  final AtomicLong written,
                                  final CountDownLatch go) {
        return () -> {
            go.await();
            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            long hits = 0;
            long misses = 0;
            while (run.get()) {
                final long maxSeq = written.get();
                if (maxSeq < 1) {
                    Thread.yield();
                    continue;
                }
                final long target = ThreadLocalRandom.current().nextLong(maxSeq);
                final AtomicBuffer key = keyOf(target);
                final boolean found = storage.get(
                        key, 0, key.capacity(), consumer);
                if (found) {
                    checkValue(consumer.array(), consumer.valueSize(),
                            "get(key=" + target + ")");
                    hits++;
                } else {
                    misses++;
                }
            }
            assertTrue(hits + misses > 0,
                    "reader must perform operations");
            return null;
        };
    }

    @Test
    void writerWithConcurrentReaders() throws Exception {
        final KeyValueRing ring = createRing(4, MEDIUM_HOT_TIER, 4);
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong written = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        fs.add(ex.submit(writer(ring, run, written, go)));
        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(reader(ring, run, written, go)));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        assertTrue(written.get() > 1000,
                "Writer must produce significant volume; got " + written.get());
        System.out.printf("  [writerWithReaders] wrote %,d entries%n",
                written.get());
    }

    @Test
    void heavyEviction() throws Exception {
        final KeyValueRing ring = createRing(2, SMALL_HOT_TIER, 3);
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong written = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        fs.add(ex.submit(writer(ring, run, written, go)));
        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(reader(ring, run, written, go)));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        assertTrue(written.get() > 1000,
                "Writer must produce significant volume; got " + written.get());
        System.out.printf("  [heavyEviction] wrote %,d entries%n",
                written.get());
    }

    @Test
    void updateInPlace() throws Exception {
        final KeyValueRing ring = createRing(4, MEDIUM_HOT_TIER, 4);
        final int keySpace = 500;
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong ops = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        // Writer: continuously updates a fixed set of keys
        fs.add(ex.submit(() -> {
            go.await();
            final UnsafeBuffer valueBuf = new UnsafeBuffer(new byte[VALUE_SIZE]);
            long seq = 0;
            while (run.get()) {
                final long keyId = seq % keySpace;
                final AtomicBuffer key = keyOf(keyId);
                final int hash = KeyValueSupport.hash(key);
                encodeValue(valueBuf, 0, seq, hash);

                ring.put(key, 0, key.capacity(),
                        valueBuf, 0, VALUE_SIZE);
                seq++;
                if ((seq & 0x3F) == 0) {
                    ops.lazySet(seq);
                }
            }
            ops.set(seq);
            return null;
        }));

        // Readers: verify every retrieved value has a consistent checksum
        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(() -> {
                go.await();
                final ByteArrayValueConsumer consumer =
                        new ByteArrayValueConsumer();
                while (run.get()) {
                    final long maxOps = ops.get();
                    if (maxOps < keySpace) {
                        Thread.yield();
                        continue;
                    }
                    final long keyId = ThreadLocalRandom.current()
                            .nextLong(keySpace);
                    final AtomicBuffer key = keyOf(keyId);
                    if (ring.get(key, 0, key.capacity(), consumer)) {
                        checkValue(consumer.array(), consumer.valueSize(),
                                "update-get(key=" + keyId + ")");
                    }
                }
                return null;
            }));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        assertTrue(ops.get() > keySpace,
                "Writer must produce significant volume; got " + ops.get());
        System.out.printf("  [updateInPlace] ops %,d (key space %d)%n",
                ops.get(), keySpace);
    }

    @Test
    void completenessAfterDrain() {
        final KeyValueRing ring = createRing(4, MEDIUM_HOT_TIER, 8);
        final int n = 5_000;
        final UnsafeBuffer valueBuf = new UnsafeBuffer(new byte[VALUE_SIZE]);

        for (int i = 0; i < n; i++) {
            final AtomicBuffer key = keyOf(i);
            final int hash = KeyValueSupport.hash(key);
            encodeValue(valueBuf, 0, i, hash);
            ring.put(key, 0, key.capacity(), valueBuf, 0, VALUE_SIZE);
        }

        // Read every key back and verify
        final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
        int found = 0;
        for (int i = 0; i < n; i++) {
            final AtomicBuffer key = keyOf(i);
            if (ring.get(key, 0, key.capacity(), consumer)) {
                checkValue(consumer.array(), consumer.valueSize(),
                        "completeness(key=" + i + ")");
                found++;
            }
        }

        assertEquals(n, found, "All keys must be retrievable");
        System.out.printf("  [completeness] verified %,d keys%n", n);
    }

    @Test
    void completenessWithConcurrentReaders() throws Exception {
        final KeyValueRing ring = createRing(4, MEDIUM_HOT_TIER, 8);
        final int n = 5_000;
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong phase = new AtomicLong(0); // 0 = writing, 1 = done
        final AtomicLong written = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        // Writer: sequential put of n entries then stops
        fs.add(ex.submit(() -> {
            go.await();
            final UnsafeBuffer valueBuf =
                    new UnsafeBuffer(new byte[VALUE_SIZE]);
            for (int i = 0; i < n; i++) {
                final AtomicBuffer key = keyOf(i);
                final int hash = KeyValueSupport.hash(key);
                encodeValue(valueBuf, 0, i, hash);
                ring.put(key, 0, key.capacity(),
                        valueBuf, 0, VALUE_SIZE);
                if ((i & 0x3F) == 0) {
                    written.lazySet(i);
                }
            }
            written.set(n);
            phase.set(1);
            while (run.get()) {
                LockSupport.parkNanos(5_000_000);
            }
            return null;
        }));

        // Readers: continuously get random keys, verify checksum
        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(reader(ring, run, written, go)));
        }

        go.countDown();
        while (phase.get() == 0) {
            Thread.sleep(50);
        }
        Thread.sleep(2000);
        run.set(false);
        join(fs);
        ex.shutdown();

        // Final completeness check
        final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
        int found = 0;
        for (int i = 0; i < n; i++) {
            final AtomicBuffer key = keyOf(i);
            if (ring.get(key, 0, key.capacity(), consumer)) {
                checkValue(consumer.array(), consumer.valueSize(),
                        "concurrent-completeness(key=" + i + ")");
                found++;
            }
        }
        assertEquals(n, found,
                "All keys must be retrievable after concurrent access");
        System.out.printf("  [completeness+readers] verified %,d keys%n", n);
    }

    @Test
    void heavyEvictionWithConcurrentReaders() throws Exception {
        // Very small hot tier with limited mmap tiers -> frequent eviction
        final KeyValueRing ring = createRing(2, SMALL_HOT_TIER, 2);
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong written = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final AtomicLong readerHits = new AtomicLong();
        final AtomicLong readerMisses = new AtomicLong();
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        fs.add(ex.submit(writer(ring, run, written, go)));

        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(() -> {
                go.await();
                final ByteArrayValueConsumer consumer =
                        new ByteArrayValueConsumer();
                while (run.get()) {
                    final long maxSeq = written.get();
                    if (maxSeq < 1) {
                        Thread.yield();
                        continue;
                    }
                    final long target =
                            ThreadLocalRandom.current().nextLong(maxSeq);
                    final AtomicBuffer key = keyOf(target);
                    if (ring.get(key, 0, key.capacity(), consumer)) {
                        checkValue(consumer.array(),
                                consumer.valueSize(),
                                "eviction-get(key=" + target + ")");
                        readerHits.incrementAndGet();
                    } else {
                        readerMisses.incrementAndGet();
                    }
                }
                return null;
            }));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        assertTrue(written.get() > 1000);
        // Under heavy eviction, misses are expected
        System.out.printf(
                "  [heavyEviction+readers] wrote %,d  hits %,d  misses %,d%n",
                written.get(), readerHits.get(), readerMisses.get());
    }

    @Test
    void valueOverwriteConsistency() throws Exception {
        final KeyValueRing ring = createRing(4, MEDIUM_HOT_TIER, 4);
        final int keySpace = 100;
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong rounds = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        // Seed all keys first
        final UnsafeBuffer seedBuf = new UnsafeBuffer(new byte[VALUE_SIZE]);
        for (int i = 0; i < keySpace; i++) {
            final AtomicBuffer key = keyOf(i);
            final int hash = KeyValueSupport.hash(key);
            encodeValue(seedBuf, 0, 0, hash);
            ring.put(key, 0, key.capacity(), seedBuf, 0, VALUE_SIZE);
        }

        // Writer: round-robin update all keys with increasing sequence
        fs.add(ex.submit(() -> {
            go.await();
            final UnsafeBuffer valueBuf =
                    new UnsafeBuffer(new byte[VALUE_SIZE]);
            long round = 1;
            while (run.get()) {
                for (int i = 0; i < keySpace && run.get(); i++) {
                    final AtomicBuffer key = keyOf(i);
                    final int hash = KeyValueSupport.hash(key);
                    encodeValue(valueBuf, 0, round, hash);
                    ring.put(key, 0, key.capacity(),
                            valueBuf, 0, VALUE_SIZE);
                }
                rounds.set(round);
                round++;
            }
            return null;
        }));

        // Readers: fetch random keys and verify checksum integrity
        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(() -> {
                go.await();
                final ByteArrayValueConsumer consumer =
                        new ByteArrayValueConsumer();
                while (run.get()) {
                    final int keyId =
                            ThreadLocalRandom.current().nextInt(keySpace);
                    final AtomicBuffer key = keyOf(keyId);
                    if (ring.get(key, 0, key.capacity(), consumer)) {
                        checkValue(consumer.array(),
                                consumer.valueSize(),
                                "overwrite-get(key=" + keyId + ")");
                    }
                }
                return null;
            }));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        assertTrue(rounds.get() > 10,
                "Writer must complete many rounds; got " + rounds.get());
        System.out.printf(
                "  [overwriteConsistency] %,d rounds over %d keys%n",
                rounds.get(), keySpace);
    }

    @Test
    void multiSegmentStress() throws Exception {
        final KeyValueRing ring = createRing(8, MEDIUM_HOT_TIER, 4);
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong written = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        fs.add(ex.submit(writer(ring, run, written, go)));
        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(reader(ring, run, written, go)));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        assertTrue(written.get() > 1000);
        System.out.printf("  [multiSegment] wrote %,d entries across %d segments%n",
                written.get(), ring.numberOfSegments());
    }

    @Test
    void realtimeTailing() throws Exception {
        final KeyValueRing ring = createRing(4, MEDIUM_HOT_TIER, 4);
        final int keySpace = 200;
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong written = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        // Slow writer: ~1000 ops/s
        fs.add(ex.submit(() -> {
            go.await();
            final UnsafeBuffer valueBuf =
                    new UnsafeBuffer(new byte[VALUE_SIZE]);
            long seq = 0;
            while (run.get()) {
                final long keyId = seq % keySpace;
                final AtomicBuffer key = keyOf(keyId);
                final int hash = KeyValueSupport.hash(key);
                encodeValue(valueBuf, 0, seq, hash);
                ring.put(key, 0, key.capacity(),
                        valueBuf, 0, VALUE_SIZE);
                seq++;
                written.set(seq);
                LockSupport.parkNanos(1_000_000); // 1 ms
            }
            return null;
        }));

        // Reader: continuously polls recently written keys
        fs.add(ex.submit(() -> {
            go.await();
            final ByteArrayValueConsumer consumer =
                    new ByteArrayValueConsumer();
            long checked = 0;
            while (run.get()) {
                final long maxSeq = written.get();
                if (maxSeq < 1) {
                    Thread.yield();
                    continue;
                }
                // Check recently-written keys
                final long start = Math.max(0, maxSeq - keySpace);
                for (long s = start; s < maxSeq; s++) {
                    final long keyId = s % keySpace;
                    final AtomicBuffer key = keyOf(keyId);
                    if (ring.get(key, 0, key.capacity(), consumer)) {
                        checkValue(consumer.array(),
                                consumer.valueSize(),
                                "tailing(key=" + keyId + ")");
                        checked++;
                    }
                }
                LockSupport.parkNanos(500_000); // 0.5 ms
            }
            assertTrue(checked > 100,
                    "Tailing reader must verify values; got " + checked);
            return null;
        }));

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        System.out.printf("  [realtimeTailing] wrote %,d entries%n",
                written.get());
    }

    @Test
    void readerChurn() throws Exception {
        final KeyValueRing ring = createRing(4, MEDIUM_HOT_TIER, 4);
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong written = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final AtomicLong totalCycles = new AtomicLong();
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        fs.add(ex.submit(writer(ring, run, written, go)));

        // Many threads each rapidly create a consumer, do a few gets,
        // Then discard - stress allocation/GC/lock churn
        final int churners = 8;
        for (int r = 0; r < churners; r++) {
            fs.add(ex.submit(() -> {
                go.await();
                long cycles = 0;
                while (run.get()) {
                    final long maxSeq = written.get();
                    if (maxSeq < 1) {
                        Thread.yield();
                        continue;
                    }
                    final ByteArrayValueConsumer consumer =
                            new ByteArrayValueConsumer();
                    final int batch = ThreadLocalRandom.current()
                            .nextInt(1, 20);
                    for (int b = 0; b < batch; b++) {
                        final long target =
                                ThreadLocalRandom.current().nextLong(maxSeq);
                        final AtomicBuffer key = keyOf(target);
                        if (ring.get(key, 0, key.capacity(), consumer)) {
                            checkValue(consumer.array(),
                                    consumer.valueSize(),
                                    "churn-get(key=" + target + ")");
                        }
                    }
                    cycles++;
                }
                totalCycles.addAndGet(cycles);
                return null;
            }));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        assertTrue(totalCycles.get() > 1000,
                "churn cycles too low: " + totalCycles.get());
        System.out.printf("  [readerChurn] cycles %,d  wrote %,d%n",
                totalCycles.get(), written.get());
    }

    @Test
    void mixedGetAndMissStress() throws Exception {
        final KeyValueRing ring = createRing(4, MEDIUM_HOT_TIER, 4);
        final int existingKeys = 1000;
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong ops = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);

        // Seed keys
        final UnsafeBuffer valueBuf = new UnsafeBuffer(new byte[VALUE_SIZE]);
        for (int i = 0; i < existingKeys; i++) {
            final AtomicBuffer key = keyOf(i);
            final int hash = KeyValueSupport.hash(key);
            encodeValue(valueBuf, 0, i, hash);
            ring.put(key, 0, key.capacity(), valueBuf, 0, VALUE_SIZE);
        }

        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        // Writer: continuously adds new keys beyond existingKeys
        fs.add(ex.submit(() -> {
            go.await();
            final UnsafeBuffer vBuf = new UnsafeBuffer(new byte[VALUE_SIZE]);
            long seq = existingKeys;
            while (run.get()) {
                final AtomicBuffer key = keyOf(seq);
                final int hash = KeyValueSupport.hash(key);
                encodeValue(vBuf, 0, seq, hash);
                ring.put(key, 0, key.capacity(), vBuf, 0, VALUE_SIZE);
                seq++;
                ops.lazySet(seq);
            }
            ops.set(seq);
            return null;
        }));

        // Readers: mix of existing keys, new keys, and non-existent keys
        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(() -> {
                go.await();
                final ByteArrayValueConsumer consumer =
                        new ByteArrayValueConsumer();
                long hits = 0;
                long misses = 0;
                while (run.get()) {
                    final long maxOps = ops.get();
                    final long target;
                    final double dice = ThreadLocalRandom.current()
                            .nextDouble();
                    if (dice < 0.4) {
                        // Existing key
                        target = ThreadLocalRandom.current()
                                .nextLong(existingKeys);
                    } else if (dice < 0.7 && maxOps > existingKeys) {
                        // Recently written key
                        target = existingKeys
                                + ThreadLocalRandom.current().nextLong(
                                maxOps - existingKeys);
                    } else {
                        // Non-existent key (negative space)
                        target = maxOps
                                + ThreadLocalRandom.current()
                                .nextLong(10_000);
                    }
                    final AtomicBuffer key = keyOf(target);
                    if (ring.get(key, 0, key.capacity(), consumer)) {
                        checkValue(consumer.array(),
                                consumer.valueSize(),
                                "mixed-get(key=" + target + ")");
                        hits++;
                    } else {
                        misses++;
                    }
                }
                assertTrue(hits + misses > 0);
                return null;
            }));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        System.out.printf("  [mixedGetMiss] ops %,d%n", ops.get());
    }

    @Test
    void tieredStorageStress() throws Exception {
        final KeyValueStorage storage = KeyValueStorage.builder()
                .withTotalMainMemory(MEDIUM_HOT_TIER * 4L)
                .withHeapMainMemory()
                .withMmapFileSize(MMAP_TIER)
                .withRingSize(4)
                .withPrepareMmapFilesOnStart()
                .withMemoryMappedFilesFolder(dir)
                .build();

        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong written = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        fs.add(ex.submit(writer(storage, run, written, go)));
        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(reader(storage, run, written, go)));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        assertTrue(written.get() > 1000);
        System.out.printf("  [tieredStorage] wrote %,d entries%n",
                written.get());
    }
}
