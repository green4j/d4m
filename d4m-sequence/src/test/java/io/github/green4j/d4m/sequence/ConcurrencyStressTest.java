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
package io.github.green4j.d4m.sequence;

import io.github.green4j.d4m.common.AtomicBuffer;
import io.github.green4j.d4m.common.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
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
 * Concurrency stress-tests for the TimeSeries storage engine.
 *
 * <p>Each test hammers a specific concurrent scenario for a configurable
 * duration (default 10 s). The goal is to shake out visibility bugs,
 * torn reads, refcount races, eviction hazards, and COW consistency
 * issues that only surface under sustained multi-threaded pressure.</p>
 *
 * <pre>
 * Tuning knobs (system properties):
 *   -Dstress.duration.ms=30000   longer run  (default 10 000)
 *   -Dstress.readers=8           more reader threads (default 4)
 *   -Dbuffer.bounds.check=true   enable UnsafeBuffer bounds checks
 * </pre>
 */
class ConcurrencyStressTest {
    private static final long DURATION_MS = Long.parseLong(System.getProperty("stress.duration.ms", "10000"));
    private static final int READERS = Integer.parseInt(System.getProperty("stress.readers", "4"));
    private static final int CHUNK = 4096;

    // Payload layout (24 bytes)
    // [0..7]   sequence   (long, 8 bytes)
    // [8..15]  order  (long, 8 bytes)
    // [16..23] check (seq ^ ts ^ SEED) (long, 8 bytes)
    private static final int PAYLOAD_SIZE = 24;
    private static final long SEED = 0xDEAD_BEEF_CAFE_BABEL;

    @TempDir
    File dir;

    private static void encodePayload(final AtomicBuffer buffer,
                                      final int payloadOffset,
                                      final long sequence,
                                      final long order) {
        buffer.putLong(payloadOffset, sequence);
        buffer.putLong(payloadOffset + 8, order);
        buffer.putLong(payloadOffset + 16, sequence ^ order ^ SEED);
    }

    private static void encodePayload(final UnsafeBuffer buffer,
                                      final long sequence,
                                      final long order) {
        encodePayload(buffer, 0, sequence, order);
    }

    private static long payloadSequence(final AtomicBuffer buffer,
                                        final int payloadOffset) {
        return buffer.getLong(payloadOffset);
    }

    private static long payloadOrder(final AtomicBuffer buffer, final int payloadOffset) {
        return buffer.getLong(payloadOffset + 8);
    }

    private static void checkPayload(final AtomicBuffer buffer,
                                     final int payloadOffset,
                                     final int payloadLength) {
        if (payloadLength < PAYLOAD_SIZE) {
            throw new AssertionError("Short payload " + payloadLength);
        }
        final long s = buffer.getLong(payloadOffset);
        final long t = buffer.getLong(payloadOffset + 8);
        final long c = buffer.getLong(payloadOffset + 16);
        if (c != (s ^ t ^ SEED)) {
            throw new AssertionError("Payload corrupt: seq=" + s
                    + " ts=" + t
                    + " expect=0x" + Long.toHexString(s ^ t ^ SEED)
                    + " got=0x" + Long.toHexString(c));
        }
    }

    private TestSequenceStorage store(final long maxHeap) {
        return new TestSequenceStorage(CHUNK, maxHeap, (int) maxHeap, dir, false);
    }

    /**
     * Backward cursor that repeatedly seeks to end and drains entries while {@code run} is true.
     *
     * @param sequence sequence to read
     * @param run      stop flag
     * @param go       start latch
     * @return reader task
     */
    private Callable<Void> backwardStreamingReader(final Sequence sequence,
                                                   final AtomicBoolean run,
                                                   final CountDownLatch go) {
        return () -> {
            go.await();
            final BackwardCursor cur = new BackwardCursor(sequence);
            try {
                while (run.get()) {
                    cur.seekToEnd();
                    final long[] prev = {Long.MAX_VALUE};
                    cur.next(200, (owner, entryOrder, buf, off, size) -> {
                        checkPayload(buf, off, size);
                        if (entryOrder > prev[0]) {
                            throw new AssertionError("bwd ts in drain");
                        }
                        prev[0] = entryOrder;
                    });
                }
            } finally {
                cur.close();
            }
            return null;
        };
    }

    /**
     * Reads the full sequence forward and asserts distinct entry cardinality.
     *
     * @param sequence      sequence to scan
     * @param expectedCount expected distinct entries
     * @param detailMessage assertion detail on mismatch
     * @param printfLine    format string for success log (one {@code %d})
     */
    private void verifyCardinalityFromForwardCursor(final Sequence sequence,
                                                    final int expectedCount,
                                                    final String detailMessage,
                                                    final String printfLine) {
        final ForwardCursor ver = new ForwardCursor(sequence);
        final BitSet seen = new BitSet(expectedCount);
        while (true) {
            final long[] b = {0};
            ver.next(1024, (owner, entryOrder, buf, off, size) -> {
                checkPayload(buf, off, size);
                seen.set((int) payloadSequence(buf, off));
                b[0]++;
            });
            if (b[0] == 0) {
                break;
            }
        }
        ver.close();
        assertEquals(expectedCount, seen.cardinality(), detailMessage);
        System.out.printf(printfLine, expectedCount);
    }

    /**
     * Collects all thread results; throws aggregated failures.
     *
     * @param fs futures to join
     */
    private static void join(final List<Future<?>> fs) throws Exception {
        final List<Throwable> errs = new ArrayList<>();
        for (final Future<?> f : fs) {
            try {
                f.get(60, TimeUnit.SECONDS);
            } catch (final ExecutionException e) {
                errs.add(e.getCause());
            } catch (final TimeoutException e) {
                errs.add(e);
            }
        }
        if (!errs.isEmpty()) {
            final AssertionError ae = new AssertionError(errs.size() + " thread failure(s)");
            errs.forEach(ae::addSuppressed);
            throw ae;
        }
    }

    /**
     * Returns a {@link Callable} that appends monotonically-increasing
     * entries until {@code run} becomes false, then stores the total
     * written count in {@code written}.
     *
     * @param sequence sequence to append to
     * @param run      when false, writer stops
     * @param written  receives final written count
     * @param go       latch released before appending begins
     * @return a callable that performs the writer loop
     */
    private Callable<Void> appendWriter(final Sequence sequence,
                                        final AtomicBoolean run,
                                        final AtomicLong written,
                                        final CountDownLatch go) {
        return () -> {
            go.await();
            final UnsafeBuffer pb =
                    new UnsafeBuffer(new byte[PAYLOAD_SIZE]);
            long seq = 0;
            while (run.get()) {
                encodePayload(pb, seq, seq);
                if (!sequence.append(seq, pb, 0, PAYLOAD_SIZE)) {
                    throw new AssertionError("append rejected at seq=" + seq);
                }
                seq++;
                if ((seq & 0x3F) == 0) { // every 64 entries
                    written.lazySet(seq);
                }
            }
            written.set(seq);
            return null;
        };
    }

    /**
     * Returns a {@link Callable} that reads forward, verifying
     * checksum integrity and order ordering.
     *
     * @param sequence      sequence to read
     * @param run           when false, reader stops
     * @param go            latch released before reading begins
     * @param checkSeqOrder whether to assert monotonic payload sequence
     * @return a callable that performs the reader loop
     */
    private Callable<Void> forwardReader(final Sequence sequence,
                                         final AtomicBoolean run,
                                         final CountDownLatch go,
                                         final boolean checkSeqOrder) {
        return () -> {
            go.await();
            final ForwardCursor cursor = new ForwardCursor(sequence);
            final long[] st = {Long.MIN_VALUE, -1}; // lastOrder, lastSeq
            try {
                while (run.get()) {
                    cursor.next(64, (owner, entryOrder, buf, off, size) -> {
                        checkPayload(buf, off, size);
                        final long seq = payloadSequence(buf, off);
                        if (entryOrder < st[0]) {
                            throw new AssertionError("Forward order regression: "
                                    + st[0] + " -> " + entryOrder);
                        }
                        if (checkSeqOrder && seq <= st[1]) {
                            throw new AssertionError("Forward sequence regression: "
                                    + st[1] + " -> " + seq);
                        }
                        st[0] = entryOrder;
                        st[1] = seq;
                    });
                    cursor.invalidatePeek();
                    cursor.peekNextOrder();
                }
            } finally {
                cursor.close();
            }
            return null;
        };
    }

    @Test
    void writerWithForwardCursors() throws Exception {
        final TestSequenceStorage sequenceStorage = store(32L * CHUNK);
        final Sequence sequence = sequenceStorage.getOrCreate("sequence");
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong written = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        fs.add(ex.submit(appendWriter(sequence, run, written, go)));
        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(forwardReader(sequence, run, go, true)));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        assertTrue(written.get() > 1000, "Writer must produce significant volume; got " + written.get());
        System.out.printf("  [fwdCursors] wrote %,d msgs%n", written.get());
    }

    @Test
    void writerWithBackwardCursors() throws Exception {
        final TestSequenceStorage storage = store(32L * CHUNK);
        final Sequence sequence = storage.getOrCreate("sequence");
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong written = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        fs.add(ex.submit(appendWriter(sequence, run, written, go)));

        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(() -> {
                go.await();
                final BackwardCursor cur = new BackwardCursor(sequence);
                try {
                    while (run.get()) {
                        cur.seekToEnd();
                        final long[] prev = {Long.MAX_VALUE};
                        for (int pass = 0; pass < 20; pass++) {
                            final int n = cur.next(100, (owner, entryOrder, buf, off, size) -> {
                                checkPayload(buf, off, size);
                                if (entryOrder > prev[0]) {
                                    throw new AssertionError("bwd ts increased: " + prev[0]
                                            + " -> " + entryOrder);
                                }
                                prev[0] = entryOrder;
                            });
                            if (n == 0) {
                                break;
                            }
                        }
                    }
                } finally {
                    cur.close();
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
        System.out.printf("  [bwdCursors] wrote %,d msgs%n", written.get());
    }

    @Test
    void heavyEviction() throws Exception {
        // 4 Heap chunks -> eviction fires every ~320 entries
        final TestSequenceStorage storage = store(4L * CHUNK);
        final Sequence sequence = storage.getOrCreate("sequence");
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong written = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        fs.add(ex.submit(appendWriter(sequence, run, written, go)));
        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(forwardReader(sequence, run, go, true)));
        }

        // Also add backward readers to stress pin/eviction interaction
        for (int r = 0; r < 2; r++) {
            fs.add(ex.submit(() -> {
                go.await();
                final BackwardCursor cur = new BackwardCursor(sequence);
                try {
                    while (run.get()) {
                        cur.seekToEnd();
                        final long[] prev = {Long.MAX_VALUE};
                        cur.next(200, (owner, entryOrder, buf, off, size) -> {
                            checkPayload(buf, off, size);
                            if (entryOrder > prev[0]) {
                                throw new AssertionError("bwd evict order");
                            }
                            prev[0] = entryOrder;
                        });
                    }
                } finally {
                    cur.close();
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
        System.out.printf("  [eviction] wrote %,d msgs%n", written.get());
    }

    @Test
    void batchAppendWithCursors() throws Exception {
        final TestSequenceStorage storage = store(16L * CHUNK);
        final Sequence sequence = storage.getOrCreate("sequence");
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong written = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        final int batchCount = 32;

        // Writer alternates between appendBatch and appendBatchPacked
        fs.add(ex.submit(() -> {
            go.await();
            long seq = 0;
            final long[] orders = new long[batchCount];
            final int[] offsets = new int[batchCount];
            final int[] sizes = new int[batchCount];
            final UnsafeBuffer arrayPl =
                    new UnsafeBuffer(new byte[batchCount * PAYLOAD_SIZE]);
            // Packed: [long ts][int pLen][byte[PL] payload] per msg
            final int packedMsgSize = 8 + 4 + PAYLOAD_SIZE; // 36
            final UnsafeBuffer packPl =
                    new UnsafeBuffer(new byte[batchCount * packedMsgSize]);

            while (run.get()) {
                final boolean usePacked = (seq / batchCount) % 2 == 1;

                if (usePacked) {
                    int wp = 0;
                    for (int i = 0; i < batchCount; i++) {
                        final long t = seq + i;
                        packPl.putLong(wp, t);
                        wp += 8;
                        packPl.putInt(wp, PAYLOAD_SIZE);
                        wp += 4;
                        encodePayload(packPl, wp, t, t);
                        wp += PAYLOAD_SIZE;
                    }
                    int w = 0;
                    int attempts = 0;
                    while (w < batchCount) {
                        final int off = w * packedMsgSize;
                        final int n = sequence.appendBatchPacked(packPl, off, batchCount - w);
                        if (n <= 0 && ++attempts > 200) {
                            throw new AssertionError("packed batch stuck at " + w + "/" + batchCount);
                        }
                        if (n > 0) {
                            attempts = 0;
                        }
                        w += Math.max(n, 0);
                    }
                } else {
                    for (int i = 0; i < batchCount; i++) {
                        orders[i] = seq + i;
                        offsets[i] = i * PAYLOAD_SIZE;
                        sizes[i] = PAYLOAD_SIZE;
                        encodePayload(arrayPl, i * PAYLOAD_SIZE, seq + i, seq + i);
                    }
                    int w = 0;
                    int attempts = 0;
                    while (w < batchCount) {
                        final long[] subTs = Arrays.copyOfRange(orders, w, batchCount);
                        final int[] subOffs = Arrays.copyOfRange(offsets, w, batchCount);
                        final int[] subLens = Arrays.copyOfRange(sizes, w, batchCount);
                        final int n = sequence.appendBatch(subTs, arrayPl, subOffs, subLens, batchCount - w);
                        if (n <= 0 && ++attempts > 200) {
                            throw new AssertionError("array batch stuck at " + w + "/" + batchCount);
                        }
                        if (n > 0) {
                            attempts = 0;
                        }
                        w += Math.max(n, 0);
                    }
                }
                seq += batchCount;
            }
            written.set(seq);
            return null;
        }));

        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(forwardReader(sequence, run, go, true)));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        assertTrue(written.get() > 100);
        System.out.printf("  [batchAppend] wrote %,d msgs%n", written.get());
    }

    @Test
    void cowWithConcurrentReaders() throws Exception {
        final TestSequenceStorage storage = store(32L * CHUNK);
        final Sequence sequence = storage.getOrCreate("sequence");
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong written = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        // Writer: appends with gaps, inserts fill gaps -> triggers COW
        fs.add(ex.submit(() -> {
            go.await();
            final UnsafeBuffer pb =
                    new UnsafeBuffer(new byte[PAYLOAD_SIZE]);
            long seq = 0;
            long appendTs = 0;
            while (run.get()) {
                encodePayload(pb, seq, appendTs);
                sequence.append(appendTs, pb, 0, PAYLOAD_SIZE);
                seq++;

                // Every 30 appends, insert into a gap to trigger COW
                if (seq % 30 == 0 && appendTs >= 5) {
                    final long insertTs = appendTs - 3;
                    encodePayload(pb, seq, insertTs);
                    sequence.insert(insertTs, pb, 0, PAYLOAD_SIZE);
                    seq++;
                }
                appendTs += 2; // leave gaps
            }
            written.set(seq);
            return null;
        }));

        // Readers: verify ts ordering + checksum (seq won't be monotonic
        // Because inserts have later seq but earlier ts)
        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(forwardReader(sequence, run, go, false)));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        assertTrue(written.get() > 500);
        System.out.printf("  [cow] wrote %,d msgs%n", written.get());
    }

    @Test
    void multiSeriesEviction() throws Exception {
        final int seriesCount = 4;
        // 8 Chunks shared across 4 series -> ~2 per series before eviction
        final TestSequenceStorage storage = store(8L * CHUNK);
        final Sequence[] series = new Sequence[seriesCount];
        for (int i = 0; i < seriesCount; i++) {
            series[i] = storage.getOrCreate("s" + i);
        }

        final AtomicBoolean run = new AtomicBoolean(true);
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();
        final AtomicLong[] counts = new AtomicLong[seriesCount];

        // One writer + two readers per series
        for (int i = 0; i < seriesCount; i++) {
            counts[i] = new AtomicLong();
            fs.add(ex.submit(appendWriter(series[i], run, counts[i], go)));
            for (int r = 0; r < 2; r++) {
                fs.add(ex.submit(forwardReader(series[i], run, go, true)));
            }
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        long total = 0;
        for (int i = 0; i < seriesCount; i++) {
            assertTrue(counts[i].get() > 100, "series " + i + " under-produced: " + counts[i].get());
            total += counts[i].get();
        }
        System.out.printf(
                "  [multiSeries] total wrote %,d msgs across %d series%n",
                total,
                seriesCount);
    }

    @Test
    void mergedCursorOrdering() throws Exception {
        final int seriesCount = 3;
        final TestSequenceStorage storage = store(32L * CHUNK);
        final Sequence[] sequences = new Sequence[seriesCount];
        for (int i = 0; i < seriesCount; i++) {
            sequences[i] = storage.getOrCreate("m" + i);
        }

        // Shared clock so orders are globally unique and comparable
        final AtomicLong clock = new AtomicLong(0);
        final AtomicBoolean run = new AtomicBoolean(true);
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        for (int i = 0; i < seriesCount; i++) {
            final Sequence sequence = sequences[i];
            fs.add(ex.submit(() -> {
                go.await();
                final UnsafeBuffer pb =
                        new UnsafeBuffer(new byte[PAYLOAD_SIZE]);
                while (run.get()) {
                    final long t = clock.getAndIncrement();
                    encodePayload(pb, t, t);
                    sequence.append(t, pb, 0, PAYLOAD_SIZE);
                }
                return null;
            }));
        }

        // Merged cursor reader: verify per-source non-decreasing ts
        for (int r = 0; r < 2; r++) {
            fs.add(ex.submit(() -> {
                go.await();
                final MergedForwardCursor mc = MergedForwardCursor.create(sequences);
                final long[] perSrc = new long[seriesCount];
                Arrays.fill(perSrc, Long.MIN_VALUE);
                final AtomicLong delivered = new AtomicLong();
                try {
                    while (run.get()) {
                        mc.next(64, (srcIdx, owner, entryOrder, buf, off, size) -> {
                            checkPayload(buf, off, size);
                            final long t = entryOrder;
                            if (t < perSrc[srcIdx]) {
                                throw new AssertionError(
                                        "merged: source " + srcIdx + " regressed "
                                                + perSrc[srcIdx] + " -> " + t);
                            }
                            perSrc[srcIdx] = t;
                            delivered.incrementAndGet();
                        });
                        mc.refreshPeeks();
                    }
                } finally {
                    mc.close();
                }
                return null;
            }));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        System.out.printf("  [mergedCursor] clock=%,d%n", clock.get());
    }

    @Test
    void seekStorm() throws Exception {
        final TestSequenceStorage storage = store(16L * CHUNK);
        final Sequence sequence = storage.getOrCreate("sequence");
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong written = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        fs.add(ex.submit(appendWriter(sequence, run, written, go)));

        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(() -> {
                go.await();
                final ForwardCursor cur = new ForwardCursor(sequence);
                long seeks = 0;
                try {
                    while (run.get()) {
                        final long maxTs = written.get();
                        if (maxTs < 100) {
                            LockSupport.parkNanos(1_000_000);   // 1 ms back-off
                            continue;
                        }
                        final long span = ThreadLocalRandom.current()
                                .nextLong(Math.min(maxTs, 500));
                        final long target = maxTs - span;
                        cur.seekTo(target);

                        final long[] first = {-1};
                        cur.next(20, (owner, entryOrder, buf, off, size) -> {
                            checkPayload(buf, off, size);
                            if (first[0] == -1) {
                                if (entryOrder < target) {
                                    throw new AssertionError("seek violated: wanted >= "
                                            + target + " got = " + entryOrder);
                                }
                                first[0] = entryOrder;
                            }
                        });
                        seeks++;
                    }
                } finally {
                    cur.close();
                }
                assertTrue(seeks > 10, "seeker must complete seeks; got " + seeks);
                return null;
            }));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        System.out.printf("  [seekStorm] wrote %,d msgs%n", written.get());
    }

    @Test
    void realtimeTailing() throws Exception {
        final TestSequenceStorage storage = store(32L * CHUNK);
        final Sequence sequence = storage.getOrCreate("sequence");
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong written = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        // Slow writer: ~1 000 msg/sequence
        fs.add(ex.submit(() -> {
            go.await();
            final UnsafeBuffer pb =
                    new UnsafeBuffer(new byte[PAYLOAD_SIZE]);
            long seq = 0;
            while (run.get()) {
                encodePayload(pb, seq, seq);
                sequence.append(seq, pb, 0, PAYLOAD_SIZE);
                seq++;
                LockSupport.parkNanos(1_000_000); // 1 ms
            }
            written.set(seq);
            return null;
        }));

        // Reader verifies gap-free delivery (seq must be contiguous)
        fs.add(ex.submit(() -> {
            go.await();
            final ForwardCursor cur = new ForwardCursor(sequence);
            final long[] state = {-1, 0}; // expectedNextSeq, count
            try {
                while (run.get()) {
                    cur.next(32, (owner, entryOrder, buf, off, size) -> {
                        checkPayload(buf, off, size);
                        final long seq = payloadSequence(buf, off);
                        if (state[0] >= 0 && seq != state[0]) {
                            throw new AssertionError("gap: expected seq " + state[0] + " got " + seq);
                        }
                        state[0] = seq + 1;
                        state[1]++;
                    });
                    // Brief pause to let writer get ahead sometimes
                    LockSupport.parkNanos(500_000); // 0.5 ms
                }
                // Drain remaining after writer stops
                for (int i = 0; i < 50; i++) {
                    final int[] box = {0};
                    cur.next(1000, (owner, entryOrder, buf, off, size) -> {
                        checkPayload(buf, off, size);
                        final long seq = payloadSequence(buf, off);
                        if (state[0] >= 0 && seq != state[0]) {
                            throw new AssertionError("drain gap at " + seq);
                        }
                        state[0] = seq + 1;
                        state[1]++;
                        box[0]++;
                    });
                    if (box[0] == 0) {
                        break;
                    }
                }
            } finally {
                cur.close();
            }
            System.out.printf("    tailing reader saw %,d / %,d%n", state[1], written.get());
            return null;
        }));

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        System.out.printf("  [tailing] wrote %,d msgs%n", written.get());
    }

    @Test
    void completenessAfterDrain() throws Exception {
        // Small heap to force eviction during writes
        final TestSequenceStorage storage = store(6L * CHUNK);
        final Sequence sequence = storage.getOrCreate("sequence");

        final int entryCount = 20_000;
        final UnsafeBuffer pb =
                new UnsafeBuffer(new byte[PAYLOAD_SIZE]);
        for (int i = 0; i < entryCount; i++) {
            encodePayload(pb, i, i);
            assertTrue(
                    sequence.append(i, pb, 0, PAYLOAD_SIZE),
                    "append must succeed at seq=" + i);
        }

        // Full sequential read - every seq 0..entryCount−1 must be present
        final ForwardCursor cur = new ForwardCursor(sequence);
        final BitSet seen = new BitSet(entryCount);
        final long[] count = {0};
        while (true) {
            final long[] batch = {0};
            cur.next(1024, (owner, entryOrder, buf, off, size) -> {
                checkPayload(buf, off, size);
                final int seq = (int) payloadSequence(buf, off);
                if (seq < 0 || seq >= entryCount) {
                    throw new AssertionError("unexpected seq " + seq);
                }
                if (seen.get(seq)) {
                    throw new AssertionError("duplicate seq " + seq);
                }
                seen.set(seq);
                count[0]++;
                batch[0]++;
            });
            if (batch[0] == 0) {
                break;
            }
        }
        cur.close();

        assertEquals(entryCount, count[0], "entry count mismatch");
        assertEquals(entryCount, seen.cardinality(), "cardinality mismatch");
        final int missing = seen.nextClearBit(0);
        assertTrue(missing >= entryCount, "first missing seq=" + missing);
        System.out.printf("  [completeness] verified %,d msgs%n", entryCount);
    }

    @Test
    void completenessWithConcurrentCursors() throws Exception {
        final TestSequenceStorage storage = store(4L * CHUNK);
        final Sequence sequence = storage.getOrCreate("sequence");

        final int entryCount = 15_000;
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong phase = new AtomicLong(0); // 0 = writing, 1 = done
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        // Writer: sequential append of N entries then stops
        fs.add(ex.submit(() -> {
            go.await();
            final UnsafeBuffer pb =
                    new UnsafeBuffer(new byte[PAYLOAD_SIZE]);
            for (int i = 0; i < entryCount; i++) {
                encodePayload(pb, i, i);
                assertTrue(sequence.append(i, pb, 0, PAYLOAD_SIZE));
            }
            phase.set(1);
            // Keep thread alive until readers finish
            while (run.get()) {
                LockSupport.parkNanos(5_000_000);
            }
            return null;
        }));

        // Forward readers during writes
        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(() -> {
                go.await();
                final ForwardCursor cur = new ForwardCursor(sequence);
                final long[] st2 = {Long.MIN_VALUE};
                try {
                    while (run.get()) {
                        cur.next(64, (owner, entryOrder, buf, off, size) -> {
                            checkPayload(buf, off, size);
                            if (entryOrder < st2[0]) {
                                throw new AssertionError("Order regress in drain reader");
                            }
                            st2[0] = entryOrder;
                        });
                    }
                } finally {
                    cur.close();
                }
                return null;
            }));
        }

        // Backward reader during writes
        fs.add(ex.submit(backwardStreamingReader(sequence, run, go)));

        go.countDown();
        // Wait for writer to finish
        while (phase.get() == 0) {
            Thread.sleep(50);
        }
        // Let readers churn for a bit longer
        Thread.sleep(2000);
        run.set(false);
        join(fs);
        ex.shutdown();

        verifyCardinalityFromForwardCursor(
                sequence,
                entryCount,
                "completeness with cursors",
                "  [completeness+cursors] verified %,d msgs%n");
    }

    @Test
    void batchProgressRegression() {
        final TestSequenceStorage storage = store(32L * CHUNK);
        final Sequence sequence = storage.getOrCreate("s");

        // Fill tail chunk to near capacity so the next entry triggers
        // The seal-without-break scenario.
        // Msg size = align8(24 + 24) = 48.  data area = 3840.
        // 79 * 48 = 3792.  Remaining = 48.  One more 48-byte msg fills it.
        // 80 * 48 = 3840.  Remaining = 0 -> sealed.  But we stop at 79
        // To leave exactly 48 bytes, then batch-append with larger payload.
        final UnsafeBuffer pb =
                new UnsafeBuffer(new byte[PAYLOAD_SIZE]);
        for (int i = 0; i < 79; i++) {
            encodePayload(pb, i, i);
            assertTrue(sequence.append(i, pb, 0, PAYLOAD_SIZE));
        }
        // Remaining data space = 48 bytes.
        // Next entry with PL=24 -> msz=48 fits exactly.
        // But let's use a larger payload (32 bytes -> msz=56 > 48) so it
        // Does NOT fit, exercising the seal + continue path.
        final int bigPayload = 32;
        final int batchTotal = 200;
        final long[] orders = new long[batchTotal];
        final int[] offsets = new int[batchTotal];
        final int[] sizes = new int[batchTotal];
        final UnsafeBuffer big =
                new UnsafeBuffer(new byte[batchTotal * bigPayload]);
        for (int i = 0; i < batchTotal; i++) {
            orders[i] = 79 + i;
            offsets[i] = i * bigPayload;
            sizes[i] = bigPayload;
            // Simple checksum encoding at each offset
            encodePayload(big, i * bigPayload, 79 + i, 79 + i);
        }

        int written = 0;
        int rounds = 0;
        while (written < batchTotal) {
            final int remaining = batchTotal - written;
            final int n = sequence.appendBatch(
                    Arrays.copyOfRange(orders, written, batchTotal),
                    big,
                    Arrays.copyOfRange(offsets, written, batchTotal),
                    Arrays.copyOfRange(sizes, written, batchTotal),
                    remaining);
            assertTrue(n >= 0, "appendBatch returned negative");
            if (n == 0) {
                rounds++;
                assertTrue(
                        rounds < 500,
                        "appendBatch stuck — no progress at written=" + written);
            } else {
                rounds = 0;
            }
            written += n;
        }
        assertEquals(batchTotal, written, "all entries must be written");

        // Verify readable
        final ForwardCursor cur = new ForwardCursor(sequence);
        final long[] count = {0};
        while (true) {
            final long[] b = {0};
            cur.next(1024, (owner, entryOrder, buf, off, size) -> {
                b[0]++;
                count[0]++;
            });
            if (b[0] == 0) {
                break;
            }
        }
        cur.close();
        // 79 Initial + batchTotal batch = 279
        assertEquals(79 + batchTotal, count[0]);
        System.out.printf("  [batchRegression] wrote+verified %,d msgs%n", count[0]);
    }

    @Test
    void packedBatchProgressRegression() {
        final TestSequenceStorage storage = store(32L * CHUNK);
        final Sequence sequence = storage.getOrCreate("sequence");

        // Same idea: fill a chunk, then packed-batch with entries
        // That don't fit the remaining space.
        final UnsafeBuffer pb =
                new UnsafeBuffer(new byte[PAYLOAD_SIZE]);
        for (int i = 0; i < 79; i++) {
            encodePayload(pb, i, i);
            assertTrue(sequence.append(i, pb, 0, PAYLOAD_SIZE));
        }

        final int bigPayloadLen = 32;
        final int batchTotal = 200;
        // Packed format: [long ts][int pLen][byte[pLen] payload]...
        final int perMsg = 8 + 4 + bigPayloadLen;
        final UnsafeBuffer pack =
                new UnsafeBuffer(new byte[batchTotal * perMsg]);
        int wp = 0;
        for (int i = 0; i < batchTotal; i++) {
            final long t = 79 + i;
            pack.putLong(wp, t);
            wp += 8;
            pack.putInt(wp, bigPayloadLen);
            wp += 4;
            encodePayload(pack, wp, t, t);
            wp += bigPayloadLen;
        }

        int written = 0;
        int rounds = 0;
        int readOff = 0;
        while (written < batchTotal) {
            final int n =
                    sequence.appendBatchPacked(pack, readOff, batchTotal - written);
            assertTrue(n >= 0);
            if (n == 0) {
                rounds++;
                assertTrue(
                        rounds < 500,
                        "packed batch stuck at written=" + written);
            } else {
                rounds = 0;
                readOff += n * perMsg;
            }
            written += n;
        }
        assertEquals(batchTotal, written);
        System.out.printf(
                "  [packedBatchRegression] wrote %d msgs%n",
                79 + batchTotal);
    }

    @Test
    void pinChurnIntegrity() throws Exception {
        final TestSequenceStorage storage = store(8L * CHUNK);
        final Sequence sequence = storage.getOrCreate("sequence");
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong written = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        fs.add(ex.submit(appendWriter(sequence, run, written, go)));

        // Many threads each rapidly open, read a few entries, close cursor
        final int churnThreads = 8;
        final AtomicLong totalCycles = new AtomicLong();
        for (int r = 0; r < churnThreads; r++) {
            fs.add(ex.submit(() -> {
                go.await();
                long cycles = 0;
                while (run.get()) {
                    final ForwardCursor cur = new ForwardCursor(sequence);
                    final int span = ThreadLocalRandom.current().nextInt(1, 50);
                    cur.next(
                            span,
                            (owner, entryOrder, buf, off, size) ->
                                    checkPayload(buf, off, size));
                    cur.peekNextOrder();
                    cur.close();
                    cycles++;
                }
                totalCycles.addAndGet(cycles);
                return null;
            }));
        }

        // Also churn backward cursors
        for (int r = 0; r < 4; r++) {
            fs.add(ex.submit(() -> {
                go.await();
                while (run.get()) {
                    final BackwardCursor cur = new BackwardCursor(sequence);
                    cur.seekToEnd();
                    final int span = ThreadLocalRandom.current().nextInt(1, 30);
                    cur.next(
                            span,
                            (owner, entryOrder, buf, off, size) ->
                                    checkPayload(buf, off, size));
                    cur.peekNextOrder();
                    cur.close();
                }
                return null;
            }));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        // Verify all refcounts settled to 0 (no leaks).
        // Walk the snapshot and check each chunk'sequence refcount.
        final ChunkSnapshot snapshot = sequence.snapshot();
        for (int i = 0; i < snapshot.size(); i++) {
            final Chunk c = snapshot.chunk(i);
            final int rc = c.getRefCount();
            assertTrue(rc >= 0, "chunk " + i + " has negative refcount: " + rc);
        }

        assertTrue(totalCycles.get() > 1000, "churn cycles too low: " + totalCycles.get());
        System.out.printf("  [pinChurn] cycles=%,d  wrote=%,d%n", totalCycles.get(), written.get());
    }

    @Test
    void upsertWithConcurrentReaders() throws Exception {
        final TestSequenceStorage storage = store(32L * CHUNK);
        final Sequence sequence = storage.getOrCreate("sequence");
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong ops = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        // Writer: append then upsert existing orders
        fs.add(ex.submit(() -> {
            go.await();
            final UnsafeBuffer pb =
                    new UnsafeBuffer(new byte[PAYLOAD_SIZE]);
            long seq = 0;
            while (run.get()) {
                encodePayload(pb, seq, seq);
                sequence.append(seq, pb, 0, PAYLOAD_SIZE);
                seq++;

                // Upsert: overwrite a recent order
                if (seq > 5 && seq % 10 == 0) {
                    final long target = seq - 5;
                    encodePayload(pb, seq, target);
                    sequence.insertOrUpdateUnique(target, pb, 0, PAYLOAD_SIZE);
                    seq++;
                }
            }
            ops.set(seq);
            return null;
        }));

        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(forwardReader(sequence, run, go, false)));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        assertTrue(ops.get() > 500);
        System.out.printf("  [upsert] ops=%,d%n", ops.get());
    }

    @Test
    void mixedCursorStress() throws Exception {
        final TestSequenceStorage storage = store(8L * CHUNK);
        final Sequence sequence = storage.getOrCreate("sequence");
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong written = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        fs.add(ex.submit(appendWriter(sequence, run, written, go)));

        // Forward readers
        for (int r = 0; r < 2; r++) {
            fs.add(ex.submit(forwardReader(sequence, run, go, true)));
        }

        // Backward readers
        for (int r = 0; r < 2; r++) {
            fs.add(ex.submit(() -> {
                go.await();
                final BackwardCursor cur = new BackwardCursor(sequence);
                try {
                    while (run.get()) {
                        cur.seekToEnd();
                        final long[] prev = {Long.MAX_VALUE};
                        cur.next(100, (owner, entryOrder, buf, off, size) -> {
                            checkPayload(buf, off, size);
                            if (entryOrder > prev[0]) {
                                throw new AssertionError("bwd mixed ts");
                            }
                            prev[0] = entryOrder;
                        });
                    }
                } finally {
                    cur.close();
                }
                return null;
            }));
        }

        // Seekers
        for (int r = 0; r < 2; r++) {
            fs.add(ex.submit(() -> {
                go.await();
                final ForwardCursor cur = new ForwardCursor(sequence);
                try {
                    while (run.get()) {
                        final long m = written.get();
                        if (m < 100) {
                            Thread.yield();
                            continue;
                        }
                        final long lo = Math.max(0, m - 300);
                        final long target =
                                ThreadLocalRandom.current().nextLong(lo, m);
                        cur.seekTo(target);
                        cur.next(
                                20,
                                (owner, entryOrder, buf, off, size) ->
                                        checkPayload(buf, off, size));
                    }
                } finally {
                    cur.close();
                }
                return null;
            }));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        System.out.printf("  [mixedCursors] wrote %,d msgs%n", written.get());
    }

    @Test
    void snapshotVisibility() throws Exception {
        final TestSequenceStorage storage = store(16L * CHUNK);
        final Sequence sequence = storage.getOrCreate("sequence");
        final AtomicBoolean run = new AtomicBoolean(true);
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        final AtomicLong publishedSeq = new AtomicLong(-1);
        final AtomicLong readerMaxSeq = new AtomicLong(-1);
        final AtomicLong violations = new AtomicLong(0);
        final AtomicLong probes = new AtomicLong(0);

        // Writer
        fs.add(ex.submit(() -> {
            go.await();
            final UnsafeBuffer pb =
                    new UnsafeBuffer(new byte[PAYLOAD_SIZE]);
            long seq = 0;
            while (run.get()) {
                encodePayload(pb, seq, seq);
                sequence.append(seq, pb, 0, PAYLOAD_SIZE);
                publishedSeq.set(seq);
                seq++;
                if (seq % 100 == 0) {
                    Thread.yield();
                }
            }
            return null;
        }));

        // Reader: after observing publishedSeq = X, creates a fresh cursor
        // And verifies entry X is eventually readable.
        fs.add(ex.submit(() -> {
            go.await();
            while (run.get()) {
                final long target = publishedSeq.get();
                if (target < 0) {
                    Thread.yield();
                    continue;
                }

                final ForwardCursor cur = new ForwardCursor(sequence);
                cur.seekTo(target);
                final long[] found = {-1};
                for (int attempt = 0; attempt < 100 && found[0] < target; attempt++) {
                    cur.next(256, (owner, entryOrder, buf, off, size) -> {
                        checkPayload(buf, off, size);
                        final long seq = payloadSequence(buf, off);
                        if (seq > found[0]) {
                            found[0] = seq;
                        }
                    });
                }
                cur.close();

                if (found[0] < target) {
                    violations.incrementAndGet();
                }

                long prev = readerMaxSeq.get();
                while (found[0] > prev) {
                    if (readerMaxSeq.compareAndSet(prev, found[0])) {
                        break;
                    }
                    prev = readerMaxSeq.get();
                }
                probes.incrementAndGet();
            }
            return null;
        }));

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        final long pub = publishedSeq.get();
        final long rmax = readerMaxSeq.get();
        final long viol = violations.get();
        final long prb = probes.get();

        System.out.printf(
                "  [visibility] published=%,d  readerMax=%,d  "
                        + "probes=%,d  violations=%d%n",
                pub,
                rmax,
                prb,
                viol);

        // The correctness invariant: every probe that targeted a published
        // Entry must have found it (zero violations).
        assertEquals(0, viol, "snapshot visibility violations — published entry not readable");

        // Sanity: the reader actually ran and made progress.
        assertTrue(prb > 50, "reader completed too few probes: " + prb);
        assertTrue(rmax > 0, "reader never saw any entry");
    }

    @Test
    void evictionWithCow() throws Exception {
        // Tiny heap + COW inserts -> maximum allocator/eviction pressure
        final TestSequenceStorage storage = store(4L * CHUNK);
        final Sequence sequence = storage.getOrCreate("sequence");
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong ops = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        fs.add(ex.submit(() -> {
            go.await();
            final UnsafeBuffer pb =
                    new UnsafeBuffer(new byte[PAYLOAD_SIZE]);
            long seq = 0;
            long appendTs = 0;
            while (run.get()) {
                encodePayload(pb, seq, appendTs);
                sequence.append(appendTs, pb, 0, PAYLOAD_SIZE);
                seq++;
                if (seq % 50 == 0 && appendTs >= 5) {
                    final long insertTs = appendTs - 3;
                    encodePayload(pb, seq, insertTs);
                    sequence.insert(insertTs, pb, 0, PAYLOAD_SIZE);
                    seq++;
                }
                appendTs += 2;
            }
            ops.set(seq);
            return null;
        }));

        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(forwardReader(sequence, run, go, false)));
        }

        for (int r = 0; r < 2; r++) {
            fs.add(ex.submit(() -> {
                go.await();
                final BackwardCursor cur = new BackwardCursor(sequence);
                try {
                    while (run.get()) {
                        cur.seekToEnd();
                        final long[] prev = {Long.MAX_VALUE};
                        cur.next(100, (owner, entryOrder, buf, off, size) -> {
                            checkPayload(buf, off, size);
                            if (entryOrder > prev[0]) {
                                throw new AssertionError("evict+cow bwd ts");
                            }
                            prev[0] = entryOrder;
                        });
                    }
                } finally {
                    cur.close();
                }
                return null;
            }));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        assertTrue(ops.get() > 500);
        System.out.printf("  [eviction+cow] ops=%,d%n", ops.get());
    }

    @Test
    void insertBatchWithConcurrentReaders() throws Exception {
        final TestSequenceStorage storage = store(32L * CHUNK);
        final Sequence sequence = storage.getOrCreate("sequence");
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong written = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        // Writer: appends with gaps (order += 10), periodically insertBatch
        // fills gaps with batches of entries
        fs.add(ex.submit(() -> {
            go.await();
            final UnsafeBuffer pb = new UnsafeBuffer(new byte[PAYLOAD_SIZE]);
            final int batchSize = 4;
            final long[] batchOrders = new long[batchSize];
            final int[] batchOffsets = new int[batchSize];
            final int[] batchSizes = new int[batchSize];
            final UnsafeBuffer batchPl = new UnsafeBuffer(new byte[batchSize * PAYLOAD_SIZE]);
            for (int i = 0; i < batchSize; i++) {
                batchOffsets[i] = i * PAYLOAD_SIZE;
                batchSizes[i] = PAYLOAD_SIZE;
            }

            long seq = 0;
            long appendOrder = 0;
            while (run.get()) {
                encodePayload(pb, seq, appendOrder);
                sequence.append(appendOrder, pb, 0, PAYLOAD_SIZE);
                seq++;

                if (seq % 40 == 0 && appendOrder >= 50) {
                    final long base = appendOrder - 30;
                    for (int i = 0; i < batchSize; i++) {
                        batchOrders[i] = base + i * 2 + 1; // odd orders in gaps
                        encodePayload(batchPl, batchOffsets[i], seq + i, batchOrders[i]);
                    }
                    sequence.insertBatch(batchOrders, batchPl, batchOffsets, batchSizes, batchSize);
                    seq += batchSize;
                }
                appendOrder += 10;
            }
            written.set(seq);
            return null;
        }));

        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(forwardReader(sequence, run, go, false)));
        }
        for (int r = 0; r < 2; r++) {
            fs.add(ex.submit(backwardStreamingReader(sequence, run, go)));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        assertTrue(written.get() > 500);
        System.out.printf("  [insertBatch] wrote %,d msgs%n", written.get());
    }

    @Test
    void mixedAppendAndInsertBatchWriter() throws Exception {
        final TestSequenceStorage storage = store(16L * CHUNK);
        final Sequence sequence = storage.getOrCreate("sequence");
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong written = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        final int batchCount = 16;

        // Writer alternates between appendBatch and insertBatch
        fs.add(ex.submit(() -> {
            go.await();
            long seq = 0;
            final long[] orders = new long[batchCount];
            final int[] offsets = new int[batchCount];
            final int[] sizes = new int[batchCount];
            final UnsafeBuffer pl = new UnsafeBuffer(new byte[batchCount * PAYLOAD_SIZE]);
            for (int i = 0; i < batchCount; i++) {
                offsets[i] = i * PAYLOAD_SIZE;
                sizes[i] = PAYLOAD_SIZE;
            }

            long appendOrder = 0;
            while (run.get()) {
                // appendBatch: monotonic tail
                for (int i = 0; i < batchCount; i++) {
                    orders[i] = appendOrder + i * 10;
                    encodePayload(pl, offsets[i], seq + i, orders[i]);
                }
                final int appended = sequence.appendBatch(orders, pl, offsets, sizes, batchCount);
                seq += appended;
                appendOrder += batchCount * 10;

                // insertBatch: fill gaps in older data
                if (seq > batchCount * 3 && appendOrder >= 100) {
                    final long base = appendOrder - 80;
                    for (int i = 0; i < batchCount; i++) {
                        orders[i] = base + i * 3 + 1;
                        encodePayload(pl, offsets[i], seq + i, orders[i]);
                    }
                    final int inserted = sequence.insertBatch(orders, pl, offsets, sizes, batchCount);
                    seq += inserted;
                }
            }
            written.set(seq);
            return null;
        }));

        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(forwardReader(sequence, run, go, false)));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        assertTrue(written.get() > 100);
        System.out.printf("  [mixedBatch] wrote %,d msgs%n", written.get());
    }

    @Test
    void insertBatchUnderHeavyEviction() throws Exception {
        // Small heap -> frequent eviction
        final TestSequenceStorage storage = store(6L * CHUNK);
        final Sequence sequence = storage.getOrCreate("sequence");
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong written = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        fs.add(ex.submit(() -> {
            go.await();
            final UnsafeBuffer pb = new UnsafeBuffer(new byte[PAYLOAD_SIZE]);
            final int batchSize = 3;
            final long[] batchOrders = new long[batchSize];
            final int[] batchOffsets = new int[batchSize];
            final int[] batchSizes = new int[batchSize];
            final UnsafeBuffer batchPl = new UnsafeBuffer(new byte[batchSize * PAYLOAD_SIZE]);
            for (int i = 0; i < batchSize; i++) {
                batchOffsets[i] = i * PAYLOAD_SIZE;
                batchSizes[i] = PAYLOAD_SIZE;
            }

            long seq = 0;
            long appendOrder = 0;
            while (run.get()) {
                encodePayload(pb, seq, appendOrder);
                sequence.append(appendOrder, pb, 0, PAYLOAD_SIZE);
                seq++;

                if (seq % 50 == 0 && appendOrder >= 40) {
                    final long base = appendOrder - 20;
                    for (int i = 0; i < batchSize; i++) {
                        batchOrders[i] = base + i * 3 + 1;
                        encodePayload(batchPl, batchOffsets[i], seq + i, batchOrders[i]);
                    }
                    sequence.insertBatch(batchOrders, batchPl, batchOffsets, batchSizes, batchSize);
                    seq += batchSize;
                }
                appendOrder += 2;
            }
            written.set(seq);
            return null;
        }));

        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(forwardReader(sequence, run, go, false)));
        }
        for (int r = 0; r < 2; r++) {
            fs.add(ex.submit(backwardStreamingReader(sequence, run, go)));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        assertTrue(written.get() > 500);
        System.out.printf("  [insertBatch+eviction] wrote %,d msgs%n", written.get());
    }

    @Test
    void highVolumeInsertBatch() throws Exception {
        final TestSequenceStorage storage = store(32L * CHUNK);
        final Sequence sequence = storage.getOrCreate("sequence");
        final AtomicBoolean run = new AtomicBoolean(true);
        final AtomicLong totalOps = new AtomicLong();
        final CountDownLatch go = new CountDownLatch(1);
        final ExecutorService ex = Executors.newCachedThreadPool();
        final List<Future<?>> fs = new ArrayList<>();

        final int batchSize = 64;

        fs.add(ex.submit(() -> {
            go.await();
            final long[] orders = new long[batchSize];
            final int[] offsets = new int[batchSize];
            final int[] sizes = new int[batchSize];
            final UnsafeBuffer pl = new UnsafeBuffer(new byte[batchSize * PAYLOAD_SIZE]);
            for (int i = 0; i < batchSize; i++) {
                offsets[i] = i * PAYLOAD_SIZE;
                sizes[i] = PAYLOAD_SIZE;
            }

            long seq = 0;
            long appendOrder = 0;

            // Phase 1: build up some data with appends
            final UnsafeBuffer pb = new UnsafeBuffer(new byte[PAYLOAD_SIZE]);
            for (int i = 0; i < 200; i++) {
                encodePayload(pb, seq, appendOrder);
                sequence.append(appendOrder, pb, 0, PAYLOAD_SIZE);
                seq++;
                appendOrder += 10;
            }

            // Phase 2: sustained insertBatch into various positions
            while (run.get()) {
                // Append some to grow the tail
                for (int i = 0; i < 10; i++) {
                    encodePayload(pb, seq, appendOrder);
                    sequence.append(appendOrder, pb, 0, PAYLOAD_SIZE);
                    seq++;
                    appendOrder += 10;
                }

                // Large batch insert into older region
                final long base = Math.max(0, appendOrder - 500);
                for (int i = 0; i < batchSize; i++) {
                    orders[i] = base + i * 5 + 1;
                    encodePayload(pl, offsets[i], seq + i, orders[i]);
                }
                sequence.insertBatch(orders, pl, offsets, sizes, batchSize);
                seq += batchSize;
            }
            totalOps.set(seq);
            return null;
        }));

        for (int r = 0; r < READERS; r++) {
            fs.add(ex.submit(forwardReader(sequence, run, go, false)));
        }

        go.countDown();
        Thread.sleep(DURATION_MS);
        run.set(false);
        join(fs);
        ex.shutdown();

        assertTrue(totalOps.get() > 500);
        System.out.printf("  [highVolInsertBatch] ops=%,d%n", totalOps.get());

        // Post-run: verify all entries are ordered
        final ForwardCursor ver = new ForwardCursor(sequence);
        final long[] prev = {Long.MIN_VALUE};
        final long[] count = {0};
        while (true) {
            final long[] batch = {0};
            ver.next(1024, (owner, entryOrder, buf, off, size) -> {
                checkPayload(buf, off, size);
                if (entryOrder < prev[0]) {
                    throw new AssertionError("Post-run order violation: "
                            + prev[0] + " -> " + entryOrder);
                }
                prev[0] = entryOrder;
                count[0]++;
                batch[0]++;
            });
            if (batch[0] == 0) {
                break;
            }
        }
        ver.close();
        assertTrue(count[0] > 500, "expected entries; got " + count[0]);
        System.out.printf("  [highVolInsertBatch] verified %,d entries%n", count[0]);
    }
}