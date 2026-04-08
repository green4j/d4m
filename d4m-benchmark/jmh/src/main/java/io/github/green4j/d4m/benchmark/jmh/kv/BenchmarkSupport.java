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
package io.github.green4j.d4m.benchmark.jmh.kv;

import io.github.green4j.d4m.common.UnsafeBuffer;
import io.github.green4j.d4m.kv.KeyValueRing;
import io.github.green4j.d4m.kv.KeyValueSegment;
import io.github.green4j.d4m.kv.MmapTierFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

/**
 * Shared utilities and constants for JMH benchmarks in the d4m-kv module.
 */
public final class BenchmarkSupport {
    public static final int KEY_SIZE = 32;
    public static final int VALUE_SIZE = 200;

    static final int RING_SIZE = 8;
    static final int SHUFFLE_MULTIPLIER = 16;
    static final int INITIAL_CAPACITY = 65536;

    static final int ENTRY_SIZE_ESTIMATE = 240;
    static final int WRITE_CYCLE_SIZE = 500_000;

    static final int HOT_TIER_NO_EVICTION = 128 * 1024 * 1024;
    static final int HOT_TIER_EVICT_WRITE = 2 * 1024 * 1024;
    static final int MMAP_TIER_SIZE = 256 * 1024 * 1024;

    private BenchmarkSupport() {
    }

    /**
     * Creates a {@link KeyValueRing} with a large hot tier that avoids eviction.
     *
     * @return a ring configured for no-eviction benchmarks
     */
    static KeyValueRing createRingNoEviction() {
        return createRing(RING_SIZE, HOT_TIER_NO_EVICTION);
    }

    /**
     * Creates a {@link KeyValueRing} with a large hot tier that avoids eviction,
     * using the specified number of segments.
     *
     * @param ringSize the number of ring segments
     * @return a ring configured for no-eviction benchmarks
     */
    static KeyValueRing createRingNoEviction(final int ringSize) {
        return createRing(ringSize, HOT_TIER_NO_EVICTION);
    }

    /**
     * Creates a {@link KeyValueRing} with a hot tier sized so that approximately
     * 30% of the given population spills to mmap tiers.
     *
     * @param totalPopulation the total number of entries to be stored
     * @return a ring configured for 30%-eviction benchmarks
     */
    static KeyValueRing createRingEvict30(final int totalPopulation) {
        return createRingEvict30(RING_SIZE, totalPopulation);
    }

    /**
     * Creates a {@link KeyValueRing} with a hot tier sized so that approximately
     * 30% of the given population spills to mmap tiers, using the specified
     * number of segments.
     *
     * @param ringSize the number of ring segments
     * @param totalPopulation the total number of entries to be stored
     * @return a ring configured for 30%-eviction benchmarks
     */
    static KeyValueRing createRingEvict30(final int ringSize,
                                          final int totalPopulation) {
        final int entriesPerSegment = totalPopulation / ringSize;
        int hotSize = (int) ((long) entriesPerSegment * ENTRY_SIZE_ESTIMATE * 7 / 10);
        hotSize = Math.max(hotSize, 64 * 1024);
        hotSize = Integer.highestOneBit(hotSize);
        return createRing(ringSize, hotSize);
    }

    /**
     * Creates a {@link KeyValueRing} with a fixed small hot tier
     * suitable for continuous-write eviction benchmarks.
     *
     * @return a ring configured for write-eviction benchmarks
     */
    static KeyValueRing createRingEvictWrite() {
        return createRing(RING_SIZE, HOT_TIER_EVICT_WRITE);
    }

    /**
     * Creates a {@link KeyValueRing} with a fixed small hot tier
     * suitable for continuous-write eviction benchmarks, using the
     * specified number of segments.
     *
     * @param ringSize the number of ring segments
     * @return a ring configured for write-eviction benchmarks
     */
    static KeyValueRing createRingEvictWrite(final int ringSize) {
        return createRing(ringSize, HOT_TIER_EVICT_WRITE);
    }

    /**
     * Creates a {@link KeyValueRing} with the given ring size and
     * hot tier size per segment.
     *
     * @param ringSize the number of ring segments
     * @param hotTierSizePerSegment the hot tier buffer size in bytes for each segment
     * @return a fully configured ring
     */
    private static KeyValueRing createRing(final int ringSize,
                                           final int hotTierSizePerSegment) {
        final File dir;
        try {
            dir = Files.createTempDirectory("d4m-kv-jmh-").toFile();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        dir.deleteOnExit();

        return new KeyValueRing(
                ringSize,
                SHUFFLE_MULTIPLIER,
                index -> new KeyValueSegment(
                        1,
                        new MmapTierFactory(
                                index,
                                hotTierSizePerSegment,
                                false,
                                INITIAL_CAPACITY,
                                MMAP_TIER_SIZE,
                                INITIAL_CAPACITY,
                                dir,
                                Integer.MAX_VALUE,
                                null
                        ),
                        null
                )
        );
    }

    /**
     * Creates a reusable key buffer of {@link #KEY_SIZE} bytes,
     * pre-filled with a zero-padding prefix.
     *
     * @return a reusable 32-byte key buffer
     */
    static UnsafeBuffer createKeyBuffer() {
        final byte[] keyBytes = new byte[KEY_SIZE];
        Arrays.fill(keyBytes, (byte) '0');
        return new UnsafeBuffer(keyBytes);
    }

    /**
     * Writes the ASCII representation of the given sequence number
     * into the key buffer, right-aligned within {@link #KEY_SIZE} bytes.
     * The leading bytes are left as-is (expected to be '0' padding).
     *
     * @param keyBuf the pre-allocated key buffer
     * @param seq the sequence number to encode
     */
    static void writeKeyInPlace(final UnsafeBuffer keyBuf, final long seq) {
        long val = seq;
        int pos = KEY_SIZE - 1;
        if (val == 0) {
            keyBuf.putByte(pos, (byte) '0');
            for (int i = 0; i < pos; i++) {
                keyBuf.putByte(i, (byte) '0');
            }
            return;
        }
        while (val > 0 && pos >= 0) {
            keyBuf.putByte(pos--, (byte) ('0' + (int) (val % 10)));
            val /= 10;
        }
        for (int i = 0; i <= pos; i++) {
            keyBuf.putByte(i, (byte) '0');
        }
    }

    /**
     * Creates a reusable value buffer of {@link #VALUE_SIZE} bytes.
     *
     * @return a 200-byte value buffer
     */
    static UnsafeBuffer createValueBuffer() {
        final byte[] data = new byte[VALUE_SIZE];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        return new UnsafeBuffer(data);
    }

    /**
     * Populates the ring with {@code count} key-value pairs using
     * pre-allocated, reusable buffers.
     *
     * @param ring the ring to populate
     * @param count the number of entries to insert
     */
    static void populate(final KeyValueRing ring, final int count) {
        final UnsafeBuffer keyBuf = createKeyBuffer();
        final UnsafeBuffer valueBuf = createValueBuffer();
        for (int i = 0; i < count; i++) {
            writeKeyInPlace(keyBuf, i);
            valueBuf.putLong(0, i);
            ring.put(keyBuf, 0, KEY_SIZE, valueBuf, 0, VALUE_SIZE);
        }
    }
}
