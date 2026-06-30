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

    /**
     * Pre-built key count for the noEviction read / write / concurrent
     * benchmarks. Power-of-two so the hot loop can index with AND instead
     * of {@code %}.
     */
    public static final int KEY_ARRAY_SIZE = 131_072;

    /** Mask paired with {@link #KEY_ARRAY_SIZE} for index lookups. */
    public static final int KEY_INDEX_MASK = KEY_ARRAY_SIZE - 1;

    /**
     * Pre-built key count for the {@code evict30} read benchmarks. Sized so
     * that, with {@link #RING_SIZE} segments and {@link #ENTRY_SIZE_ESTIMATE}
     * bytes per entry, the working set per segment (~60 MB) is clearly
     * larger than the Apple M1 Pro 24 MB system-level cache - the reader
     * therefore takes real CPU-cache misses on top of mmap accesses, which
     * is the realistic shape of a workload that has spilled past the hot
     * tier. Power-of-two so the hot loop still indexes with an AND mask.
     */
    public static final int EVICT_KEY_ARRAY_SIZE = 2 * 1024 * 1024;

    /** Mask paired with {@link #EVICT_KEY_ARRAY_SIZE} for index lookups. */
    public static final int EVICT_KEY_INDEX_MASK = EVICT_KEY_ARRAY_SIZE - 1;

    static final int RING_SIZE = 8;
    static final int SHUFFLE_MULTIPLIER = 16;
    static final int INITIAL_CAPACITY = 65536;

    static final int ENTRY_SIZE_ESTIMATE = 240;

    static final int HOT_TIER_NO_EVICTION = 128 * 1024 * 1024;
    /**
     * Per-segment hot tier for the {@code evict30} profile. Sized to exceed
     * the Apple M1 Pro 24 MB system-level cache so the writer's / reader's
     * hot working set is cache-cold and takes real CPU-cache misses, while
     * still forcing continuous eviction to mmap once the tier fills.
     */
    static final int HOT_TIER_EVICT = 32 * 1024 * 1024;
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
     * Creates a {@link KeyValueRing} with a {@link #HOT_TIER_EVICT} hot
     * tier per segment - large enough that the hot working set is
     * cache-cold (so the writer/reader takes real CPU-cache misses), yet
     * small enough to force continuous eviction to mmap once it fills.
     * Use for the {@code evict30} profile.
     *
     * @return a ring configured for the evict30 profile
     */
    static KeyValueRing createRingEvict30() {
        return createRing(RING_SIZE, HOT_TIER_EVICT);
    }

    /**
     * Creates a {@link KeyValueRing} with a {@link #HOT_TIER_EVICT} hot
     * tier per segment, using the specified number of segments. Use for
     * the {@code evict30} profile.
     *
     * @param ringSize the number of ring segments
     * @return a ring configured for the evict30 profile
     */
    static KeyValueRing createRingEvict30(final int ringSize) {
        return createRing(ringSize, HOT_TIER_EVICT);
    }

    /**
     * Creates a {@link KeyValueRing} with the given ring size and
     * hot tier size per segment.
     *
     * @param ringSize              the number of ring segments
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
     * @param seq    the sequence number to encode
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
     * Writes just the trailing ASCII digits of {@code seq} into the buffer,
     * starting from byte {@code KEY_SIZE - 1} and walking left. Assumes the
     * buffer was previously filled with {@code '0'} and that {@code seq} only
     * ever grows (cycles back would leave leftover high-order digits, but
     * the eviction-write benchmarks use strictly increasing sequence
     * numbers). Much cheaper than {@link #writeKeyInPlace} which also
     * rewrites the leading padding every call.
     *
     * @param keyBuf the pre-allocated key buffer (must have leading '0' padding)
     * @param seq    the strictly monotonic sequence number to encode
     */
    static void writeKeyTail(final UnsafeBuffer keyBuf, final long seq) {
        long val = seq;
        int pos = KEY_SIZE - 1;
        do {
            keyBuf.putByte(pos--, (byte) ('0' + (int) (val % 10)));
            val /= 10;
        } while (val > 0);
    }

    /**
     * Builds {@link #KEY_ARRAY_SIZE} pre-encoded, immutable key buffers.
     * Each key is a {@link #KEY_SIZE}-byte right-aligned ASCII number
     * (key {@code i} encodes {@code i}). Benchmarks index into this array
     * via {@code seq & KEY_INDEX_MASK} in the hot loop so the measured op
     * is just the storage call - no per-op ASCII conversion or modulo.
     *
     * @return an array of {@link #KEY_ARRAY_SIZE} pre-encoded key buffers
     */
    static UnsafeBuffer[] createKeyArray() {
        return createKeyArray(KEY_ARRAY_SIZE);
    }

    /**
     * Builds {@link #EVICT_KEY_ARRAY_SIZE} pre-encoded, immutable key
     * buffers for the {@code evict30} read profile. Same encoding and
     * hot-loop shape as {@link #createKeyArray()} but with a much larger
     * working set so the read benchmarks take real CPU-cache misses on
     * top of the mmap accesses.
     *
     * @return an array of {@link #EVICT_KEY_ARRAY_SIZE} pre-encoded key buffers
     */
    static UnsafeBuffer[] createEvictKeyArray() {
        return createKeyArray(EVICT_KEY_ARRAY_SIZE);
    }

    private static UnsafeBuffer[] createKeyArray(final int count) {
        final UnsafeBuffer[] keys = new UnsafeBuffer[count];
        for (int i = 0; i < count; i++) {
            final byte[] bytes = new byte[KEY_SIZE];
            Arrays.fill(bytes, (byte) '0');
            keys[i] = new UnsafeBuffer(bytes);
            writeKeyInPlace(keys[i], i);
        }
        return keys;
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

}
