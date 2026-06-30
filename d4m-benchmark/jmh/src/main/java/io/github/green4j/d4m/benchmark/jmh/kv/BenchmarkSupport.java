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

/**
 * Shared utilities and constants for JMH benchmarks in the d4m-kv module.
 *
 * <p>Both eviction profiles share the same {@link #HOT_TIER} size;
 * profile differentiation comes from <strong>how many entries each
 * profile writes</strong> within a single bounded JMH iteration:
 * {@code noEviction} writes a count that fits in the hot tier (0 %
 * spill), {@code evict30} writes 30 % more (30 % spill to mmap).
 */
public final class BenchmarkSupport {
    public static final int KEY_SIZE = 32;
    public static final int VALUE_SIZE = 200;

    static final int RING_SIZE = 8;
    static final int SHUFFLE_MULTIPLIER = 16;
    static final int INITIAL_CAPACITY = 65536;

    static final int ENTRY_SIZE_ESTIMATE = 240;

    /**
     * Estimated KeyLists per-list footprint in the segment's binary
     * buffer: one metadata entry (~57 B) plus {@code ENTRIES_PER_LIST
     * = 10} data entries (~224 B each).
     */
    static final int LIST_FOOTPRINT_ESTIMATE = 57 + 10 * 224;

    /**
     * Fraction of {@code evict30}'s working set that lives in the hot
     * tier; the remaining {@code 1 - EVICT30_HEAP_FRACTION} (~30 %)
     * spills to mmap.
     */
    static final double EVICT30_HEAP_FRACTION = 0.70;

    /**
     * Single per-segment hot tier shared by both eviction profiles.
     * Must be a power of two (see {@code KeyValueBuffer}). 32 MB
     * exceeds the M1 Pro 24 MB SLC, so the hot working set stays
     * cache-cold.
     */
    static final int HOT_TIER = 32 * 1024 * 1024;

    static final int MMAP_TIER_SIZE = 256 * 1024 * 1024;

    /**
     * KeyValues op count for the {@code noEviction} profile: every
     * entry fits in {@link #HOT_TIER}, so 0 % spill to mmap.
     */
    public static final int KV_KEYS_NO_EVICTION =
            (int) ((long) RING_SIZE * HOT_TIER / ENTRY_SIZE_ESTIMATE);

    /**
     * KeyValues op count for the {@code evict30} profile: 30 % more
     * entries than fit in {@link #HOT_TIER}, so 30 % spill to mmap.
     */
    public static final int KV_KEYS_EVICT_30 =
            (int) (KV_KEYS_NO_EVICTION / EVICT30_HEAP_FRACTION);

    /**
     * KeyLists list count for the {@code noEviction} profile: each
     * list (one metadata entry + {@code ENTRIES_PER_LIST = 10} data
     * entries) fits in {@link #HOT_TIER}.
     */
    public static final int KL_LISTS_NO_EVICTION =
            (int) ((long) RING_SIZE * HOT_TIER / LIST_FOOTPRINT_ESTIMATE);

    /**
     * KeyLists list count for the {@code evict30} profile: 30 % more
     * lists than fit in {@link #HOT_TIER}, so 30 % spill to mmap.
     */
    public static final int KL_LISTS_EVICT_30 =
            (int) (KL_LISTS_NO_EVICTION / EVICT30_HEAP_FRACTION);

    private BenchmarkSupport() {
    }

    /**
     * Creates a {@link KeyValueRing} with the shared {@link #HOT_TIER}
     * per segment and the default {@link #RING_SIZE} segments.
     *
     * @return a ring shared by both eviction profiles
     */
    static KeyValueRing createRing() {
        return createRing(RING_SIZE);
    }

    /**
     * Creates a {@link KeyValueRing} with the shared {@link #HOT_TIER}
     * per segment and a custom segment count. Used by the concurrent
     * benchmarks.
     *
     * @param ringSize the number of ring segments
     * @return a fully configured ring
     */
    static KeyValueRing createRing(final int ringSize) {
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
                                HOT_TIER,
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
     * Allocates a reusable {@link #KEY_SIZE}-byte key buffer. The
     * bytes are left at their JVM-zero default; the write / read
     * benchmarks stamp the per-call identifier into the trailing 8
     * bytes via {@code putLong(KEY_SIZE - Long.BYTES, seq)} on every
     * op.
     *
     * @return a reusable 32-byte key buffer
     */
    static UnsafeBuffer createKeyBuffer() {
        return new UnsafeBuffer(new byte[KEY_SIZE]);
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
