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
 * {@code noEviction} writes a count that fills 95 % of the hot tiers
 * (0 % spill), {@code evict30} writes as much as fills the hot tiers
 * 1 / 0.70 times (30 % spill to mmap). The counts scale with the number
 * of segments of the ring.
 */
public final class BenchmarkSupport {
    public static final int KEY_SIZE = 32;
    public static final int VALUE_SIZE = 200;

    static final int RING_SIZE = 8;
    static final int SHUFFLE_MULTIPLIER = 16;
    static final int INITIAL_CAPACITY = 65536;

    /**
     * KeyValues entry in the segment's binary buffer: 16-byte header,
     * 32-byte key and 200-byte value, all 8-byte aligned.
     */
    static final int ENTRY_SIZE_ESTIMATE = 16 + 32 + 200;

    /**
     * KeyLists metadata entry: 16-byte header, the 33-byte prefixed
     * key padded to 40 bytes and the 8-byte metadata value.
     */
    static final int LIST_METADATA_ENTRY_SIZE = 16 + 40 + 8;

    /**
     * KeyLists data entry: 16-byte header, 8-byte synthetic key and
     * 200-byte value.
     */
    static final int LIST_DATA_ENTRY_SIZE = 16 + 8 + 200;

    /**
     * Estimated KeyLists per-list footprint of the read benchmark: one
     * metadata entry plus {@code ENTRIES_PER_LIST = 10} data entries.
     */
    static final int LIST_FOOTPRINT_ESTIMATE = listFootprint(KeyListsBenchmarkSupport.ENTRIES_PER_LIST);

    /**
     * Fraction of {@code evict30}'s working set that lives in the hot
     * tier; the remaining {@code 1 - EVICT30_HEAP_FRACTION} (~30 %)
     * spills to mmap.
     */
    static final double EVICT30_HEAP_FRACTION = 0.70;

    /**
     * Fraction of the hot tiers the {@code noEviction} profile fills. The
     * hash spreads keys over the segments unevenly by a fraction of a
     * percent; filling them completely would overflow some of them, and
     * a cyclic update of an overflowing segment misses on every access.
     */
    static final double NO_EVICTION_FILL = 0.95;

    /**
     * Single per-segment hot tier shared by both eviction profiles.
     * Must be a power of two (see {@code KeyValueBuffer}). 32 MB
     * exceeds the M1 Pro 24 MB SLC, so the hot working set stays
     * cache-cold.
     */
    static final int HOT_TIER = 32 * 1024 * 1024;

    static final int MMAP_TIER_SIZE = 256 * 1024 * 1024;

    /**
     * KeyValues op count for the {@code noEviction} profile with
     * {@link #RING_SIZE} segments: the entries fill 95 % of the hot
     * tiers, so 0 % spill to mmap.
     */
    public static final int KV_KEYS_NO_EVICTION = kvKeysNoEviction(RING_SIZE);

    /**
     * KeyValues op count for the {@code evict30} profile with
     * {@link #RING_SIZE} segments: 30 % of the entries spill to mmap.
     */
    public static final int KV_KEYS_EVICT_30 = kvKeysEvict30(RING_SIZE);

    /**
     * KeyLists list count for the {@code noEviction} profile of the read
     * benchmark: the lists (one metadata entry + {@code ENTRIES_PER_LIST
     * = 10} data entries each) fill 95 % of the hot tiers.
     */
    public static final int KL_LISTS_NO_EVICTION =
            klListsNoEviction(RING_SIZE, KeyListsBenchmarkSupport.ENTRIES_PER_LIST);

    /**
     * KeyLists list count for the {@code evict30} profile of the read
     * benchmark: 30 % of the lists spill to mmap.
     */
    public static final int KL_LISTS_EVICT_30 =
            klListsEvict30(RING_SIZE, KeyListsBenchmarkSupport.ENTRIES_PER_LIST);

    private BenchmarkSupport() {
    }

    /**
     * Returns the footprint of a list in the segment's binary buffer.
     *
     * @param entriesPerList the number of entries of the list
     * @return the footprint in bytes
     */
    static int listFootprint(final int entriesPerList) {
        return LIST_METADATA_ENTRY_SIZE + entriesPerList * LIST_DATA_ENTRY_SIZE;
    }

    /**
     * Returns the KeyValues op count of the {@code noEviction} profile.
     *
     * @param ringSize the number of segments of the ring
     * @return the op count
     */
    static int kvKeysNoEviction(final int ringSize) {
        return (int) (hotCapacity(ringSize, ENTRY_SIZE_ESTIMATE) * NO_EVICTION_FILL);
    }

    /**
     * Returns the KeyValues op count of the {@code evict30} profile.
     *
     * @param ringSize the number of segments of the ring
     * @return the op count
     */
    static int kvKeysEvict30(final int ringSize) {
        return (int) (hotCapacity(ringSize, ENTRY_SIZE_ESTIMATE) / EVICT30_HEAP_FRACTION);
    }

    /**
     * Returns the number of lists of {@code entriesPerList} entries of the
     * {@code noEviction} profile.
     *
     * @param ringSize       the number of segments of the ring
     * @param entriesPerList the number of entries per list
     * @return the list count
     */
    static int klListsNoEviction(final int ringSize,
                                 final int entriesPerList) {
        return (int) (hotCapacity(ringSize, listFootprint(entriesPerList)) * NO_EVICTION_FILL);
    }

    /**
     * Returns the number of lists of {@code entriesPerList} entries of the
     * {@code evict30} profile.
     *
     * @param ringSize       the number of segments of the ring
     * @param entriesPerList the number of entries per list
     * @return the list count
     */
    static int klListsEvict30(final int ringSize,
                              final int entriesPerList) {
        return (int) (hotCapacity(ringSize, listFootprint(entriesPerList)) / EVICT30_HEAP_FRACTION);
    }

    // How many items of the given size fill the hot tiers of all segments.
    private static long hotCapacity(final int ringSize,
                                    final int itemSize) {
        return (long) ringSize * HOT_TIER / itemSize;
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
