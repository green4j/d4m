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
import io.github.green4j.d4m.kv.KeyListStorage;
import io.github.green4j.d4m.kv.KeyListsWriter;
import io.github.green4j.d4m.kv.KeyValueRing;

/**
 * KeyLists-specific helpers for JMH benchmarks of the {@link KeyListStorage}
 * API layered on top of {@link KeyValueRing}. Generic ring construction,
 * eviction profiles, and buffer helpers come from {@link BenchmarkSupport}.
 */
public final class KeyListsBenchmarkSupport {

    /**
     * Number of entries appended per list during pre-population for the
     * KeyLists read/list benchmarks.
     */
    public static final int ENTRIES_PER_LIST = 10;

    private KeyListsBenchmarkSupport() {
    }

    /**
     * Creates a {@link KeyListStorage} wrapping a no-eviction ring.
     *
     * @return a key-lists store backed by a ring that never evicts to mmap
     */
    static KeyListStorage createKeyListsNoEviction() {
        return new KeyListStorage(BenchmarkSupport.createRingNoEviction());
    }

    /**
     * Creates a {@link KeyListStorage} wrapping a {@link
     * BenchmarkSupport#HOT_TIER_EVICT}-hot-tier ring for the {@code evict30}
     * profile - hot tier sized so the working set is cache-cold (real
     * CPU-cache misses) while still forcing eviction to mmap when the tier
     * fills.
     *
     * @return a key-lists store for the evict30 profile
     */
    static KeyListStorage createKeyListsEvict30() {
        return new KeyListStorage(BenchmarkSupport.createRingEvict30());
    }

    /**
     * Same as {@link #createKeyListsEvict30()} but with a custom number of
     * segments (used by the concurrent benchmarks).
     *
     * @param ringSize the number of ring segments
     * @return a key-lists store for the evict30 profile
     */
    static KeyListStorage createKeyListsEvict30(final int ringSize) {
        return new KeyListStorage(BenchmarkSupport.createRingEvict30(ringSize));
    }

    /**
     * Pre-populates the store with {@code listCount} lists, each holding
     * {@code entriesPerList} entries, using the supplied pre-built key
     * array and shared value buffer. The same arrays are reused by the
     * benchmark's hot loop so the steady-state and warm-up paths exercise
     * identical buffer layouts.
     *
     * @param lists            the store to populate
     * @param keys             pre-built key buffers
     * @param value            shared value buffer
     * @param listCount        the number of distinct lists (user keys); must be {@code <= keys.length}
     * @param entriesPerList   the number of appends per list
     */
    static void populate(final KeyListStorage lists,
                         final UnsafeBuffer[] keys,
                         final UnsafeBuffer value,
                         final int listCount,
                         final int entriesPerList) {
        final KeyListsWriter writer = lists.newWriter();
        for (int i = 0; i < listCount; i++) {
            final UnsafeBuffer key = keys[i];
            for (int e = 0; e < entriesPerList; e++) {
                writer.append(key, 0, BenchmarkSupport.KEY_SIZE,
                        value, 0, BenchmarkSupport.VALUE_SIZE);
            }
        }
    }
}
