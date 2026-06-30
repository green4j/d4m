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
 * KeyLists-specific helpers for JMH benchmarks of the {@link
 * KeyListStorage} API layered on top of {@link KeyValueRing}.
 * Generic ring construction and buffer helpers come from
 * {@link BenchmarkSupport}.
 */
public final class KeyListsBenchmarkSupport {

    /**
     * Number of entries appended per list during pre-population for
     * the KeyLists read benchmark. Must match the {@code 10} baked
     * into {@link BenchmarkSupport#LIST_FOOTPRINT_ESTIMATE}.
     */
    public static final int ENTRIES_PER_LIST = 10;

    private KeyListsBenchmarkSupport() {
    }

    /**
     * Creates a {@link KeyListStorage} backed by a ring with the
     * shared {@link BenchmarkSupport#HOT_TIER}-sized hot tier.
     *
     * @return a key-lists store
     */
    static KeyListStorage createKeyLists() {
        return new KeyListStorage(BenchmarkSupport.createRing());
    }

    /**
     * Same as {@link #createKeyLists()} but with a custom segment
     * count (used by the concurrent benchmarks).
     *
     * @param ringSize the number of ring segments
     * @return a key-lists store
     */
    static KeyListStorage createKeyLists(final int ringSize) {
        return new KeyListStorage(BenchmarkSupport.createRing(ringSize));
    }

    /**
     * Pre-populates the store with {@code listCount} lists, each
     * holding {@code entriesPerList} entries. The key is encoded
     * in-buffer via {@code keyBuf.putLong(KEY_SIZE - Long.BYTES, i)}
     * on each step, matching the hot loop's encoding so steady-state
     * and warm-up paths exercise identical buffer layouts.
     *
     * @param lists          the store to populate
     * @param keyBuf         reusable key buffer (mutated per list)
     * @param value          shared value buffer
     * @param listCount      the number of distinct lists
     * @param entriesPerList the number of appends per list
     */
    static void populate(final KeyListStorage lists,
                         final UnsafeBuffer keyBuf,
                         final UnsafeBuffer value,
                         final int listCount,
                         final int entriesPerList) {
        final KeyListsWriter writer = lists.newWriter();
        for (int i = 0; i < listCount; i++) {
            keyBuf.putLong(BenchmarkSupport.KEY_SIZE - Long.BYTES, i);
            for (int e = 0; e < entriesPerList; e++) {
                writer.append(keyBuf, 0, BenchmarkSupport.KEY_SIZE,
                        value, 0, BenchmarkSupport.VALUE_SIZE);
            }
        }
    }
}
