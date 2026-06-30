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

/**
 * KeyValues-specific helpers for JMH benchmarks of the raw
 * {@link KeyValueRing} API. Generic ring construction and buffer
 * helpers live in {@link BenchmarkSupport}.
 */
public final class KeyValuesBenchmarkSupport {

    private KeyValuesBenchmarkSupport() {
    }

    /**
     * Populates the ring with {@code count} key-value pairs by
     * stamping each sequence number into the trailing 8 bytes of the
     * shared key buffer via {@code keyBuf.putLong(KEY_SIZE -
     * Long.BYTES, i)} and re-using the shared value buffer. The
     * populate path and the measured hot loop share identical buffer
     * layouts.
     *
     * @param ring   the ring to populate
     * @param keyBuf reusable key buffer (mutated per entry)
     * @param value  shared value buffer
     * @param count  the number of entries to insert
     */
    static void populate(final KeyValueRing ring,
                         final UnsafeBuffer keyBuf,
                         final UnsafeBuffer value,
                         final int count) {
        for (int i = 0; i < count; i++) {
            keyBuf.putLong(BenchmarkSupport.KEY_SIZE - Long.BYTES, i);
            ring.put(keyBuf, 0, BenchmarkSupport.KEY_SIZE,
                    value, 0, BenchmarkSupport.VALUE_SIZE);
        }
    }
}
