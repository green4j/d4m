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
 * {@link KeyValueRing} API. Generic ring construction, eviction
 * profiles, and buffer helpers live in {@link BenchmarkSupport}.
 */
public final class KeyValuesBenchmarkSupport {

    private KeyValuesBenchmarkSupport() {
    }

    /**
     * Populates the ring with the first {@code count} pre-built keys, each
     * mapped to the shared {@code value} buffer. Lets benchmark @Setup reuse
     * the same key array later in the hot loop, so the populate path and
     * the measured path share the identical buffer layout.
     *
     * @param ring   the ring to populate
     * @param keys   pre-built key buffers (e.g. from {@link BenchmarkSupport#createKeyArray()})
     * @param value  shared value buffer (e.g. from {@link BenchmarkSupport#createValueBuffer()})
     * @param count  the number of entries to insert; must be {@code <= keys.length}
     */
    static void populate(final KeyValueRing ring,
                         final UnsafeBuffer[] keys,
                         final UnsafeBuffer value,
                         final int count) {
        for (int i = 0; i < count; i++) {
            final UnsafeBuffer key = keys[i];
            ring.put(key, 0, BenchmarkSupport.KEY_SIZE,
                    value, 0, BenchmarkSupport.VALUE_SIZE);
        }
    }
}
