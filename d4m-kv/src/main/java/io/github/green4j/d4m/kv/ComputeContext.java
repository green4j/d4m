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

/**
 * Reusable, mutable handle that a {@link ComputeAction} or
 * {@link TwoKeyComputeAction} uses to interact with one key during a
 * {@link KeyValues#compute} call.
 *
 * <p>Action implementations own one (or two) {@code ComputeContext} instances
 * as fields and return them from {@code context()} / {@code key1Context()} /
 * {@code key2Context()}. The {@code KeyValues} implementation populates the
 * package-private fields before invoking {@code execute()}; the action then
 * uses {@link #get} and {@link #put} to operate on the key.
 *
 * <p>The bound key buffer reference, offset and size are valid only for the
 * duration of one {@code execute()} call. Do not retain them across calls.
 */
public final class ComputeContext {
    KeyValueSegment segment;
    int hash;
    AtomicBuffer key;
    int keyOffset;
    int keySize;

    /**
     * Creates an unbound context. Bind via the owning {@link KeyValues}
     * implementation by passing the enclosing action to
     * {@link KeyValues#compute}.
     */
    public ComputeContext() {
    }

    /**
     * Reads the value currently associated with this context's bound key,
     * delivering it to the consumer.
     *
     * @param consumer the consumer that receives the value, if present
     * @return {@code true} if a value was found and delivered
     */
    public boolean get(final KeyValueConsuming.ValueConsumer<KeyValueConsuming.Value> consumer) {
        return segment.get(hash, key, keyOffset, keySize, consumer);
    }

    /**
     * Writes the given value under this context's bound key. If a value
     * already exists, it is updated in place when the size matches; a size
     * change throws {@link IllegalArgumentException}.
     *
     * @param value       the buffer containing the value
     * @param valueOffset the offset of the value within the buffer
     * @param valueSize   the size of the value in bytes
     */
    public void put(final AtomicBuffer value, final int valueOffset, final int valueSize) {
        segment.put(hash, key, keyOffset, keySize, value, valueOffset, valueSize);
    }

    void bind(final KeyValueSegment seg, final int h,
              final AtomicBuffer k, final int ko, final int ks) {
        this.segment = seg;
        this.hash = h;
        this.key = k;
        this.keyOffset = ko;
        this.keySize = ks;
    }
}
