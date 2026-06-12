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
 * A per-thread writer for a {@link KeyLists} store. Obtain one via
 * {@link KeyLists#newWriter()} and reuse it across all of one thread's
 * {@code append} calls. Writers own their per-thread reusable buffers, so
 * multiple writers may operate concurrently against the same underlying
 * {@link KeyValues} as long as that store is itself thread-safe.
 *
 * <p>Each {@code KeyListsWriter} instance is bound to a single thread; do not
 * share an instance across threads.
 */
public interface KeyListsWriter {

    /**
     * Appends a value to the list associated with the given key. If no list
     * exists for the key, a new list is created.
     *
     * @param key         the buffer containing the key
     * @param keyOffset   the offset of the key within the buffer
     * @param keySize     the size of the key in bytes
     * @param value       the buffer containing the value to append
     * @param valueOffset the offset of the value within the buffer
     * @param valueSize   the size of the value in bytes
     */
    void append(AtomicBuffer key, int keyOffset, int keySize,
                AtomicBuffer value, int valueOffset, int valueSize);
}
