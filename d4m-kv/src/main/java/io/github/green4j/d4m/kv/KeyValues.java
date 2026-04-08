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
 * A storage abstraction for inserting and retrieving key-value pairs backed by {@link AtomicBuffer}s.
 */
public interface KeyValues {

    /**
     * Stores a key-value pair. If the key already exists, the value is updated in place
     * (the new value must be the same size as the existing one).
     *
     * @param key the buffer containing the key
     * @param keyOffset the offset of the key within the buffer
     * @param keySize the size of the key in bytes
     * @param value the buffer containing the value
     * @param valueOffset the offset of the value within the buffer
     * @param valueSize the size of the value in bytes
     */
    void put(AtomicBuffer key,
             int keyOffset,
             int keySize,
             AtomicBuffer value,
             int valueOffset,
             int valueSize);

    /**
     * Retrieves the value associated with the given key.
     *
     * @param key the buffer containing the key to look up
     * @param keyOffset the offset of the key within the buffer
     * @param keySize the size of the key in bytes
     * @param consumer the consumer that will receive the value if found
     * @return {@code true} if the key was found and the value delivered to the consumer
     */
    boolean get(AtomicBuffer key,
                int keyOffset,
                int keySize,
                KeyValueConsuming.ValueConsumer<KeyValueConsuming.Value> consumer);

}
