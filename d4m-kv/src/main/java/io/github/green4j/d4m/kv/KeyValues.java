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
     * @param key         the buffer containing the key
     * @param keyOffset   the offset of the key within the buffer
     * @param keySize     the size of the key in bytes
     * @param value       the buffer containing the value
     * @param valueOffset the offset of the value within the buffer
     * @param valueSize   the size of the value in bytes
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
     * @param key       the buffer containing the key to look up
     * @param keyOffset the offset of the key within the buffer
     * @param keySize   the size of the key in bytes
     * @param consumer  the consumer that will receive the value if found
     * @return {@code true} if the key was found and the value delivered to the consumer
     */
    boolean get(AtomicBuffer key,
                int keyOffset,
                int keySize,
                KeyValueConsuming.ValueConsumer<KeyValueConsuming.Value> consumer);

    /**
     * Atomically runs the given action against one key. The implementation
     * may acquire whatever locking is appropriate (none, in the
     * single-threaded case) before invoking the action's
     * {@link ComputeAction#execute()}.
     *
     * <p>The action must not call any other {@link KeyValues} method during
     * {@code execute()} - only the {@link ComputeContext} returned by
     * {@link ComputeAction#context()} may be used.
     *
     * @param key       the buffer containing the key
     * @param keyOffset the offset of the key within the buffer
     * @param keySize   the size of the key in bytes
     * @param action    the action to run atomically
     */
    void compute(AtomicBuffer key,
                 int keyOffset,
                 int keySize,
                 ComputeAction action);

    /**
     * Atomically runs the given action against two keys. The implementation
     * acquires whatever locks the two keys imply in a canonical order so
     * that concurrent calls with the same two keys (in either order) cannot
     * deadlock.
     *
     * <p>The action must not call any other {@link KeyValues} method during
     * {@code execute()} - only the {@link ComputeContext}s returned by
     * {@link TwoKeyComputeAction#key1Context()} and
     * {@link TwoKeyComputeAction#key2Context()} may be used.
     *
     * @param key1       the buffer containing the first key
     * @param key1Offset the offset of the first key within its buffer
     * @param key1Size   the size of the first key in bytes
     * @param key2       the buffer containing the second key
     * @param key2Offset the offset of the second key within its buffer
     * @param key2Size   the size of the second key in bytes
     * @param action     the action to run atomically
     */
    void compute(AtomicBuffer key1,
                 int key1Offset,
                 int key1Size,
                 AtomicBuffer key2,
                 int key2Offset,
                 int key2Size,
                 TwoKeyComputeAction action);

}
