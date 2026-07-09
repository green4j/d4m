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
 * Listener notified when a key-value entry is evicted from a tier or segment,
 * intended for logging and metrics.
 *
 * <p>Threading: on a {@link KeyValueRing}, {@link #onEviction} is invoked inline
 * on the caller's {@code put} / {@code compute} thread while that segment's
 * exclusive write lock is held (a two-key {@code compute} holds two segment write
 * locks). On a {@link SingleThreadedKeyValueRing} it runs on the single caller
 * thread with no lock. Implementations must be fast and non-blocking, must not
 * call back into the store (doing so risks re-entrant locking or deadlock), and
 * must not retain the supplied {@code keyValue} buffer beyond the call.</p>
 */
public interface EvictionListener {

    /**
     * Called when a key-value entry is being evicted.
     *
     * @param notifier    the object (tier or segment) that triggered the eviction
     * @param hash        the hash of the evicted key
     * @param keyValue    the buffer containing both key and value data
     * @param keyOffset   the offset of the key within the buffer
     * @param keySize     the size of the key in bytes
     * @param valueOffset the offset of the value within the buffer
     * @param valueSize   the size of the value in bytes
     */
    void onEviction(Object notifier,
                    int hash,
                    AtomicBuffer keyValue,
                    int keyOffset,
                    int keySize,
                    int valueOffset,
                    int valueSize);

}
