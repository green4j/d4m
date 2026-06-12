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
 * An append-only list storage keyed by {@link AtomicBuffer} keys. Each key
 * maps to an ordered sequence of values that can only grow. Delete is not
 * supported.
 *
 * <h2>Threading</h2>
 * {@code KeyLists} inherits the threading contract of the backing
 * {@link KeyValues} store:
 * <ul>
 *   <li>Single-threaded backing store -> all calls (including the writer's
 *       {@code append} and any reader's {@code list} / accessor iteration)
 *       must come from one thread. No locks are taken above what the backing
 *       store already does, i.e. none.</li>
 *   <li>Thread-safe backing store -> any number of writer threads may
 *       {@code append} concurrently on their own {@link KeyListsWriter}
 *       instances (obtained from {@link #newWriter()}), including against the
 *       same user key. Concurrently, any number of reader threads may call
 *       {@link #list} and iterate via their own {@link ListAccessor}
 *       instances. No extra synchronization is added at the {@code KeyLists}
 *       layer; concurrency cost is paid only by the chosen
 *       {@link KeyValues}.</li>
 * </ul>
 *
 * <p>One rule per thread: a single {@link KeyListsWriter} or
 * {@link ListAccessor} instance must not be shared across threads.
 */
public interface KeyLists {

    /**
     * Returns a new {@link KeyListsWriter} bound to this {@code KeyLists}
     * instance. The returned writer owns reusable per-thread buffers and is
     * intended to be reused across many {@code append} calls on its owning
     * thread.
     *
     * @return a fresh writer
     */
    KeyListsWriter newWriter();

    /**
     * Binds the given accessor to the list stored under {@code key}. After
     * this call, the accessor is associated with this {@code KeyLists}
     * instance and can be used to read entries via {@link ListAccessor#get}
     * and {@link ListAccessor#forEach}.
     *
     * <p>Calling this method replaces any previously bound state in the
     * accessor.
     *
     * @param accessor  the accessor to populate (created independently via
     *                  {@link ListAccessor#ListAccessor()})
     * @param key       the buffer containing the key
     * @param keyOffset the offset of the key within the buffer
     * @param keySize   the size of the key in bytes
     * @return {@code true} if the list exists (has at least one entry)
     */
    boolean list(ListAccessor accessor, AtomicBuffer key, int keyOffset, int keySize);
}
