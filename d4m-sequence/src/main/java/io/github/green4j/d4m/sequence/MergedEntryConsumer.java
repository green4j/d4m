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
package io.github.green4j.d4m.sequence;

import io.github.green4j.d4m.common.AtomicBuffer;

/**
 * Callback for consuming entries from a merged cursor, where each entry
 * is tagged with the index of the source cursor that produced it.
 */
@FunctionalInterface
public interface MergedEntryConsumer {

    /**
     * Called for each entry delivered by a merged cursor.
     *
     * @param cursorIndex index of the source cursor (its position in the array
     *                    passed to the merged cursor's constructor, or the
     *                    corresponding index in {@code create(...)}) that
     *                    produced this entry
     * @param owner       the sequence the entry belongs to
     * @param order       order of the entry
     * @param buffer      the buffer containing the entry payload
     * @param offset      starting offset of the payload within the buffer
     * @param size        size of the payload in bytes
     */
    void onEntry(int cursorIndex,
                 Sequence owner,
                 long order,
                 AtomicBuffer buffer,
                 int offset,
                 int size);

}
