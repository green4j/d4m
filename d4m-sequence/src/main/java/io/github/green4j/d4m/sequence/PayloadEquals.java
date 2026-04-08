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

/**
 * Predicate for comparing two entry payloads for logical equality.
 * Used by insert-or-update operations to locate a matching existing entry.
 */
@FunctionalInterface
public interface PayloadEquals {

    /**
     * Returns {@code true} if the two payload regions are considered equal.
     *
     * @param payload1       buffer containing the first payload
     * @param payload1Offset offset of the first payload within its buffer
     * @param payload1Size   size of the first payload in bytes
     * @param payload2       buffer containing the second payload
     * @param payload2Offset offset of the second payload within its buffer
     * @param payload2Size   size of the second payload in bytes
     * @return {@code true} if the payloads are equal
     */
    boolean eq(io.github.green4j.d4m.common.AtomicBuffer payload1, int payload1Offset, int payload1Size,
               io.github.green4j.d4m.common.AtomicBuffer payload2, int payload2Offset, int payload2Size);
}
