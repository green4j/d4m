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
package io.github.green4j.d4m.queue;

import io.github.green4j.d4m.common.AtomicBuffer;

/**
 * Producer side of a single-producer/single-consumer binary queue.
 * Must be used by one thread only.
 */
public interface QueueWriter extends AutoCloseable {
    /**
     * Returned by {@link #offer} and {@link #claim} when the queue cannot accept the message now.
     */
    long REJECTED = -1;

    /**
     * Appends a copy of a message. Never blocks and never performs I/O.
     *
     * @param src    buffer holding the message
     * @param offset offset of the message in {@code src}
     * @param length length of the message, at most {@link #maxMessageLength()}
     * @return the sequence number assigned to the message, or {@link #REJECTED}
     * @throws IllegalArgumentException if {@code length} is negative or above {@link #maxMessageLength()}
     * @throws IllegalStateException    if the writer is closed or a claim is pending
     */
    long offer(AtomicBuffer src, int offset, int length);

    /**
     * Reserves room for a message of up to {@code maxLength} bytes in the queue itself,
     * so the message can be encoded in place without a copy. The message becomes
     * visible to the reader only on {@link #commit(int)}. Never blocks and never
     * performs I/O.
     *
     * @param maxLength the maximal length of the message, at most {@link #maxMessageLength()}
     * @return offset of the reserved room in {@link #claimBuffer()}, or {@code (int) REJECTED}
     * @throws IllegalArgumentException if {@code maxLength} is negative or above {@link #maxMessageLength()}
     * @throws IllegalStateException    if the writer is closed or a claim is pending
     */
    int claim(int maxLength);

    /**
     * Returns the buffer the last successful {@link #claim} reserved room in. Valid until
     * {@link #commit(int)} or {@link #abort()}; undefined while no claim is pending.
     *
     * @return the buffer of the pending claim
     */
    AtomicBuffer claimBuffer();

    /**
     * Publishes the message encoded into the room reserved by {@link #claim}.
     *
     * @param length the actual length of the message, at most the claimed length
     * @return the sequence number assigned to the message
     * @throws IllegalArgumentException if {@code length} is negative or above the claimed length
     * @throws IllegalStateException    if nothing is claimed
     */
    long commit(int length);

    /**
     * Discards the room reserved by {@link #claim}; nothing is published.
     */
    void abort();

    /**
     * Returns the maximal message length accepted by {@link #offer} and {@link #claim}.
     *
     * @return the maximal message length in bytes
     */
    int maxMessageLength();

    /**
     * Stops accepting messages: {@link #offer} and {@link #claim} throw afterwards. Already
     * offered messages stay readable, and a message claimed before can still be committed
     * or aborted.
     */
    @Override
    void close();
}
