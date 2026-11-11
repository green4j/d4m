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

/**
 * Single-producer/single-consumer queue of binary messages.
 *
 * <p>Every message gets a sequence number: {@code 0, 1, 2, ...}. The writer and
 * the reader may run on any two threads; each of them must stay on its own
 * thread. The counters below are safe to read from any thread.</p>
 */
public interface BinaryQueue {

    /**
     * Returns the writer. Must be used by one thread only.
     *
     * @return the writer
     */
    QueueWriter writer();

    /**
     * Returns the reader. Must be used by one thread only.
     *
     * @return the reader
     */
    QueueReader reader();

    /**
     * Returns the sequence number of the last message appended by {@link QueueWriter#offer}
     * or {@link QueueWriter#commit}.
     *
     * @return the last appended sequence number
     */
    long writerSeq();

    /**
     * Returns the sequence number of the last message processed by the reader.
     *
     * @return the last processed sequence number
     */
    long readerSeq();

    /**
     * Returns how many times {@link QueueWriter#offer} or {@link QueueWriter#claim} returned
     * {@link QueueWriter#REJECTED}.
     *
     * @return the number of rejected offers and claims
     */
    long rejectedOffers();
}
