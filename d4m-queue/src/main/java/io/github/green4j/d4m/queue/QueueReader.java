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
 * Consumer side of a single-producer/single-consumer binary queue.
 * Must be used by one thread only.
 */
public interface QueueReader {

    /**
     * Delivers up to {@code limit} messages to the handler. Never blocks.
     *
     * @param handler receives the messages
     * @param limit   maximal number of messages to deliver, positive
     * @return number of messages delivered
     * @throws IllegalArgumentException if {@code limit} is not positive
     */
    int poll(MessageHandler handler, int limit);

    /**
     * Returns the sequence number of the last message delivered or skipped.
     *
     * @return the last processed sequence number; before the first message, the sequence
     *         number just below the first message the reader starts from
     */
    long position();

    /**
     * Makes the reader skip all messages with sequence numbers below {@code seq}.
     * Only moves forward: a {@code seq} at or below the current position has no effect.
     *
     * <p>A reader that saves the sequence number of the last message it delivered can
     * resume with {@code seek(saved + 1)} without duplicates as long as sequence numbers
     * are never reused. An implementation documents when this holds; for
     * {@link SegmentedQueue} see {@link DiskMode}.</p>
     *
     * @param seq sequence number of the first message to deliver
     * @return sequence number of the next message to be delivered
     */
    long seek(long seq);
}
