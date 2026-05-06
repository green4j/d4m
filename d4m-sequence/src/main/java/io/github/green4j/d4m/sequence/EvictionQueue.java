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

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Thread-safe queue of sealed heap chunks that are candidates for
 * eviction to memory-mapped storage. Writers enqueue chunks after
 * sealing; the eviction path polls and processes them.
 */
public final class EvictionQueue {

    /**
     * Snapshot of a chunk and its epoch at the time of enqueue,
     * together with a reference to the owning sequence.
     */
    public static final class Item {
        /**
         * The sequence that owns the chunk.
         */
        public final Sequence owner;
        /**
         * The heap-backed chunk to be evicted.
         */
        public final Chunk heapChunk;
        /**
         * The chunk epoch captured at enqueue time for staleness detection.
         */
        public final long epoch;

        /**
         * Creates an eviction item capturing the chunk's current epoch.
         *
         * @param owner the sequence that owns the chunk
         * @param chunk the heap-backed chunk to be evicted
         */
        public Item(final Sequence owner,
                    final Chunk chunk) {
            this.owner = owner;
            heapChunk = chunk;
            epoch = chunk.getChunkEpoch();
        }
    }

    private final ConcurrentLinkedQueue<Item> queue =
            new ConcurrentLinkedQueue<>();

    /**
     * Adds an eviction item to the queue.
     *
     * @param e the eviction item to enqueue
     */
    public void enqueue(final Item e) {
        queue.offer(e);
    }

    /**
     * Retrieves and removes the head of the queue, or returns {@code null}
     * if the queue is empty.
     *
     * @return the next eviction item, or {@code null}
     */
    public Item poll() {
        return queue.poll();
    }
}
