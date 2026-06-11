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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests for {@link EvictionQueue}.
 */
class EvictionQueueTest {
    @Nested
    class EnqueueDequeue {
        @Test
        void pollReturnsNullWhenEmpty() {
            final EvictionQueue queue = new EvictionQueue();
            assertNull(queue.poll());
        }

        @Test
        void fifoOrdering() {
            final EvictionQueue queue = new EvictionQueue();
            final TestHarness harness = new TestHarness(2048);
            final Sequence sequence = harness.createSequence("test");
            final Chunk chunk1 = harness.allocHeapChunk();
            final Chunk chunk2 = harness.allocHeapChunk();

            final EvictionQueue.Item e1 = new EvictionQueue.Item(sequence, chunk1);
            final EvictionQueue.Item e2 = new EvictionQueue.Item(sequence, chunk2);

            queue.enqueue(e1);
            queue.enqueue(e2);

            assertSame(e1, queue.poll());
            assertSame(e2, queue.poll());
            assertNull(queue.poll());
        }

        @Test
        void entryCapturesEpochAtCreation() {
            final TestHarness harness = new TestHarness(2048);
            final Sequence sequence = harness.createSequence("test");
            final Chunk chunk = harness.allocHeapChunk();
            final long epochAtCreation = chunk.getChunkEpoch();

            final EvictionQueue.Item entry = new EvictionQueue.Item(sequence, chunk);

            assertEquals(epochAtCreation, entry.epoch);
            assertSame(chunk, entry.heapChunk);
            assertSame(sequence, entry.owner);
        }
    }
}