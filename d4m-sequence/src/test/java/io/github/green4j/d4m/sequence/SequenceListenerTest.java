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
import io.github.green4j.d4m.common.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that {@link Sequence.Listener} callbacks fire for the structural
 * lifecycle events (seal, eviction, swap, COW rebuild, snapshot publish).
 */
class SequenceListenerTest {
    private static final int CHUNK_SIZE = 1024;
    private static final int PAYLOAD_SIZE = 100;

    private static final class RecordingListener implements Sequence.Listener {
        int sealed;
        int evicted;
        int swapped;
        int cow;
        int snapshots;
        long lastSnapshotVersion;
        Sequence lastEvictionOwner;

        @Override
        public void onChunkSealedForEviction(final Sequence notifier,
                                             final long chunkEpoch) {
            sealed++;
        }

        @Override
        public void onChunkEvictedToMmap(final Sequence evictor,
                                         final Sequence owner,
                                         final long heapEpoch,
                                         final long mmapEpoch) {
            evicted++;
            lastEvictionOwner = owner;
        }

        @Override
        public void onHeapChunkSwapped(final Sequence notifier,
                                       final int chunkIndex,
                                       final long mmapEpoch) {
            swapped++;
        }

        @Override
        public void onCowRebuild(final Sequence notifier,
                                 final int oldChunkIndex,
                                 final int newChunkCount) {
            cow++;
        }

        @Override
        public void onSnapshotPublished(final Sequence notifier,
                                        final long version,
                                        final int chunkCount) {
            snapshots++;
            lastSnapshotVersion = version;
        }
    }

    private static AtomicBuffer payload() {
        return new UnsafeBuffer(new byte[PAYLOAD_SIZE]);
    }

    private Sequence newSequence(final RecordingListener listener, final File mmapDir) {
        final AtomicLong epoch = new AtomicLong();
        // Only 2 heap chunks -> the writer is forced to evict to mmap quickly.
        final HeapChunkAllocator heap =
                new HeapChunkAllocator(CHUNK_SIZE, 2L * CHUNK_SIZE, CHUNK_SIZE, epoch);
        final MmapChunkAllocator mmap =
                new MmapChunkAllocator(CHUNK_SIZE, mmapDir, false, epoch);
        final EvictionQueue evictQ = new EvictionQueue();
        return new Sequence("seq", CHUNK_SIZE, heap, mmap, evictQ, listener);
    }

    @Test
    void evictionLifecycleCallbacksFire(@TempDir final File mmapDir) {
        final RecordingListener listener = new RecordingListener();
        final Sequence sequence = newSequence(listener, mmapDir);
        final AtomicBuffer payload = payload();

        for (int i = 0; i < 2000; i++) {
            assertTrue(sequence.append(i * 10L, payload, 0, PAYLOAD_SIZE));
        }

        assertTrue(listener.snapshots > 0, "snapshot published");
        assertTrue(listener.sealed > 0, "chunk sealed for eviction");
        assertTrue(listener.evicted > 0, "chunk evicted to mmap");
        assertTrue(listener.swapped > 0, "heap chunk swapped");
        assertSame(sequence, listener.lastEvictionOwner);
        assertEquals(sequence.snapshot().version(), listener.lastSnapshotVersion);
    }

    @Test
    void cowRebuildCallbackFires(@TempDir final File mmapDir) {
        final RecordingListener listener = new RecordingListener();
        final Sequence sequence = newSequence(listener, mmapDir);
        final AtomicBuffer payload = payload();

        for (int i = 0; i < 200; i++) {
            assertTrue(sequence.append(i * 10L, payload, 0, PAYLOAD_SIZE));
        }
        final int cowBefore = listener.cow;

        // Insert an order into the middle -> forces a copy-on-write rebuild.
        sequence.insertOrUpdateUnique(155L, payload, 0, PAYLOAD_SIZE);

        assertTrue(listener.cow > cowBefore, "COW rebuild fired");
    }
}
