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

import com.sun.management.ThreadMXBean;
import io.github.green4j.d4m.common.AtomicBuffer;
import io.github.green4j.d4m.common.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Asserts that the steady-state hot loops of {@link KeyListsWriter#append}
 * and {@link ListAccessor#forEach} allocate nothing.
 *
 * <p>Uses {@link ThreadMXBean#getThreadAllocatedBytes(long)} to measure
 * thread-local allocation. JIT compilation, class initialization and one-off
 * lazy work can produce a small amount of allocation, so the assertion uses
 * a generous threshold per hot-loop call (a few bytes on average across the
 * loop). Anything beyond that signals the introduction of a real per-call
 * allocation (e.g. a lambda or autoboxing).
 */
class KeyListsAllocationTest {

    @Test
    void appendIsAllocationFree() {
        final ThreadMXBean tmx = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!tmx.isThreadAllocatedMemorySupported()) {
            // Skip on JVMs that don't expose the counter.
            return;
        }
        if (!tmx.isThreadAllocatedMemoryEnabled()) {
            tmx.setThreadAllocatedMemoryEnabled(true);
        }

        // Pre-size the tier large enough that the hot loop never triggers
        // expand() - expansion allocates a new metadata array and would skew
        // the measurement. 262144 slots is comfortably above the warmup +
        // hot-loop entry count.
        final KeyValueRing ring = new KeyValueRing(4, index -> new KeyValueSegment(
                1,
                (currentTiers, currentSize, evictionListener) -> new Tier(
                        262_144,
                        new UnsafeBuffer(new byte[8 * 1024 * 1024]),
                        evictionListener),
                null
        ));
        final KeyListStorage lists = new KeyListStorage(ring);
        final KeyListsWriter writer = lists.newWriter();

        final AtomicBuffer keyBuffer = new UnsafeBuffer(new byte[16]);
        final AtomicBuffer valueBuffer = new UnsafeBuffer(new byte[16]);
        final byte[] kb = "k".getBytes();
        keyBuffer.putBytes(0, kb);
        for (int i = 0; i < 8; i++) {
            valueBuffer.putByte(i, (byte) i);
        }

        // Warm-up: trigger JIT, lazy classes, the first metadata insert, the
        // initial growth of any reusable buffers, etc.
        for (int i = 0; i < 20_000; i++) {
            writer.append(keyBuffer, 0, kb.length, valueBuffer, 0, 8);
        }

        final long tid = Thread.currentThread().getId();
        final long before = tmx.getThreadAllocatedBytes(tid);
        final int hotIterations = 100_000;
        for (int i = 0; i < hotIterations; i++) {
            writer.append(keyBuffer, 0, kb.length, valueBuffer, 0, 8);
        }
        final long after = tmx.getThreadAllocatedBytes(tid);
        final long delta = after - before;

        // A clean run on Hotspot is typically 0 - but JIT recompilation, GC
        // safepoints and tracking instrumentation can introduce small noise.
        // Anything beyond ~1 byte/iter on average means we leaked a per-call
        // allocation somewhere.
        assertTrue(delta < hotIterations,
                "append leaked " + delta + " bytes across " + hotIterations + " calls");
    }

    @Test
    void loadAndForEachIsAllocationFree() {
        final ThreadMXBean tmx = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        if (!tmx.isThreadAllocatedMemorySupported()) {
            return;
        }
        if (!tmx.isThreadAllocatedMemoryEnabled()) {
            tmx.setThreadAllocatedMemoryEnabled(true);
        }

        // Pre-size the tier large enough that the hot loop never triggers
        // expand() - expansion allocates a new metadata array and would skew
        // the measurement. 262144 slots is comfortably above the warmup +
        // hot-loop entry count.
        final KeyValueRing ring = new KeyValueRing(4, index -> new KeyValueSegment(
                1,
                (currentTiers, currentSize, evictionListener) -> new Tier(
                        262_144,
                        new UnsafeBuffer(new byte[8 * 1024 * 1024]),
                        evictionListener),
                null
        ));
        final KeyListStorage lists = new KeyListStorage(ring);
        final KeyListsWriter writer = lists.newWriter();

        final AtomicBuffer keyBuffer = new UnsafeBuffer(new byte[16]);
        final AtomicBuffer valueBuffer = new UnsafeBuffer(new byte[16]);
        final byte[] kb = "load-k".getBytes();
        keyBuffer.putBytes(0, kb);
        for (int i = 0; i < 8; i++) {
            valueBuffer.putByte(i, (byte) i);
        }
        // Populate.
        for (int i = 0; i < 1_000; i++) {
            writer.append(keyBuffer, 0, kb.length, valueBuffer, 0, 8);
        }

        final ListAccessor accessor = new ListAccessor();
        final CountingValueConsumer consumer = new CountingValueConsumer();

        // Warm-up.
        for (int i = 0; i < 10_000; i++) {
            lists.list(accessor, keyBuffer, 0, kb.length);
            accessor.forEach(consumer);
        }

        final long tid = Thread.currentThread().getId();
        final long before = tmx.getThreadAllocatedBytes(tid);
        final int hotIterations = 5_000;
        for (int i = 0; i < hotIterations; i++) {
            lists.list(accessor, keyBuffer, 0, kb.length);
            accessor.forEach(consumer);
        }
        final long after = tmx.getThreadAllocatedBytes(tid);
        final long delta = after - before;

        assertTrue(delta < hotIterations,
                "load+forEach leaked " + delta + " bytes across " + hotIterations + " calls");
    }

    private static final class CountingValueConsumer
            implements KeyValueConsuming.ValueConsumer<KeyValueConsuming.Value>,
                       KeyValueConsuming.Value, BinaryContent {

        private final UnsafeBuffer buf = new UnsafeBuffer(new byte[8]);
        int applied;

        @Override
        public KeyValueConsuming.Value putValue(final int valueSize) {
            return valueSize == 8 ? this : null;
        }

        @Override
        public BinaryContent valueContent() {
            return this;
        }

        @Override
        public AtomicBuffer buffer() {
            return buf;
        }

        @Override
        public int offset() {
            return 0;
        }

        @Override
        public void apply() {
            applied++;
        }
    }
}
