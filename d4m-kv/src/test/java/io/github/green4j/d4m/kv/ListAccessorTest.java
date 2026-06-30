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
import io.github.green4j.d4m.common.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ListAccessor}.
 */
class ListAccessorTest {
    private static final int KV_BUFFER_SIZE = 4096;
    private static final int TIER_INITIAL_CAPACITY = 64;

    private AtomicBuffer keyBuffer;
    private AtomicBuffer valueBuffer;
    private KeyListStorage storage;
    private KeyListsWriter writer;

    @BeforeEach
    void setUp() {
        keyBuffer = new UnsafeBuffer(new byte[128]);
        valueBuffer = new UnsafeBuffer(new byte[128]);
        final KeyValueRing ring = new KeyValueRing(4, segmentFactory());
        storage = new KeyListStorage(ring);
        writer = storage.newWriter();
    }

    private static SegmentFactory segmentFactory() {
        return index -> new KeyValueSegment(
                1,
                (currentTiers, currentSize, evictionListener) ->
                        new Tier(
                                TIER_INITIAL_CAPACITY,
                                new UnsafeBuffer(new byte[KV_BUFFER_SIZE]),
                                evictionListener
                        ),
                null
        );
    }

    private void append(final String key, final String value) {
        keyBuffer.putBytes(0, key.getBytes());
        valueBuffer.putBytes(0, value.getBytes());
        writer.append(
                keyBuffer, 0, key.length(),
                valueBuffer, 0, value.length()
        );
    }

    private boolean load(final ListAccessor accessor, final String key) {
        keyBuffer.putBytes(0, key.getBytes());
        return storage.list(accessor, keyBuffer, 0, key.length());
    }

    private static String stringAt(final ByteArrayValueConsumer consumer) {
        return new String(Arrays.copyOf(consumer.array(), consumer.valueSize()));
    }

    @Test
    void getBeforeLoadThrows() {
        final ListAccessor acc = new ListAccessor();
        final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
        assertThrows(IllegalStateException.class, () -> acc.get(0, consumer));
    }

    @Test
    void forEachBeforeLoadThrows() {
        final ListAccessor acc = new ListAccessor();
        final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
        assertThrows(IllegalStateException.class, () -> acc.forEach(consumer));
    }

    @Test
    void getNegativeIndexThrows() {
        append("k", "v");
        final ListAccessor acc = new ListAccessor();
        assertTrue(load(acc, "k"));
        final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
        assertThrows(IndexOutOfBoundsException.class, () -> acc.get(-1, consumer));
    }

    @Test
    void getIndexAtSizeThrows() {
        append("k", "v");
        final ListAccessor acc = new ListAccessor();
        assertTrue(load(acc, "k"));
        final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
        assertThrows(IndexOutOfBoundsException.class, () -> acc.get(1, consumer));
    }

    @Test
    void loadOnMissingKeyClearsState() {
        // First load against an existing list - accessor is populated.
        append("present", "x");
        final ListAccessor acc = new ListAccessor();
        assertTrue(load(acc, "present"));
        assertEquals(1, acc.size());

        // Now reload against a missing key - state must be reset.
        assertFalse(load(acc, "absent"));
        assertEquals(0, acc.size());
        assertFalse(acc.exists());
    }

    @Test
    void accessorReuseAcrossKeys() {
        for (int i = 0; i < 3; i++) {
            append("first", "f" + i);
        }
        for (int i = 0; i < 5; i++) {
            append("second", "s" + i);
        }

        final ListAccessor acc = new ListAccessor();
        final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();

        assertTrue(load(acc, "first"));
        assertEquals(3, acc.size());
        assertTrue(acc.get(2, consumer));
        assertEquals("f2", stringAt(consumer));

        assertTrue(load(acc, "second"));
        assertEquals(5, acc.size());
        assertTrue(acc.get(4, consumer));
        assertEquals("s4", stringAt(consumer));
    }

    @Test
    void copyToProducesIndependentReader() {
        for (int i = 0; i < 4; i++) {
            append("k", "v" + i);
        }

        final ListAccessor src = new ListAccessor();
        assertTrue(load(src, "k"));

        final ListAccessor dst = new ListAccessor();
        src.copyTo(dst);

        assertEquals(src.size(), dst.size());
        assertEquals(4, dst.size());

        // Re-bind src to something else; dst must be readable.
        assertFalse(load(src, "missing"));
        assertEquals(0, src.size());

        final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
        for (int i = 0; i < 4; i++) {
            assertTrue(dst.get(i, consumer));
            assertEquals("v" + i, stringAt(consumer));
        }
    }

    @Test
    void forEachStopsAtChosenIndex() {
        for (int i = 0; i < 5; i++) {
            append("k", "v" + i);
        }
        final ListAccessor acc = new ListAccessor();
        assertTrue(load(acc, "k"));

        final CollectingStoppableConsumer consumer = new CollectingStoppableConsumer();
        consumer.stopAfter = 3; // deliver v0, v1, v2, then stop

        final int delivered = acc.forEach(consumer);

        assertEquals(3, delivered);
        assertEquals(List.of("v0", "v1", "v2"), consumer.visited);
    }

    @Test
    void forEachStopFalseDeliversAll() {
        for (int i = 0; i < 5; i++) {
            append("k", "v" + i);
        }
        final ListAccessor acc = new ListAccessor();
        assertTrue(load(acc, "k"));

        final CollectingStoppableConsumer consumer = new CollectingStoppableConsumer();
        // stopAfter left at its default (never stops)

        final int delivered = acc.forEach(consumer);

        assertEquals(acc.size(), delivered);
        assertEquals(5, delivered);
        assertEquals(List.of("v0", "v1", "v2", "v3", "v4"), consumer.visited);
    }

    @Test
    void forEachStoppableBeforeLoadThrows() {
        final ListAccessor acc = new ListAccessor();
        final CollectingStoppableConsumer consumer = new CollectingStoppableConsumer();
        assertThrows(IllegalStateException.class, () -> acc.forEach(consumer));
    }

    /**
     * Test-only {@link KeyValueConsuming.StoppableValueConsumer} that captures
     * each delivered value (in insertion order) and stops once
     * {@link #stopAfter} entries have been delivered.
     */
    private static final class CollectingStoppableConsumer
            implements KeyValueConsuming.StoppableValueConsumer<KeyValueConsuming.Value> {

        private final ByteArrayValueConsumer delegate = new ByteArrayValueConsumer();
        private final List<String> visited = new ArrayList<>();
        private int stopAfter = Integer.MAX_VALUE;

        @Override
        public KeyValueConsuming.Value putValue(final int valueSize) {
            return delegate.putValue(valueSize);
        }

        @Override
        public boolean stopped() {
            // Polled after the entry has been delivered: the delegate now
            // holds the just-received value.
            visited.add(new String(Arrays.copyOf(delegate.array(), delegate.valueSize())));
            return visited.size() >= stopAfter;
        }
    }
}
