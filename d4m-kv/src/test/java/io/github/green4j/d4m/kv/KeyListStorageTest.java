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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link KeyListStorage}.
 */
class KeyListStorageTest {
    private static final int KV_BUFFER_SIZE = 4096;
    private static final int TIER_INITIAL_CAPACITY = 64;
    private static final int RING_SIZE = 4;

    private AtomicBuffer keyBuffer;
    private AtomicBuffer valueBuffer;
    private KeyListStorage storage;
    private KeyListsWriter writer;

    @BeforeEach
    void setUp() {
        keyBuffer = new UnsafeBuffer(new byte[256]);
        valueBuffer = new UnsafeBuffer(new byte[256]);
        final KeyValueRing ring = new KeyValueRing(RING_SIZE, segmentFactory());
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
        return new String(
                Arrays.copyOf(consumer.array(), consumer.valueSize())
        );
    }

    @Nested
    class AppendAndLoad {
        @Test
        void appendAndLoadSingleEntry() {
            append("k1", "v1");

            final ListAccessor acc = new ListAccessor();
            assertTrue(load(acc, "k1"));
            assertEquals(1, acc.size());
            assertTrue(acc.exists());

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            assertTrue(acc.get(0, consumer));
            assertEquals("v1", stringAt(consumer));
        }

        @Test
        void appendMultipleEntriesSameKey() {
            for (int i = 0; i < 50; i++) {
                append("k", "value-" + i);
            }

            final ListAccessor acc = new ListAccessor();
            assertTrue(load(acc, "k"));
            assertEquals(50, acc.size());

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            for (int i = 0; i < 50; i++) {
                assertTrue(acc.get(i, consumer));
                assertEquals("value-" + i, stringAt(consumer));
            }
        }

        @Test
        void distinctKeysIsolated() {
            for (int i = 0; i < 10; i++) {
                append("alpha", "a" + i);
            }
            for (int i = 0; i < 5; i++) {
                append("beta", "b" + i);
            }

            final ListAccessor accA = new ListAccessor();
            final ListAccessor accB = new ListAccessor();
            assertTrue(load(accA, "alpha"));
            assertTrue(load(accB, "beta"));
            assertEquals(10, accA.size());
            assertEquals(5, accB.size());

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            for (int i = 0; i < 10; i++) {
                assertTrue(accA.get(i, consumer));
                assertEquals("a" + i, stringAt(consumer));
            }
            for (int i = 0; i < 5; i++) {
                assertTrue(accB.get(i, consumer));
                assertEquals("b" + i, stringAt(consumer));
            }
        }

        @Test
        void loadMissingKeyReturnsFalse() {
            final ListAccessor acc = new ListAccessor();
            assertFalse(load(acc, "absent"));
            assertEquals(0, acc.size());
            assertFalse(acc.exists());
        }

        @Test
        void updatePreservesMetadataInPlace() {
            // 200 appends to the same key - each call updates the 8-byte metadata.
            // If metadata weren't fixed-size, Tier.updateValue would throw
            // IllegalArgumentException ("Update value must be same size as existing value").
            for (int i = 0; i < 200; i++) {
                append("hot", "v");
            }
            final ListAccessor acc = new ListAccessor();
            assertTrue(load(acc, "hot"));
            assertEquals(200, acc.size());
        }
    }

    @Nested
    class EdgeCases {
        @Test
        void emptyKeyAndEmptyValueRoundTrip() {
            // empty key
            writer.append(keyBuffer, 0, 0, valueBuffer, 0, 0);

            final ListAccessor acc = new ListAccessor();
            assertTrue(storage.list(acc, keyBuffer, 0, 0));
            assertEquals(1, acc.size());

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            assertTrue(acc.get(0, consumer));
            assertEquals(0, consumer.valueSize());
        }

        /**
         * Under the original {@code putLong}-based design a 7-byte user key
         * (yielding an 8-byte prefixed metadata key starting with {@code 0x01})
         * could byte-collide with synthetic entry keys on little-endian
         * platforms. With byte-by-byte writes and the bit-7 marker on byte&nbsp;0
         * of every synthetic key, this is now impossible regardless of content.
         */
        @Test
        void sevenByteKeyDoesNotCollideWithSyntheticKeys() {
            for (int i = 0; i < 64; i++) {
                append("seven-K", "entry-" + i); // 7 bytes
            }

            final ListAccessor acc = new ListAccessor();
            assertTrue(load(acc, "seven-K"));
            assertEquals(64, acc.size());

            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            for (int i = 0; i < 64; i++) {
                assertTrue(acc.get(i, consumer));
                assertEquals("entry-" + i, stringAt(consumer));
            }
        }

        @Test
        void variableLengthUserKeysAreIndependent() {
            // Lengths chosen so prefixed-key length straddles the 8-byte synthetic-key length.
            final String[] keys = {"a", "ab", "abcdefg", "abcdefgh", "abcdefghi"};
            for (final String k : keys) {
                for (int i = 0; i < 8; i++) {
                    append(k, k + "-" + i);
                }
            }

            final ListAccessor acc = new ListAccessor();
            final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
            for (final String k : keys) {
                assertTrue(load(acc, k));
                assertEquals(8, acc.size());
                for (int i = 0; i < 8; i++) {
                    assertTrue(acc.get(i, consumer));
                    assertEquals(k + "-" + i, stringAt(consumer));
                }
            }
        }
    }

    @Nested
    class BoundsAndOverflow {
        @Test
        void entryCountOverflowThrows() {
            // Use a stub KeyValues that returns pre-cooked metadata with count = MAX,
            // so a single append trips the bound check without doing millions of writes.
            final KeyValues nearMaxStore = new KeyValues() {
                @Override
                public void put(final AtomicBuffer k, final int ko, final int ks,
                                final AtomicBuffer v, final int vo, final int vs) {
                }

                @Override
                public boolean get(final AtomicBuffer k, final int ko, final int ks,
                                   final KeyValueConsuming.ValueConsumer<KeyValueConsuming.Value> c) {
                    final KeyValueConsuming.Value v = c.putValue(KeyListStorage.METADATA_VALUE_SIZE);
                    if (v == null) {
                        return false;
                    }
                    final BinaryContent content = v.valueContent();
                    KeyListStorage.writeMetadataValue(
                            content.buffer(), content.offset(),
                            42L, KeyListStorage.MAX_ENTRY_COUNT
                    );
                    v.apply();
                    return true;
                }

                @Override
                public void compute(final AtomicBuffer k, final int ko, final int ks,
                                    final ComputeAction action) {
                    throw new AssertionError("bounds check must throw before compute");
                }

                @Override
                public void compute(final AtomicBuffer k1, final int k1o, final int k1s,
                                    final AtomicBuffer k2, final int k2o, final int k2s,
                                    final TwoKeyComputeAction action) {
                    throw new AssertionError("bounds check must throw before compute");
                }
            };

            final KeyListStorage atCap = new KeyListStorage(nearMaxStore);
            final KeyListsWriter atCapWriter = atCap.newWriter();
            keyBuffer.putBytes(0, "any".getBytes());
            valueBuffer.putBytes(0, "x".getBytes());
            final IllegalStateException ex = assertThrows(
                    IllegalStateException.class,
                    () -> atCapWriter.append(keyBuffer, 0, 3, valueBuffer, 0, 1)
            );
            assertTrue(ex.getMessage().contains("maximum capacity"));
        }
    }

    @Nested
    class SyntheticKeyEncoding {
        @Test
        void writeAndRoundTripRespectsBitBudget() {
            final AtomicBuffer buf = new UnsafeBuffer(new byte[KeyListStorage.SYNTHETIC_KEY_SIZE]);

            final long uki = KeyListStorage.MAX_USER_KEY_INDEX;
            final int ei = KeyListStorage.MAX_ENTRY_COUNT;
            KeyListStorage.writeSyntheticKey(buf, 0, uki, ei);

            // byte 0 marker bit must be set
            assertEquals(0x80, buf.getByte(0) & 0x80);

            // Reconstruct the values from the 8 bytes:
            final long roundUki = (((long) (buf.getByte(0) & 0x7F)) << 32)
                    | (((long) (buf.getByte(1) & 0xFF)) << 24)
                    | (((long) (buf.getByte(2) & 0xFF)) << 16)
                    | (((long) (buf.getByte(3) & 0xFF)) << 8)
                    | ((long) (buf.getByte(4) & 0xFF));
            final int roundEi = ((buf.getByte(5) & 0xFF) << 16)
                    | ((buf.getByte(6) & 0xFF) << 8)
                    | (buf.getByte(7) & 0xFF);

            assertEquals(uki, roundUki);
            assertEquals(ei, roundEi);
        }

        @Test
        void byteZeroAlwaysHasMarkerBitSet() {
            final AtomicBuffer buf = new UnsafeBuffer(new byte[KeyListStorage.SYNTHETIC_KEY_SIZE]);

            // Try values that span byte 0's content bits.
            final long[] indices = {0L, 1L, 0x7FL, 0xFFL, 0x100000000L, KeyListStorage.MAX_USER_KEY_INDEX};
            for (final long uki : indices) {
                KeyListStorage.writeSyntheticKey(buf, 0, uki, 0);
                assertTrue((buf.getByte(0) & 0x80) != 0,
                        "byte 0 must have bit 7 set for uki=" + uki);
            }
        }

        @Test
        void metadataRoundTripsAllBits() {
            final AtomicBuffer buf = new UnsafeBuffer(new byte[KeyListStorage.METADATA_VALUE_SIZE]);
            KeyListStorage.writeMetadataValue(buf, 0,
                    KeyListStorage.MAX_USER_KEY_INDEX, KeyListStorage.MAX_ENTRY_COUNT);

            assertEquals(KeyListStorage.MAX_USER_KEY_INDEX,
                    KeyListStorage.readMetadataUserKeyIndex(buf, 0));
            assertEquals(KeyListStorage.MAX_ENTRY_COUNT,
                    KeyListStorage.readMetadataEntryCount(buf, 0));
        }
    }

    @Nested
    class ForEach {
        @Test
        void forEachDeliversAllInOrder() {
            for (int i = 0; i < 16; i++) {
                append("fe", "x" + i);
            }
            final ListAccessor acc = new ListAccessor();
            assertTrue(load(acc, "fe"));

            final CollectingConsumer consumer = new CollectingConsumer();
            final int delivered = acc.forEach(consumer);
            assertEquals(16, delivered);
            final String expected = "x0,x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,";
            assertEquals(expected, consumer.collected.toString());
        }
    }

    /**
     * A value consumer that records each delivered value as a UTF-8 string,
     * appending into an internal {@link StringBuilder}. Used by the forEach test.
     */
    private static final class CollectingConsumer
            implements KeyValueConsuming.ValueConsumer<KeyValueConsuming.Value>,
                       KeyValueConsuming.Value,
                       BinaryContent {

        private final byte[] array = new byte[256];
        private final AtomicBuffer buffer = new UnsafeBuffer(array);
        private int valueSize;
        final StringBuilder collected = new StringBuilder();

        @Override
        public KeyValueConsuming.Value putValue(final int size) {
            this.valueSize = size;
            return this;
        }

        @Override
        public BinaryContent valueContent() {
            return this;
        }

        @Override
        public AtomicBuffer buffer() {
            return buffer;
        }

        @Override
        public int offset() {
            return 0;
        }

        @Override
        public void apply() {
            collected.append(new String(array, 0, valueSize)).append(',');
        }
    }

    @Test
    void verifyArrayValueRoundTripUtility() {
        // Sanity: the helper Arrays.copyOf works as expected.
        keyBuffer.putBytes(0, "x".getBytes());
        valueBuffer.putBytes(0, "hello".getBytes());
        writer.append(keyBuffer, 0, 1, valueBuffer, 0, 5);
        final ListAccessor acc = new ListAccessor();
        assertTrue(storage.list(acc, keyBuffer, 0, 1));
        final ByteArrayValueConsumer consumer = new ByteArrayValueConsumer();
        assertTrue(acc.get(0, consumer));
        assertArrayEquals("hello".getBytes(), Arrays.copyOf(consumer.array(), consumer.valueSize()));
    }
}
