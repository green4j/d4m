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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ByteArrayKeyValueConsumer}.
 */
class ByteArrayKeyValueConsumerTest {
    private ByteArrayKeyValueConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new ByteArrayKeyValueConsumer();
    }

    @Nested
    class InitialState {
        @Test
        void fieldsAreZeroedBeforeFirstPut() {
            assertEquals(0, consumer.slotIndex());
            assertEquals(0, consumer.keySize());
            assertEquals(0, consumer.valueSize());
            assertNull(consumer.array());
        }
    }

    @Nested
    class ArrayAllocation {
        @Test
        void allocatesArrayOnFirstPut() {
            final KeyValueConsuming.KeyValue kv = consumer.putKeyValue(5, 10, 20);

            assertNotNull(kv);
            assertNotNull(consumer.array());
            assertTrue(consumer.array().length >= 30);
        }

        @Test
        void allocatesMinimumCapacityForSmallData() {
            consumer.putKeyValue(1, 0, 0);

            assertNotNull(consumer.array());
            assertEquals(ByteArrayKeyValueConsumer.MINIMUM_ARRAY_CAPACITY, consumer.array().length);
        }

        @Test
        void growsArrayWhenDataExceedsCurrent() {
            consumer.putKeyValue(1, 5, 5);
            final byte[] initial = consumer.array();

            consumer.putKeyValue(2, 45, 1024);

            assertTrue(consumer.array().length > initial.length);
            assertTrue(consumer.array().length >= 45 + 1024);
        }

        @Test
        void reusesArrayWhenDataFitsCurrent() {
            consumer.putKeyValue(1, 20, 30);
            final byte[] initial = consumer.array();

            consumer.putKeyValue(2, 10, 15);

            assertSame(initial, consumer.array());
        }

        @Test
        void doublesRequiredSizeWhenGrowing() {
            final int keyLen = 300;
            final int valLen = 300;
            consumer.putKeyValue(0, keyLen, valLen);

            assertTrue(consumer.array().length >= (keyLen + valLen) * 2);
        }

        @Test
        void bufferCapacityMatchesArrayLength() {
            final KeyValueConsuming.KeyValue kv = consumer.putKeyValue(0, 10, 20);

            assertEquals(consumer.array().length, kv.content().buffer().capacity());
        }
    }

    @Nested
    class MetadataTracking {
        @Test
        void recordsSlotIndexKeySizeValueSize() {
            consumer.putKeyValue(5, 10, 20);

            assertEquals(5, consumer.slotIndex());
            assertEquals(10, consumer.keySize());
            assertEquals(20, consumer.valueSize());
        }

        @Test
        void updatesMetadataOnSubsequentPuts() {
            consumer.putKeyValue(1, 10, 20);
            consumer.putKeyValue(7, 3, 9);

            assertEquals(7, consumer.slotIndex());
            assertEquals(3, consumer.keySize());
            assertEquals(9, consumer.valueSize());
        }

        @Test
        void zeroSizeKeyAndValueSetsMetadata() {
            consumer.putKeyValue(1, 0, 0);

            assertEquals(1, consumer.slotIndex());
            assertEquals(0, consumer.keySize());
            assertEquals(0, consumer.valueSize());
        }

        @Test
        void largeSlotIndexPreserved() {
            consumer.putKeyValue(Integer.MAX_VALUE, 1, 1);

            assertEquals(Integer.MAX_VALUE, consumer.slotIndex());
        }

        @Test
        void largeKeyAndValueSizesPreserved() {
            final int largeKeyLen = 1000;
            final int largeValLen = 2000;
            consumer.putKeyValue(1, largeKeyLen, largeValLen);

            assertEquals(largeKeyLen, consumer.keySize());
            assertEquals(largeValLen, consumer.valueSize());
            assertTrue(consumer.array().length >= largeKeyLen + largeValLen);
        }
    }

    @Nested
    class ContentAccess {
        @Test
        void contentOffsetIsAlwaysZero() {
            final KeyValueConsuming.KeyValue kv = consumer.putKeyValue(1, 10, 20);

            final BinaryContent content = kv.content();
            assertNotNull(content);
            assertEquals(0, content.offset());
        }

        @Test
        void contentBufferMatchesArrayCapacity() {
            final KeyValueConsuming.KeyValue kv = consumer.putKeyValue(1, 10, 20);

            assertEquals(consumer.array().length, kv.content().buffer().capacity());
        }

        @Test
        void applyIsIdempotent() {
            final KeyValueConsuming.KeyValue kv = consumer.putKeyValue(1, 10, 20);
            kv.apply();
            kv.apply();
        }

        @Test
        void contentReturnedOnZeroSizeData() {
            final KeyValueConsuming.KeyValue kv = consumer.putKeyValue(1, 0, 0);

            assertNotNull(kv.content());
            assertNotNull(kv.content().buffer());
        }
    }

    @Nested
    class SequentialPuts {
        @Test
        void firstPutThenApplyThenSecondPut() {
            final KeyValueConsuming.KeyValue kv1 = consumer.putKeyValue(1, 10, 20);
            kv1.apply();

            final KeyValueConsuming.KeyValue kv2 = consumer.putKeyValue(2, 5, 10);

            assertEquals(2, consumer.slotIndex());
            assertEquals(5, consumer.keySize());
            assertEquals(10, consumer.valueSize());
            assertEquals(consumer.array().length, kv2.content().buffer().capacity());
            assertEquals(0, kv2.content().offset());
        }

        @Test
        void multiplePutsWithoutApplyOverwriteMetadata() {
            consumer.putKeyValue(1, 10, 20);
            consumer.putKeyValue(2, 5, 10);
            consumer.putKeyValue(3, 8, 16);

            assertEquals(3, consumer.slotIndex());
            assertEquals(8, consumer.keySize());
            assertEquals(16, consumer.valueSize());
        }
    }
}
