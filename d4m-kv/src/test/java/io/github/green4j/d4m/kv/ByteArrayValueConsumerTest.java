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
 * Tests for {@link ByteArrayValueConsumer}.
 */
class ByteArrayValueConsumerTest {
    private ByteArrayValueConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new ByteArrayValueConsumer();
    }

    @Nested
    class InitialState {
        @Test
        void fieldsAreZeroedBeforeFirstPut() {
            assertEquals(0, consumer.valueSize());
            assertNull(consumer.array());
        }
    }

    @Nested
    class ArrayAllocation {
        @Test
        void allocatesArrayOnFirstPut() {
            final KeyValueConsuming.Value v = consumer.putValue(20);

            assertNotNull(v);
            assertNotNull(consumer.array());
            assertTrue(consumer.array().length >= 20);
        }

        @Test
        void allocatesMinimumCapacityForSmallData() {
            consumer.putValue(0);

            assertNotNull(consumer.array());
            assertEquals(ByteArrayValueConsumer.MINIMUM_ARRAY_CAPACITY, consumer.array().length);
        }

        @Test
        void growsArrayWhenValueExceedsCurrentCapacity() {
            consumer.putValue(5);
            final byte[] initial = consumer.array();

            consumer.putValue(2048);

            assertTrue(consumer.array().length > initial.length);
            assertTrue(consumer.array().length >= 2048);
        }

        @Test
        void reusesArrayWhenValueFitsCurrentCapacity() {
            consumer.putValue(50);
            final byte[] initial = consumer.array();

            consumer.putValue(10);

            assertSame(initial, consumer.array());
        }

        @Test
        void doublesRequiredSizeWhenGrowing() {
            final int valueLen = 600;
            consumer.putValue(valueLen);

            assertTrue(consumer.array().length >= valueLen * 2);
        }
    }

    @Nested
    class MetadataTracking {
        @Test
        void recordsValueSize() {
            consumer.putValue(42);

            assertEquals(42, consumer.valueSize());
        }

        @Test
        void updatesValueSizeOnSubsequentPuts() {
            consumer.putValue(10);
            consumer.putValue(25);

            assertEquals(25, consumer.valueSize());
        }

        @Test
        void zeroSizeValueSetsMetadata() {
            consumer.putValue(0);

            assertEquals(0, consumer.valueSize());
        }
    }

    @Nested
    class ContentAccess {
        @Test
        void valueContentOffsetIsAlwaysZero() {
            final KeyValueConsuming.Value v = consumer.putValue(20);

            final BinaryContent content = v.valueContent();
            assertNotNull(content);
            assertEquals(0, content.offset());
        }

        @Test
        void valueContentBufferMatchesArrayCapacity() {
            final KeyValueConsuming.Value v = consumer.putValue(20);

            assertEquals(consumer.array().length, v.valueContent().buffer().capacity());
        }

        @Test
        void applyIsNoOp() {
            final KeyValueConsuming.Value v = consumer.putValue(20);
            v.apply();
        }

        @Test
        void largeValueAllocatesCorrectly() {
            final KeyValueConsuming.Value v = consumer.putValue(5000);

            assertTrue(consumer.array().length >= 5000);
            assertEquals(consumer.array().length, v.valueContent().buffer().capacity());
            assertEquals(0, v.valueContent().offset());
        }
    }
}
