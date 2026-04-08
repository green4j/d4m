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
package io.github.green4j.d4m.common;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link UnsafeBuffer} covering construction, rewrapping,
 * plain/volatile/ordered access, CAS operations, and bulk byte copies.
 */
class UnsafeBufferTest {
    /**
     * Verifies buffer construction from byte arrays and ByteBuffers
     * (heap, direct, sliced, and region-based).
     */
    @Nested
    class Construction {
        @Test
        void fromByteArray() {
            final byte[] data = new byte[64];
            final UnsafeBuffer buffer = new UnsafeBuffer(data);

            assertEquals(64, buffer.capacity());
            assertFalse(buffer.isDirect());
            assertNull(buffer.byteBuffer());
        }

        @Test
        void fromByteArrayRegion() {
            final byte[] data = new byte[128];
            data[32] = 0x7F;
            final UnsafeBuffer buffer = new UnsafeBuffer(data, 32, 64);

            assertEquals(64, buffer.capacity());
            assertEquals(0x7F, buffer.getByte(0));
        }

        @Test
        void fromHeapByteBuffer() {
            final ByteBuffer byteBuffer = ByteBuffer.allocate(128);
            final UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);

            assertEquals(128, buffer.capacity());
            assertFalse(buffer.isDirect());
            assertSame(byteBuffer, buffer.byteBuffer());
        }

        @Test
        void fromDirectByteBuffer() {
            final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(128);
            final UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);

            assertEquals(128, buffer.capacity());
            assertTrue(buffer.isDirect());
            assertSame(byteBuffer, buffer.byteBuffer());
        }

        @Test
        void fromHeapByteBufferSlice() {
            final ByteBuffer byteBuffer = ByteBuffer.allocate(256);
            byteBuffer.position(64).limit(192);
            final ByteBuffer slice = byteBuffer.slice();
            final UnsafeBuffer buffer = new UnsafeBuffer(slice);

            assertEquals(128, buffer.capacity());
            assertFalse(buffer.isDirect());
        }

        @Test
        void fromDirectByteBufferRegion() {
            final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(256);
            final UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer, 64, 128);

            assertEquals(128, buffer.capacity());
            assertTrue(buffer.isDirect());
        }
    }

    /**
     * Verifies that {@code wrap()} re-binds the buffer to a different
     * backing store, switching between heap and direct memory.
     */
    @Nested
    class Rewrap {
        @Test
        void rewrapByteArraySwitchesBackingStore() {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[32]);
            buffer.putInt(0, 42);

            final byte[] newData = new byte[64];
            buffer.wrap(newData, 0, 64);

            assertEquals(64, buffer.capacity());
            assertEquals(0, buffer.getInt(0)); // fresh backing
        }

        @Test
        void rewrapByteBufferSwitchesToDirect() {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[32]);
            assertFalse(buffer.isDirect());

            buffer.wrap(ByteBuffer.allocateDirect(64));

            assertTrue(buffer.isDirect());
            assertEquals(64, buffer.capacity());
        }
    }

    /**
     * Round-trip tests for plain (non-volatile) read/write of each
     * primitive type on both heap and direct buffers.
     */
    @Nested
    class PlainAccess {
        @Test
        void byteRoundTrip() {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[16]);
            buffer.putByte(0, (byte) 0xAB);
            buffer.putByte(15, (byte) 0xCD);

            assertEquals((byte) 0xAB, buffer.getByte(0));
            assertEquals((byte) 0xCD, buffer.getByte(15));
        }

        @Test
        void shortRoundTrip() {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[16]);
            buffer.putShort(0, (short) 0x1234);
            buffer.putShort(4, Short.MIN_VALUE);

            assertEquals((short) 0x1234, buffer.getShort(0));
            assertEquals(Short.MIN_VALUE, buffer.getShort(4));
        }

        @Test
        void intRoundTrip() {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[16]);
            buffer.putInt(0, Integer.MAX_VALUE);
            buffer.putInt(8, Integer.MIN_VALUE);

            assertEquals(Integer.MAX_VALUE, buffer.getInt(0));
            assertEquals(Integer.MIN_VALUE, buffer.getInt(8));
        }

        @Test
        void longRoundTrip() {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[32]);
            buffer.putLong(0, Long.MAX_VALUE);
            buffer.putLong(16, 0xDEAD_BEEF_CAFE_BABEL);

            assertEquals(Long.MAX_VALUE, buffer.getLong(0));
            assertEquals(0xDEAD_BEEF_CAFE_BABEL, buffer.getLong(16));
        }

        @Test
        void floatRoundTrip() {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[16]);
            buffer.putFloat(0, 3.14f);
            buffer.putFloat(8, Float.NaN);

            assertEquals(3.14f, buffer.getFloat(0));
            assertTrue(Float.isNaN(buffer.getFloat(8)));
        }

        @Test
        void doubleRoundTrip() {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[32]);
            buffer.putDouble(0, Math.PI);
            buffer.putDouble(16, Double.NEGATIVE_INFINITY);

            assertEquals(Math.PI, buffer.getDouble(0));
            assertEquals(Double.NEGATIVE_INFINITY, buffer.getDouble(16));
        }

        @Test
        void directBufferPlainAccessWorks() {
            final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(64));
            buffer.putLong(0, 123456789L);
            buffer.putInt(8, -1);
            buffer.putByte(12, (byte) 99);

            assertEquals(123456789L, buffer.getLong(0));
            assertEquals(-1, buffer.getInt(8));
            assertEquals((byte) 99, buffer.getByte(12));
        }
    }

    /**
     * Tests volatile-semantics read/write for {@code int} and {@code long}.
     */
    @Nested
    class VolatileAccess {
        @Test
        void intVolatileRoundTrip() {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[16]);
            buffer.putIntVolatile(0, 0x0BADF00D);

            assertEquals(0x0BADF00D, buffer.getIntVolatile(0));
        }

        @Test
        void longVolatileRoundTrip() {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[16]);
            buffer.putLongVolatile(0, 0x0102030405060708L);

            assertEquals(0x0102030405060708L, buffer.getLongVolatile(0));
        }
    }

    /**
     * Tests release-store (ordered) writes, verifying they are visible
     * via subsequent volatile reads.
     */
    @Nested
    class OrderedStore {
        @Test
        void intOrderedIsReadableByVolatileLoad() {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[16]);
            buffer.putIntOrdered(0, 42);

            assertEquals(42, buffer.getIntVolatile(0));
        }

        @Test
        void longOrderedIsReadableByVolatileLoad() {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[16]);
            buffer.putLongOrdered(0, 999_999_999_999L);

            assertEquals(999_999_999_999L, buffer.getLongVolatile(0));
        }
    }

    /**
     * Tests atomic compare-and-set operations for {@code int} and {@code long},
     * covering both successful and failing CAS attempts.
     */
    @Nested
    class CompareAndSet {
        @Test
        void casIntSucceedsOnMatch() {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[16]);
            buffer.putInt(0, 10);

            assertTrue(buffer.compareAndSetInt(0, 10, 20));
            assertEquals(20, buffer.getInt(0));
        }

        @Test
        void casIntFailsOnMismatch() {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[16]);
            buffer.putInt(0, 10);

            assertFalse(buffer.compareAndSetInt(0, 99, 20));
            assertEquals(10, buffer.getInt(0));
        }

        @Test
        void casLongSucceedsOnMatch() {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[16]);
            buffer.putLong(0, 100L);

            assertTrue(buffer.compareAndSetLong(0, 100L, 200L));
            assertEquals(200L, buffer.getLong(0));
        }

        @Test
        void casLongFailsOnMismatch() {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[16]);
            buffer.putLong(0, 100L);

            assertFalse(buffer.compareAndSetLong(0, 999L, 200L));
            assertEquals(100L, buffer.getLong(0));
        }
    }

    /**
     * Tests bulk byte copy operations between buffers and byte arrays,
     * including cross-heap/direct transfers and zero-length edge cases.
     */
    @Nested
    class BulkOperations {
        @Test
        void getBytesToArray() {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[]{1, 2, 3, 4, 5});
            final byte[] dst = new byte[3];
            buffer.getBytes(1, dst, 0, 3);

            assertArrayEquals(new byte[]{2, 3, 4}, dst);
        }

        @Test
        void putBytesFromArray() {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[8]);
            buffer.putBytes(2, new byte[]{10, 20, 30}, 0, 3);

            assertEquals(10, buffer.getByte(2));
            assertEquals(20, buffer.getByte(3));
            assertEquals(30, buffer.getByte(4));
        }

        @Test
        void getBytesToArrayWithOffset() {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[]{0, 0, 0xA, 0xB, 0});
            final byte[] dst = new byte[8];
            buffer.getBytes(2, dst, 4, 2);

            assertEquals(0xA, dst[4]);
            assertEquals(0xB, dst[5]);
        }

        @Test
        void putBytesFromAtomicBuffer() {
            final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[]{10, 20, 30, 40, 50});
            final UnsafeBuffer dstBuffer = new UnsafeBuffer(new byte[8]);

            dstBuffer.putBytes(1, srcBuffer, 2, 3);

            assertEquals(30, dstBuffer.getByte(1));
            assertEquals(40, dstBuffer.getByte(2));
            assertEquals(50, dstBuffer.getByte(3));
        }

        @Test
        void getBytesToAtomicBuffer() {
            final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[]{1, 2, 3, 4, 5});
            final UnsafeBuffer dstBuffer = new UnsafeBuffer(new byte[8]);

            srcBuffer.getBytes(1, dstBuffer, 3, 3);

            assertEquals(2, dstBuffer.getByte(3));
            assertEquals(3, dstBuffer.getByte(4));
            assertEquals(4, dstBuffer.getByte(5));
        }

        @Test
        void zeroLengthCopyIsNoOp() {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[]{99});
            final byte[] dst = new byte[0];

            assertDoesNotThrow(() -> buffer.getBytes(0, dst, 0, 0));
            assertDoesNotThrow(() -> buffer.putBytes(0, dst, 0, 0));
        }

        @Test
        void crossHeapDirectCopy() {
            final UnsafeBuffer heapBuffer = new UnsafeBuffer(new byte[]{1, 2, 3, 4});
            final UnsafeBuffer directBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(8));

            directBuffer.putBytes(0, heapBuffer, 0, 4);

            assertEquals(1, directBuffer.getByte(0));
            assertEquals(4, directBuffer.getByte(3));
        }
    }
}