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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link BitSupport}.
 */
class BitSupportTest {

    @Nested
    class IsAlignedToLong {
        @Test
        void alignedValuesReturnTrue() {
            assertTrue(BitSupport.isAlignedToLong(0));
            assertTrue(BitSupport.isAlignedToLong(8));
            assertTrue(BitSupport.isAlignedToLong(16));
            assertTrue(BitSupport.isAlignedToLong(1024));
        }

        @Test
        void unalignedValuesReturnFalse() {
            assertFalse(BitSupport.isAlignedToLong(1));
            assertFalse(BitSupport.isAlignedToLong(3));
            assertFalse(BitSupport.isAlignedToLong(7));
            assertFalse(BitSupport.isAlignedToLong(9));
        }
    }

    @Nested
    class IsAlignedToInt {
        @Test
        void alignedValuesReturnTrue() {
            assertTrue(BitSupport.isAlignedToInt(0));
            assertTrue(BitSupport.isAlignedToInt(4));
            assertTrue(BitSupport.isAlignedToInt(8));
            assertTrue(BitSupport.isAlignedToInt(128));
        }

        @Test
        void unalignedValuesReturnFalse() {
            assertFalse(BitSupport.isAlignedToInt(1));
            assertFalse(BitSupport.isAlignedToInt(2));
            assertFalse(BitSupport.isAlignedToInt(3));
            assertFalse(BitSupport.isAlignedToInt(7));
        }
    }

    @Nested
    class AlignToLong {
        @Test
        void zeroRemainsZero() {
            assertEquals(0, BitSupport.alignToLong(0));
        }

        @Test
        void alreadyAlignedValueUnchanged() {
            assertEquals(8, BitSupport.alignToLong(8));
            assertEquals(16, BitSupport.alignToLong(16));
            assertEquals(24, BitSupport.alignToLong(24));
        }

        @Test
        void unalignedValueRoundsUp() {
            assertEquals(8, BitSupport.alignToLong(1));
            assertEquals(8, BitSupport.alignToLong(7));
            assertEquals(16, BitSupport.alignToLong(9));
            assertEquals(16, BitSupport.alignToLong(15));
        }
    }

    @Nested
    class IsPowerOfTwo {
        @Test
        void powersOfTwoReturnTrue() {
            assertTrue(BitSupport.isPowerOfTwo(1));
            assertTrue(BitSupport.isPowerOfTwo(2));
            assertTrue(BitSupport.isPowerOfTwo(4));
            assertTrue(BitSupport.isPowerOfTwo(8));
            assertTrue(BitSupport.isPowerOfTwo(1024));
        }

        @Test
        void nonPowersOfTwoReturnFalse() {
            assertFalse(BitSupport.isPowerOfTwo(0));
            assertFalse(BitSupport.isPowerOfTwo(3));
            assertFalse(BitSupport.isPowerOfTwo(6));
            assertFalse(BitSupport.isPowerOfTwo(100));
        }

        @Test
        void negativeReturnsFalse() {
            assertFalse(BitSupport.isPowerOfTwo(-1));
            assertFalse(BitSupport.isPowerOfTwo(-8));
        }
    }

    @Nested
    class NextPowerOfTwo {
        @Test
        void alreadyPowerOfTwoReturnsItself() {
            assertEquals(1, BitSupport.nextPowerOfTwo(1));
            assertEquals(2, BitSupport.nextPowerOfTwo(2));
            assertEquals(4, BitSupport.nextPowerOfTwo(4));
            assertEquals(1024, BitSupport.nextPowerOfTwo(1024));
        }

        @Test
        void nonPowerRoundsUp() {
            assertEquals(4, BitSupport.nextPowerOfTwo(3));
            assertEquals(8, BitSupport.nextPowerOfTwo(5));
            assertEquals(16, BitSupport.nextPowerOfTwo(9));
            assertEquals(128, BitSupport.nextPowerOfTwo(100));
        }

        @Test
        void zeroReturnsOne() {
            assertEquals(1, BitSupport.nextPowerOfTwo(0));
        }

        @Test
        void negativeReturnsOne() {
            assertEquals(1, BitSupport.nextPowerOfTwo(-1));
            assertEquals(1, BitSupport.nextPowerOfTwo(-100));
        }
    }
}
