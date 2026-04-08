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

/**
 * Bit-manipulation utilities
 */
public abstract class BitSupport {
    /**
     * Number of bytes in a Java {@code int}.
     */
    public static final int SIZE_OF_INT = 4;

    /**
     * Number of bytes in a Java {@code long}.
     */
    public static final int SIZE_OF_LONG = 8;

    /**
     * Number of bits in a Java {@code int} (same as {@link Integer#SIZE}).
     */
    public static final int INT_BITS = 32;

    /**
     * Bitmask that keeps the lower 32 bits of a {@code long}.
     */
    public static final long MASK_32_BITS = 0xFFFFFFFFL;

    /**
     * Rounds the given value up to the nearest multiple of {@link #SIZE_OF_LONG} (8).
     *
     * @param value the value to align
     * @return the aligned value
     */
    public static int alignToLong(final int value) {
        return (value + SIZE_OF_LONG - 1) & ~(SIZE_OF_LONG - 1);
    }

    /**
     * Checks whether the given value is aligned to a long (8-byte) boundary.
     *
     * @param value the value to check
     * @return {@code true} if aligned
     */
    public static boolean isAlignedToLong(final long value) {
        return (value & (SIZE_OF_LONG - 1)) == 0;
    }

    /**
     * Checks whether the given value is aligned to an int (4-byte) boundary.
     *
     * @param value the value to check
     * @return {@code true} if aligned
     */
    public static boolean isAlignedToInt(final long value) {
        return (value & (SIZE_OF_INT - 1)) == 0;
    }

    /**
     * Checks whether the given integer is a positive power of two.
     *
     * @param n the integer to check
     * @return {@code true} if {@code n} is a power of two
     */
    public static boolean isPowerOfTwo(final int n) {
        return n > 0 && (n & (n - 1)) == 0;
    }

    /**
     * Returns the smallest power of two that is greater than or equal to the given value.
     * For values &le; 1 (including negatives and zero), returns 1.
     *
     * @param v the input value
     * @return the next power of two
     */
    public static int nextPowerOfTwo(final int v) {
        if (v <= 1) {
            return 1;
        }
        return 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(v - 1));
    }

    private BitSupport() {
    }
}
