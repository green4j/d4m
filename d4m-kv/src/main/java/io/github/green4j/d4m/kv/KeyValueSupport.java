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
import io.github.green4j.d4m.common.BitSupport;

/**
 * Utility methods for hashing and equality comparison of buffer-backed key-value data.
 */
public abstract class KeyValueSupport {

    private static final long HASH_SEED = 0x9E3779B97F4A7C15L;
    private static final long HASH_MULTIPLIER = 0xC2B2AE3D27D4EB4FL;

    /**
     * Returns a positive hash value for the entire content of the given buffer.
     *
     * @param data the buffer whose content to hash
     * @return a positive hash value
     */
    public static int hash(final AtomicBuffer data) {
        return hash(
                data,
                0,
                data.capacity()
        );
    }

    /**
     * Returns a positive hash value for a region of the given buffer. The region is
     * consumed in 8-byte little-endian words, each multiplied by a large odd constant
     * and mixed into the state, and the state is finalized with the MurmurHash3
     * 64-bit finalizer; so keys that differ in a few bytes, e.g. counters, do not
     * collide, and the low bits used to pick a segment and a slot are well mixed.
     * The result depends only on the bytes, not on the alignment of the region or
     * the byte order of the platform.
     *
     * @param data   the buffer containing the data to hash
     * @param offset the starting offset within the buffer
     * @param size   the number of bytes to hash
     * @return a hash value in {@code [1, 2^31)}: the metadata of a tier keeps it in
     *         31 bits next to the occupied flag
     */
    public static int hash(final AtomicBuffer data,
                           final int offset,
                           final int size) {
        if (size == 0) {
            return 1;
        }

        long hash = HASH_SEED ^ (size * HASH_MULTIPLIER);
        int index = offset;
        final int end = offset + size;

        while (end - index >= Long.BYTES) {
            hash = mix(hash, readLongLittleEndian(data, index, Long.BYTES));
            index += Long.BYTES;
        }
        if (index < end) {
            hash = mix(hash, readLongLittleEndian(data, index, end - index));
        }

        hash ^= hash >>> 33;
        hash *= 0xFF51AFD7ED558CCDL;
        hash ^= hash >>> 33;
        hash *= 0xC4CEB9FE1A85EC53L;
        hash ^= hash >>> 33;

        final int result = (int) (hash ^ (hash >>> 32)) & 0x7FFFFFFF;
        return result == 0 ? 1 : result;
    }

    private static long mix(final long hash,
                            final long word) {
        return Long.rotateLeft(hash ^ (word * HASH_MULTIPLIER), 31) * HASH_SEED;
    }

    // Assembled from bytes, so it is the same for any alignment and byte order.
    private static long readLongLittleEndian(final AtomicBuffer data,
                                             final int index,
                                             final int length) {
        long word = 0;
        for (int i = 0; i < length; i++) {
            word |= (data.getByte(index + i) & 0xFFL) << (i << 3);
        }
        return word;
    }

    /**
     * Compares two regions of {@link AtomicBuffer}s for byte-level equality,
     * using widened reads (long then int) when alignment allows.
     *
     * @param array1  the first buffer
     * @param offset1 the offset in the first buffer
     * @param size1   the number of bytes to compare from the first buffer
     * @param array2  the second buffer
     * @param offset2 the offset in the second buffer
     * @param size2   the number of bytes to compare from the second buffer
     * @return {@code true} if the two regions contain identical bytes
     */
    public static boolean equals(final AtomicBuffer array1,
                                 final int offset1,
                                 final int size1,
                                 final AtomicBuffer array2,
                                 final int offset2,
                                 final int size2) {
        if (size1 != size2) {
            return false;
        }

        if (size1 == 0) {
            return true;
        }

        int index1 = offset1;
        int index2 = offset2;
        int remaining = size1;

        // 8-byte aligned comparison
        if (BitSupport.isAlignedToLong(index1) && BitSupport.isAlignedToLong(index2)
                && remaining >= BitSupport.SIZE_OF_LONG) {
            while (remaining >= BitSupport.SIZE_OF_LONG) {
                final long val1 = array1.getLong(index1);
                final long val2 = array2.getLong(index2);
                if (val1 != val2) {
                    return false;
                }
                index1 += BitSupport.SIZE_OF_LONG;
                index2 += BitSupport.SIZE_OF_LONG;
                remaining -= BitSupport.SIZE_OF_LONG;
            }
        }

        // 4-byte aligned comparison for remaining data
        if (BitSupport.isAlignedToInt(index1) && BitSupport.isAlignedToInt(index2)
                && remaining >= BitSupport.SIZE_OF_INT) {
            while (remaining >= BitSupport.SIZE_OF_INT) {
                final int val1 = array1.getInt(index1);
                final int val2 = array2.getInt(index2);
                if (val1 != val2) {
                    return false;
                }
                index1 += BitSupport.SIZE_OF_INT;
                index2 += BitSupport.SIZE_OF_INT;
                remaining -= BitSupport.SIZE_OF_INT;
            }
        }

        // Compare remaining bytes
        while (remaining > 0) {
            final byte val1 = array1.getByte(index1);
            final byte val2 = array2.getByte(index2);
            if (val1 != val2) {
                return false;
            }
            index1++;
            index2++;
            remaining--;
        }

        return true;
    }
}
