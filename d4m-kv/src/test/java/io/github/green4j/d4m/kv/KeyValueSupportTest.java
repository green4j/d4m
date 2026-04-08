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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link KeyValueSupport} (hash and equals).
 */
class KeyValueSupportTest {

    @Nested
    class HashFunction {
        @Test
        void hashIsAlwaysPositive() {
            final AtomicBuffer buf = new UnsafeBuffer("hello world".getBytes());
            final int h = KeyValueSupport.hash(buf);
            assertTrue(h > 0);
        }

        @Test
        void zeroSizeHashIsOne() {
            final AtomicBuffer buf = new UnsafeBuffer(new byte[0]);
            assertEquals(1, KeyValueSupport.hash(buf, 0, 0));
        }

        @Test
        void sameContentProducesSameHash() {
            final AtomicBuffer buf1 = new UnsafeBuffer("test-key".getBytes());
            final AtomicBuffer buf2 = new UnsafeBuffer("test-key".getBytes());

            assertEquals(
                    KeyValueSupport.hash(buf1, 0, buf1.capacity()),
                    KeyValueSupport.hash(buf2, 0, buf2.capacity())
            );
        }

        @Test
        void differentContentProducesDifferentHash() {
            final AtomicBuffer buf1 = new UnsafeBuffer("alpha".getBytes());
            final AtomicBuffer buf2 = new UnsafeBuffer("bravo".getBytes());

            final int h1 = KeyValueSupport.hash(buf1, 0, buf1.capacity());
            final int h2 = KeyValueSupport.hash(buf2, 0, buf2.capacity());

            assertFalse(h1 == h2, "different content should produce different hashes");
        }

        @Test
        void hashOfSubRangeMatchesIsolatedBuffer() {
            final byte[] data = "prefix-key-suffix".getBytes();
            final AtomicBuffer full = new UnsafeBuffer(data);
            final AtomicBuffer sub = new UnsafeBuffer("key".getBytes());

            final int start = "prefix-".length();
            assertEquals(
                    KeyValueSupport.hash(sub, 0, 3),
                    KeyValueSupport.hash(full, start, 3)
            );
        }

        @Test
        void singleByteHashPositive() {
            final AtomicBuffer buf = new UnsafeBuffer(new byte[]{42});
            final int h = KeyValueSupport.hash(buf, 0, 1);
            assertTrue(h > 0);
        }

        @Test
        void wholeBufferHashMatchesRangedHash() {
            final AtomicBuffer buf = new UnsafeBuffer("full-buffer".getBytes());
            assertEquals(
                    KeyValueSupport.hash(buf),
                    KeyValueSupport.hash(buf, 0, buf.capacity())
            );
        }

        @Test
        void largeBufferHashPositive() {
            final byte[] data = new byte[4096];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) (i & 0xFF);
            }
            final AtomicBuffer buf = new UnsafeBuffer(data);
            assertTrue(KeyValueSupport.hash(buf) > 0);
        }
    }

    @Nested
    class Equals {
        @Test
        void identicalContentReturnsTrue() {
            final AtomicBuffer a = new UnsafeBuffer("hello".getBytes());
            final AtomicBuffer b = new UnsafeBuffer("hello".getBytes());

            assertTrue(KeyValueSupport.equals(a, 0, 5, b, 0, 5));
        }

        @Test
        void differentContentReturnsFalse() {
            final AtomicBuffer a = new UnsafeBuffer("hello".getBytes());
            final AtomicBuffer b = new UnsafeBuffer("world".getBytes());

            assertFalse(KeyValueSupport.equals(a, 0, 5, b, 0, 5));
        }

        @Test
        void differentSizeReturnsFalse() {
            final AtomicBuffer a = new UnsafeBuffer("hello".getBytes());
            final AtomicBuffer b = new UnsafeBuffer("hell".getBytes());

            assertFalse(KeyValueSupport.equals(a, 0, 5, b, 0, 4));
        }

        @Test
        void emptyBuffersAreEqual() {
            final AtomicBuffer a = new UnsafeBuffer(new byte[0]);
            final AtomicBuffer b = new UnsafeBuffer(new byte[0]);

            assertTrue(KeyValueSupport.equals(a, 0, 0, b, 0, 0));
        }

        @Test
        void subRangesComparedCorrectly() {
            final AtomicBuffer a = new UnsafeBuffer("xxhelloxx".getBytes());
            final AtomicBuffer b = new UnsafeBuffer("yyhelloyy".getBytes());

            assertTrue(KeyValueSupport.equals(a, 2, 5, b, 2, 5));
        }

        @Test
        void singleByteComparison() {
            final AtomicBuffer a = new UnsafeBuffer(new byte[]{0x7F});
            final AtomicBuffer b = new UnsafeBuffer(new byte[]{0x7F});

            assertTrue(KeyValueSupport.equals(a, 0, 1, b, 0, 1));
        }

        @Test
        void singleByteMismatch() {
            final AtomicBuffer a = new UnsafeBuffer(new byte[]{0x01});
            final AtomicBuffer b = new UnsafeBuffer(new byte[]{0x02});

            assertFalse(KeyValueSupport.equals(a, 0, 1, b, 0, 1));
        }

        @Test
        void largeIdenticalBuffersAreEqual() {
            final byte[] data = new byte[64];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }
            final AtomicBuffer a = new UnsafeBuffer(data.clone());
            final AtomicBuffer b = new UnsafeBuffer(data.clone());

            assertTrue(KeyValueSupport.equals(a, 0, 64, b, 0, 64));
        }

        @Test
        void largeBuffersSingleByteDifferenceFails() {
            final byte[] data = new byte[64];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }
            final AtomicBuffer a = new UnsafeBuffer(data);
            final byte[] copy = data.clone();
            copy[63] = (byte) 0xFF;
            final AtomicBuffer b = new UnsafeBuffer(copy);

            assertFalse(KeyValueSupport.equals(a, 0, 64, b, 0, 64));
        }

        @Test
        void differentOffsetsCompareCorrectContent() {
            final AtomicBuffer a = new UnsafeBuffer("___abc".getBytes());
            final AtomicBuffer b = new UnsafeBuffer("abc___".getBytes());

            assertTrue(KeyValueSupport.equals(a, 3, 3, b, 0, 3));
        }
    }
}
