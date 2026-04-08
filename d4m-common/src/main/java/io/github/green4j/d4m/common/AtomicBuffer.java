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

import java.nio.ByteBuffer;

/**
 * Buffer abstraction providing plain, volatile, ordered, and CAS access
 * to memory regions backed by heap byte arrays or direct/mapped ByteBuffers.
 *
 * <p>All multi-byte accessors use <b>native byte order</b>. Indices are
 * zero-based byte offsets into the buffer region. Volatile, ordered, and
 * CAS operations on {@code int} and {@code long} require naturally aligned
 * offsets (4-byte and 8-byte respectively) for correctness on all
 * architectures.</p>
 */
public interface AtomicBuffer {

    /**
     * Returns the usable capacity in bytes.
     *
     * @return capacity in bytes
     */
    int capacity();

    /**
     * Returns whether this buffer is backed by off-heap/direct memory.
     *
     * @return {@code true} when off-heap/direct, {@code false} if heap
     */
    boolean isDirect();

    /**
     * Returns the underlying {@link ByteBuffer}, or {@code null} if this
     * buffer was created from a raw {@code byte[]}.
     *
     * <p>Primarily intended for the {@code isDirect()} check that
     * distinguishes heap chunks from mmap chunks.  Callers must not
     * assume that the returned buffer's position/limit/capacity match
     * this buffer's region.</p>
     *
     * @return the underlying {@link ByteBuffer}, or {@code null} if
     *         backed by a raw {@code byte[]}
     */
    ByteBuffer byteBuffer();

    /**
     * Reads a single byte at the given byte offset.
     *
     * @param index zero-based byte offset
     * @return the byte value at the offset
     */
    byte getByte(int index);

    /**
     * Writes a single byte at the given byte offset.
     *
     * @param index zero-based byte offset
     * @param value the byte value to write
     */
    void putByte(int index,
                 byte value);

    /**
     * Reads a 16-bit short in native byte order.
     *
     * @param index zero-based byte offset
     * @return the short value at the offset
     */
    short getShort(int index);

    /**
     * Writes a 16-bit short in native byte order.
     *
     * @param index zero-based byte offset
     * @param value the short value to write
     */
    void putShort(int index,
                  short value);

    /**
     * Reads a 32-bit int in native byte order.
     *
     * @param index zero-based byte offset
     * @return the int value at the offset
     */
    int getInt(int index);

    /**
     * Writes a 32-bit int in native byte order.
     *
     * @param index zero-based byte offset
     * @param value the int value to write
     */
    void putInt(int index,
                int value);

    /**
     * Reads a 64-bit long in native byte order.
     *
     * @param index zero-based byte offset
     * @return the long value at the offset
     */
    long getLong(int index);

    /**
     * Writes a 64-bit long in native byte order.
     *
     * @param index zero-based byte offset
     * @param value the long value to write
     */
    void putLong(int index,
                 long value);

    /**
     * Reads a 32-bit float in native byte order.
     *
     * @param index zero-based byte offset
     * @return the float value at the offset
     */
    float getFloat(int index);

    /**
     * Writes a 32-bit float in native byte order.
     *
     * @param index zero-based byte offset
     * @param value the float value to write
     */
    void putFloat(int index,
                  float value);

    /**
     * Reads a 64-bit double in native byte order.
     *
     * @param index zero-based byte offset
     * @return the double value at the offset
     */
    double getDouble(int index);

    /**
     * Writes a 64-bit double in native byte order.
     *
     * @param index zero-based byte offset
     * @param value the double value to write
     */
    void putDouble(int index,
                   double value);

    /**
     * Reads a 32-bit int with volatile (acquire) semantics.
     *
     * @param index zero-based byte offset (must be 4-byte aligned)
     * @return the int value at the offset
     */
    int getIntVolatile(int index);

    /**
     * Writes a 32-bit int with volatile (full-fence) semantics.
     *
     * @param index zero-based byte offset (must be 4-byte aligned)
     * @param value the int value to write
     */
    void putIntVolatile(int index,
                        int value);

    /**
     * Reads a 64-bit long with volatile (acquire) semantics.
     *
     * @param index zero-based byte offset (must be 8-byte aligned)
     * @return the long value at the offset
     */
    long getLongVolatile(int index);

    /**
     * Writes a 64-bit long with volatile (full-fence) semantics.
     *
     * @param index zero-based byte offset (must be 8-byte aligned)
     * @param value the long value to write
     */
    void putLongVolatile(int index,
                         long value);

    /**
     * Release-store of an {@code int}.  All prior plain stores by the
     * calling thread are guaranteed to be visible to any thread that
     * later performs an acquire-load (volatile read) of the same
     * location and observes the written value.
     *
     * @param index zero-based byte offset (must be 4-byte aligned)
     * @param value the int value to write
     */
    void putIntOrdered(int index,
                       int value);

    /**
     * Release-store of a {@code long}. Same ordering guarantees as
     * {@link #putIntOrdered(int, int)}.
     *
     * @param index zero-based byte offset (must be 8-byte aligned)
     * @param value the long value to write
     */
    void putLongOrdered(int index,
                        long value);

    /**
     * Atomically sets the 32-bit int at {@code index} to {@code update}
     * if the current value equals {@code expected}.
     *
     * @param index    zero-based byte offset (must be 4-byte aligned)
     * @param expected the expected current value
     * @param update   the new value to set
     * @return {@code true} if the CAS succeeded
     */
    boolean compareAndSetInt(int index,
                             int expected,
                             int update);

    /**
     * Atomically sets the 64-bit long at {@code index} to {@code update}
     * if the current value equals {@code expected}.
     *
     * @param index    zero-based byte offset (must be 8-byte aligned)
     * @param expected the expected current value
     * @param update   the new value to set
     * @return {@code true} if the CAS succeeded
     */
    boolean compareAndSetLong(int index,
                              long expected,
                              long update);

    /**
     * Copies {@code size} bytes from this buffer into a destination byte array.
     *
     * @param index     source offset in this buffer
     * @param dst       destination byte array
     * @param dstOffset offset within the destination array
     * @param size      number of bytes to copy
     */
    void getBytes(int index, byte[] dst,
                  int dstOffset,
                  int size);

    /**
     * Copies {@code size} bytes from a source byte array into this buffer.
     *
     * @param index     destination offset in this buffer
     * @param src       source byte array
     * @param srcOffset offset within the source array
     * @param size      number of bytes to copy
     */
    void putBytes(int index,
                  byte[] src,
                  int srcOffset,
                  int size);

    /**
     * Convenience overload that copies the entire source array into this buffer
     * starting at {@code index}.
     *
     * @param index destination offset in this buffer
     * @param src   source byte array to copy in full
     */
    default void putBytes(int index,
                          byte[] src) {
        putBytes(index, src, 0, src.length);
    }

    /**
     * Copies {@code size} bytes from this buffer at {@code index}
     * into {@code dst} starting at {@code dstIndex}.
     *
     * @param index    source offset in this buffer
     * @param dst      destination buffer
     * @param dstIndex offset within the destination buffer
     * @param size     number of bytes to copy
     */
    void getBytes(int index,
                  AtomicBuffer dst,
                  int dstIndex,
                  int size);

    /**
     * Copies {@code size} bytes from {@code src} starting at
     * {@code srcIndex} into this buffer at {@code index}.
     *
     * @param index    destination offset in this buffer
     * @param src      source buffer
     * @param srcIndex offset within the source buffer
     * @param size   number of bytes to copy
     */
    void putBytes(int index,
                  AtomicBuffer src,
                  int srcIndex,
                  int size);
}
