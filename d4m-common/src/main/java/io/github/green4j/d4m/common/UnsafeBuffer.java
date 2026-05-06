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

import jdk.internal.misc.Unsafe;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * {@link AtomicBuffer} implementation backed by {@link Unsafe}.
 *
 * <p>Supports three backing stores:
 * <ul>
 *   <li>Raw {@code byte[]} - used for user-constructed payloads.</li>
 *   <li>Heap {@link ByteBuffer} (incl. slices) - used for heap-chunk slabs.</li>
 *   <li>Direct/{@link java.nio.MappedByteBuffer} - used for mmap chunks.</li>
 * </ul>
 *
 * <p>All accessors use a unified addressing scheme:
 * <pre>
 *   effectiveAddress = base + index
 *   UNSAFE.getXxx(heap, effectiveAddress)
 * </pre>
 * where {@code heap} is the backing {@code byte[]} (or {@code null} for
 * direct memory) and {@code base} is the precomputed base offset.
 *
 * <p>Optional bounds checking is enabled by setting the system property
 * {@code buffer.bounds.check=true} (off by default for production
 * throughput).
 *
 * <p><b>Module flags required:</b>
 * {@code --add-opens java.base/jdk.internal.misc=ALL-UNNAMED}
 * {@code --add-opens java.base/java.nio=ALL-UNNAMED}
 */
@SuppressWarnings("deprecation")
public final class UnsafeBuffer implements AtomicBuffer {
    private static final Unsafe UNSAFE;
    private static final long BYTE_ARRAY_BASE;
    private static final VarHandle BUFFER_ADDRESS;
    private static final boolean BOUNDS_CHECK;

    static {
        try {
            UNSAFE = Unsafe.getUnsafe();
            BYTE_ARRAY_BASE = UNSAFE.arrayBaseOffset(byte[].class);
            BUFFER_ADDRESS = MethodHandles
                    .privateLookupIn(Buffer.class, MethodHandles.lookup())
                    .findVarHandle(Buffer.class, "address", long.class);
            BOUNDS_CHECK = "true".equals(
                    System.getProperty("buffer.bounds.check"));
        } catch (final ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Non-null for heap-backed buffers; null for direct memory.
     * Passed as the {@code Object} argument to every {@code Unsafe} call
     * so that the same code path handles both heap and off-heap.
     */
    private byte[] heap;

    /**
     * Pre-computed base offset.
     * <ul>
     *   <li>Heap byte[]:      {@code BYTE_ARRAY_BASE + userOffset}</li>
     *   <li>Heap ByteBuffer:  {@code BYTE_ARRAY_BASE + arrayOffset() + userOffset}</li>
     *   <li>Direct ByteBuffer:{@code nativeAddress + userOffset}</li>
     * </ul>
     */
    private long base;

    private int capacity;
    private ByteBuffer byteBuffer;

    /**
     * Wraps the entire byte array.
     *
     * @param array the byte array to wrap
     */
    public UnsafeBuffer(final byte[] array) {
        wrap(array, 0, array.length);
    }

    /**
     * Wraps a region of a byte array.
     *
     * @param array  the byte array to wrap
     * @param offset starting byte offset within the array
     * @param size   number of bytes to expose
     */
    public UnsafeBuffer(final byte[] array,
                        final int offset,
                        final int size) {
        wrap(array, offset, size);
    }

    /**
     * Wraps the entire {@link ByteBuffer} (0 to capacity).
     *
     * @param buffer the ByteBuffer to wrap (heap or direct)
     */
    public UnsafeBuffer(final ByteBuffer buffer) {
        wrap(buffer, 0, buffer.capacity());
    }

    /**
     * Wraps a region within a {@link ByteBuffer}.
     *
     * @param buffer the ByteBuffer to wrap (heap or direct)
     * @param offset starting byte offset within the ByteBuffer
     * @param size   number of bytes to expose
     */
    public UnsafeBuffer(final ByteBuffer buffer,
                        final int offset,
                        final int size) {
        wrap(buffer, offset, size);
    }

    /**
     * Re-binds this buffer to the entire byte array.
     *
     * @param array the byte array to wrap
     */
    public void wrap(final byte[] array) {
        wrap(array, 0, array.length);
    }

    /**
     * Re-binds this buffer to a region of a byte array.
     *
     * @param array  the byte array to wrap
     * @param offset starting byte offset within the array
     * @param size   number of bytes to expose
     */
    public void wrap(final byte[] array,
                     final int offset,
                     final int size) {
        Objects.requireNonNull(array, "array");

        this.heap = array;
        this.base = BYTE_ARRAY_BASE + offset;
        this.capacity = size;
        this.byteBuffer = null;
    }

    /**
     * Re-binds this buffer to the entire {@link ByteBuffer}.
     *
     * @param buffer the ByteBuffer to wrap (heap or direct)
     */
    public void wrap(final ByteBuffer buffer) {
        wrap(buffer, 0, buffer.capacity());
    }

    /**
     * Re-binds this buffer to a region within a {@link ByteBuffer}.
     *
     * @param buffer the ByteBuffer to wrap (heap or direct)
     * @param offset starting byte offset within the ByteBuffer
     * @param size   number of bytes to expose
     */
    public void wrap(final ByteBuffer buffer,
                     final int offset,
                     final int size) {
        Objects.requireNonNull(buffer, "buffer");

        this.byteBuffer = buffer;
        this.capacity = size;
        if (buffer.isDirect()) {
            this.heap = null;
            this.base = directAddress(buffer) + offset;
        } else {
            this.heap = buffer.array();
            this.base = BYTE_ARRAY_BASE + buffer.arrayOffset() + offset;
        }
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public boolean isDirect() {
        return heap == null;
    }

    @Override
    public ByteBuffer byteBuffer() {
        return byteBuffer;
    }

    @Override
    public byte getByte(final int index) {
        boundsCheck(index, Byte.BYTES);
        return UNSAFE.getByte(heap, base + index);
    }

    @Override
    public void putByte(final int index,
                        final byte value) {
        boundsCheck(index, Byte.BYTES);
        UNSAFE.putByte(heap, base + index, value);
    }

    @Override
    public short getShort(final int index) {
        boundsCheck(index, Short.BYTES);
        return UNSAFE.getShort(heap, base + index);
    }

    @Override
    public void putShort(final int index,
                         final short value) {
        boundsCheck(index, Short.BYTES);
        UNSAFE.putShort(heap, base + index, value);
    }

    @Override
    public int getInt(final int index) {
        boundsCheck(index, Integer.BYTES);
        return UNSAFE.getInt(heap, base + index);
    }

    @Override
    public void putInt(final int index,
                       final int value) {
        boundsCheck(index, Integer.BYTES);
        UNSAFE.putInt(heap, base + index, value);
    }

    @Override
    public long getLong(final int index) {
        boundsCheck(index, Long.BYTES);
        return UNSAFE.getLong(heap, base + index);
    }

    @Override
    public void putLong(final int index,
                        final long value) {
        boundsCheck(index, Long.BYTES);
        UNSAFE.putLong(heap, base + index, value);
    }

    @Override
    public float getFloat(final int index) {
        boundsCheck(index, Float.BYTES);
        return UNSAFE.getFloat(heap, base + index);
    }

    @Override
    public void putFloat(final int index,
                         final float value) {
        boundsCheck(index, Float.BYTES);
        UNSAFE.putFloat(heap, base + index, value);
    }

    @Override
    public double getDouble(final int index) {
        boundsCheck(index, Double.BYTES);
        return UNSAFE.getDouble(heap, base + index);
    }

    @Override
    public void putDouble(final int index,
                          final double value) {
        boundsCheck(index, Double.BYTES);
        UNSAFE.putDouble(heap, base + index, value);
    }

    @Override
    public int getIntVolatile(final int index) {
        boundsCheck(index, Integer.BYTES);
        return UNSAFE.getIntVolatile(heap, base + index);
    }

    @Override
    public void putIntVolatile(final int index,
                               final int value) {
        boundsCheck(index, Integer.BYTES);
        UNSAFE.putIntVolatile(heap, base + index, value);
    }

    @Override
    public long getLongVolatile(final int index) {
        boundsCheck(index, Long.BYTES);
        return UNSAFE.getLongVolatile(heap, base + index);
    }

    @Override
    public void putLongVolatile(final int index,
                                final long value) {
        boundsCheck(index, Long.BYTES);
        UNSAFE.putLongVolatile(heap, base + index, value);
    }

    @Override
    public void putIntOrdered(final int index,
                              final int value) {
        boundsCheck(index, Integer.BYTES);
        UNSAFE.putIntRelease(heap, base + index, value);
    }

    @Override
    public void putLongOrdered(final int index,
                               final long value) {
        boundsCheck(index, Long.BYTES);
        UNSAFE.putLongRelease(heap, base + index, value);
    }

    @Override
    public boolean compareAndSetInt(final int index,
                                    final int expected,
                                    final int update) {
        boundsCheck(index, Integer.BYTES);
        return UNSAFE.compareAndSetInt(heap, base + index, expected, update);
    }

    @Override
    public boolean compareAndSetLong(final int index,
                                     final long expected,
                                     final long update) {
        boundsCheck(index, Long.BYTES);
        return UNSAFE.compareAndSetLong(heap, base + index, expected, update);
    }

    @Override
    public void getBytes(final int index,
                         final byte[] dst,
                         final int dstOffset,
                         final int size) {
        if (size == 0) {
            return;
        }
        boundsCheck(index, size);
        arrayBoundsCheck(dst.length, dstOffset, size);
        UNSAFE.copyMemory(heap, base + index,
                dst, BYTE_ARRAY_BASE + dstOffset,
                size);
    }

    @Override
    public void putBytes(final int index,
                         final byte[] src,
                         final int srcOffset,
                         final int size) {
        if (size == 0) {
            return;
        }
        boundsCheck(index, size);
        arrayBoundsCheck(src.length, srcOffset, size);
        UNSAFE.copyMemory(src, BYTE_ARRAY_BASE + srcOffset,
                heap, base + index,
                size);
    }

    @Override
    public void getBytes(final int index,
                         final AtomicBuffer dst,
                         final int dstIndex,
                         final int size) {
        if (size == 0) {
            return;
        }
        boundsCheck(index, size);
        final UnsafeBuffer d = (UnsafeBuffer) dst;
        d.boundsCheck(dstIndex, size);
        UNSAFE.copyMemory(this.heap, this.base + index,
                d.heap, d.base + dstIndex,
                size);
    }

    @Override
    public void putBytes(final int index,
                         final AtomicBuffer src,
                         final int srcIndex,
                         final int size) {
        if (size == 0) {
            return;
        }
        boundsCheck(index, size);
        final UnsafeBuffer s = (UnsafeBuffer) src;
        s.boundsCheck(srcIndex, size);
        UNSAFE.copyMemory(s.heap, s.base + srcIndex,
                this.heap, this.base + index,
                size);
    }

    private void boundsCheck(final int index,
                             final int accessSize) {
        if (!BOUNDS_CHECK) {
            return;
        }
        if (index < 0 || accessSize < 0 || index + accessSize > capacity) {
            throw new IndexOutOfBoundsException(
                    "index=" + index + " size=" + accessSize
                            + " capacity=" + capacity);
        }
    }

    private static void arrayBoundsCheck(final int arraySize,
                                         final int offset,
                                         final int size) {
        if (!BOUNDS_CHECK) {
            return;
        }
        if (offset < 0 || size < 0 || offset + size > arraySize) {
            throw new IndexOutOfBoundsException(
                    "offset=" + offset + " size=" + size
                            + " arrayLength=" + arraySize);
        }

    }

    private static long directAddress(final ByteBuffer buffer) {
        return (long) BUFFER_ADDRESS.get(buffer);
    }

    @Override
    public String toString() {
        return "UnsafeBuffer{capacity=" + capacity
                + ", direct=" + isDirect()
                + ", base=0x" + Long.toHexString(base)
                + '}';
    }
}
