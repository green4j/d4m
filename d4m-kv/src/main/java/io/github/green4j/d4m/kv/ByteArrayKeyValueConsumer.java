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

/**
 * A {@link KeyValueConsuming.KeyValueConsumer} that stores received key-value data
 * into a growable byte array. The key and value are written contiguously.
 */
public class ByteArrayKeyValueConsumer implements
        KeyValueConsuming.KeyValueConsumer<KeyValueConsuming.KeyValue> {

    public static final int MINIMUM_ARRAY_CAPACITY = 1024; // power of 2

    private int slotIndex;
    private int keySize;
    private int valueSize;

    private byte[] array;
    private AtomicBuffer buffer;

    /**
     * Returns the metadata slot index of the most recently received entry.
     *
     * @return the slot index
     */
    public int slotIndex() {
        return slotIndex;
    }

    /**
     * Returns the size of the most recently received key in bytes.
     *
     * @return the key size
     */
    public int keySize() {
        return keySize;
    }

    /**
     * Returns the size of the most recently received value in bytes.
     *
     * @return the value size
     */
    public int valueSize() {
        return valueSize;
    }

    /**
     * Returns the backing byte array containing the received key-value data.
     *
     * @return the byte array, or {@code null} if no data has been received
     */
    public byte[] array() {
        return array;
    }

    private final ByteArrayKeyValue kv = new ByteArrayKeyValue();

    /**
     * {@inheritDoc}
     */
    @Override
    public KeyValueConsuming.KeyValue putKeyValue(final int slotIndex,
                                                  final int keySize,
                                                  final int valueSize) {
        this.slotIndex = slotIndex;
        this.keySize = keySize;
        this.valueSize = valueSize;

        final int dataSize = keySize + valueSize;

        if (array == null || array.length < dataSize) {
            array = new byte[Math.max(
                    dataSize << 1,
                    MINIMUM_ARRAY_CAPACITY
            )];
            buffer = new UnsafeBuffer(array);
        }

        return kv;
    }

    private final class ByteArrayKeyValue implements
            KeyValueConsuming.KeyValue, BinaryContent {

        @Override
        public AtomicBuffer buffer() {
            return buffer;
        }

        @Override
        public int offset() {
            return 0;
        }

        @Override
        public BinaryContent content() {
            return this;
        }

        @Override
        public void apply() {
        }
    }
}
