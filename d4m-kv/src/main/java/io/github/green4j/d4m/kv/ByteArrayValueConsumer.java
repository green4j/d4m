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
 * A {@link KeyValueConsuming.ValueConsumer} that stores received value data
 * into a growable byte array.
 */
public class ByteArrayValueConsumer implements
        KeyValueConsuming.ValueConsumer<KeyValueConsuming.Value> {

    public static final int MINIMUM_ARRAY_CAPACITY = 1024; // power of 2

    private int valueSize;

    private byte[] array;
    private AtomicBuffer buffer;

    /**
     * Returns the size of the most recently received value in bytes.
     *
     * @return the value size
     */
    public int valueSize() {
        return valueSize;
    }

    /**
     * Returns the backing byte array containing the received value data.
     *
     * @return the byte array, or {@code null} if no value has been received
     */
    public byte[] array() {
        return array;
    }

    private final ByteArrayValue v = new ByteArrayValue();

    /**
     * {@inheritDoc}
     */
    @Override
    public KeyValueConsuming.Value putValue(final int valueSize) {
        this.valueSize = valueSize;

        if (array == null || array.length < valueSize) {
            array = new byte[Math.max(
                    valueSize << 1,
                    MINIMUM_ARRAY_CAPACITY
            )];
            buffer = new UnsafeBuffer(array);
        }

        return v;
    }

    private final class ByteArrayValue implements
            KeyValueConsuming.Value, BinaryContent {

        @Override
        public AtomicBuffer buffer() {
            return buffer;
        }

        @Override
        public int offset() {
            return 0;
        }

        @Override
        public BinaryContent valueContent() {
            return this;
        }

        @Override
        public void apply() {
        }
    }
}
