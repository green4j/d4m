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

/**
 * Defines the consuming protocol for reading and writing key-value data
 * in the storage. Consumers and produced entries follow a two-phase pattern:
 * first obtain a writable region, then call {@link Applicable#apply()} to commit.
 */
public interface KeyValueConsuming {

    /**
     * A writable key-value entry whose binary content can be populated before applying.
     */
    interface KeyValue extends Applicable {
        /**
         * Returns the writable binary region for both key and value data.
         *
         * @return the binary content handle
         */
        BinaryContent content();
    }

    /**
     * A writable key-only entry whose binary content can be populated before applying.
     */
    interface Key extends Applicable {
        /**
         * Returns the writable binary region for the key data.
         *
         * @return the binary content handle for the key
         */
        BinaryContent keyContent();
    }

    /**
     * A writable value-only entry whose binary content can be populated before applying.
     */
    interface Value extends Applicable {
        /**
         * Returns the writable binary region for the value data.
         *
         * @return the binary content handle for the value
         */
        BinaryContent valueContent();
    }

    /**
     * A {@link KeyValue} that also carries a positional index within the buffer.
     */
    interface IndexedKeyValue extends KeyValue, Indexable {
    }

    /**
     * Combines both {@link Key} and {@link Value} access into a single entry.
     */
    interface KeyAndValue extends Key, Value {
    }

    /**
     * Consumer that receives a value allocation for a given value size.
     *
     * @param <C> the concrete {@link Value} type
     */
    interface ValueConsumer<C extends Value> {
        /**
         * Allocates a writable value entry of the specified size.
         *
         * @param valueSize the size of the value in bytes
         * @return a writable {@link Value}, or {@code null} to skip
         */
        C putValue(
                int valueSize
        );
    }

    /**
     * A {@link ValueConsumer} that can request early termination of an
     * iteration (e.g. {@link ListAccessor#forEach(StoppableValueConsumer)}).
     * The iterator polls {@link #stopped()} after delivering each entry.
     *
     * @param <C> the concrete {@link Value} type
     */
    interface StoppableValueConsumer<C extends Value> extends ValueConsumer<C> {
        /**
         * Polled by iteration methods after each entry has been delivered
         * (i.e. after the entry's {@code putValue}/{@code apply} cycle).
         *
         * @return {@code true} to stop iterating (no further entries are
         *         delivered), {@code false} to continue
         */
        boolean stopped();
    }

    /**
     * Consumer that receives a key-value allocation for given key and value sizes.
     *
     * @param <C> the concrete {@link KeyValue} type
     */
    interface KeyValueConsumer<C extends KeyValue> {
        /**
         * Allocates a writable key-value entry of the specified sizes.
         *
         * @param slotIndex the metadata slot index assigned to this entry
         * @param keySize   the size of the key in bytes
         * @param valueSize the size of the value in bytes
         * @return a writable {@link KeyValue}, or {@code null} to skip
         */
        C putKeyValue(
                int slotIndex,
                int keySize,
                int valueSize
        );
    }

    /**
     * Consumer that receives a key-and-value allocation with separate key and value content regions.
     *
     * @param <C> the concrete {@link KeyAndValue} type
     */
    interface KeyAndValueConsumer<C extends KeyAndValue> {
        /**
         * Allocates a writable key-and-value entry of the specified sizes.
         *
         * @param slotIndex the metadata slot index assigned to this entry
         * @param keySize   the size of the key in bytes
         * @param valueSize the size of the value in bytes
         * @return a writable {@link KeyAndValue}, or {@code null} to skip
         */
        C putKeyAndValue(
                int slotIndex,
                int keySize,
                int valueSize
        );
    }
}
