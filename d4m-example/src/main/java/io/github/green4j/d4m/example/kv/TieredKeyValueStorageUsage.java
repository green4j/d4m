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
package io.github.green4j.d4m.example.kv;

import io.github.green4j.d4m.common.AtomicBuffer;
import io.github.green4j.d4m.common.UnsafeBuffer;
import io.github.green4j.d4m.kv.ByteArrayValueConsumer;
import io.github.green4j.d4m.kv.KeyValueRing;
import io.github.green4j.d4m.kv.KeyValueSegment;
import io.github.green4j.d4m.kv.KeyValueStorage;
import io.github.green4j.d4m.kv.Tier;

import static io.github.green4j.d4m.common.BitSupport.SIZE_OF_LONG;
import static io.github.green4j.d4m.example.ExampleSupport.BR;
import static io.github.green4j.d4m.example.ExampleSupport.PERFORMANCE_RESULT_TITLE;
import static io.github.green4j.d4m.example.ExampleSupport.formatBytesToHumanReadable;

/**
 * Demonstrates basic usage of {@link KeyValueStorage}: bulk put/get
 * operations, performance measurement, and storage statistics reporting.
 *
 * <p><b>Module flags required:</b>
 * {@code --add-opens java.base/jdk.internal.misc=ALL-UNNAMED}
 * {@code --add-opens java.base/java.nio=ALL-UNNAMED}
 */
public class TieredKeyValueStorageUsage {
    private static final int TOTAL_NUMBER_OF_KEY_VALUES = 10_000_000;
    private static final int WARMUP_NUMBER_OF_KEY_VALUES = 1_000_000;

    private static final int NUMBER_OF_MEASURED_KEY_VALUES = TOTAL_NUMBER_OF_KEY_VALUES - WARMUP_NUMBER_OF_KEY_VALUES;

    private static final String KEY_PREFIX = "k".repeat(12);
    private static final String VALUE_PREFIX = "v".repeat(200);

    private static final byte[] KEY_PREFIX_BYTES = KEY_PREFIX.getBytes();
    private static final byte[] VALUE_PREFIX_BYTES = VALUE_PREFIX.getBytes();

    public static void main(final String[] args) {
        final KeyValueStorage.Builder builder = KeyValueStorage.builder();

        final KeyValueStorage storage = builder.build();

        final long startPutTime = putAllKeyValues(storage);
        final long endPutTime = System.nanoTime();

        final long startGetTime = getAllKeyValues(storage);
        final long endGetTime = System.nanoTime();

        printPerformanceMetrics(
                endPutTime - startPutTime,
                endGetTime - startGetTime
        );

        printStatistics(
                storage,
                builder
        );

        System.out.println("Let's make some probes from the Storage...");
        printSomeKeyValues(
                storage,
                0,
                10
        ); // oldest ones
        printSomeKeyValues(
                storage,
                TOTAL_NUMBER_OF_KEY_VALUES - 10,
                10
        ); // latest ones
    }

    private static long putAllKeyValues(final KeyValueStorage storage) {
        final AtomicBuffer keyBuffer = new UnsafeBuffer(new byte[128]);
        final AtomicBuffer valueBuffer = new UnsafeBuffer(new byte[512]);

        keyBuffer.putBytes(0, KEY_PREFIX_BYTES);
        valueBuffer.putBytes(0, VALUE_PREFIX_BYTES);

        long startPutTime = 0;
        for (int i = 0; i < TOTAL_NUMBER_OF_KEY_VALUES; i++) {
            if (i == WARMUP_NUMBER_OF_KEY_VALUES) {
                startPutTime = System.nanoTime();
            }

            final int keyLen = KEY_PREFIX_BYTES.length
                    + putIntAsAscii(keyBuffer, KEY_PREFIX_BYTES.length, i);
            final int valueLen = VALUE_PREFIX_BYTES.length
                    + putIntAsAscii(valueBuffer, VALUE_PREFIX_BYTES.length, i);

            storage.put(keyBuffer, 0, keyLen, valueBuffer, 0, valueLen);
        }
        return startPutTime;
    }

    private static long getAllKeyValues(final KeyValueStorage storage) {
        final AtomicBuffer keyBuffer = new UnsafeBuffer(new byte[128]);

        final ByteArrayValueConsumer valueConsumer = new ByteArrayValueConsumer();

        keyBuffer.putBytes(0, KEY_PREFIX_BYTES);

        long startGetTime = 0;
        for (int i = 0; i < TOTAL_NUMBER_OF_KEY_VALUES; i++) {
            if (i == WARMUP_NUMBER_OF_KEY_VALUES) {
                startGetTime = System.nanoTime();
            }

            final int keyLen = KEY_PREFIX_BYTES.length
                    + putIntAsAscii(keyBuffer, KEY_PREFIX_BYTES.length, i);

            if (!storage.get(keyBuffer, 0, keyLen, valueConsumer)) {
                throw new IllegalStateException("Key not found for index " + i);
            }
        }
        return startGetTime;
    }

    private static void printSomeKeyValues(final KeyValueStorage storage,
                                           final int firstIndex,
                                           final int size) {
        final AtomicBuffer keyBuffer = new UnsafeBuffer(new byte[128]);

        final ByteArrayValueConsumer valueConsumer = new ByteArrayValueConsumer();

        keyBuffer.putBytes(0, KEY_PREFIX_BYTES);

        for (int i = firstIndex; i < firstIndex + size; i++) {
            final int keyLen = KEY_PREFIX_BYTES.length
                    + putIntAsAscii(keyBuffer, KEY_PREFIX_BYTES.length, i);

            storage.get(keyBuffer, 0, keyLen, valueConsumer);

            System.out.println('[' + KEY_PREFIX + i
                    + "] = ["
                    + new String(
                    valueConsumer.array(),
                    0,
                    valueConsumer.valueSize()
            )
                    + ']');
        }
    }

    private static int putIntAsAscii(final AtomicBuffer buffer,
                                     final int offset,
                                     final int value) {
        if (value == 0) {
            buffer.putByte(offset, (byte) '0');
            return 1;
        }

        int temp = value;
        int digits = 0;
        while (temp > 0) {
            digits++;
            temp /= 10;
        }

        int pos = offset + digits - 1;
        int remainder = value;
        while (remainder > 0) {
            buffer.putByte(pos--, (byte) ('0' + (remainder % 10)));
            remainder /= 10;
        }

        return digits;
    }

    private static void printPerformanceMetrics(final long putTime,
                                                final long getTime) {
        System.out.println(PERFORMANCE_RESULT_TITLE);
        final double nanosPerSecond = 1_000_000_000.0;
        final double putsPerSecond = NUMBER_OF_MEASURED_KEY_VALUES / (putTime / nanosPerSecond);
        System.out.printf("%-2s%-5s: %10.4f per sec%n", " ", "PUTs", putsPerSecond);
        final double getsPerSecond = NUMBER_OF_MEASURED_KEY_VALUES / (getTime / nanosPerSecond);
        System.out.printf("%-2s%-5s: %10.4f per sec%n", " ", "GETs", getsPerSecond);
    }

    private static void printStatistics(
            final KeyValueStorage storage,
            final KeyValueStorage.Builder builder) {
        long totalKeyValuesStored = 0;

        long totalMetaMemoryAllocated = 0;
        long totalMetaMemoryUsed = 0;
        long totalMainMemoryAllocated = 0;
        long totalMainMemoryUsed = 0;
        long totalMmapMemoryAllocated = 0;
        long totalMmapMemoryUsed = 0;
        long totalMemoryAllocated = 0;
        long totalMemoryUsed = 0;

        final KeyValueRing ring = storage.ring();
        final int numberOfSegments = ring.numberOfSegments();

        System.out.printf(
                "%s%s%s%n",
                "-".repeat(7), "[ Key Value Storage Statistics ]", "-".repeat(7)
        );

        System.out.printf("%-5s: %2d%n", "Number of Segments", numberOfSegments);

        for (int i = 0; i < ring.numberOfSegments(); i++) {
            final KeyValueSegment segment = ring.getSegment(i);
            System.out.printf("%-2s%-5s: %2d tiers%n", " ", "Segment " + i, segment.size());
            for (int t = 0; t < segment.size(); t++) {
                final Tier tier = segment.getTier(t);
                final int size = tier.size();
                System.out.printf(
                        "%-4s%-16s: %10d key-values%n",
                        " ",
                        "Tier " + t + " ["
                                + (t > 0 ? "mmap" : (builder.useOffHeapMainMemory() ? "offh" : "heap"))
                                + ']',
                        size
                );

                final int metadataCapacity = tier.metadataCapacity();
                final int binaryCapacity = tier.binaryCapacity();
                final int binaryUsedSpace = tier.binaryUsedSpace();

                final String tierMemoryFormat = "%-10s%-10s: %10d bytes%n";
                System.out.printf(tierMemoryFormat, " ", "Allocated", binaryCapacity);
                System.out.printf(tierMemoryFormat, " ", "Used", binaryUsedSpace);

                totalKeyValuesStored += size;

                if (t == 0) {
                    totalMainMemoryAllocated += binaryCapacity;
                    totalMainMemoryUsed += binaryUsedSpace;
                } else {
                    totalMmapMemoryAllocated += binaryCapacity;
                    totalMmapMemoryUsed += binaryUsedSpace;
                }

                totalMetaMemoryAllocated += (long) metadataCapacity * SIZE_OF_LONG;
                totalMetaMemoryUsed += (long) size * SIZE_OF_LONG;

                totalMemoryAllocated += binaryCapacity;
                totalMemoryUsed += binaryUsedSpace;
            }
        }

        System.out.println(BR);

        System.out.printf("%-28s: %13d%n", "Total Key-Values", totalKeyValuesStored);
        final String totalMemoryValueFormat = "%-28s: %13s%n";
        System.out.printf(totalMemoryValueFormat, "Total Meta Memory Allocated",
                formatBytesToHumanReadable(totalMetaMemoryAllocated));
        System.out.printf(totalMemoryValueFormat, "Total Meta Memory Used",
                formatBytesToHumanReadable(totalMetaMemoryUsed));
        System.out.printf(totalMemoryValueFormat, "Total Main Memory Allocated",
                formatBytesToHumanReadable(totalMainMemoryAllocated));
        System.out.printf(totalMemoryValueFormat, "Total Main Memory Used",
                formatBytesToHumanReadable(totalMainMemoryUsed));
        System.out.printf(totalMemoryValueFormat, "Total Mmap Memory Allocated",
                formatBytesToHumanReadable(totalMmapMemoryAllocated));
        System.out.printf(totalMemoryValueFormat, "Total Mmap Memory Used",
                formatBytesToHumanReadable(totalMmapMemoryUsed));

        System.out.println(BR);

        System.out.printf(totalMemoryValueFormat, "Total Memory Allocated",
                formatBytesToHumanReadable(totalMemoryAllocated + totalMetaMemoryAllocated));
        System.out.printf(totalMemoryValueFormat, "Total Memory Used",
                formatBytesToHumanReadable(totalMemoryUsed + totalMetaMemoryUsed));

        System.out.println(BR);
    }
}
