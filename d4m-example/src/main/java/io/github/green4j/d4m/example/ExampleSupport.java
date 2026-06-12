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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package io.github.green4j.d4m.example;

import io.github.green4j.d4m.common.AtomicBuffer;
import io.github.green4j.d4m.kv.KeyValueSegment;
import io.github.green4j.d4m.kv.KeyValueSegments;
import io.github.green4j.d4m.kv.KeyValueStorage;
import io.github.green4j.d4m.kv.Tier;

import static io.github.green4j.d4m.common.BitSupport.SIZE_OF_LONG;

public abstract class ExampleSupport {
    public static final long KILOBYTE = 1024L;
    public static final long MEGABYTE = KILOBYTE * 1024L;
    public static final long GIGABYTE = MEGABYTE * 1024L;
    public static final long TERABYTE = GIGABYTE * 1024L;
    public static final String BR = "-".repeat(45);

    public static final String PERFORMANCE_RESULT_TITLE = String.format(
            "%s%s%s",
            "-".repeat(11), "[ Performance Results ]",
            "-".repeat(11)
    );

    private static final double NANOS_PER_SECOND = 1_000_000_000.0;

    public static String formatBytesToHumanReadable(final long bytes) {
        if (bytes < KILOBYTE) {
            return String.format("%d%s", bytes, "B");
        }

        final double value;
        final String unit;

        if (bytes >= TERABYTE) {
            value = (double) bytes / TERABYTE;
            unit = "T";
        } else if (bytes >= GIGABYTE) {
            value = (double) bytes / GIGABYTE;
            unit = "G";
        } else if (bytes >= MEGABYTE) {
            value = (double) bytes / MEGABYTE;
            unit = "M";
        } else {
            value = (double) bytes / KILOBYTE;
            unit = "K";
        }

        return String.format("%.2f%s", value, unit);
    }

    /**
     * Writes the given non-negative {@code int} as ASCII digits into the
     * buffer starting at {@code offset}, and returns the number of bytes
     * written. Used by examples that build per-iteration keys/values without
     * allocating.
     *
     * @param buffer the target buffer
     * @param offset the byte offset to write at
     * @param value  the non-negative value to encode
     * @return the number of bytes written
     */
    public static int putIntAsAscii(final AtomicBuffer buffer,
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

    /**
     * Prints one row of throughput metrics ({@code label : ops per sec}).
     * Caller is responsible for printing the surrounding title and any
     * additional rows.
     *
     * @param label        the metric name (e.g. {@code "PUTs"})
     * @param count        the number of measured operations
     * @param elapsedNanos the elapsed time, in nanoseconds, over which they
     *                     ran
     */
    public static void printPerformanceMetric(final String label,
                                              final long count,
                                              final long elapsedNanos) {
        final double opsPerSecond = count / (elapsedNanos / NANOS_PER_SECOND);
        System.out.printf("%-2s%-8s: %12.4f per sec%n", " ", label, opsPerSecond);
    }

    /**
     * Prints a tabular dump of the {@link KeyValueStorage}'s underlying ring
     * (segments, tiers, allocated/used bytes), then a totals block. Used by
     * examples to make the storage layout visible after a workload run.
     *
     * @param storage          the storage to walk
     * @param builder          the builder used to construct {@code storage}
     *                         (read for the off-heap flag in the tier label)
     * @param title            the title string placed between {@code "-".repeat(7)}
     *                         dashes at the top of the section
     * @param extraHeaderRows  optional callback invoked after the title and
     *                         before the segment dump; {@code null} for none
     */
    public static void printKeyValueStorageStatistics(
            final KeyValueStorage storage,
            final KeyValueStorage.Builder builder,
            final String title,
            final Runnable extraHeaderRows) {
        long totalKeyValuesStored = 0;

        long totalMetaMemoryAllocated = 0;
        long totalMetaMemoryUsed = 0;
        long totalMainMemoryAllocated = 0;
        long totalMainMemoryUsed = 0;
        long totalMmapMemoryAllocated = 0;
        long totalMmapMemoryUsed = 0;
        long totalMemoryAllocated = 0;
        long totalMemoryUsed = 0;

        final KeyValueSegments ring = storage.ring();
        final int numberOfSegments = ring.numberOfSegments();

        System.out.printf("%s%s%s%n", "-".repeat(7), title, "-".repeat(7));

        if (extraHeaderRows != null) {
            extraHeaderRows.run();
        }

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

    protected ExampleSupport() {
    }
}
