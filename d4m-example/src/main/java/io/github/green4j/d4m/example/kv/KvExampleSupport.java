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
package io.github.green4j.d4m.example.kv;

import io.github.green4j.d4m.example.ExampleSupport;
import io.github.green4j.d4m.kv.KeyValueSegment;
import io.github.green4j.d4m.kv.KeyValueSegments;
import io.github.green4j.d4m.kv.KeyValueStorage;
import io.github.green4j.d4m.kv.Tier;

import static io.github.green4j.d4m.common.BitSupport.SIZE_OF_LONG;

public abstract class KvExampleSupport extends ExampleSupport {

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

    protected KvExampleSupport() {
    }
}
