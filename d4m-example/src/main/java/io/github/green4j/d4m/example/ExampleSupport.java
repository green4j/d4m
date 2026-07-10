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
     * Reads a non-negative {@code int} configuration value from the system
     * property {@code prop}, returning {@code def} if the property is unset,
     * blank, or not a valid non-negative integer. Lets examples run at a
     * smaller scale (e.g. for documentation) without changing their default
     * behaviour.
     *
     * @param prop the system property name
     * @param def  the default value when the property is absent or invalid
     * @return the parsed value, or {@code def}
     */
    public static int getInt(final String prop, final int def) {
        final String raw = System.getProperty(prop);
        if (raw == null || raw.trim().isEmpty()) {
            return def;
        }
        try {
            final int value = Integer.parseInt(raw.trim());
            return value >= 0 ? value : def;
        } catch (final NumberFormatException e) {
            return def;
        }
    }

    /**
     * Reads a non-negative {@code long} configuration value from the system
     * property {@code prop}, returning {@code def} if the property is unset,
     * blank, or not a valid non-negative long.
     *
     * @param prop the system property name
     * @param def  the default value when the property is absent or invalid
     * @return the parsed value, or {@code def}
     */
    public static long getLong(final String prop, final long def) {
        final String raw = System.getProperty(prop);
        if (raw == null || raw.trim().isEmpty()) {
            return def;
        }
        try {
            final long value = Long.parseLong(raw.trim());
            return value >= 0 ? value : def;
        } catch (final NumberFormatException e) {
            return def;
        }
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

    protected ExampleSupport() {
    }
}
