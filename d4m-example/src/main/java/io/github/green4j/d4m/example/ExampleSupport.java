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

    protected ExampleSupport() {
    }
}
