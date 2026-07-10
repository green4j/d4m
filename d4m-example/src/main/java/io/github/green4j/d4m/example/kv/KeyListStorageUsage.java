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
import io.github.green4j.d4m.example.ExampleSupport;
import io.github.green4j.d4m.kv.ByteArrayValueConsumer;
import io.github.green4j.d4m.kv.KeyListStorage;
import io.github.green4j.d4m.kv.KeyListsWriter;
import io.github.green4j.d4m.kv.KeyValueStorage;
import io.github.green4j.d4m.kv.ListAccessor;

import static io.github.green4j.d4m.example.ExampleSupport.BR;
import static io.github.green4j.d4m.example.ExampleSupport.PERFORMANCE_RESULT_TITLE;

/**
 * Demonstrates bulk usage of {@link KeyListStorage} over a
 * {@link KeyValueStorage} backing store: appends 10 distinct values to each
 * of one million keys, reads them all back, and reports throughput, storage
 * statistics and a few probe lists.
 *
 * <p><b>Module flags required:</b>
 * {@code --add-opens java.base/jdk.internal.misc=ALL-UNNAMED}
 * {@code --add-opens java.base/java.nio=ALL-UNNAMED}
 */
public class KeyListStorageUsage {
    // Sizes are overridable so the example can run small (e.g. for the
    // README memory-consumption figures) without changing its defaults.
    private static final int TOTAL_NUMBER_OF_LISTS =
            ExampleSupport.getInt("d4m.klist.lists", 1_000_000);
    private static final int ENTRIES_PER_LIST =
            ExampleSupport.getInt("d4m.klist.entries", 10);
    private static final int WARMUP_NUMBER_OF_LISTS =
            ExampleSupport.getInt("d4m.klist.warmup", TOTAL_NUMBER_OF_LISTS / 10);

    private static final int NUMBER_OF_MEASURED_LISTS =
            TOTAL_NUMBER_OF_LISTS - WARMUP_NUMBER_OF_LISTS;
    private static final int NUMBER_OF_MEASURED_APPENDS =
            NUMBER_OF_MEASURED_LISTS * ENTRIES_PER_LIST;

    private static final int PROBE_COUNT = 5;

    private static final String KEY_PREFIX = "klist";
    private static final String VALUE_PREFIX = "v".repeat(200);

    private static final byte[] KEY_PREFIX_BYTES = KEY_PREFIX.getBytes();
    private static final byte[] VALUE_PREFIX_BYTES = VALUE_PREFIX.getBytes();
    private static final byte VALUE_SEPARATOR_UNDERSCORE = (byte) '_';
    private static final byte VALUE_SEPARATOR_E = (byte) 'e';

    public static void main(final String[] args) {
        final KeyValueStorage.Builder builder = KeyValueStorage.builder();

        final KeyValueStorage kvStorage = builder.build();

        final KeyListStorage lists = new KeyListStorage(kvStorage);
        final KeyListsWriter writer = lists.newWriter();

        final long startAppendTime = appendAllLists(writer);
        final long endAppendTime = System.nanoTime();

        final long startLoadTime = listAllLists(lists);
        final long endLoadTime = System.nanoTime();

        System.out.println(PERFORMANCE_RESULT_TITLE);
        ExampleSupport.printPerformanceMetric("APPENDs", NUMBER_OF_MEASURED_APPENDS,
                endAppendTime - startAppendTime);
        ExampleSupport.printPerformanceMetric("LOADs", NUMBER_OF_MEASURED_LISTS,
                endLoadTime - startLoadTime);

        KvExampleSupport.printKeyValueStorageStatistics(
                kvStorage,
                builder,
                "[ Key List Storage Statistics ]",
                () -> {
                    System.out.printf("%-28s: %13d%n", "Number of Key-Lists",
                            TOTAL_NUMBER_OF_LISTS);
                    System.out.printf("%-28s: %13d%n", "Entries per Key-List",
                            ENTRIES_PER_LIST);
                    System.out.printf("%-28s: %13d%n", "Total Values",
                            (long) TOTAL_NUMBER_OF_LISTS * ENTRIES_PER_LIST);
                    System.out.println(BR);
                });

        System.out.println("Let's make some probes from the Storage...");
        printSomeLists(lists, 0, PROBE_COUNT);                                  // oldest lists
        printSomeLists(lists, TOTAL_NUMBER_OF_LISTS - PROBE_COUNT, PROBE_COUNT); // newest lists
    }

    private static long appendAllLists(final KeyListsWriter writer) {
        final AtomicBuffer keyBuffer = new UnsafeBuffer(new byte[128]);
        final AtomicBuffer valueBuffer = new UnsafeBuffer(new byte[512]);

        keyBuffer.putBytes(0, KEY_PREFIX_BYTES);
        valueBuffer.putBytes(0, VALUE_PREFIX_BYTES);

        long startAppendTime = 0;
        for (int listIdx = 0; listIdx < TOTAL_NUMBER_OF_LISTS; listIdx++) {
            if (listIdx == WARMUP_NUMBER_OF_LISTS) {
                startAppendTime = System.nanoTime();
            }

            final int keyLen = KEY_PREFIX_BYTES.length
                    + ExampleSupport.putIntAsAscii(keyBuffer, KEY_PREFIX_BYTES.length, listIdx);

            final int valueBaseLen = VALUE_PREFIX_BYTES.length
                    + ExampleSupport.putIntAsAscii(valueBuffer, VALUE_PREFIX_BYTES.length, listIdx);
            valueBuffer.putByte(valueBaseLen, VALUE_SEPARATOR_UNDERSCORE);
            valueBuffer.putByte(valueBaseLen + 1, VALUE_SEPARATOR_E);
            final int valueEntryOffset = valueBaseLen + 2;

            for (int entryIdx = 0; entryIdx < ENTRIES_PER_LIST; entryIdx++) {
                final int valueLen = valueEntryOffset
                        + ExampleSupport.putIntAsAscii(valueBuffer, valueEntryOffset, entryIdx);
                writer.append(keyBuffer, 0, keyLen, valueBuffer, 0, valueLen);
            }
        }
        return startAppendTime;
    }

    private static long listAllLists(final KeyListStorage lists) {
        final AtomicBuffer keyBuffer = new UnsafeBuffer(new byte[128]);
        final ListAccessor accessor = new ListAccessor();
        final ByteArrayValueConsumer valueConsumer = new ByteArrayValueConsumer();

        keyBuffer.putBytes(0, KEY_PREFIX_BYTES);

        long startLoadTime = 0;
        for (int listIdx = 0; listIdx < TOTAL_NUMBER_OF_LISTS; listIdx++) {
            if (listIdx == WARMUP_NUMBER_OF_LISTS) {
                startLoadTime = System.nanoTime();
            }

            final int keyLen = KEY_PREFIX_BYTES.length
                    + ExampleSupport.putIntAsAscii(keyBuffer, KEY_PREFIX_BYTES.length, listIdx);

            if (!lists.list(accessor, keyBuffer, 0, keyLen)) {
                throw new IllegalStateException(
                        "List not found for index " + listIdx);
            }
            final int delivered = accessor.forEach(valueConsumer);
            if (delivered != ENTRIES_PER_LIST) {
                throw new IllegalStateException(
                        "Incomplete list at index " + listIdx
                                + ": expected " + ENTRIES_PER_LIST
                                + " got " + delivered);
            }
        }
        return startLoadTime;
    }

    private static void printSomeLists(final KeyListStorage lists,
                                       final int firstListIdx,
                                       final int count) {
        final AtomicBuffer keyBuffer = new UnsafeBuffer(new byte[128]);
        final ListAccessor accessor = new ListAccessor();
        final ByteArrayValueConsumer valueConsumer = new ByteArrayValueConsumer();

        keyBuffer.putBytes(0, KEY_PREFIX_BYTES);

        for (int listIdx = firstListIdx; listIdx < firstListIdx + count; listIdx++) {
            final int keyLen = KEY_PREFIX_BYTES.length
                    + ExampleSupport.putIntAsAscii(keyBuffer, KEY_PREFIX_BYTES.length, listIdx);

            lists.list(accessor, keyBuffer, 0, keyLen);
            System.out.println('[' + KEY_PREFIX + listIdx + "] size=" + accessor.size());

            for (int entryIdx = 0; entryIdx < accessor.size(); entryIdx++) {
                accessor.get(entryIdx, valueConsumer);
                System.out.println(
                        "  [" + entryIdx + "] = ["
                                + new String(
                                        valueConsumer.array(),
                                        0,
                                        valueConsumer.valueSize())
                                + ']'
                );
            }
        }
    }
}
