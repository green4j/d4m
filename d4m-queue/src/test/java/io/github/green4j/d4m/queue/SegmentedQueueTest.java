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
package io.github.green4j.d4m.queue;

import io.github.green4j.d4m.common.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Single-threaded tests of {@link SegmentedQueue} driven by {@link SegmentManager#doWork()}.
 */
class SegmentedQueueTest {
    private static final int SEGMENT = 4096;
    private static final int PAYLOAD = 100;

    @TempDir
    Path dir;

    private final UnsafeBuffer message = new UnsafeBuffer(new byte[SEGMENT]);

    private static SegmentManager.Builder manager() {
        return SegmentManager.builder().withSegmentSize(SEGMENT).withMaxMemoryBytes(64L * SEGMENT);
    }

    private long offer(final SegmentManager manager,
                       final SegmentedQueue queue,
                       final long value) throws IOException {
        message.putLong(0, value);
        message.putLong(PAYLOAD - 8, ~value);
        for (int i = 0; i < 100; i++) {
            final long seq = queue.writer().offer(message, 0, PAYLOAD);
            if (seq != QueueWriter.REJECTED) {
                return seq;
            }
            manager.doWork();
        }
        throw new AssertionError("Offer rejected");
    }

    private static List<Long> drain(final SegmentManager manager,
                                    final SegmentedQueue queue,
                                    final int expected) throws IOException {
        final List<Long> values = new ArrayList<>();
        final MessageHandler handler = (seq, buffer, offset, length) -> {
            assertEquals(PAYLOAD, length);
            final long value = buffer.getLong(offset);
            assertEquals(~value, buffer.getLong(offset + PAYLOAD - 8));
            assertEquals(value, seq, "The value does not match its seq");
            values.add(value);
            return true;
        };
        for (int idle = 0; values.size() < expected && idle < 1000; idle++) {
            final int limit = Math.min(64, expected - values.size());
            if (queue.reader().poll(handler, limit) + manager.doWork() > 0) {
                idle = 0;
            }
        }
        return values;
    }

    private static void assertSequence(final List<Long> values,
                                       final long from,
                                       final int count) {
        assertEquals(count, values.size());
        for (int i = 0; i < count; i++) {
            assertEquals(from + i, values.get(i));
        }
    }

    private long fileCount() throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(p -> p.toString().endsWith(SegmentManager.FILE_SUFFIX)).count();
        }
    }

    private static final class RecordingListener implements SegmentManager.Listener {
        final List<String> events = new ArrayList<>();
        long rejected;

        long count(final String prefix) {
            return events.stream().filter(e -> e.startsWith(prefix)).count();
        }

        @Override
        public void onSegmentRecovered(final SegmentManager notifier,
                                       final SegmentedQueue queue,
                                       final long baseSeq,
                                       final long lastSeq) {
            events.add("recovered " + baseSeq + ".." + lastSeq);
        }

        @Override
        public void onSegmentTruncated(final SegmentManager notifier,
                                       final SegmentedQueue queue,
                                       final long baseSeq,
                                       final long lastSeq) {
            events.add("truncated " + baseSeq + ".." + lastSeq);
        }

        @Override
        public void onSegmentPersisted(final SegmentManager notifier,
                                       final SegmentedQueue queue,
                                       final long baseSeq,
                                       final long lastSeq) {
            events.add("persisted " + baseSeq + ".." + lastSeq);
        }

        @Override
        public void onSegmentEvicted(final SegmentManager notifier,
                                     final SegmentedQueue queue,
                                     final long baseSeq) {
            events.add("evicted " + baseSeq);
        }

        @Override
        public void onSegmentLoaded(final SegmentManager notifier,
                                    final SegmentedQueue queue,
                                    final long baseSeq,
                                    final long lastSeq) {
            events.add("loaded " + baseSeq + ".." + lastSeq);
        }

        @Override
        public void onSegmentDeleted(final SegmentManager notifier,
                                     final SegmentedQueue queue,
                                     final long baseSeq) {
            events.add("deleted " + baseSeq);
        }

        @Override
        public void onOffersRejected(final SegmentManager notifier,
                                     final SegmentedQueue queue,
                                     final long count) {
            rejected += count;
        }
    }

    @Test
    void listenerReportsOverflowLifecycle() throws IOException {
        final RecordingListener listener = new RecordingListener();
        try (SegmentManager manager = manager().withMaxMemoryBytes(5L * SEGMENT).withListener(listener).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            message.putLong(0, 0);
            assertEquals(QueueWriter.REJECTED, queue.writer().offer(message, 0, PAYLOAD));
            manager.doWork();
            assertEquals(1, listener.rejected);
            assertEquals(1, queue.rejectedOffers());

            for (int i = 0; i < 600; i++) {
                offer(manager, queue, i);
            }
            assertSequence(drain(manager, queue, 600), 0, 600);
            manager.doWork();

            final long evicted = listener.count("evicted");
            assertTrue(evicted > 0);
            assertEquals(evicted, listener.count("persisted"));
            assertEquals(evicted, listener.count("loaded"));
            assertEquals(evicted, listener.count("deleted"));
        }
    }

    @Test
    void listenerReportsRecoveryAndTruncation() throws IOException {
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            for (int i = 0; i < 10; i++) {
                offer(manager, queue, i);
            }
        }
        final byte[] garbage = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17};
        Files.write(dir.resolve(SegmentManager.fileName(0)), garbage, StandardOpenOption.APPEND);
        final RecordingListener listener = new RecordingListener();
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).withListener(listener).build()) {
            manager.open(dir, ReadMode.TAILING);

            assertEquals(List.of("recovered 0..9", "truncated 0..9"), listener.events);
        }
    }

    @Test
    void sequenceContinuesAfterCleanCloseOfFullyConsumedQueue() throws IOException {
        for (final DiskMode mode : DiskMode.values()) {
            final Path queueDir = dir.resolve(mode.name());
            try (SegmentManager manager = manager().withDiskMode(mode).build()) {
                final SegmentedQueue queue = manager.open(queueDir, ReadMode.TAILING);
                for (int i = 0; i < 100; i++) {
                    offer(manager, queue, i);
                }
                assertSequence(drain(manager, queue, 100), 0, 100);
            }
            try (SegmentManager manager = manager().withDiskMode(mode).build()) {
                final SegmentedQueue queue = manager.open(queueDir, ReadMode.TAILING);
                assertEquals(99, queue.writerSeq(), mode.name());
                queue.reader().seek(100);
                assertEquals(100, offer(manager, queue, 100));
                assertSequence(drain(manager, queue, 1), 100, 1);
            }
        }
    }

    @Test
    void tornTailIsRepairedOnRecovery() throws IOException {
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            for (int i = 0; i < 10; i++) {
                offer(manager, queue, i);
            }
        }
        final byte[] garbage = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17};
        Files.write(dir.resolve(SegmentManager.fileName(0)), garbage, StandardOpenOption.APPEND);
        final RecordingListener first = new RecordingListener();
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).withListener(first).build()) {
            manager.open(dir, ReadMode.TAILING);
        }
        final RecordingListener second = new RecordingListener();
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).withListener(second).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            assertEquals(1, first.count("truncated"));
            assertEquals(0, second.count("truncated"));
            assertSequence(drain(manager, queue, 10), 0, 10);
        }
    }

    @Test
    void hugeRetainSegmentsDoNotFitThePool() {
        assertThrows(IllegalArgumentException.class,
                () -> manager().withRetainSegments(Integer.MAX_VALUE).build());
    }

    @Test
    void failedTailRepairDoesNotLeakMemory() throws IOException {
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            for (int i = 0; i < 10; i++) {
                offer(manager, queue, i);
            }
        }
        final Path file = dir.resolve(SegmentManager.fileName(0));
        Files.write(file, new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9}, StandardOpenOption.APPEND);
        assertTrue(file.toFile().setWritable(false), "The segment file could not be made read-only");
        try {
            assumeFalse(Files.isWritable(file), "Read-only files are writable for this user");
            try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).build()) {
                assertThrows(IOException.class, () -> manager.open(dir, ReadMode.TAILING));
                assertEquals(0, manager.usedMemoryBytes(), "The failed repair kept its buffer");
            }
        } finally {
            assertTrue(file.toFile().setWritable(true), "The segment file could not be made writable again");
        }
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            assertSequence(drain(manager, queue, 10), 0, 10);
        }
    }

    @Test
    void invalidFrameOfTheLastSixteenBytesIsRepaired() throws IOException {
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            for (int i = 0; i < 10; i++) {
                offer(manager, queue, i);
            }
        }
        // a frame header with a wrong seq, so the END frame takes exactly its place
        Files.write(dir.resolve(SegmentManager.fileName(0)), new byte[16], StandardOpenOption.APPEND);
        final RecordingListener first = new RecordingListener();
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).withListener(first).build()) {
            manager.open(dir, ReadMode.TAILING);
        }
        final RecordingListener second = new RecordingListener();
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).withListener(second).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);

            assertEquals(1, first.count("truncated"));
            assertEquals(0, second.count("truncated"), "The invalid frame was met again after the repair");
            assertSequence(drain(manager, queue, 10), 0, 10);
        }
    }

    @Test
    void exhaustedSequenceNumbersAreRefused() throws IOException {
        Files.createFile(dir.resolve(SegmentManager.fileName(Long.MAX_VALUE >> 1)));
        try (SegmentManager manager = manager().build()) {
            assertThrows(IOException.class, () -> manager.open(dir, ReadMode.TAILING));
        }
    }

    @Test
    void closeReportsTheNewestSegmentAsPersisted() throws IOException {
        final RecordingListener listener = new RecordingListener();
        try (SegmentManager manager = manager().withListener(listener).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            for (int i = 0; i < 10; i++) {
                offer(manager, queue, i);
            }
            assertEquals(0, listener.count("persisted"));

            queue.close();

            assertEquals(List.of("persisted 0..9"), listener.events);
        }
    }

    @Test
    void journalWithFsyncIsReplayedAfterReopen() throws IOException {
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).withFsync(true).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.PERSISTED_ONLY);
            for (int i = 0; i < 300; i++) {
                offer(manager, queue, i);
            }
            assertSequence(drain(manager, queue, 100), 0, 100);
        }
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).withFsync(true).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.PERSISTED_ONLY);
            queue.reader().seek(100);

            assertSequence(drain(manager, queue, 200), 100, 200);
        }
    }

    @Test
    void claimedMessagesAreEncodedInPlace() throws IOException {
        try (SegmentManager manager = manager().withMaxMemoryBytes(5L * SEGMENT).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            final QueueWriter writer = queue.writer();
            final int count = 600; // several segments, buffers are reused
            for (int i = 0; i < count; i++) {
                int at = writer.claim(writer.maxMessageLength());
                while (at == (int) QueueWriter.REJECTED) {
                    manager.doWork();
                    at = writer.claim(writer.maxMessageLength());
                }
                final int length = i % 3 == 0 ? PAYLOAD : 24; // commit less than claimed
                assertEquals(0, (writer.claimBuffer().addressOffset() + at) & 7, "The payload is not 8-byte aligned");
                writer.claimBuffer().putLong(at, i);
                writer.claimBuffer().putLong(at + length - 8, ~(long) i);
                assertEquals(i, writer.commit(length));
            }
            final List<Long> values = new ArrayList<>();
            final MessageHandler handler = (seq, buffer, offset, length) -> {
                assertEquals(seq % 3 == 0 ? PAYLOAD : 24, length);
                assertEquals(0, (buffer.addressOffset() + offset) & 7, "The payload is not 8-byte aligned");
                assertEquals(seq, buffer.getLong(offset));
                assertEquals(~seq, buffer.getLong(offset + length - 8));
                values.add(seq);
                return true;
            };
            for (int idle = 0; values.size() < count && idle < 1000; idle++) {
                if (queue.reader().poll(handler, 64) + manager.doWork() > 0) {
                    idle = 0;
                }
            }
            assertSequence(values, 0, count);
        }
    }

    @Test
    void abortedClaimPublishesNothing() throws IOException {
        try (SegmentManager manager = manager().build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            manager.doWork();
            final QueueWriter writer = queue.writer();
            final int at = writer.claim(PAYLOAD);
            writer.claimBuffer().putLong(at, 0xBAD);
            assertThrows(IllegalStateException.class, () -> writer.offer(message, 0, 8));
            assertThrows(IllegalStateException.class, () -> writer.claim(8));
            assertThrows(IllegalArgumentException.class, () -> writer.commit(PAYLOAD + 1));
            writer.abort();
            assertThrows(IllegalStateException.class, () -> writer.commit(0));

            assertEquals(0, queue.reader().poll((seq, b, o, l) -> true, 10));
            assertEquals(0, offer(manager, queue, 0));
            assertSequence(drain(manager, queue, 1), 0, 1);
        }
    }

    @Test
    void garbageAfterEndIsReportedOnLoad() throws IOException {
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            for (int i = 0; i < 100; i++) {
                offer(manager, queue, i);
            }
        }
        final byte[] garbage = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17};
        Files.write(dir.resolve(SegmentManager.fileName(0)), garbage, StandardOpenOption.APPEND);
        final RecordingListener listener = new RecordingListener();
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).withListener(listener).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);

            assertSequence(drain(manager, queue, 100), 0, 100);
            assertEquals(1, listener.count("truncated 0.."));
        }
    }

    @Test
    void failedLoadDoesNotLeakMemory() throws IOException {
        try (SegmentManager manager = manager().withMaxMemoryBytes(5L * SEGMENT).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            for (int i = 0; i < 600; i++) {
                offer(manager, queue, i);
            }
            final Path oldest;
            try (Stream<Path> files = Files.list(dir)) {
                oldest = files.sorted().findFirst().orElseThrow();
            }
            Files.delete(oldest);

            int failures = 0;
            for (int i = 0; i < 200; i++) {
                queue.reader().poll((seq, b, o, l) -> true, 64);
                try {
                    manager.doWork();
                } catch (final IOException e) {
                    failures++;
                }
            }
            assertTrue(failures > 10, "Loading the deleted segment stopped failing");

            // with leaked buffers the writer would never get an empty segment again
            for (int i = 600; i < 1200; i++) {
                message.putLong(0, i);
                long seq = QueueWriter.REJECTED;
                for (int attempt = 0; attempt < 100 && seq == QueueWriter.REJECTED; attempt++) {
                    seq = queue.writer().offer(message, 0, PAYLOAD);
                    if (seq == QueueWriter.REJECTED) {
                        try {
                            manager.doWork();
                        } catch (final IOException e) {
                            failures++;
                        }
                    }
                }
                assertEquals(i, seq);
            }
        }
    }

    @Test
    void pollRejectsNonPositiveLimit() throws IOException {
        try (SegmentManager manager = manager().build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);

            assertThrows(IllegalArgumentException.class, () -> queue.reader().poll((seq, b, o, l) -> true, 0));
        }
    }

    @Test
    void writerIsRejectedUntilManagerPreparesSegment() throws IOException {
        try (SegmentManager manager = manager().build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            message.putLong(0, 0);

            assertEquals(QueueWriter.REJECTED, queue.writer().offer(message, 0, PAYLOAD));
            manager.doWork();
            assertEquals(0, queue.writer().offer(message, 0, PAYLOAD));
            assertEquals(0, queue.writerSeq());
        }
    }

    @Test
    void deliversInOrderAcrossSegments() throws IOException {
        try (SegmentManager manager = manager().build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            final int count = 1000; // about 30 segments
            final List<Long> values = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                assertEquals(i, offer(manager, queue, i));
                if (i % 7 == 6) {
                    values.addAll(drain(manager, queue, 5));
                }
            }
            values.addAll(drain(manager, queue, count - values.size()));

            assertSequence(values, 0, count);
            assertEquals(count - 1, queue.reader().position());
            assertEquals(count - 1, queue.readerSeq());
            assertEquals(0, fileCount(), "OVERFLOW with enough memory wrote to the disk");
        }
    }

    @Test
    void rejectsInvalidLengthAndClosedWriter() throws IOException {
        try (SegmentManager manager = manager().build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            final QueueWriter writer = queue.writer();
            manager.doWork();

            assertEquals(SEGMENT - 64 - 32, writer.maxMessageLength());
            assertThrows(IllegalArgumentException.class,
                    () -> writer.offer(message, 0, writer.maxMessageLength() + 1));
            assertEquals(0, writer.offer(message, 0, writer.maxMessageLength()));
            writer.close();
            assertThrows(IllegalStateException.class, () -> writer.offer(message, 0, 1));
        }
    }

    @Test
    void refusedMessageIsDeliveredAgain() throws IOException {
        try (SegmentManager manager = manager().build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            offer(manager, queue, 0);
            offer(manager, queue, 1);
            final List<Long> seen = new ArrayList<>();

            assertEquals(0, queue.reader().poll((seq, b, o, l) -> {
                seen.add(seq);
                return false;
            }, 10));
            assertEquals(-1, queue.reader().position());
            assertEquals(2, queue.reader().poll((seq, b, o, l) -> seen.add(seq), 10));
            assertEquals(List.of(0L, 0L, 1L), seen);
        }
    }

    @Test
    void overflowsToDiskAndReadsBack() throws IOException {
        try (SegmentManager manager = manager().withMaxMemoryBytes(5L * SEGMENT).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            final int count = 600; // about 18 segments, only 5 fit into memory
            for (int i = 0; i < count; i++) {
                offer(manager, queue, i);
            }
            assertTrue(fileCount() > 0, "No segment overflowed to disk");

            assertSequence(drain(manager, queue, count), 0, count);
            manager.doWork();
            assertEquals(0, fileCount(), "Consumed segments were not deleted");
        }
    }

    @Test
    void persistedOnlyReaderWaitsForDisk() throws IOException {
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.PERSISTED_ONLY);
            manager.doWork();
            message.putLong(0, 0);
            message.putLong(PAYLOAD - 8, ~0L);
            assertEquals(0, queue.writer().offer(message, 0, PAYLOAD));

            assertEquals(0, queue.reader().poll((seq, b, o, l) -> true, 10));
            assertEquals(-1, queue.persistedSeq());
            manager.doWork();
            assertEquals(0, queue.persistedSeq());
            assertEquals(1, queue.reader().poll((seq, b, o, l) -> true, 10));
        }
    }

    @Test
    void persistedOnlyRequiresJournal() {
        final SegmentManager manager = manager().build();

        assertThrows(IllegalArgumentException.class, () -> manager.open(dir, ReadMode.PERSISTED_ONLY));
    }

    @Test
    void journalIsReplayedAfterReopen() throws IOException {
        final int count = 300;
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            for (int i = 0; i < count; i++) {
                offer(manager, queue, i);
            }
            manager.doWork();
            assertEquals(count - 1, queue.persistedSeq());
        }
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            assertEquals(count - 1, queue.writerSeq());
            assertEquals(count, offer(manager, queue, count));

            assertSequence(drain(manager, queue, count + 1), 0, count + 1);
        }
    }

    @Test
    void unconsumedMessagesSurviveCleanCloseInOverflowMode() throws IOException {
        try (SegmentManager manager = manager().build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            for (int i = 0; i < 200; i++) {
                offer(manager, queue, i);
            }
            assertSequence(drain(manager, queue, 50), 0, 50);
        }
        try (SegmentManager manager = manager().build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            assertEquals(199, queue.writerSeq());
            queue.reader().seek(50);

            assertSequence(drain(manager, queue, 150), 50, 150);
        }
    }

    @Test
    void seekSkipsDeliveredMessagesAfterRestart() throws IOException {
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            for (int i = 0; i < 300; i++) {
                offer(manager, queue, i);
            }
        }
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            assertEquals(201, queue.reader().seek(201));
            assertEquals(201, queue.reader().seek(10), "seek moved back");

            assertSequence(drain(manager, queue, 99), 201, 99);
        }
    }

    @Test
    void tornTailIsCutAtLastValidMessage() throws IOException {
        final int count = 100;
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).withDeleteConsumed(false).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            for (int i = 0; i < count; i++) {
                offer(manager, queue, i);
            }
        }
        final Path last;
        try (Stream<Path> files = Files.list(dir)) {
            last = files.sorted().reduce((a, b) -> b).orElseThrow();
        }
        final long lastBase = Long.parseLong(last.getFileName().toString().substring(0, 20));
        final long size = Files.size(last);
        final int frame = 16 + 104; // header + aligned payload
        for (long cut = size; cut >= 64; cut -= 13) {
            try (FileChannel channel = FileChannel.open(last, StandardOpenOption.WRITE)) {
                channel.truncate(cut);
            }
            final long expectedInLast = Math.max(0, (cut - 64) / frame);
            final long expectedTotal = lastBase + expectedInLast;
            try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).withDeleteConsumed(false).build()) {
                final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
                assertEquals(expectedTotal - 1, queue.writerSeq(), "Wrong writerSeq after a cut at " + cut);
                assertSequence(drain(manager, queue, (int) expectedTotal), 0, (int) expectedTotal);
                queue.close();
            }
            if (!Files.exists(last)) {
                break;
            }
        }
    }

    @Test
    void garbageTailIsIgnored() throws IOException {
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            for (int i = 0; i < 10; i++) {
                offer(manager, queue, i);
            }
        }
        final Path file = dir.resolve(SegmentManager.fileName(0));
        final byte[] garbage = new byte[300];
        for (int i = 0; i < garbage.length; i++) {
            garbage[i] = (byte) (i * 31 + 7);
        }
        Files.write(file, garbage, StandardOpenOption.APPEND);

        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            assertEquals(9, queue.writerSeq());
            assertSequence(drain(manager, queue, 10), 0, 10);
        }
    }

    @Test
    void oversizedFrameLengthEndsTheSegment() throws IOException {
        final int frame = 16 + 104; // header + aligned payload
        for (final int corrupt : new int[] {Integer.MAX_VALUE - 6, Integer.MAX_VALUE - 15, SEGMENT}) {
            final Path queueDir = dir.resolve(Integer.toString(corrupt));
            try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).build()) {
                final SegmentedQueue queue = manager.open(queueDir, ReadMode.TAILING);
                for (int i = 0; i < 10; i++) {
                    offer(manager, queue, i);
                }
            }
            final ByteBuffer length = ByteBuffer.allocate(4).order(ByteOrder.nativeOrder()).putInt(0, corrupt);
            try (FileChannel channel = FileChannel.open(queueDir.resolve(SegmentManager.fileName(0)),
                    StandardOpenOption.WRITE)) {
                channel.write(length, 64 + 2 * frame);
            }
            final RecordingListener listener = new RecordingListener();
            try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).withListener(listener).build()) {
                final SegmentedQueue queue = manager.open(queueDir, ReadMode.TAILING);

                assertEquals(1, queue.writerSeq(), "Wrong writerSeq after a frame length of " + corrupt);
                assertEquals(1, listener.count("truncated 0..1"));
                assertSequence(drain(manager, queue, 2), 0, 2);
            }
        }
    }

    @Test
    void failedCloseKeepsTheQueueOpenForRetry() throws IOException {
        final Path queueDir = dir.resolve("q");
        try (SegmentManager manager = manager().build()) {
            final SegmentedQueue queue = manager.open(queueDir, ReadMode.TAILING);
            for (int i = 0; i < 10; i++) {
                offer(manager, queue, i);
            }
            Files.delete(queueDir);

            assertThrows(IOException.class, queue::close);
            Files.createDirectories(queueDir);
            queue.close();
            assertEquals(0, manager.usedMemoryBytes());
        }
        try (SegmentManager manager = manager().build()) {
            final SegmentedQueue queue = manager.open(queueDir, ReadMode.TAILING);

            assertEquals(9, queue.writerSeq());
            assertSequence(drain(manager, queue, 10), 0, 10);
        }
    }

    @Test
    void queueClosesItselfOnceInTryWithResources() throws IOException {
        try (SegmentManager manager = manager().build()) {
            final SegmentedQueue closed;
            try (SegmentedQueue queue = manager.open(dir, ReadMode.TAILING)) {
                assertEquals(manager, queue.manager());
                for (int i = 0; i < 10; i++) {
                    offer(manager, queue, i);
                }
                queue.close(); // the try block closes it again
                closed = queue;
            }

            assertEquals(0, manager.usedMemoryBytes());
            assertThrows(IllegalArgumentException.class,
                    () -> manager.segments(closed, (baseSeq, lastSeq, resident, onDisk) -> { }));
            assertEquals(9, manager.open(dir, ReadMode.TAILING).writerSeq());
        }
    }

    @Test
    void managerClosesTheQueuesLeftOpen() throws IOException {
        final SegmentedQueue queue;
        try (SegmentManager manager = manager().build()) {
            queue = manager.open(dir, ReadMode.TAILING);
            for (int i = 0; i < 10; i++) {
                offer(manager, queue, i);
            }
        }
        queue.close(); // closed by the manager already
        try (SegmentManager manager = manager().build()) {
            assertEquals(9, manager.open(dir, ReadMode.TAILING).writerSeq());
        }
    }

    @Test
    void fileNameBeyondLongRangeIsIgnored() throws IOException {
        Files.createFile(dir.resolve("99999999999999999999" + SegmentManager.FILE_SUFFIX));
        try (SegmentManager manager = manager().build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);

            assertEquals(-1, queue.writerSeq());
        }
    }

    @Test
    void retainedSegmentsAreKeptBehindReader() throws IOException {
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).withRetainSegments(1).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            for (int i = 0; i < 300; i++) {
                offer(manager, queue, i);
            }
            assertSequence(drain(manager, queue, 300), 0, 300);
            manager.doWork();
            final List<Long> bases = new ArrayList<>();
            manager.segments(queue, (baseSeq, lastSeq, resident, onDisk) -> bases.add(baseSeq));

            assertEquals(2, bases.size(), "Other segments than the reader's one and one retained are kept");
            assertEquals(2, fileCount());
        }
    }

    @Test
    void foreignSegmentSizeIsRejected() throws IOException {
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            offer(manager, queue, 0);
        }
        try (SegmentManager other = SegmentManager.builder()
                .withSegmentSize(2 * SEGMENT)
                .withMaxMemoryBytes(8L * 2 * SEGMENT)
                .build()) {
            assertThrows(IOException.class, () -> other.open(dir, ReadMode.TAILING));
        }
    }

    @Test
    void severalQueuesShareOneManager() throws IOException {
        try (SegmentManager manager = manager().withMaxMemoryBytes(10L * SEGMENT).build()) {
            final SegmentedQueue a = manager.open(dir.resolve("a"), ReadMode.TAILING);
            final SegmentedQueue b = manager.open(dir.resolve("b"), ReadMode.TAILING);
            assertThrows(IllegalStateException.class, () -> manager.open(dir.resolve("a"), ReadMode.TAILING));
            for (int i = 0; i < 400; i++) {
                offer(manager, a, i);
                offer(manager, b, i);
            }
            assertSequence(drain(manager, a, 400), 0, 400);
            assertSequence(drain(manager, b, 400), 0, 400);
            try (Stream<Path> files = Files.list(dir.resolve("a"))) {
                assertFalse(files.findAny().isPresent());
            }
        }
    }

    @Test
    void memoryIsAllocatedUpFrontAndAccounted() throws IOException {
        assertThrows(IllegalArgumentException.class, () -> manager().withMaxMemoryBytes(Long.MAX_VALUE).build());
        try (SegmentManager manager = manager().withMaxMemoryBytes(10L * SEGMENT + 100).build()) {
            assertEquals(10L * SEGMENT, manager.maxMemoryBytes(), "The pool is not rounded down to whole segments");
            assertEquals(0, manager.usedMemoryBytes());

            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            manager.doWork();
            assertEquals(SEGMENT, manager.usedMemoryBytes(),
                    "Not exactly one spare segment is prepared for the writer");

            for (int i = 0; i < 400; i++) {
                offer(manager, queue, i);
            }
            assertEquals(manager.maxMemoryBytes(), manager.usedMemoryBytes(), "The backlog did not fill the pool");

            assertSequence(drain(manager, queue, 400), 0, 400);
            manager.doWork();
            assertTrue(manager.usedMemoryBytes() < manager.maxMemoryBytes());

            queue.close();
            assertEquals(0, manager.usedMemoryBytes());
        }
    }

    @Test
    void queueIsRecoveredWhileAnotherQueueHoldsAllMemory() throws IOException {
        try (SegmentManager manager = manager().build()) {
            final SegmentedQueue b = manager.open(dir.resolve("b"), ReadMode.TAILING);
            for (int i = 0; i < 100; i++) {
                offer(manager, b, i);
            }
        }
        try (SegmentManager manager = manager().withMaxMemoryBytes(8L * SEGMENT).build()) {
            final SegmentedQueue a = manager.open(dir.resolve("a"), ReadMode.TAILING);
            for (int i = 0; i < 400; i++) {
                offer(manager, a, i);
            }
            assertEquals(manager.maxMemoryBytes(), manager.usedMemoryBytes(), "Queue a does not hold the whole pool");

            final SegmentedQueue b = manager.open(dir.resolve("b"), ReadMode.TAILING);

            assertSequence(drain(manager, b, 100), 0, 100);
            assertSequence(drain(manager, a, 400), 0, 400);
        }
    }

    @Test
    void journalQueueIsRecoveredWhileAnotherQueueHoldsAllMemory() throws IOException {
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL).build()) {
            final SegmentedQueue b = manager.open(dir.resolve("b"), ReadMode.TAILING);
            for (int i = 0; i < 100; i++) {
                offer(manager, b, i);
            }
        }
        try (SegmentManager manager = manager().withDiskMode(DiskMode.JOURNAL)
                .withMaxMemoryBytes(8L * SEGMENT)
                .build()) {
            final SegmentedQueue a = manager.open(dir.resolve("a"), ReadMode.TAILING);
            for (int i = 0; i < 400; i++) {
                offer(manager, a, i);
            }
            assertEquals(manager.maxMemoryBytes(), manager.usedMemoryBytes(), "Queue a does not hold the whole pool");

            final SegmentedQueue b = manager.open(dir.resolve("b"), ReadMode.TAILING);

            assertSequence(drain(manager, b, 100), 0, 100);
            assertSequence(drain(manager, a, 400), 0, 400);
        }
    }

    @Test
    void steadyStateDoesNotAllocate() throws IOException {
        final com.sun.management.ThreadMXBean threads =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        try (SegmentManager manager = manager().withMaxMemoryBytes(8L * SEGMENT).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            final QueueWriter writer = queue.writer();
            final QueueReader reader = queue.reader();
            final MessageHandler handler = (seq, buffer, offset, length) -> true;
            for (int round = 0; round < 3; round++) {
                final long before = threads.getCurrentThreadAllocatedBytes();
                for (int i = 0; i < 20_000; i++) { // about 600 segments
                    while (writer.offer(message, 0, PAYLOAD) == QueueWriter.REJECTED) {
                        manager.doWork();
                    }
                    reader.poll(handler, 16);
                    manager.doWork();
                }
                final long allocated = threads.getCurrentThreadAllocatedBytes() - before;
                if (round > 0) { // the first round fills the pools and links call sites
                    assertEquals(0, allocated, "The writer, the reader or the manager allocated");
                }
            }
        }
    }

    @Test
    void readerSeeksOverEvictedSegmentsWhileDescriptorsAreReused() throws IOException {
        try (SegmentManager manager = manager().withMaxMemoryBytes(5L * SEGMENT).build()) {
            final SegmentedQueue queue = manager.open(dir, ReadMode.TAILING);
            for (int i = 0; i < 1000; i++) {
                offer(manager, queue, i);
            }
            assertEquals(800, queue.reader().seek(800));
            for (int i = 1000; i < 2000; i++) {
                offer(manager, queue, i);
            }
            assertSequence(drain(manager, queue, 1200), 800, 1200);
        }
    }
}
