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

import io.github.green4j.d4m.common.AtomicBuffer;
import io.github.green4j.d4m.common.UnsafeBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static io.github.green4j.d4m.common.BitSupport.CACHE_LINE;
import static io.github.green4j.d4m.queue.Segment.FLAG_END;
import static io.github.green4j.d4m.queue.Segment.FRAME_FLAGS_OFFSET;
import static io.github.green4j.d4m.queue.Segment.FRAME_HEADER_LENGTH;
import static io.github.green4j.d4m.queue.Segment.FRAME_LENGTH_OFFSET;
import static io.github.green4j.d4m.queue.Segment.FRAME_SEQ_OFFSET;
import static io.github.green4j.d4m.queue.Segment.HEADER_LENGTH;
import static io.github.green4j.d4m.queue.Segment.PINNED;
import static io.github.green4j.d4m.queue.Segment.RESIDENT;

/**
 * {@link BinaryQueue} made of fixed-size segments.
 *
 * <p>The queue itself never performs I/O and never allocates on the hot path.
 * Segments are supplied, persisted, evicted, loaded and deleted by the
 * {@link SegmentManager} that opened the queue. The writer and the reader may
 * run on any two threads; each of them must stay on its own thread.
 * {@link #close()} is a call into the manager and follows its threading rule.</p>
 */
public final class SegmentedQueue implements BinaryQueue, AutoCloseable {
    private static final int WRITER_SEQ_OFFSET = CACHE_LINE;
    private static final int READER_SEQ_OFFSET = 2 * CACHE_LINE;
    private static final int READER_SEGMENT_OFFSET = 3 * CACHE_LINE;
    private static final int PERSISTED_SEQ_OFFSET = 4 * CACHE_LINE;
    private static final int REJECTED_OFFERS_OFFSET = 5 * CACHE_LINE;
    private static final int COUNTERS_LENGTH = 7 * CACHE_LINE;

    private final SegmentManager manager;
    private final int segmentSize;
    private final int maxMessageLength;
    private final ReadMode readMode;
    private final UnsafeBuffer counters = new UnsafeBuffer(ByteBuffer.allocateDirect(COUNTERS_LENGTH));
    private final AtomicReferenceArray<Segment> spares;
    private final long startSeq;
    private final Writer writer;
    private final Reader reader;

    private volatile Segment head;
    private long spareTail; // manager thread only

    SegmentedQueue(final SegmentManager manager,
                   final int segmentSize,
                   final ReadMode readMode,
                   final int spareSegments,
                   final Segment firstRecovered,
                   final Segment lastRecovered,
                   final long startSeq,
                   final long firstNewSegmentIndex) {
        this.manager = manager;
        this.segmentSize = segmentSize;
        this.maxMessageLength = segmentSize - HEADER_LENGTH - 2 * FRAME_HEADER_LENGTH;
        this.readMode = readMode;
        this.spares = new AtomicReferenceArray<>(spareSegments);
        this.startSeq = startSeq;
        this.head = firstRecovered;
        counters.verifyAlignment();
        counters.putLong(WRITER_SEQ_OFFSET, startSeq - 1);
        counters.putLong(PERSISTED_SEQ_OFFSET, startSeq - 1);
        final long firstSeq = firstRecovered != null ? firstRecovered.baseSeq : startSeq;
        counters.putLong(READER_SEQ_OFFSET, firstSeq - 1);
        counters.putLong(READER_SEGMENT_OFFSET,
                firstRecovered != null ? firstRecovered.index : firstNewSegmentIndex);
        this.writer = new Writer(lastRecovered);
        this.reader = new Reader(firstSeq - 1);
    }

    @Override
    public QueueWriter writer() {
        return writer;
    }

    @Override
    public QueueReader reader() {
        return reader;
    }

    /**
     * Returns the read mode.
     *
     * @return the read mode
     */
    public ReadMode readMode() {
        return readMode;
    }

    /**
     * Returns the segment size.
     *
     * @return the segment size in bytes
     */
    public int segmentSize() {
        return segmentSize;
    }

    @Override
    public long writerSeq() {
        return counters.getLongVolatile(WRITER_SEQ_OFFSET);
    }

    @Override
    public long readerSeq() {
        return counters.getLongVolatile(READER_SEQ_OFFSET);
    }

    /**
     * Returns the sequence number of the last message written to disk. Safe to call from any thread.
     * Without {@code fsync} the message may still be in the OS page cache.
     *
     * @return the last persisted sequence number
     */
    public long persistedSeq() {
        return counters.getLongVolatile(PERSISTED_SEQ_OFFSET);
    }

    @Override
    public long rejectedOffers() {
        return counters.getLongVolatile(REJECTED_OFFERS_OFFSET);
    }

    /**
     * Returns the manager that opened the queue.
     *
     * @return the manager
     */
    public SegmentManager manager() {
        return manager;
    }

    /**
     * Closes the queue in its manager. Must be called after the writer and the reader
     * have stopped, on the manager thread, like {@link SegmentManager#doWork()}, or on
     * another thread once the manager thread has stopped (e.g. after joining it).
     * Segments the reader has passed are freed; the remaining messages not consumed
     * yet (and, with {@link DiskMode#JOURNAL}, all remaining messages) are written to
     * disk, so the queue can be reopened. The newest segment is always written, so
     * sequence numbers continue after reopening. {@link SegmentManager#close()}
     * closes the queues it has open the same way.
     *
     * <p>If a file operation fails, the queue stays open with nothing lost, and the
     * call can be retried. Closing a closed queue does nothing.</p>
     *
     * @throws IOException if a file operation failed
     */
    @Override
    public void close() throws IOException {
        manager.close(this);
    }

    // Manager-side access, called on the manager thread only.

    Segment head() {
        return head;
    }

    void head(final Segment segment) {
        head = segment;
    }

    long readerSegmentIndex() {
        return counters.getLongVolatile(READER_SEGMENT_OFFSET);
    }

    void publishPersistedSeq(final long seq) {
        counters.putLongOrdered(PERSISTED_SEQ_OFFSET, seq);
    }

    boolean canOfferSpare() {
        return spares.get((int) (spareTail % spares.length())) == null;
    }

    void offerSpare(final Segment segment) {
        spares.set((int) (spareTail % spares.length()), segment);
        spareTail++;
    }

    void clearSpares() {
        for (int i = 0; i < spares.length(); i++) {
            spares.set(i, null);
        }
    }

    private final class Writer implements QueueWriter {
        private Segment lastRecovered; // linked to the first new segment, then dropped
        private Segment current;
        private UnsafeBuffer buffer;
        private int offset = segmentSize;
        private long nextSeq = startSeq;
        private long spareHead;
        private long rejected;
        private int claimedLength = -1;
        private boolean closed;

        Writer(final Segment lastRecovered) {
            this.lastRecovered = lastRecovered;
        }

        @Override
        public long offer(final AtomicBuffer src,
                          final int srcOffset,
                          final int length) {
            if (!reserve(length)) {
                return REJECTED;
            }
            buffer.putBytes(offset + FRAME_HEADER_LENGTH, src, srcOffset, length);
            return publish(length);
        }

        @Override
        public int claim(final int maxLength) {
            if (!reserve(maxLength)) {
                return (int) REJECTED;
            }
            claimedLength = maxLength;
            return offset + FRAME_HEADER_LENGTH;
        }

        @Override
        public AtomicBuffer claimBuffer() {
            return buffer;
        }

        @Override
        public long commit(final int length) {
            if (claimedLength < 0) {
                throw new IllegalStateException("Nothing is claimed");
            }
            if (length < 0 || length > claimedLength) {
                throw new IllegalArgumentException("Invalid length " + length + ", claimed " + claimedLength);
            }
            claimedLength = -1;
            return publish(length);
        }

        @Override
        public void abort() {
            claimedLength = -1;
        }

        // Makes sure the current segment has room for a message of the given length.
        private boolean reserve(final int length) {
            if (length < 0 || length > maxMessageLength) {
                throw new IllegalArgumentException("Invalid message length: " + length);
            }
            if (closed || claimedLength >= 0) {
                throw new IllegalStateException(closed ? "Writer is closed" : "A claim is pending");
            }
            final int step = Segment.frameStep(FRAME_HEADER_LENGTH + length);
            if (offset + step + FRAME_HEADER_LENGTH > segmentSize && !roll()) {
                counters.putLongOrdered(REJECTED_OFFERS_OFFSET, ++rejected);
                return false;
            }
            return true;
        }

        // Commits the frame at the current offset. Buffers are reused without being
        // cleared, so the length word of the next frame is zeroed before the release
        // store that makes this frame visible.
        private long publish(final int length) {
            final UnsafeBuffer b = buffer;
            final int at = offset;
            final long seq = nextSeq;
            final int frameLength = FRAME_HEADER_LENGTH + length;
            final int next = at + Segment.frameStep(frameLength);
            b.putLong(at + FRAME_SEQ_OFFSET, seq);
            b.putInt(at + FRAME_FLAGS_OFFSET, 0);
            b.putInt(next + FRAME_LENGTH_OFFSET, 0);
            b.putIntOrdered(at + FRAME_LENGTH_OFFSET, frameLength);
            offset = next;
            nextSeq = seq + 1;
            counters.putLongOrdered(WRITER_SEQ_OFFSET, seq);
            return seq;
        }

        private boolean roll() {
            final int slot = (int) (spareHead % spares.length());
            final Segment next = spares.get(slot);
            if (next == null) {
                return false;
            }
            spares.set(slot, null);
            spareHead++;

            final UnsafeBuffer nextBuffer = next.buffer;
            Segment.writeHeader(nextBuffer, segmentSize, nextSeq);
            nextBuffer.putInt(HEADER_LENGTH + FRAME_LENGTH_OFFSET, 0);
            next.baseSeq = nextSeq;
            final Segment prev = current;
            if (prev == null) {
                if (lastRecovered != null) {
                    lastRecovered.next = next;
                    lastRecovered = null;
                } else {
                    head = next;
                }
            } else {
                prev.next = next;
                final UnsafeBuffer b = buffer;
                b.putLong(offset + FRAME_SEQ_OFFSET, nextSeq);
                b.putInt(offset + FRAME_FLAGS_OFFSET, FLAG_END);
                b.putIntOrdered(offset + FRAME_LENGTH_OFFSET, FRAME_HEADER_LENGTH);
            }
            current = next;
            buffer = nextBuffer;
            offset = HEADER_LENGTH;
            return true;
        }

        @Override
        public int maxMessageLength() {
            return maxMessageLength;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private final class Reader implements QueueReader {
        private final boolean persistedOnly = readMode == ReadMode.PERSISTED_ONLY;
        private Segment current;
        private Segment pending; // failed to pin; its index is published
        private UnsafeBuffer buffer;
        private int offset;
        private long position;
        private long publishedPosition;
        private long skipUntil;
        private long persistedLimit = Long.MAX_VALUE;

        Reader(final long position) {
            this.position = position;
            this.publishedPosition = position;
            if (persistedOnly) {
                persistedLimit = position;
            }
        }

        @Override
        public int poll(final MessageHandler handler,
                        final int limit) {
            if (limit <= 0) {
                throw new IllegalArgumentException("limit must be positive: " + limit);
            }
            if (current == null && !enter(pending != null ? pending : head)) {
                return 0;
            }
            int count = 0;
            try {
                while (count < limit) {
                    final UnsafeBuffer b = buffer;
                    final int at = offset;
                    final int length = b.getIntVolatile(at + FRAME_LENGTH_OFFSET);
                    if (length == 0) {
                        break;
                    }
                    if ((b.getInt(at + FRAME_FLAGS_OFFSET) & FLAG_END) != 0) {
                        if (!enter(pending != null ? pending : current.next)) {
                            break;
                        }
                        continue;
                    }
                    final long seq = b.getLong(at + FRAME_SEQ_OFFSET);
                    if (seq > persistedLimit) {
                        persistedLimit = persistedSeq();
                        if (seq > persistedLimit) {
                            break;
                        }
                    }
                    if (seq >= skipUntil) {
                        if (!handler.onMessage(seq, b, at + FRAME_HEADER_LENGTH, length - FRAME_HEADER_LENGTH)) {
                            break;
                        }
                        count++;
                    }
                    position = seq;
                    offset = at + Segment.frameStep(length);
                }
            } finally {
                if (position != publishedPosition) {
                    publishedPosition = position;
                    counters.putLongOrdered(READER_SEQ_OFFSET, position);
                }
            }
            return count;
        }

        /**
         * Moves to {@code segment}, skipping whole segments below {@link #skipUntil}.
         * The current segment stays pinned until the new one is pinned. The index of the
         * target is published before pinning, so the manager may free and reuse the
         * segments before it; a target that cannot be pinned yet is remembered as
         * {@link #pending}, and the next attempt starts from it.
         *
         * @param segment the segment to enter, may be {@code null}
         * @return {@code true} if the reader moved to a pinned segment
         */
        private boolean enter(final Segment segment) {
            Segment target = segment;
            if (target == null) {
                return false;
            }
            Segment next = target.next;
            while (next != null && next.baseSeq <= skipUntil) {
                target = next;
                next = target.next;
            }
            counters.putLongOrdered(READER_SEGMENT_OFFSET, target.index);
            if (!target.casMemState(RESIDENT, PINNED)) {
                pending = target;
                return false;
            }
            pending = null;
            if (current != null) {
                current.memState(RESIDENT);
            }
            if (position < target.baseSeq - 1) {
                position = target.baseSeq - 1;
            }
            current = target;
            buffer = target.buffer;
            offset = HEADER_LENGTH;
            return true;
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public long seek(final long seq) {
            if (seq > skipUntil) {
                skipUntil = seq;
            }
            return Math.max(position + 1, skipUntil);
        }
    }
}
