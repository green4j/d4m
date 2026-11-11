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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static io.github.green4j.d4m.queue.Segment.BASE_SEQ_OFFSET;
import static io.github.green4j.d4m.queue.Segment.EVICTED;
import static io.github.green4j.d4m.queue.Segment.FLAG_END;
import static io.github.green4j.d4m.queue.Segment.FRAME_FLAGS_OFFSET;
import static io.github.green4j.d4m.queue.Segment.FRAME_HEADER_LENGTH;
import static io.github.green4j.d4m.queue.Segment.FRAME_LENGTH_OFFSET;
import static io.github.green4j.d4m.queue.Segment.FRAME_SEQ_OFFSET;
import static io.github.green4j.d4m.queue.Segment.GONE;
import static io.github.green4j.d4m.queue.Segment.HEADER_LENGTH;
import static io.github.green4j.d4m.queue.Segment.MAGIC;
import static io.github.green4j.d4m.queue.Segment.MAGIC_OFFSET;
import static io.github.green4j.d4m.queue.Segment.RESIDENT;
import static io.github.green4j.d4m.queue.Segment.SEGMENT_SIZE_OFFSET;
import static io.github.green4j.d4m.queue.Segment.VERSION;
import static io.github.green4j.d4m.queue.Segment.VERSION_OFFSET;

/**
 * Owns the memory and the files of one or more {@link SegmentedQueue}s.
 *
 * <p>Each queue lives in its own directory; a segment file is named
 * {@code <baseSeq, 20 digits>.seg}, where {@code baseSeq} is the sequence number
 * of its first message.</p>
 *
 * <p>The manager is single-threaded: {@link #open}, {@link #doWork()},
 * {@link #segments}, {@link SegmentedQueue#close()} and {@link #close()} must be
 * called from one thread, typically an event loop that calls {@link #doWork()}
 * repeatedly; another thread may take over once that thread has stopped (e.g. to
 * close the manager after joining it). The manager is the only party touching files; the writer and the
 * reader of a queue only exchange segments with it through lock-free hand-offs.</p>
 *
 * <p>All segment buffers of all queues come from one off-heap pool of
 * {@code maxMemoryBytes}, allocated in full by {@link Builder#build()}. The manager
 * never allocates segment memory afterwards: when the pool is used up, it evicts
 * segments to disk or the writer gets {@link QueueWriter#REJECTED}.</p>
 */
public final class SegmentManager implements AutoCloseable {
    /**
     * Suffix of segment file names.
     */
    public static final String FILE_SUFFIX = ".seg";

    private static final int MIN_SEGMENT_SIZE = 4096;
    private static final int FILE_NAME_DIGITS = 20;
    private static final String MAX_FILE_NAME_DIGITS = "0" + Long.MAX_VALUE;
    private static final int MAX_LOADS_PER_STEP = 4;
    private static final long MAX_START_SEQ = Long.MAX_VALUE >> 1;
    private static final boolean IS_WINDOWS =
            System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows");

    /**
     * Listener for segment lifecycle events, intended for logging and metrics.
     *
     * <p>Threading: every callback is invoked on the manager thread, from within
     * {@link #open}, {@link #doWork()}, {@link SegmentedQueue#close()} or {@link #close()},
     * never on the writer or reader thread. Callbacks receive only primitives and existing objects, so they allocate
     * nothing; implementations must be fast and must not call back into the manager.</p>
     */
    public interface Listener {
        /**
         * Called by {@link #open} for every segment file found in the queue directory.
         * Only the newest file is read by {@code open}; the others are validated when they
         * are loaded for the reader.
         *
         * @param notifier the manager
         * @param queue    the queue being opened
         * @param baseSeq  sequence number of the first message of the segment
         * @param lastSeq  sequence number of the last message of the segment; for any file
         *                 but the newest one, the sequence number just below the next file's
         *                 {@code baseSeq}, which a damaged file may not reach
         */
        void onSegmentRecovered(SegmentManager notifier,
                                SegmentedQueue queue,
                                long baseSeq,
                                long lastSeq);

        /**
         * Called when a segment file contained bytes after the last valid message,
         * e.g. a write torn by a crash. These bytes are ignored.
         *
         * @param notifier the manager
         * @param queue    the queue
         * @param baseSeq  sequence number of the first message of the segment
         * @param lastSeq  sequence number of the last valid message of the segment
         */
        void onSegmentTruncated(SegmentManager notifier,
                                SegmentedQueue queue,
                                long baseSeq,
                                long lastSeq);

        /**
         * Called when a segment has been completely written to disk: a sealed segment, or
         * the newest one when {@link SegmentedQueue#close()} writes it.
         *
         * @param notifier the manager
         * @param queue    the queue
         * @param baseSeq  sequence number of the first message of the segment
         * @param lastSeq  sequence number of the last message of the segment
         */
        void onSegmentPersisted(SegmentManager notifier,
                                SegmentedQueue queue,
                                long baseSeq,
                                long lastSeq);

        /**
         * Called when a segment ahead of the reader has been removed from memory.
         *
         * @param notifier the manager
         * @param queue    the queue
         * @param baseSeq  sequence number of the first message of the segment
         */
        void onSegmentEvicted(SegmentManager notifier,
                              SegmentedQueue queue,
                              long baseSeq);

        /**
         * Called when an evicted segment has been loaded back into memory.
         *
         * @param notifier the manager
         * @param queue    the queue
         * @param baseSeq  sequence number of the first message of the segment
         * @param lastSeq  sequence number of the last message of the segment
         */
        void onSegmentLoaded(SegmentManager notifier,
                             SegmentedQueue queue,
                             long baseSeq,
                             long lastSeq);

        /**
         * Called when the file of a consumed segment has been deleted.
         *
         * @param notifier the manager
         * @param queue    the queue
         * @param baseSeq  sequence number of the first message of the segment
         */
        void onSegmentDeleted(SegmentManager notifier,
                              SegmentedQueue queue,
                              long baseSeq);

        /**
         * Called when the writer has had offers or claims rejected since the previous call,
         * because no prepared segment was available.
         *
         * @param notifier the manager
         * @param queue    the queue
         * @param rejected number of offers and claims rejected since the previous call
         */
        void onOffersRejected(SegmentManager notifier,
                              SegmentedQueue queue,
                              long rejected);
    }

    /**
     * Receives segment descriptions from {@link #segments}.
     */
    @FunctionalInterface
    public interface SegmentConsumer {
        /**
         * Called for every live segment of a queue, oldest first.
         *
         * @param baseSeq  sequence number of the first message of the segment
         * @param lastSeq  sequence number of the last message of the segment known to the manager;
         *                 for a recovered segment not loaded yet, the sequence number just below
         *                 the next segment's {@code baseSeq}
         * @param resident whether the segment is in memory
         * @param onDisk   whether the segment is completely written to disk
         */
        void onSegment(long baseSeq, long lastSeq, boolean resident, boolean onDisk);
    }

    /**
     * Builder of {@link SegmentManager}.
     */
    public static final class Builder {
        private int segmentSize = 32 * 1024 * 1024;
        private long maxMemoryBytes = 256L * 1024 * 1024;
        private DiskMode diskMode = DiskMode.OVERFLOW;
        private boolean fsync;
        private boolean deleteConsumed = true;
        private int retainSegments;
        private int spareSegments = 1;
        private Listener listener;

        private Builder() {
        }

        /**
         * Sets the segment size, a multiple of 8 and at least 4096 bytes. Default 32 MB.
         *
         * @param segmentSize segment size in bytes
         * @return this builder
         */
        public Builder withSegmentSize(final int segmentSize) {
            this.segmentSize = segmentSize;
            return this;
        }

        /**
         * Sets the off-heap memory for segment buffers of all queues, rounded down to whole
         * segments and allocated in full by {@link #build()}. Default 256 MB.
         *
         * @param maxMemoryBytes the limit in bytes
         * @return this builder
         */
        public Builder withMaxMemoryBytes(final long maxMemoryBytes) {
            this.maxMemoryBytes = maxMemoryBytes;
            return this;
        }

        /**
         * Sets what the disk is used for. Default {@link DiskMode#OVERFLOW}.
         *
         * @param diskMode the disk mode
         * @return this builder
         */
        public Builder withDiskMode(final DiskMode diskMode) {
            this.diskMode = diskMode;
            return this;
        }

        /**
         * Sets whether every write to disk is followed by {@code fsync}, and every file created
         * or deleted by an {@code fsync} of the queue directory. Default {@code false}.
         *
         * @param fsync whether to force writes to the storage device
         * @return this builder
         */
        public Builder withFsync(final boolean fsync) {
            this.fsync = fsync;
            return this;
        }

        /**
         * Sets whether segment files are deleted once the reader has passed them. Default {@code true}.
         *
         * @param deleteConsumed whether to delete consumed segment files
         * @return this builder
         */
        public Builder withDeleteConsumed(final boolean deleteConsumed) {
            this.deleteConsumed = deleteConsumed;
            return this;
        }

        /**
         * Sets how many segments right behind the reader are kept before being freed. Default 0.
         *
         * @param retainSegments number of retained segments
         * @return this builder
         */
        public Builder withRetainSegments(final int retainSegments) {
            this.retainSegments = retainSegments;
            return this;
        }

        /**
         * Sets how many empty segments are prepared for the writer in advance. Default 1.
         *
         * @param spareSegments number of spare segments per queue
         * @return this builder
         */
        public Builder withSpareSegments(final int spareSegments) {
            this.spareSegments = spareSegments;
            return this;
        }

        /**
         * Sets the lifecycle listener. Default {@code null}, no notifications.
         *
         * @param listener the listener, may be {@code null}
         * @return this builder
         */
        public Builder withListener(final Listener listener) {
            this.listener = listener;
            return this;
        }

        /**
         * Builds the manager and allocates its memory pool.
         *
         * @return the manager
         * @throws IllegalArgumentException if the segment size, the memory limit, the disk mode
         *                                  or the number of retained or spare segments is invalid,
         *                                  or the pool does not fit the segments of one queue
         * @throws OutOfMemoryError         if the pool exceeds the direct memory available to the JVM
         *                                  ({@code -XX:MaxDirectMemorySize})
         */
        public SegmentManager build() {
            return new SegmentManager(this);
        }
    }

    /**
     * What the manager knows about a segment of a queue, paired with the segment for
     * life: both are pooled together. Manager thread only.
     */
    private static final class Tracked {
        final Segment segment = new Segment();
        boolean sealed;
        int scannedLength;
        long scannedLastSeq;
        int persistedLength;
        boolean onDisk;
        Path file;
        FileChannel channel;
        Tracked older; // in the segment list of a queue
        Tracked newer; // in the segment list, the spare list or the pool

        void reset() {
            sealed = false;
            scannedLength = HEADER_LENGTH;
            scannedLastSeq = segment.baseSeq - 1;
            persistedLength = 0;
            onDisk = false;
            file = null;
            channel = null;
            older = null;
            newer = null;
        }
    }

    private static final class Recovery {
        final List<Tracked> segments = new ArrayList<>();
        long startSeq;
        boolean truncated;
    }

    private static final class QueueState {
        final SegmentedQueue queue;
        final Path directory;
        Tracked oldest; // live segments of the chain, oldest to newest
        Tracked newest;
        Tracked firstSpare; // offered to the writer, not linked into the chain yet
        Tracked lastSpare;
        long nextIndex;
        long reportedRejects;

        QueueState(final SegmentedQueue queue,
                   final Path directory) {
            this.queue = queue;
            this.directory = directory;
        }
    }

    private final int segmentSize;
    private final int maxBuffers;
    private final DiskMode diskMode;
    private final boolean fsync;
    private final boolean deleteConsumed;
    private final int retainSegments;
    private final int spareSegments;
    private final Listener listener;
    private final ArrayDeque<UnsafeBuffer> freeBuffers;
    private final List<QueueState> queues = new ArrayList<>();
    private Tracked freeTracked;
    private volatile int usedBuffers;

    private SegmentManager(final Builder builder) {
        if (builder.segmentSize < MIN_SEGMENT_SIZE || (builder.segmentSize & 7) != 0) {
            throw new IllegalArgumentException("Invalid segment size: " + builder.segmentSize);
        }
        if (builder.retainSegments < 0 || builder.spareSegments < 1 || builder.diskMode == null) {
            throw new IllegalArgumentException("Invalid retainSegments, spareSegments or diskMode");
        }
        final long buffers = builder.maxMemoryBytes / builder.segmentSize;
        if (buffers > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("maxMemoryBytes is too large: " + builder.maxMemoryBytes);
        }
        this.segmentSize = builder.segmentSize;
        this.maxBuffers = (int) buffers;
        this.diskMode = builder.diskMode;
        this.fsync = builder.fsync;
        this.deleteConsumed = builder.deleteConsumed;
        this.retainSegments = builder.retainSegments;
        this.spareSegments = builder.spareSegments;
        this.listener = builder.listener;
        if (buffersPerQueue() > maxBuffers) {
            throw new IllegalArgumentException("maxMemoryBytes must fit at least "
                    + buffersPerQueue() + " segments");
        }
        this.freeBuffers = new ArrayDeque<>(maxBuffers);
        for (int i = 0; i < maxBuffers; i++) {
            final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(segmentSize));
            buffer.verifyAlignment();
            freeBuffers.addLast(buffer);
        }
    }

    /**
     * Returns a new builder.
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the size of the memory pool: {@code maxMemoryBytes} rounded down to whole
     * segments. Safe to call from any thread.
     *
     * @return the pool size in bytes
     */
    public long maxMemoryBytes() {
        return (long) maxBuffers * segmentSize;
    }

    /**
     * Returns the memory of the pool held by the queues: resident segments and segments
     * prepared for the writers. Safe to call from any thread.
     *
     * @return the used memory in bytes
     */
    public long usedMemoryBytes() {
        return (long) usedBuffers * segmentSize;
    }

    /**
     * Opens a queue in {@code directory}, recovering the segments found there.
     * The reader starts from the oldest recovered message, the writer continues
     * after the newest one.
     *
     * @param directory directory of the queue, created if missing
     * @param readMode  read mode of the queue
     * @return the queue
     * @throws IOException              if the directory cannot be read, contains foreign segment
     *                                  files or a segment whose sequence numbers cannot continue
     * @throws IllegalArgumentException if {@code readMode} is {@link ReadMode#PERSISTED_ONLY}
     *                                  and the disk mode is not {@link DiskMode#JOURNAL}
     * @throws IllegalStateException    if the directory is already open, or the memory pool
     *                                  cannot fit one more queue
     */
    public SegmentedQueue open(final Path directory,
                               final ReadMode readMode) throws IOException {
        if (readMode == ReadMode.PERSISTED_ONLY && diskMode != DiskMode.JOURNAL) {
            throw new IllegalArgumentException("PERSISTED_ONLY requires DiskMode.JOURNAL");
        }
        final Path dir = directory.toAbsolutePath().normalize();
        for (int i = 0; i < queues.size(); i++) {
            if (queues.get(i).directory.equals(dir)) {
                throw new IllegalStateException("Queue is already open: " + dir);
            }
        }
        if ((queues.size() + 1) * buffersPerQueue() > maxBuffers) {
            throw new IllegalStateException("maxMemoryBytes is too small for one more queue");
        }
        Files.createDirectories(dir);

        final Recovery recovery = recover(dir);
        final List<Tracked> recovered = recovery.segments;
        final Segment first = recovered.isEmpty() ? null : recovered.get(0).segment;
        final Tracked last = recovered.isEmpty() ? null : recovered.get(recovered.size() - 1);
        final long nextIndex = recovered.size();

        final SegmentedQueue queue = new SegmentedQueue(this, segmentSize, readMode, spareSegments,
                first, last == null ? null : last.segment, recovery.startSeq, nextIndex);
        final QueueState state = new QueueState(queue, dir);
        for (int i = 0; i < recovered.size(); i++) {
            addNewest(state, recovered.get(i));
        }
        state.nextIndex = nextIndex;
        queues.add(state);

        if (listener != null) {
            for (int i = 0; i < recovered.size(); i++) {
                final Tracked t = recovered.get(i);
                listener.onSegmentRecovered(this, queue, t.segment.baseSeq, t.scannedLastSeq);
            }
            if (recovery.truncated) {
                listener.onSegmentTruncated(this, queue, last.segment.baseSeq, last.scannedLastSeq);
            }
        }
        return queue;
    }

    /**
     * Performs one step of work for every open queue: tracks new data, writes to
     * disk, frees consumed segments, prepares segments for writers and loads
     * evicted segments for readers.
     *
     * @return amount of work done; zero means there was nothing to do
     * @throws IOException if a file operation failed; the step can be retried
     */
    public int doWork() throws IOException {
        int work = 0;
        for (int i = 0; i < queues.size(); i++) {
            final QueueState state = queues.get(i);
            work += scan(state);
            if (diskMode == DiskMode.JOURNAL) {
                work += persist(state);
            }
            work += releaseConsumed(state);
            work += refillSpares(state);
            for (int loads = 0; loads < MAX_LOADS_PER_STEP && load(state); loads++) {
                work++;
            }
            reportRejects(state);
        }
        return work;
    }

    /**
     * Lists the live segments of a queue, oldest first, as known after the last {@link #doWork()}.
     * Segments already freed behind the reader are not listed, even if their files are kept
     * on disk because {@code deleteConsumed} is off.
     *
     * @param queue    the queue
     * @param consumer receives the segments
     * @throws IllegalArgumentException if the queue is not open in this manager
     */
    public void segments(final SegmentedQueue queue,
                         final SegmentConsumer consumer) {
        for (Tracked t = state(queue).oldest; t != null; t = t.newer) {
            consumer.onSegment(t.segment.baseSeq, t.scannedLastSeq, t.segment.buffer != null, t.onDisk);
        }
    }

    // Implements SegmentedQueue.close(), see there. A queue that is not open any more
    // is skipped, so closing twice does nothing.
    void close(final SegmentedQueue queue) throws IOException {
        final QueueState state = findState(queue);
        if (state == null) {
            return;
        }
        scan(state);
        if (diskMode == DiskMode.JOURNAL) {
            persist(state);
        }
        releaseConsumed(state);
        final long readerSeq = queue.readerSeq();
        for (Tracked t = state.oldest; t != null; t = t.newer) {
            if (t.segment.buffer != null && !t.onDisk
                    && (t == state.newest || t.scannedLastSeq > readerSeq)) {
                writeRange(state, t, t.persistedLength, t.scannedLength);
                persisted(state, t);
            }
            closeFile(t);
        }

        Tracked t;
        while ((t = state.oldest) != null) {
            state.oldest = t.newer;
            t.segment.memState(GONE);
            if (t.segment.buffer != null) {
                releaseBuffer(t);
            }
            recycle(t);
        }
        state.newest = null;
        while ((t = state.firstSpare) != null) {
            state.firstSpare = t.newer;
            releaseBuffer(t);
            recycle(t);
        }
        state.lastSpare = null;
        queue.clearSpares();
        queues.remove(state);
    }

    /**
     * Closes all queues still open, each as {@link SegmentedQueue#close()} does. Must be
     * called after the writers and the readers have stopped, on the manager thread or
     * once it has stopped.
     *
     * @throws IOException if a file operation failed; the queues not closed yet stay open
     */
    @Override
    public void close() throws IOException {
        while (!queues.isEmpty()) {
            close(queues.get(queues.size() - 1).queue);
        }
    }

    private long buffersPerQueue() {
        // active + spares + reader's segment + retained + one to load or evict
        return (long) spareSegments + 3 + retainSegments;
    }

    private QueueState state(final SegmentedQueue queue) {
        final QueueState state = findState(queue);
        if (state == null) {
            throw new IllegalArgumentException("Queue is not open in this manager");
        }
        return state;
    }

    private QueueState findState(final SegmentedQueue queue) {
        for (int i = 0; i < queues.size(); i++) {
            final QueueState state = queues.get(i);
            if (state.queue == queue) {
                return state;
            }
        }
        return null;
    }

    // Recovery

    // Collects the segments found in dir, oldest first, and loads the newest one to find
    // where the writer continues. A file name alone already proves that the writer had
    // reached its baseSeq, so the writer never continues below the newest file name.
    private Recovery recover(final Path dir) throws IOException {
        final Recovery recovery = new Recovery();
        final List<Tracked> recovered = recovery.segments;
        final long[] baseSeqs = listSegmentFiles(dir);
        if (baseSeqs.length > 0 && baseSeqs[baseSeqs.length - 1] >= MAX_START_SEQ) {
            throw new IOException("Sequence numbers of the queue are exhausted: "
                    + dir.resolve(fileName(baseSeqs[baseSeqs.length - 1])));
        }
        for (final long baseSeq : baseSeqs) {
            final Path file = dir.resolve(fileName(baseSeq));
            recovery.startSeq = Math.max(recovery.startSeq, baseSeq);
            if (!hasHeader(file, baseSeq)) {
                Files.delete(file);
                syncDirectory(dir);
                continue;
            }
            final Tracked t = takeTracked(recovered.size(), baseSeq, null, EVICTED);
            t.sealed = true;
            t.onDisk = true;
            t.file = file;
            if (!recovered.isEmpty()) {
                final Tracked prev = recovered.get(recovered.size() - 1);
                prev.segment.next = t.segment;
                prev.scannedLastSeq = baseSeq - 1;
            }
            recovered.add(t);
        }
        if (recovered.isEmpty()) {
            return recovery;
        }
        final Tracked last = recovered.get(recovered.size() - 1);
        final UnsafeBuffer buffer = acquireForRecovery();
        final boolean truncated;
        try {
            truncated = readSegment(last, buffer);
        } catch (final IOException | RuntimeException e) {
            free(buffer);
            throw e;
        }
        if (last.scannedLastSeq < last.segment.baseSeq) {
            free(buffer);
            Files.delete(last.file);
            syncDirectory(dir);
            recycle(recovered.remove(recovered.size() - 1));
            if (!recovered.isEmpty()) {
                recovered.get(recovered.size() - 1).segment.next = null;
            }
            return recovery;
        }
        last.segment.buffer = buffer;
        last.segment.memState(RESIDENT);
        try {
            repairTail(last, truncated);
        } catch (final IOException | RuntimeException e) {
            releaseBuffer(last);
            throw e;
        }
        recovery.startSeq = Math.max(recovery.startSeq, last.scannedLastSeq + 1);
        recovery.truncated = truncated;
        return recovery;
    }

    // The pool may be held by the other queues. open() has checked that every queue keeps
    // at most its share minus one buffer that cannot be evicted, so evicting a segment of
    // another queue always frees one.
    private UnsafeBuffer acquireForRecovery() throws IOException {
        final UnsafeBuffer buffer = acquire(null, -1);
        if (buffer == null) {
            throw new IllegalStateException("No free segment buffer to recover the queue");
        }
        return buffer;
    }

    // Makes the file of a recovered segment end exactly with the END frame that
    // readSegment() placed after the last valid message, so a torn tail is not met again.
    // A truncated file may already have the right size, with an invalid frame in place
    // of the END frame.
    private void repairTail(final Tracked t,
                            final boolean truncated) throws IOException {
        final int endFrame = t.scannedLength - FRAME_HEADER_LENGTH;
        try (FileChannel channel = FileChannel.open(t.file, StandardOpenOption.WRITE)) {
            if (!truncated && channel.size() == t.scannedLength) {
                return;
            }
            final ByteBuffer source = t.segment.buffer.byteBuffer().duplicate();
            source.limit(t.scannedLength);
            source.position(endFrame);
            long position = endFrame;
            while (source.hasRemaining()) {
                position += channel.write(source, position);
            }
            channel.truncate(t.scannedLength);
            if (fsync) {
                channel.force(true);
            }
        }
    }

    // Makes created and deleted files of the directory durable. Windows cannot open a
    // directory as a file; NTFS journals directory changes itself.
    private void syncDirectory(final Path dir) throws IOException {
        if (!fsync || IS_WINDOWS) {
            return;
        }
        try (FileChannel channel = FileChannel.open(dir, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static long[] listSegmentFiles(final Path dir) throws IOException {
        long[] result = new long[16];
        int count = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*" + FILE_SUFFIX)) {
            for (final Path path : stream) {
                final String name = path.getFileName().toString();
                final String digits = name.substring(0, name.length() - FILE_SUFFIX.length());
                if (digits.length() != FILE_NAME_DIGITS || !digits.chars().allMatch(Character::isDigit)
                        || digits.compareTo(MAX_FILE_NAME_DIGITS) > 0) {
                    continue;
                }
                if (count == result.length) {
                    result = Arrays.copyOf(result, count * 2);
                }
                result[count++] = Long.parseLong(digits);
            }
        }
        final long[] sorted = Arrays.copyOf(result, count);
        Arrays.sort(sorted);
        return sorted;
    }

    private boolean hasHeader(final Path file,
                              final long baseSeq) throws IOException {
        final ByteBuffer header = ByteBuffer.allocate(HEADER_LENGTH).order(ByteOrder.nativeOrder());
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            while (header.hasRemaining() && channel.read(header, header.position()) > 0) {
                // read the header fully
            }
        }
        if (header.position() < HEADER_LENGTH || header.getLong(MAGIC_OFFSET) == 0) {
            return false; // created, but nothing was written before a crash
        }
        if (header.getLong(MAGIC_OFFSET) != MAGIC
                || header.getInt(VERSION_OFFSET) != VERSION
                || header.getInt(SEGMENT_SIZE_OFFSET) != segmentSize
                || header.getLong(BASE_SEQ_OFFSET) != baseSeq) {
            throw new IOException("Foreign or incompatible segment file: " + file);
        }
        return true;
    }

    // Reads a segment file into the buffer and validates its frames. Everything after
    // the last valid frame is replaced with an END frame, so a torn tail looks like a
    // regular end of the segment. Returns whether bytes after the last valid frame
    // were dropped.
    private boolean readSegment(final Tracked t,
                                final UnsafeBuffer buffer) throws IOException {
        final ByteBuffer target = buffer.byteBuffer().duplicate();
        target.clear();
        try (FileChannel channel = FileChannel.open(t.file, StandardOpenOption.READ)) {
            target.limit((int) Math.min(channel.size(), segmentSize));
            while (target.hasRemaining() && channel.read(target, target.position()) > 0) {
                // read the file fully
            }
        }
        final int length = target.position();
        int at = HEADER_LENGTH;
        long expected = t.segment.baseSeq;
        while (at + FRAME_HEADER_LENGTH <= length) {
            final int frameLength = buffer.getInt(at + FRAME_LENGTH_OFFSET);
            final int flags = buffer.getInt(at + FRAME_FLAGS_OFFSET);
            if (buffer.getLong(at + FRAME_SEQ_OFFSET) != expected) {
                break;
            }
            if (flags == FLAG_END && frameLength == FRAME_HEADER_LENGTH) {
                t.scannedLength = at + FRAME_HEADER_LENGTH;
                t.scannedLastSeq = expected - 1;
                return t.scannedLength < length;
            }
            if (flags != 0 || frameLength < FRAME_HEADER_LENGTH
                    || frameLength > segmentSize - FRAME_HEADER_LENGTH - at) {
                break;
            }
            final int next = at + Segment.frameStep(frameLength);
            if (next > length) {
                break;
            }
            expected++;
            at = next;
        }
        buffer.putLong(at + FRAME_SEQ_OFFSET, expected);
        buffer.putInt(at + FRAME_FLAGS_OFFSET, FLAG_END);
        buffer.putInt(at + FRAME_LENGTH_OFFSET, FRAME_HEADER_LENGTH);
        t.scannedLength = at + FRAME_HEADER_LENGTH;
        t.scannedLastSeq = expected - 1;
        return at < length;
    }

    // Duty cycle steps

    // Follows the chain of segments and the committed frames of the newest one.
    private static int scan(final QueueState state) {
        Tracked t = state.newest;
        if (t == null) {
            final Segment head = state.queue.head();
            if (head == null) {
                return 0;
            }
            t = track(state, head);
        }
        int work = 0;
        while (true) {
            if (!t.sealed) {
                work += scanFrames(t);
                if (!t.sealed) {
                    return work;
                }
            }
            final Segment next = t.segment.next;
            if (next == null) {
                return work;
            }
            t = track(state, next);
            work++;
        }
    }

    // Moves the spare the writer has linked into the chain to the segment list. The writer
    // takes spares in the order they were offered.
    private static Tracked track(final QueueState state,
                                 final Segment segment) {
        final Tracked t = state.firstSpare;
        if (t == null || t.segment != segment) {
            throw new IllegalStateException("Segment was not offered as a spare: " + segment);
        }
        state.firstSpare = t.newer;
        if (state.firstSpare == null) {
            state.lastSpare = null;
        }
        t.newer = null;
        t.scannedLastSeq = segment.baseSeq - 1; // set by the writer before linking
        addNewest(state, t);
        return t;
    }

    private static void addNewest(final QueueState state,
                                  final Tracked t) {
        t.older = state.newest;
        t.newer = null;
        if (state.newest == null) {
            state.oldest = t;
        } else {
            state.newest.newer = t;
        }
        state.newest = t;
    }

    private static int scanFrames(final Tracked t) {
        final UnsafeBuffer buffer = t.segment.buffer;
        int at = t.scannedLength;
        long lastSeq = t.scannedLastSeq;
        int work = 0;
        while (true) {
            final int length = buffer.getIntVolatile(at + FRAME_LENGTH_OFFSET);
            if (length == 0) {
                break;
            }
            if ((buffer.getInt(at + FRAME_FLAGS_OFFSET) & FLAG_END) != 0) {
                at += FRAME_HEADER_LENGTH;
                t.sealed = true;
                break;
            }
            lastSeq = buffer.getLong(at + FRAME_SEQ_OFFSET);
            at += Segment.frameStep(length);
            work++;
        }
        t.scannedLength = at;
        t.scannedLastSeq = lastSeq;
        return work;
    }

    // Writes committed data in order and publishes the persisted sequence number.
    private int persist(final QueueState state) throws IOException {
        int work = 0;
        for (Tracked t = state.oldest; t != null; t = t.newer) {
            if (t.onDisk) {
                continue;
            }
            if (t.persistedLength < t.scannedLength) {
                writeRange(state, t, t.persistedLength, t.scannedLength);
                state.queue.publishPersistedSeq(t.scannedLastSeq);
                work++;
            }
            if (!t.sealed) {
                break;
            }
            persisted(state, t);
        }
        return work;
    }

    // Frees segments the reader has passed, keeping retainSegments of them.
    private int releaseConsumed(final QueueState state) throws IOException {
        final long bound = state.queue.readerSegmentIndex() - retainSegments;
        int work = 0;
        Tracked t;
        while ((t = state.oldest) != null && t.segment.index < bound) {
            if (!t.sealed || (diskMode == DiskMode.JOURNAL && !t.onDisk)) {
                break;
            }
            final Segment s = t.segment;
            if (s.buffer != null) {
                if (!s.casMemState(RESIDENT, GONE)) {
                    break;
                }
                releaseBuffer(t);
            } else {
                s.memState(GONE);
            }
            closeFile(t);
            if (t.onDisk && deleteConsumed) {
                Files.deleteIfExists(t.file);
                syncDirectory(state.directory);
                if (listener != null) {
                    listener.onSegmentDeleted(this, state.queue, s.baseSeq);
                }
            }
            state.oldest = t.newer;
            if (state.oldest == null) {
                state.newest = null;
            } else {
                state.oldest.older = null;
            }
            state.queue.head(s.next);
            recycle(t);
            work++;
        }
        return work;
    }

    private int refillSpares(final QueueState state) throws IOException {
        int work = 0;
        while (state.queue.canOfferSpare()) {
            final UnsafeBuffer buffer = acquire(state, -1);
            if (buffer == null) {
                break;
            }
            final Tracked t = takeTracked(state.nextIndex++, -1, buffer, RESIDENT);
            if (state.lastSpare == null) {
                state.firstSpare = t;
            } else {
                state.lastSpare.newer = t;
            }
            state.lastSpare = t;
            state.queue.offerSpare(t.segment);
            work++;
        }
        return work;
    }

    // Loads the first evicted segment at or after the reader, making memory by evicting
    // a segment further ahead if needed, so the resident window follows the reader.
    // Spare segments for the writer come first.
    private boolean load(final QueueState state) throws IOException {
        final long readerIndex = state.queue.readerSegmentIndex();
        Tracked target = null;
        for (Tracked t = state.oldest; t != null; t = t.newer) {
            if (t.segment.index >= readerIndex && t.segment.memState() == EVICTED) {
                target = t;
                break;
            }
        }
        if (target == null) {
            return false;
        }
        if (target.segment.index != readerIndex && state.queue.canOfferSpare()) {
            return false; // the writer comes first
        }
        final UnsafeBuffer buffer = acquire(state, target.segment.index);
        if (buffer == null) {
            return false;
        }
        final boolean truncated;
        try {
            truncated = readSegment(target, buffer);
        } catch (final IOException | RuntimeException e) {
            free(buffer);
            throw e;
        }
        final Segment s = target.segment;
        s.buffer = buffer;
        s.memState(RESIDENT);
        if (listener != null) {
            listener.onSegmentLoaded(this, state.queue, s.baseSeq, target.scannedLastSeq);
            if (truncated) {
                listener.onSegmentTruncated(this, state.queue, s.baseSeq, target.scannedLastSeq);
            }
        }
        return true;
    }

    private void reportRejects(final QueueState state) {
        if (listener == null) {
            return;
        }
        final long rejects = state.queue.rejectedOffers();
        if (rejects != state.reportedRejects) {
            listener.onOffersRejected(this, state.queue, rejects - state.reportedRejects);
            state.reportedRejects = rejects;
        }
    }

    // Evicts from the preferred queue first, then from any other one; preferred may be null.
    private boolean evictOne(final QueueState preferred,
                             final long aboveIndex) throws IOException {
        if (preferred != null && evictFrom(preferred, aboveIndex)) {
            return true;
        }
        for (int i = 0; i < queues.size(); i++) {
            final QueueState state = queues.get(i);
            if (state != preferred && evictFrom(state, -1)) {
                return true;
            }
        }
        return false;
    }

    // Evicts the newest sealed segment ahead of the reader and above aboveIndex, writing it
    // to disk first if needed.
    private boolean evictFrom(final QueueState state,
                              final long aboveIndex) throws IOException {
        final long lowestIndex = Math.max(state.queue.readerSegmentIndex(), aboveIndex);
        for (Tracked t = state.newest; t != null; t = t.older) {
            final Segment s = t.segment;
            if (s.index <= lowestIndex) {
                return false;
            }
            if (!t.sealed || s.buffer == null || s.memState() != RESIDENT) {
                continue;
            }
            if (!t.onDisk) {
                if (diskMode == DiskMode.JOURNAL) {
                    continue; // persisted in order by persist()
                }
                writeRange(state, t, t.persistedLength, t.scannedLength);
                persisted(state, t);
            }
            if (s.casMemState(RESIDENT, EVICTED)) {
                releaseBuffer(t);
                if (listener != null) {
                    listener.onSegmentEvicted(this, state.queue, s.baseSeq);
                }
                return true;
            }
        }
        return false;
    }

    // Files and buffers

    private void persisted(final QueueState state,
                           final Tracked t) throws IOException {
        closeFile(t);
        t.onDisk = true;
        if (listener != null) {
            listener.onSegmentPersisted(this, state.queue, t.segment.baseSeq, t.scannedLastSeq);
        }
    }

    private void writeRange(final QueueState state,
                            final Tracked t,
                            final int from,
                            final int to) throws IOException {
        if (from >= to) {
            return;
        }
        boolean created = false;
        if (t.channel == null) {
            if (t.file == null) {
                t.file = state.directory.resolve(fileName(t.segment.baseSeq));
                created = true;
            }
            t.channel = FileChannel.open(t.file, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        }
        final ByteBuffer source = t.segment.buffer.byteBuffer().duplicate();
        source.limit(to);
        source.position(from);
        long position = from;
        while (source.hasRemaining()) {
            position += t.channel.write(source, position);
        }
        if (fsync) {
            t.channel.force(false);
            if (created) {
                syncDirectory(state.directory);
            }
        }
        t.persistedLength = to;
    }

    private static void closeFile(final Tracked t) throws IOException {
        final FileChannel channel = t.channel;
        if (channel != null) {
            t.channel = null;
            channel.close();
        }
    }

    // Takes a free buffer, evicting a segment if the pool is used up; null if nothing
    // can be evicted.
    private UnsafeBuffer acquire(final QueueState preferred,
                                 final long aboveIndex) throws IOException {
        UnsafeBuffer buffer = freeBuffers.pollLast();
        if (buffer == null && evictOne(preferred, aboveIndex)) {
            buffer = freeBuffers.pollLast();
        }
        if (buffer != null) {
            usedBuffers = maxBuffers - freeBuffers.size();
        }
        return buffer;
    }

    private void free(final UnsafeBuffer buffer) {
        freeBuffers.addLast(buffer);
        usedBuffers = maxBuffers - freeBuffers.size();
    }

    private void releaseBuffer(final Tracked t) {
        final UnsafeBuffer buffer = t.segment.buffer;
        t.segment.buffer = null;
        free(buffer);
    }

    // Takes a segment descriptor from the pool; the pool grows to the largest number of
    // live segments and is reused afterwards.
    private Tracked takeTracked(final long index,
                                final long baseSeq,
                                final UnsafeBuffer buffer,
                                final int memState) {
        Tracked t = freeTracked;
        if (t == null) {
            t = new Tracked();
        } else {
            freeTracked = t.newer;
        }
        t.segment.assign(index, baseSeq, buffer, memState);
        t.reset();
        return t;
    }

    // Returns the descriptor of a freed segment to the pool. Neither the writer nor the
    // reader can reach a segment that has been freed, see Segment.
    private void recycle(final Tracked t) {
        t.segment.next = null;
        t.reset();
        t.newer = freeTracked;
        freeTracked = t;
    }

    static String fileName(final long baseSeq) {
        final String digits = Long.toString(baseSeq);
        final StringBuilder sb = new StringBuilder(FILE_NAME_DIGITS + FILE_SUFFIX.length());
        for (int i = digits.length(); i < FILE_NAME_DIGITS; i++) {
            sb.append('0');
        }
        return sb.append(digits).append(FILE_SUFFIX).toString();
    }
}
