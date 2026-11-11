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

import io.github.green4j.d4m.common.BitSupport;
import io.github.green4j.d4m.common.UnsafeBuffer;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * A fixed-size block of the queue: a header followed by frames. Holds only what
 * the writer, the reader and the manager share; the manager keeps its own
 * bookkeeping about the segment elsewhere.
 *
 * <p>Segment layout: {@code magic(8) version(4) segmentSize(4) baseSeq(8)},
 * padded to {@link #HEADER_LENGTH}, then frames. The same layout is used in
 * memory and in the segment file.</p>
 *
 * <p>Frame layout: {@code length(4) flags(4) seq(8) payload}, 8-byte aligned.
 * {@code length} covers the frame header and the payload and is written last
 * with release semantics, so a non-zero length commits the frame. An END frame
 * closes the segment; the next segment is linked via {@link #next} before the
 * END frame is committed.</p>
 *
 * <p>Buffers are reused without being cleared. The writer keeps one invariant
 * instead: the length word at the first uncommitted frame position is zero. It
 * zeroes the first frame's length when it starts a segment and the next frame's
 * length before committing each frame; nobody reads past a zero length.</p>
 *
 * <p>Memory state: the reader pins a segment with one CAS
 * {@code RESIDENT -> PINNED} on entry and unpins it on exit; the manager evicts
 * or frees only from {@code RESIDENT}.</p>
 *
 * <p>The segment and its buffer are separate objects with separate lifecycles,
 * both pooled by the manager. A buffer serves whatever segment needs memory; a
 * segment keeps its place in the chain while it is evicted and gets a buffer again
 * when it is loaded. Once the reader has passed a segment and the manager has freed
 * it ({@code GONE}), the manager reuses the descriptor for a new segment. That is
 * safe because the reader touches only its pinned segment and segments at or after
 * the index it has published, which the manager never frees, and the writer touches
 * only its current segment, which is always ahead of the reader.</p>
 */
final class Segment {
    static final long MAGIC = 0x3147_4553_514D_3444L; // "D4MQSEG1" in the little-endian byte order
    static final int VERSION = 1;
    static final int MAGIC_OFFSET = 0;
    static final int VERSION_OFFSET = 8;
    static final int SEGMENT_SIZE_OFFSET = 12;
    static final int BASE_SEQ_OFFSET = 16;
    static final int HEADER_LENGTH = 64;

    static final int FRAME_LENGTH_OFFSET = 0;
    static final int FRAME_FLAGS_OFFSET = 4;
    static final int FRAME_SEQ_OFFSET = 8;
    static final int FRAME_HEADER_LENGTH = 16;
    static final int FLAG_END = 2;

    static final int RESIDENT = 0;
    static final int PINNED = 1;
    static final int EVICTED = 2;
    static final int GONE = 3;

    private static final VarHandle MEM_STATE;

    static {
        try {
            MEM_STATE = MethodHandles.lookup().findVarHandle(Segment.class, "memState", int.class);
        } catch (final ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Position of the segment in the chain of its queue; grows by one per segment.
     * Written by the manager before the segment is handed out.
     */
    long index;

    /**
     * Sequence number of the first message. Written by the writer before the segment
     * is linked into the chain, or by the manager for a recovered segment before the
     * queue is created; read-only afterwards.
     */
    long baseSeq;

    /**
     * Segment memory, or {@code null} while evicted. Written by the manager only; the
     * writer receives it through the spare slot and the reader through
     * {@link #casMemState}, both of which order the write before the read.
     */
    UnsafeBuffer buffer;

    /**
     * The next segment. Written once per use: by the writer before it commits the END
     * frame, or by the manager for recovered segments.
     */
    volatile Segment next;

    private volatile int memState;

    // Prepares the descriptor for a new segment. Manager thread only, before the segment
    // is handed out.
    void assign(final long index,
                final long baseSeq,
                final UnsafeBuffer buffer,
                final int memState) {
        this.index = index;
        this.baseSeq = baseSeq;
        this.buffer = buffer;
        this.next = null;
        this.memState = memState;
    }

    static int frameStep(final int frameLength) {
        return BitSupport.alignToLong(frameLength);
    }

    static void writeHeader(final UnsafeBuffer buffer,
                            final int segmentSize,
                            final long baseSeq) {
        buffer.putLong(MAGIC_OFFSET, MAGIC);
        buffer.putInt(VERSION_OFFSET, VERSION);
        buffer.putInt(SEGMENT_SIZE_OFFSET, segmentSize);
        buffer.putLong(BASE_SEQ_OFFSET, baseSeq);
    }

    int memState() {
        return memState;
    }

    void memState(final int state) {
        memState = state;
    }

    boolean casMemState(final int expected,
                        final int update) {
        return MEM_STATE.compareAndSet(this, expected, update);
    }

    @Override
    public String toString() {
        return "Segment{index=" + index + ", baseSeq=" + baseSeq + ", memState=" + memState + '}';
    }
}
