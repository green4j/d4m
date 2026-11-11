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

/**
 * Defines what the disk is used for.
 *
 * <p>Sequence numbers continue after a clean {@link SegmentedQueue#close()}. After a
 * crash the writer continues after the newest message found on disk, so the numbers of
 * messages lost in the crash are given to new messages. A reader can therefore rely on
 * {@code seek(saved + 1)} after a crash only with {@link #JOURNAL} and
 * {@link ReadMode#PERSISTED_ONLY}, where it never sees a message that is not on disk yet.
 * Against a crash of the OS or a power loss this holds only with
 * {@link SegmentManager.Builder#withFsync(boolean) fsync}; without it, data written to
 * disk may still be in the OS page cache.</p>
 */
public enum DiskMode {
    /**
     * The disk keeps everything: every message is written to disk shortly after
     * it was offered, including the segment being filled. Survives a process
     * crash up to the last write.
     */
    JOURNAL,

    /**
     * The disk takes only the overflow: a segment is written to disk when memory
     * runs out and it has to be evicted, and on {@link SegmentedQueue#close()},
     * which writes the unconsumed segments and the newest one. While the reader keeps up,
     * the disk is not touched until the queue is closed. A process crash loses what was
     * in memory.
     */
    OVERFLOW
}
