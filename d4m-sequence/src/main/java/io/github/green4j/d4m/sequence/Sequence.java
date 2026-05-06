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
package io.github.green4j.d4m.sequence;

import io.github.green4j.d4m.common.AtomicBuffer;

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * An ordered, append-optimised sequence of entries stored across a
 * chain of fixed-size chunks. Supports concurrent single-writer /
 * multi-reader access with lock-free snapshot publication and
 * cooperative heap-to-mmap chunk eviction.
 *
 * <p>Entries are identified by a caller-supplied <em>order</em> (long)
 * and an internally assigned monotonic <em>version</em>. In addition
 * to simple appends, the sequence supports ordered inserts and
 * insert-or-update (upsert) operations via copy-on-write rebuilds.</p>
 */
public final class Sequence {
    // Writer-owned reusable structures for
    // Different search and rebuild operations
    private final EntrySearch entrySearch = new EntrySearch();
    private final GroupEndSearch groupEndSearch = new GroupEndSearch();
    private final RebuildWriter rebuildWriter = new RebuildWriter();

    private final String name;
    private final int chunkSize;
    private final int dataCap;
    private final HeapChunkAllocator heap;
    private final MmapChunkAllocator mmap;
    private final EvictionQueue evictQ;
    private final ConcurrentLinkedQueue<PendingSwap> pendingSwaps =
            new ConcurrentLinkedQueue<>(); // can be accessed from another writers/threads
    // While cooperative eviction from evictQ

    private volatile ChunkSnapshot snapshot;
    private long snapshotVersion = 1;
    private long nextEntryVersion = 1;

    // Segmented spine: array of fixed-size segments.
    // PubAppend sets the next slot in the current segment — zero copy of existing data.
    // DrainSwaps modifies segment elements in-place (safe — see drainSwaps javadoc).
    // PubCow creates new segments for the affected region.
    private static final int SEG_SHIFT = ChunkSnapshot.SEG_SHIFT;
    private static final int SEG_SIZE = ChunkSnapshot.SEG_SIZE;
    private static final int SEG_MASK = ChunkSnapshot.SEG_MASK;

    private Chunk[][] chunkSpine = new Chunk[4][];
    private long[][] epochSpine = new long[4][];
    private int spineUsed;
    private int chunksCount;

    private int appendsSinceSwapCheck;
    private static final int SWAP_CHECK_INTERVAL = 64;

    // Monotonically advancing search-start for drainSwaps.
    // For append-only workloads, evictions are ordered (oldest-first),
    // So the next swap target is always at or after this index -> O(1) amortised.
    // Reset to 0 when pubCow rearranges the arrays.
    private int drainSwapSearchStart;

    // Number of dead/compacted elements at the front of the spine.
    // ChunkSnapshot uses this as headOffset to translate logical indices.
    // Currently always 0 — reserved for future tiered-index support.
    private int writerHeadOffset;

    // Writer-local cached tail state.
    // Eliminates volatile reads of snapshot, isSealed, getEntryCount
    // And plain reads of getDataWriteOffset, getMaxOrder on the hot path.
    private Chunk writerTail;
    private int writerTailWriteOffset;
    private int writerTailEntryCount;
    private long writerTailMaxOrder = Long.MIN_VALUE;

    // Reusable scratch arrays for insertBatch grouping (writer-thread only).
    // Grown on demand, never shrunk — avoids per-call GC pressure.
    private int[] batchGroupChunkIdx = new int[16];
    private int[] batchGroupFrom = new int[16];
    private int[] batchGroupTo = new int[16];
    private Chunk[][] batchRebuilt = new Chunk[16][];
    private Chunk[] batchOldChunks = new Chunk[16];

    /**
     * Creates a new sequence.
     *
     * @param name      name of the sequence
     * @param chunkSize size in bytes of each chunk (header + data)
     * @param heap      allocator for heap-backed chunks
     * @param mmap      allocator for memory-mapped chunks
     * @param evictQ    shared eviction queue for heap-to-mmap migration
     */
    public Sequence(final String name,
                    final int chunkSize,
                    final HeapChunkAllocator heap,
                    final MmapChunkAllocator mmap,
                    final EvictionQueue evictQ) {
        this.name = name;
        this.chunkSize = chunkSize;
        this.dataCap = chunkSize - Chunk.HEADER_SIZE;
        this.heap = heap;
        this.mmap = mmap;
        this.evictQ = evictQ;
        this.snapshot = ChunkSnapshot.EMPTY;
    }

    /**
     * Returns the name of this sequence.
     *
     * @return sequence name
     */
    public String name() {
        return name;
    }


    /**
     * Returns the current immutable snapshot of chunks. The snapshot is safe
     * to read from any thread without synchronization.
     *
     * @return current chunk snapshot (never {@code null})
     */
    public ChunkSnapshot snapshot() {
        return snapshot;
    }

    /**
     * Appends a single entry. Entries must arrive with non-decreasing orders:
     * equal orders are permitted (disambiguation is handled by the monotonic
     * internal version counter {@code nextEntryVersion}), but strictly earlier
     * orders are rejected and the call returns {@code false}.
     *
     * <p>Periodically drains pending heap-to-mmap swaps to keep the
     * writer-owned chunk array consistent.
     *
     * @param order         ordering key of the entry
     * @param payload       buffer containing the entry payload
     * @param payloadOffset offset within {@code payload}
     * @param payloadSize   size of the payload in bytes
     * @return {@code true} if the entry was appended; {@code false} if the
     * order violates the non-decreasing constraint
     * @throws IllegalArgumentException if the entry exceeds chunk data capacity
     */
    public boolean append(final long order,
                          final AtomicBuffer payload,
                          final int payloadOffset,
                          final int payloadSize) {
        final int entrySize = Chunk.entrySize(payloadSize);
        if (entrySize > dataCap) {
            throw new IllegalArgumentException("entry too large: " + payloadSize);
        }

        final Chunk tail = writerTail;
        if (tail != null) {
            if (order < writerTailMaxOrder) {
                return false;
            }
            if (writerTailWriteOffset + entrySize <= chunkSize) {
                // HOT PATH — no volatile reads, pure sequential writes
                tail.writeEntry(writerTailWriteOffset, order, nextEntryVersion++,
                        payload, payloadOffset, payloadSize);
                writerTailWriteOffset += entrySize;
                writerTailMaxOrder = order;
                tail.putDataWriteOffset(writerTailWriteOffset);
                tail.putMaxOrder(order);
                tail.putEntryCountOrdered(++writerTailEntryCount);
                if ((++appendsSinceSwapCheck & (SWAP_CHECK_INTERVAL - 1)) == 0) {
                    drainSwaps();
                }
                return true;
            }
            sealEnqueue(tail);
            writerTail = null;
        }

        if ((++appendsSinceSwapCheck & (SWAP_CHECK_INTERVAL - 1)) == 0) {
            drainSwaps();
        }
        return appendSlowPath(order, entrySize, payload, payloadOffset, payloadSize);
    }

    /**
     * Cold path for {@link #append}: either writes to an existing unsealed
     * tail found via the snapshot, or allocates a new chunk. On success,
     * populates the writer-local tail cache so subsequent calls take the
     * fast path.
     *
     * @param order         logical order key for the new entry
     * @param entrySize     total serialized entry size in bytes
     * @param payload       buffer holding the payload bytes
     * @param payloadOffset offset of the payload within {@code payload}
     * @param payloadSize   length of the payload in bytes
     * @return {@code true} if the entry was appended successfully
     */
    private boolean appendSlowPath(final long order,
                                   final int entrySize,
                                   final AtomicBuffer payload,
                                   final int payloadOffset,
                                   final int payloadSize) {
        final ChunkSnapshot currentSnapshot = snapshot;
        final Chunk tail = !currentSnapshot.isEmpty()
                ? currentSnapshot.chunk(currentSnapshot.size() - 1) : null;

        if (tail != null && order < tail.getMaxOrder()) {
            return false;
        }

        if (tail != null && !tail.isSealed()) {
            final int nextEntryOffset = tail.getDataWriteOffset();
            if (nextEntryOffset + entrySize <= chunkSize) {
                final int entryCount = tail.getEntryCount();
                tail.writeEntry(nextEntryOffset, order, nextEntryVersion++,
                        payload, payloadOffset, payloadSize);
                final int newWriteOffset = nextEntryOffset + entrySize;
                tail.putDataWriteOffset(newWriteOffset);
                tail.putMaxOrder(order);
                if (entryCount == 0) {
                    tail.putMinOrder(order);
                }
                tail.putEntryCountOrdered(entryCount + 1);
                cacheWriterTail(tail, newWriteOffset, entryCount + 1, order);
                return true;
            }
            sealEnqueue(tail);
        }

        final Chunk newChunk = allocate();
        initChunk(newChunk);
        newChunk.writeEntry(Chunk.HEADER_SIZE, order, nextEntryVersion++,
                payload, payloadOffset, payloadSize);
        final int newWriteOffset = Chunk.HEADER_SIZE + entrySize;
        newChunk.putDataWriteOffset(newWriteOffset);
        newChunk.putMinOrder(order);
        newChunk.putMaxOrder(order);
        newChunk.putEntryCountOrdered(1);
        pubAppend(newChunk);
        cacheWriterTail(newChunk, newWriteOffset, 1, order);
        return true;
    }

    private void cacheWriterTail(final Chunk tail,
                                 final int writeOffset,
                                 final int entryCount,
                                 final long maxOrder) {
        writerTail = tail;
        writerTailWriteOffset = writeOffset;
        writerTailEntryCount = entryCount;
        writerTailMaxOrder = maxOrder;
    }

    private void invalidateWriterTail() {
        writerTail = null;
    }

    /**
     * Appends up to {@code entryCount} entries in a single batch. Entries must
     * be pre-sorted in non-decreasing order. The method stops early on an
     * ordering violation and returns the number of entries successfully written.
     *
     * @param orders         ordering keys, one per entry
     * @param payloads       buffer containing all payloads
     * @param payloadOffsets per-entry offsets within {@code payloads}
     * @param payloadSizes   per-entry payload sizes in bytes
     * @param entryCount     number of entries to append
     * @return number of entries actually appended (may be less than
     * {@code entryCount} on ordering violation or if {@code entryCount < 1})
     * @throws IllegalArgumentException if any single entry exceeds chunk data capacity
     */
    public int appendBatch(final long[] orders,
                           final AtomicBuffer payloads,
                           final int[] payloadOffsets,
                           final int[] payloadSizes,
                           final int entryCount) {
        invalidateWriterTail();
        if (entryCount < 1) {
            return 0;
        }
        for (int i = 0; i < entryCount; i++) {
            if (Chunk.entrySize(payloadSizes[i]) > dataCap) {
                throw new IllegalArgumentException("entry " + i + " too large: " + payloadSizes[i]);
            }
        }
        drainSwaps();

        Chunk tail = tailChunk();
        if (tail != null && orders[0] < tail.getMaxOrder()) {
            return 0;
        }

        int writtenEntries = 0;
        while (writtenEntries < entryCount) {
            if (tail == null || tail.isSealed()) {
                tail = spineAppend();
            }

            final int currentEntryCount = tail.getEntryCount();
            int nextEntryOffset = tail.getDataWriteOffset();

            int tailWrittenEntries = 0;
            long tailMinOrder = (currentEntryCount == 0) ? Long.MAX_VALUE : tail.getMinOrder();
            long tailMaxOrder = (currentEntryCount == 0) ? Long.MIN_VALUE : tail.getMaxOrder();

            while (writtenEntries + tailWrittenEntries < entryCount) {
                final long order = orders[writtenEntries + tailWrittenEntries];
                final int payloadSize = payloadSizes[writtenEntries + tailWrittenEntries];
                final int entrySize = Chunk.entrySize(payloadSize);
                if (nextEntryOffset + entrySize > chunkSize) {
                    break;
                }
                if (order < tailMaxOrder) {
                    break;
                }

                tail.writeEntry(nextEntryOffset,
                        order,
                        nextEntryVersion++,
                        payloads,
                        payloadOffsets[writtenEntries + tailWrittenEntries],
                        payloadSize
                );
                nextEntryOffset += entrySize;
                if (order < tailMinOrder) {
                    tailMinOrder = order;
                }
                if (order > tailMaxOrder) {
                    tailMaxOrder = order;
                }
                tailWrittenEntries++;
            }
            if (tailWrittenEntries > 0) {
                tail.putDataWriteOffset(nextEntryOffset);
                if (currentEntryCount == 0) {
                    tail.putMinOrder(tailMinOrder);
                }
                tail.putMaxOrder(tailMaxOrder);
                tail.putEntryCountOrdered(currentEntryCount + tailWrittenEntries);
                writtenEntries += tailWrittenEntries;
            }

            // Seal if the next entry (or minimum header) cannot fit
            final boolean chunkFull = (writtenEntries < entryCount)
                    ? nextEntryOffset + Chunk.entrySize(payloadSizes[writtenEntries]) > chunkSize
                    : nextEntryOffset + Chunk.ENTRY_HEADER_SIZE > chunkSize;
            if (chunkFull) {
                sealEnqueue(tail);
                tail = null;
            }

            // No progress and chunk not full -> ordering violation
            if (tailWrittenEntries == 0 && writtenEntries < entryCount) {
                break;
            }
        }
        if (writtenEntries > 0) {
            publishSnapshot();
        }
        return writtenEntries;
    }

    /**
     * Appends up to {@code entryCount} entries from a packed buffer. Each packed
     * entry is laid out as {@code [long order (8 bytes), int payloadSize (4 bytes),
     * byte[] payload]}. Entries must be pre-sorted in non-decreasing order.
     *
     * @param packedEntries buffer containing the packed entry data
     * @param offset        starting offset within {@code packedEntries}
     * @param entryCount    number of entries to append
     * @return number of entries actually appended (may be less than
     * {@code entryCount} on ordering violation)
     * @throws IllegalArgumentException if any single entry exceeds chunk data capacity
     */
    public int appendBatchPacked(final AtomicBuffer packedEntries,
                                 final int offset,
                                 final int entryCount) {
        invalidateWriterTail();
        if (entryCount == 0) {
            return 0;
        }
        drainSwaps();

        Chunk tail = tailChunk();
        final long firstOrder = packedEntries.getLong(offset);
        if (tail != null && firstOrder < tail.getMaxOrder()) {
            return 0;
        }

        int readOffset = offset;
        int written = 0;
        while (written < entryCount) {
            if (tail == null || tail.isSealed()) {
                tail = spineAppend();
            }

            final int currentEntryCount = tail.getEntryCount();
            int nextEntryOffset = tail.getDataWriteOffset();

            int tailWrittenEntries = 0;

            long tailMinOrder = (currentEntryCount == 0) ? Long.MAX_VALUE : tail.getMinOrder();
            long tailMaxOrder = (currentEntryCount == 0) ? Long.MIN_VALUE : tail.getMaxOrder();

            int scan = readOffset;

            while (written + tailWrittenEntries < entryCount) {
                final long order = packedEntries.getLong(scan);
                final int payloadSize = packedEntries.getInt(scan + 8);
                final int entrySize = Chunk.entrySize(payloadSize);
                if (entrySize > dataCap) {
                    throw new IllegalArgumentException("entry too large: " + payloadSize);
                }
                if (nextEntryOffset + entrySize > chunkSize) {
                    break;
                }
                if (order < tailMaxOrder) {
                    break;
                }

                tail.writeEntry(
                        nextEntryOffset,
                        order,
                        nextEntryVersion++,
                        packedEntries,
                        scan + 12,
                        payloadSize
                );
                nextEntryOffset += entrySize;
                scan += 12 + payloadSize;
                if (order < tailMinOrder) {
                    tailMinOrder = order;
                }
                if (order > tailMaxOrder) {
                    tailMaxOrder = order;
                }
                tailWrittenEntries++;
            }
            if (tailWrittenEntries > 0) {
                tail.putDataWriteOffset(nextEntryOffset);
                if (currentEntryCount == 0) {
                    tail.putMinOrder(tailMinOrder);
                }
                tail.putMaxOrder(tailMaxOrder);
                tail.putEntryCountOrdered(currentEntryCount + tailWrittenEntries);
                written += tailWrittenEntries;
                readOffset = scan;
            }

            // Seal if the next packed entry (or minimum header) cannot fit.
            // ReadOffset always points to the start of the next unwritten packed
            // Entry (long ts + int pLen + payload), so readOffset + 8 is pLen.
            final boolean chunkFull = (written < entryCount)
                    ? nextEntryOffset + Chunk.entrySize(packedEntries.getInt(readOffset + 8)) > chunkSize
                    : nextEntryOffset + Chunk.ENTRY_HEADER_SIZE > chunkSize;
            if (chunkFull) {
                sealEnqueue(tail);
                tail = null;
            }

            // No progress and chunk not full -> order ordering violation
            if (tailWrittenEntries == 0 && written < entryCount) {
                break;
            }
        }
        if (written > 0) {
            publishSnapshot();
        }
        return written;
    }

    /**
     * Inserts a single entry at the correct position (cold path — allocations
     * tolerated). If the order is greater than or equal to the current maximum,
     * the entry is appended; otherwise a copy-on-write rebuild is performed.
     *
     * @param order         ordering key of the entry
     * @param payload       buffer containing the entry payload
     * @param payloadOffset offset within {@code payload}
     * @param payloadSize   size of the payload in bytes
     * @throws IllegalArgumentException if the entry exceeds chunk data capacity
     */
    public void insert(final long order,
                       final AtomicBuffer payload,
                       final int payloadOffset,
                       final int payloadSize) {
        checkFits(payloadSize);
        drainSwaps();

        final ChunkSnapshot currentSnapshot = snapshot;

        if (currentSnapshot.isEmpty() || order >= maxOrder(currentSnapshot)) {
            if (appendInternal(currentSnapshot, order, payload, payloadOffset, payloadSize)) {
                return;
            }
        }
        cowInsert(currentSnapshot, order, payload, payloadOffset, payloadSize);
    }

    /**
     * Inserts a batch of pre-sorted entries into the sequence using batched
     * copy-on-write. Entries whose order exceeds the current maximum are
     * appended to the tail. Entries that fall within existing chunks are
     * merged into those chunks via a single COW rebuild per affected chunk.
     * A single snapshot is published at the end of the operation, making the
     * entire batch visible to readers atomically.
     *
     * <p>The caller <b>must</b> supply entries sorted in non-decreasing order.
     * Violating this precondition produces undefined results.</p>
     *
     * @param orders         array of ordering keys (must be non-decreasing)
     * @param payloads       buffer containing all entry payloads
     * @param payloadOffsets offsets within {@code payloads} for each entry
     * @param payloadSizes   sizes of each entry's payload in bytes
     * @param entryCount     number of entries to insert
     * @return number of entries actually inserted
     * @throws IllegalArgumentException if any single entry exceeds chunk data capacity
     */
    public int insertBatch(final long[] orders,
                           final AtomicBuffer payloads,
                           final int[] payloadOffsets,
                           final int[] payloadSizes,
                           final int entryCount) {
        invalidateWriterTail();
        if (entryCount < 1) {
            return 0;
        }
        for (int i = 0; i < entryCount; i++) {
            if (Chunk.entrySize(payloadSizes[i]) > dataCap) {
                throw new IllegalArgumentException("entry " + i + " too large: " + payloadSizes[i]);
            }
        }
        drainSwaps();

        final ChunkSnapshot currentSnapshot = snapshot;

        if (currentSnapshot.isEmpty()) {
            return appendBatchInternal(orders, payloads, payloadOffsets, payloadSizes, 0, entryCount);
        }

        final long maxOrd = maxOrder(currentSnapshot);

        // Partition: find first entry that goes to the append path (order >= maxOrd)
        int cowEnd = entryCount;
        for (int i = 0; i < entryCount; i++) {
            if (orders[i] >= maxOrd) {
                cowEnd = i;
                break;
            }
        }

        if (cowEnd == 0) {
            return appendBatchInternal(orders, payloads, payloadOffsets, payloadSizes, 0, entryCount);
        }

        // Group COW entries by target chunk. For each entry, first check if it
        // falls inside an existing chunk's [min, max] range; if not (gap case),
        // use insertionChunkIndex to find the preceding chunk. Hinted search
        // exploits the non-decreasing chunk index property of sorted input.
        if (cowEnd > batchGroupChunkIdx.length) {
            final int newLen = Math.max(cowEnd, batchGroupChunkIdx.length * 2);
            batchGroupChunkIdx = new int[newLen];
            batchGroupFrom = new int[newLen];
            batchGroupTo = new int[newLen];
        }
        int groupCount = 0;

        int prevChunkIdx = -1;
        int hint = 0;
        for (int i = 0; i < cowEnd; i++) {
            int chunkIdx = searchChunkFrom(currentSnapshot, orders[i], hint);
            if (chunkIdx < 0) {
                chunkIdx = insertionChunkIndexFrom(currentSnapshot, orders[i], hint);
            }
            hint = chunkIdx;
            if (chunkIdx != prevChunkIdx) {
                if (groupCount > 0) {
                    batchGroupTo[groupCount - 1] = i;
                }
                batchGroupChunkIdx[groupCount] = chunkIdx;
                batchGroupFrom[groupCount] = i;
                groupCount++;
                prevChunkIdx = chunkIdx;
            }
        }
        batchGroupTo[groupCount - 1] = cowEnd;

        // Rebuild each affected chunk (no publish yet)
        if (groupCount > batchRebuilt.length) {
            final int newLen = Math.max(groupCount, batchRebuilt.length * 2);
            batchRebuilt = new Chunk[newLen][];
            batchOldChunks = new Chunk[newLen];
        }
        for (int g = 0; g < groupCount; g++) {
            final Chunk oldChunk = currentSnapshot.chunk(batchGroupChunkIdx[g]);
            batchOldChunks[g] = oldChunk;
            batchRebuilt[g] = cowRebuildMerged(
                    oldChunk,
                    oldChunk.getEntryCount(),
                    orders, payloads, payloadOffsets, payloadSizes,
                    batchGroupFrom[g], batchGroupTo[g]
            );
        }

        // Append tail entries into new chunks (no publish)
        final int tailFrom = cowEnd;
        final int tailCount = entryCount - tailFrom;
        Chunk[] tailChunks = null;
        int tailChunkCount = 0;
        if (tailCount > 0) {
            tailChunks = new Chunk[Math.max(1, (tailCount + 1) / 2)];
            Chunk tail = null;
            int tailWritten = 0;
            while (tailWritten < tailCount) {
                if (tail == null) {
                    tail = allocate();
                    initChunk(tail);
                    if (tailChunkCount >= tailChunks.length) {
                        tailChunks = Arrays.copyOf(tailChunks, tailChunks.length * 2);
                    }
                    tailChunks[tailChunkCount++] = tail;
                }
                final int idx = tailFrom + tailWritten;
                final int entrySize = Chunk.entrySize(payloadSizes[idx]);
                final int nextOff = tail.getDataWriteOffset();
                if (nextOff + entrySize > chunkSize) {
                    sealEnqueue(tail);
                    tail = null;
                    continue;
                }
                tail.writeEntry(nextOff, orders[idx], nextEntryVersion++,
                        payloads, payloadOffsets[idx], payloadSizes[idx]);
                tail.putDataWriteOffset(nextOff + entrySize);
                final int ec = tail.getEntryCount();
                if (ec == 0) {
                    tail.putMinOrder(orders[idx]);
                }
                tail.putMaxOrder(orders[idx]);
                tail.putEntryCountOrdered(ec + 1);
                tailWritten++;
            }
        }

        // Single-pass spine construction + single publish
        pubCowBatch(currentSnapshot, batchGroupChunkIdx, batchRebuilt, batchOldChunks,
                groupCount, tailChunks, tailChunkCount);

        // Null out chunk references to avoid retaining them across calls
        for (int g = 0; g < groupCount; g++) {
            batchRebuilt[g] = null;
            batchOldChunks[g] = null;
        }

        return entryCount;
    }

    /**
     * Inserts a new entry or replaces the first existing entry with the same
     * order (upsert semantics, assuming orders are unique). If no matching
     * entry exists and the order exceeds the current maximum, the entry is
     * appended; otherwise a copy-on-write rebuild is performed.
     *
     * @param order         ordering key of the entry
     * @param payload       buffer containing the entry payload
     * @param payloadOffset offset within {@code payload}
     * @param payloadSize   size of the payload in bytes
     * @throws IllegalArgumentException if the entry exceeds chunk data capacity
     */
    public void insertOrUpdateUnique(final long order,
                                     final AtomicBuffer payload,
                                     final int payloadOffset,
                                     final int payloadSize) {
        checkFits(payloadSize);
        drainSwaps();

        final ChunkSnapshot currentSnapshot = snapshot;

        final EntrySearch found = entrySearch.findFirst(currentSnapshot, order);
        if (found != null) {
            cowReplace(currentSnapshot, found.chunkIndex, found.entryIndex, order, payload, payloadOffset, payloadSize);
        } else if (currentSnapshot.isEmpty() || order > maxOrder(currentSnapshot)) {
            appendInternal(currentSnapshot, order, payload, payloadOffset, payloadSize);
        } else {
            cowInsert(currentSnapshot, order, payload, payloadOffset, payloadSize);
        }
    }

    /**
     * Inserts a new entry or replaces an existing entry whose payload is
     * considered equal by the supplied {@code eq} predicate. When multiple
     * entries share the same order, the group is scanned for a payload match;
     * if found the matching entry is replaced, otherwise the new entry is
     * inserted after the group.
     *
     * @param order         ordering key of the entry
     * @param payload       buffer containing the entry payload
     * @param payloadOffset offset within {@code payload}
     * @param payloadSize   size of the payload in bytes
     * @param eq            predicate used to compare payloads for equality
     * @throws IllegalArgumentException if the entry exceeds chunk data capacity
     */
    public void insertOrUpdateEqual(final long order,
                                    final AtomicBuffer payload,
                                    final int payloadOffset,
                                    final int payloadSize,
                                    final PayloadEquals eq) {
        checkFits(payloadSize);
        drainSwaps();

        final ChunkSnapshot currentSnapshot = snapshot;

        final EntrySearch found = entrySearch.findFirst(currentSnapshot, order);
        if (found != null) {
            final EntrySearch eqFound = entrySearch.findEqualInGroup(
                    currentSnapshot,
                    order,
                    payload,
                    payloadOffset,
                    payloadSize,
                    found.chunkIndex,
                    found.entryOffset,
                    found.entryIndex,
                    eq
            );
            if (eqFound != null) {
                cowReplace(
                        currentSnapshot,
                        eqFound.chunkIndex,
                        eqFound.entryIndex,
                        order,
                        payload,
                        payloadOffset,
                        payloadSize
                );
            } else {
                cowInsertAfterGroup(
                        currentSnapshot,
                        order,
                        payload,
                        payloadOffset,
                        payloadSize,
                        found.chunkIndex
                );
            }
        } else if (currentSnapshot.isEmpty() || order > maxOrder(currentSnapshot)) {
            appendInternal(
                    currentSnapshot,
                    order,
                    payload,
                    payloadOffset,
                    payloadSize
            );
        } else {
            cowInsert(
                    currentSnapshot,
                    order,
                    payload,
                    payloadOffset,
                    payloadSize
            );
        }
    }

    /**
     * Merge-rebuilds a single old chunk with a slice of sorted batch entries.
     * Walks both streams (old chunk entries and batch entries) in order,
     * emitting each to the {@link RebuildWriter}. Produces 1+ new chunks.
     *
     * @param oldChunk       the source chunk to merge into
     * @param oldEntryCount  number of entries in {@code oldChunk}
     * @param orders         batch ordering keys
     * @param payloads       batch payload buffer
     * @param payloadOffsets batch payload offsets
     * @param payloadSizes   batch payload sizes
     * @param from           inclusive start index in the batch arrays
     * @param to             exclusive end index in the batch arrays
     * @return array of newly built chunks
     */
    private Chunk[] cowRebuildMerged(final Chunk oldChunk,
                                     final int oldEntryCount,
                                     final long[] orders,
                                     final AtomicBuffer payloads,
                                     final int[] payloadOffsets,
                                     final int[] payloadSizes,
                                     final int from,
                                     final int to) {
        final int batchCount = to - from;
        int batchBytes = 0;
        for (int i = from; i < to; i++) {
            batchBytes += Chunk.entrySize(payloadSizes[i]);
        }
        final int oldData = oldChunk.getDataWriteOffset() - Chunk.HEADER_SIZE;
        final int est = Math.max(1, (oldData + batchBytes + dataCap - 1) / dataCap);
        rebuildWriter.reset(est);

        int oldOff = Chunk.HEADER_SIZE;
        int oldIdx = 0;
        int batchIdx = from;

        while (oldIdx < oldEntryCount && batchIdx < to) {
            final long oldOrder = oldChunk.entryOrder(oldOff);
            if (oldOrder <= orders[batchIdx]) {
                final int runStartOff = oldOff;
                final long runMinOrder = oldOrder;
                long runMaxOrder = oldOrder;
                int runCount = 0;
                while (oldIdx < oldEntryCount
                        && oldChunk.entryOrder(oldOff) <= orders[batchIdx]) {
                    runMaxOrder = oldChunk.entryOrder(oldOff);
                    oldOff += oldChunk.entryTotalSize(oldOff);
                    oldIdx++;
                    runCount++;
                }
                rebuildWriter.emitBulkCopy(oldChunk, runStartOff,
                        oldOff - runStartOff, runCount, runMinOrder, runMaxOrder);
            } else {
                rebuildWriter.emitNew(orders[batchIdx], nextEntryVersion++,
                        payloads, payloadOffsets[batchIdx], payloadSizes[batchIdx]);
                batchIdx++;
            }
        }

        if (oldIdx < oldEntryCount) {
            final long suffMinOrder = oldChunk.entryOrder(oldOff);
            rebuildWriter.emitBulkCopy(oldChunk, oldOff,
                    oldChunk.getDataWriteOffset() - oldOff,
                    oldEntryCount - oldIdx,
                    suffMinOrder, oldChunk.getMaxOrder());
        }

        while (batchIdx < to) {
            rebuildWriter.emitNew(orders[batchIdx], nextEntryVersion++,
                    payloads, payloadOffsets[batchIdx], payloadSizes[batchIdx]);
            batchIdx++;
        }

        return rebuildWriter.done();
    }

    /**
     * Constructs a new spine by splicing rebuilt chunk arrays in place of
     * old chunks, appending tail chunks, and publishing a single snapshot.
     * All replaced chunks are submitted for reclamation.
     *
     * @param forSnapshot  snapshot that was current when COW began
     * @param chunkIndexes indices of chunks being replaced (ascending)
     * @param rebuilt      rebuilt chunk arrays corresponding to each index
     * @param oldChunks    original chunks being replaced
     * @param groupCount   number of groups
     * @param tailChunks   newly allocated tail chunks (may be {@code null})
     * @param tailCount    number of tail chunks
     */
    private void pubCowBatch(final ChunkSnapshot forSnapshot,
                             final int[] chunkIndexes,
                             final Chunk[][] rebuilt,
                             final Chunk[] oldChunks,
                             final int groupCount,
                             final Chunk[] tailChunks,
                             final int tailCount) {
        invalidateWriterTail();
        drainSwapSearchStart = 0;

        final int oldLiveSize = forSnapshot.size();
        int totalRebuilt = 0;
        for (int g = 0; g < groupCount; g++) {
            totalRebuilt += rebuilt[g].length;
        }
        final int newLiveSize = oldLiveSize - groupCount + totalRebuilt + tailCount;

        final int newSegCount = newLiveSize == 0 ? 0
                : ((newLiveSize - 1) >> SEG_SHIFT) + 1;
        final Chunk[][] newCS = new Chunk[newSegCount][];
        final long[][] newES = new long[newSegCount][];
        for (int s = 0; s < newSegCount; s++) {
            newCS[s] = new Chunk[SEG_SIZE];
            newES[s] = new long[SEG_SIZE];
        }

        int dstPos = 0;
        int srcPos = 0;
        for (int g = 0; g < groupCount; g++) {
            final int chunkIdx = chunkIndexes[g];
            // Copy unaffected chunks before this group
            if (chunkIdx > srcPos) {
                copySegmentedElements(chunkSpine, epochSpine, srcPos,
                        newCS, newES, dstPos, chunkIdx - srcPos);
                dstPos += chunkIdx - srcPos;
            }
            // Splice rebuilt chunks
            for (int i = 0; i < rebuilt[g].length; i++) {
                newCS[dstPos >> SEG_SHIFT][dstPos & SEG_MASK] = rebuilt[g][i];
                newES[dstPos >> SEG_SHIFT][dstPos & SEG_MASK] = rebuilt[g][i].getChunkEpoch();
                dstPos++;
            }
            srcPos = chunkIdx + 1;
        }
        // Copy remaining unaffected chunks after last group
        if (srcPos < oldLiveSize) {
            copySegmentedElements(chunkSpine, epochSpine, srcPos,
                    newCS, newES, dstPos, oldLiveSize - srcPos);
            dstPos += oldLiveSize - srcPos;
        }
        // Append tail chunks
        for (int i = 0; i < tailCount; i++) {
            newCS[dstPos >> SEG_SHIFT][dstPos & SEG_MASK] = tailChunks[i];
            newES[dstPos >> SEG_SHIFT][dstPos & SEG_MASK] = tailChunks[i].getChunkEpoch();
            dstPos++;
        }

        chunkSpine = newCS;
        epochSpine = newES;
        spineUsed = newSegCount;
        chunksCount = newLiveSize;

        // Seal all chunks except the new tail
        for (int i = 0; i < newLiveSize - 1; i++) {
            final Chunk c = newCS[i >> SEG_SHIFT][i & SEG_MASK];
            if (!c.isSealed()) {
                sealEnqueue(c);
            }
        }

        publishSnapshot();

        // Reclaim old chunks
        for (int g = 0; g < groupCount; g++) {
            final Chunk old = oldChunks[g];
            if (!old.casEvictionState(Chunk.EVICTION_NONE, Chunk.EVICTION_IN_PROGRESS)) {
                old.casEvictionState(Chunk.EVICTION_CANDIDATE, Chunk.EVICTION_IN_PROGRESS);
            }
            if (old.isMmapBased()) {
                mmap.submitPendingReclamation(old);
            } else {
                heap.submitPendingReclamation(old);
            }
        }
    }

    /**
     * Internal append-batch that defers snapshot publication. Used by
     * {@link #insertBatch} when all entries go to the tail.
     */
    private int appendBatchInternal(final long[] orders,
                                    final AtomicBuffer payloads,
                                    final int[] payloadOffsets,
                                    final int[] payloadSizes,
                                    final int from,
                                    final int entryCount) {
        Chunk tail = tailChunk();
        if (tail != null && orders[from] < tail.getMaxOrder()) {
            return 0;
        }

        int writtenEntries = 0;
        while (writtenEntries < entryCount - from) {
            if (tail == null || tail.isSealed()) {
                tail = spineAppend();
            }

            final int currentEntryCount = tail.getEntryCount();
            int nextEntryOffset = tail.getDataWriteOffset();

            int tailWrittenEntries = 0;
            long tailMinOrder = (currentEntryCount == 0) ? Long.MAX_VALUE : tail.getMinOrder();
            long tailMaxOrder = (currentEntryCount == 0) ? Long.MIN_VALUE : tail.getMaxOrder();

            while (from + writtenEntries + tailWrittenEntries < entryCount) {
                final int idx = from + writtenEntries + tailWrittenEntries;
                final long order = orders[idx];
                final int payloadSize = payloadSizes[idx];
                final int entrySize = Chunk.entrySize(payloadSize);
                if (nextEntryOffset + entrySize > chunkSize) {
                    break;
                }
                if (order < tailMaxOrder) {
                    break;
                }

                tail.writeEntry(nextEntryOffset, order, nextEntryVersion++,
                        payloads, payloadOffsets[idx], payloadSize);
                nextEntryOffset += entrySize;
                if (order < tailMinOrder) {
                    tailMinOrder = order;
                }
                if (order > tailMaxOrder) {
                    tailMaxOrder = order;
                }
                tailWrittenEntries++;
            }
            if (tailWrittenEntries > 0) {
                tail.putDataWriteOffset(nextEntryOffset);
                if (currentEntryCount == 0) {
                    tail.putMinOrder(tailMinOrder);
                }
                tail.putMaxOrder(tailMaxOrder);
                tail.putEntryCountOrdered(currentEntryCount + tailWrittenEntries);
                writtenEntries += tailWrittenEntries;
            }

            final int nextIdx = from + writtenEntries;
            final boolean chunkFull = (nextIdx < entryCount)
                    ? nextEntryOffset + Chunk.entrySize(payloadSizes[nextIdx]) > chunkSize
                    : nextEntryOffset + Chunk.ENTRY_HEADER_SIZE > chunkSize;
            if (chunkFull) {
                sealEnqueue(tail);
                tail = null;
            }

            if (tailWrittenEntries == 0 && from + writtenEntries < entryCount) {
                break;
            }
        }
        if (writtenEntries > 0) {
            publishSnapshot();
        }
        return writtenEntries;
    }

    /**
     * Validates that a single entry with the given payload size fits
     * within the usable data area of a chunk.
     *
     * @param payloadSize payload size in bytes
     * @throws IllegalArgumentException if the entry is too large
     */
    private void checkFits(final int payloadSize) {
        if (Chunk.entrySize(payloadSize) > dataCap) {
            throw new IllegalArgumentException("entry too large: " + payloadSize);
        }
    }

    /**
     * Core append logic shared by the public append entry-points. Writing to
     * an existing tail chunk with available space performs zero object
     * allocations (hot path). A new {@link ChunkSnapshot} is only created
     * when the tail fills and a new chunk must be allocated.
     *
     * <p>The caller passes a previously read {@code forSnapshot} to
     * avoid a redundant volatile read of the {@code snapshot} field.
     *
     * @param forSnapshot   snapshot already read by the caller
     * @param order         ordering key of the entry
     * @param payload       buffer containing the entry payload
     * @param payloadOffset offset within {@code payload}
     * @param payloadSize   size of the payload in bytes
     * @return {@code true} if the entry was appended; {@code false} if the
     * order is strictly less than the tail's max order
     */
    private boolean appendInternal(final ChunkSnapshot forSnapshot,
                                   final long order,
                                   final AtomicBuffer payload,
                                   final int payloadOffset,
                                   final int payloadSize) {
        invalidateWriterTail();
        final Chunk tail = !forSnapshot.isEmpty()
                ? forSnapshot.chunk(forSnapshot.size() - 1) : null;

        if (tail != null && order < tail.getMaxOrder()) {
            return false;
        }

        final int entrySize = Chunk.entrySize(payloadSize);

        if (tail != null && !tail.isSealed()) {
            final int nextEntryOffset = tail.getDataWriteOffset();
            if (nextEntryOffset + entrySize <= chunkSize) {
                // HOT PATH - zero allocations, no volatile read
                final int entryCount = tail.getEntryCount();
                final long entryVersion = nextEntryVersion++;
                tail.writeEntry(nextEntryOffset, order, entryVersion, payload, payloadOffset, payloadSize);
                tail.putDataWriteOffset(nextEntryOffset + entrySize);
                tail.putMaxOrder(order);
                if (entryCount == 0) {
                    tail.putMinOrder(order);
                }
                tail.putEntryCountOrdered(entryCount + 1); // release fence
                return true;
            }
            sealEnqueue(tail);
        }

        final Chunk newChunk = allocate();
        initChunk(newChunk);
        final long entryVersion = nextEntryVersion++;
        newChunk.writeEntry(Chunk.HEADER_SIZE, order, entryVersion, payload, payloadOffset, payloadSize);
        newChunk.putDataWriteOffset(Chunk.HEADER_SIZE + entrySize);
        newChunk.putMinOrder(order);
        newChunk.putMaxOrder(order);
        newChunk.putEntryCountOrdered(1);
        pubAppend(newChunk);
        return true;
    }

    /**
     * Drains all pending heap-to-mmap swap notifications produced by the
     * eviction path. For each swap, the writer replaces the old heap chunk
     * reference with the new mmap chunk in-place and publishes a new
     * snapshot. Stale or mismatched swaps are discarded.
     *
     * <p><b>In-place array modification safety:</b> old snapshot readers
     * that still reference the shared backing arrays are safe because:
     * <ol>
     *   <li>The mmap chunk contains an identical byte-for-byte copy of the
     *       heap chunk data — readers get correct data either way.</li>
     *   <li>Reference and {@code long} writes are individually atomic on
     *       64-bit JVMs — no torn reads of array elements.</li>
     *   <li>The pin-validation protocol ({@link CursorSupport#acquirePin})
     *       detects buffer/epoch mismatches and triggers a
     *       {@code forceRefresh()}, so readers recover gracefully from any
     *       mixed (old chunk + new epoch) transient state.</li>
     *   <li>The subsequent volatile store of {@link #snapshot} provides a
     *       release fence, making both array writes visible to any thread
     *       that later reads {@code snapshot}.</li>
     * </ol>
     */
    private void drainSwaps() {
        PendingSwap swap;
        while ((swap = pendingSwaps.poll()) != null) {
            final int idx = findSwapIndex(swap);
            if (idx >= 0) {
                final long actual = swap.newMmapChunk.getChunkEpoch();
                if (actual != swap.newMmapEpoch) {
                    throw new IllegalStateException(
                            "mmap epoch drifted: expected=" + swap.newMmapEpoch
                                    + " actual=" + actual);
                }

                setChunkAt(idx, swap.newMmapChunk);
                setEpochAt(idx, swap.newMmapEpoch);
                publishSnapshot();
                drainSwapSearchStart = idx + 1;
                heap.submitPendingReclamation(swap.oldHeapChunk);
            } else {
                mmap.free(swap.newMmapChunk);
            }
        }
    }

    private int findSwapIndex(final PendingSwap swap) {
        final int start = Math.min(drainSwapSearchStart, chunksCount);
        for (int i = start; i < chunksCount; i++) {
            if (chunkAt(i).buffer() == swap.oldHeapChunk.buffer()
                    && epochAt(i) == swap.oldHeapEpoch) {
                return i;
            }
        }
        for (int i = 0; i < start; i++) {
            if (chunkAt(i).buffer() == swap.oldHeapChunk.buffer()
                    && epochAt(i) == swap.oldHeapEpoch) {
                return i;
            }
        }
        return -1;
    }


    /**
     * Returns the last chunk in the current snapshot, or {@code null} if the
     * sequence is empty. Zero allocations.
     *
     * @return tail chunk, or {@code null}
     */
    private Chunk tailChunk() {
        final ChunkSnapshot currentSnapshot = snapshot;
        return !currentSnapshot.isEmpty()
                ? currentSnapshot.chunk(currentSnapshot.size() - 1)
                : null;
    }

    /**
     * Allocates a new empty chunk, initialises its header, appends it to the
     * writer-owned array and publishes a new snapshot.
     *
     * @return the newly allocated and published chunk
     */
    private Chunk allocateAndPublish() {
        final Chunk result = allocate();
        initChunk(result);
        pubAppend(result);
        return result;
    }

    /**
     * Allocates and initialises a new chunk, appends it to the spine arrays
     * without publishing a snapshot. The caller is responsible for calling
     * {@link #publishSnapshot()} after all mutations are complete.
     *
     * @return the newly allocated chunk
     */
    private Chunk spineAppend() {
        final Chunk result = allocate();
        initChunk(result);
        ensureSegmentCapacity(chunksCount + 1);
        setChunkAt(chunksCount, result);
        setEpochAt(chunksCount, result.getChunkEpoch());
        chunksCount++;
        return result;
    }

    /**
     * Binary-searches the snapshot for the <b>leftmost</b> chunk whose order
     * range {@code [minOrder, maxOrder]} contains the given order. When a
     * matching chunk is found the search continues left ({@code high = mid - 1})
     * rather than returning immediately, so that when the same order spans
     * multiple consecutive chunks the first one is always returned. Callers
     * can then scan forward to cover all chunks in the group.
     *
     * @param forSnapshot snapshot to search
     * @param order       the order to locate
     * @return index of the leftmost chunk containing the order, or {@code -1}
     * if no chunk's range includes it
     */
    private static int searchChunk(final ChunkSnapshot forSnapshot,
                                   final long order) {
        int low = 0, high = forSnapshot.size() - 1, result = -1;
        while (low <= high) {
            final int mid = (low + high) >>> 1;
            final Chunk chunk = forSnapshot.chunk(mid);
            final long minOrder = chunk.getMinOrder();
            final long maxOrder = chunk.getMaxOrder();
            if (order < minOrder) {
                high = mid - 1;
            } else if (order > maxOrder) {
                low = mid + 1;
            } else {
                result = mid;
                high = mid - 1;
            }
        }
        return result;
    }

    /**
     * Binary-searches the snapshot for the <b>rightmost</b> chunk whose
     * {@code maxOrder <= order}. This is the chunk into which a new entry
     * with the given order should be inserted. Returns {@code 0} if the
     * order precedes all chunks.
     *
     * @param forSnapshot snapshot to search
     * @param order       the order to locate an insertion point for
     * @return target chunk index (always valid within the snapshot)
     */
    private static int insertionChunkIndex(final ChunkSnapshot forSnapshot,
                                           final long order) {
        int low = 0, high = forSnapshot.size() - 1, result = 0;
        while (low <= high) {
            final int mid = (low + high) >>> 1;
            if (forSnapshot.chunk(mid).getMaxOrder() <= order) {
                result = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return result;
    }

    /**
     * Same as {@link #searchChunk} but starts the binary search from
     * {@code lowHint}, exploiting the fact that sorted input produces
     * non-decreasing chunk indices.
     */
    private static int searchChunkFrom(final ChunkSnapshot forSnapshot,
                                       final long order,
                                       final int lowHint) {
        int low = lowHint, high = forSnapshot.size() - 1, result = -1;
        while (low <= high) {
            final int mid = (low + high) >>> 1;
            final Chunk chunk = forSnapshot.chunk(mid);
            final long minOrder = chunk.getMinOrder();
            final long maxOrder = chunk.getMaxOrder();
            if (order < minOrder) {
                high = mid - 1;
            } else if (order > maxOrder) {
                low = mid + 1;
            } else {
                result = mid;
                high = mid - 1;
            }
        }
        return result;
    }

    /**
     * Same as {@link #insertionChunkIndex} but starts the binary search from
     * {@code lowHint}, exploiting the fact that sorted input produces
     * non-decreasing chunk indices.
     */
    private static int insertionChunkIndexFrom(final ChunkSnapshot forSnapshot,
                                               final long order,
                                               final int lowHint) {
        int low = lowHint, high = forSnapshot.size() - 1, result = lowHint;
        while (low <= high) {
            final int mid = (low + high) >>> 1;
            if (forSnapshot.chunk(mid).getMaxOrder() <= order) {
                result = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return result;
    }

    /**
     * Returns the maximum order in the given snapshot (from the last chunk).
     *
     * @param forSnapshot non-empty snapshot
     * @return the maximum order value
     */
    private long maxOrder(final ChunkSnapshot forSnapshot) {
        return forSnapshot.chunk(forSnapshot.size() - 1).getMaxOrder();
    }

    /**
     * Replaces a single existing entry via copy-on-write. The target chunk is
     * rebuilt with the entry at {@code targetEntryIndex} replaced by the new
     * payload, and the result is published via {@link #pubCow}.
     *
     * @param forSnapshot      snapshot containing the target chunk
     * @param targetChunkIndex index of the chunk to rebuild
     * @param targetEntryIndex entry index within the chunk
     * @param order            ordering key of the replacement entry
     * @param payload          buffer containing the new payload
     * @param payloadOffset    offset within {@code payload}
     * @param payloadSize      size of the new payload in bytes
     */
    private void cowReplace(final ChunkSnapshot forSnapshot,
                            final int targetChunkIndex,
                            final int targetEntryIndex,
                            final long order,
                            final AtomicBuffer payload,
                            final int payloadOffset,
                            final int payloadSize) {
        final Chunk oldChunk = forSnapshot.chunk(targetChunkIndex);
        final int entryCount = oldChunk.getEntryCount();

        final Chunk[] rebuilt = rebuild(oldChunk,
                entryCount,
                targetEntryIndex,
                targetEntryIndex + 1,
                order,
                nextEntryVersion++,
                payload,
                payloadOffset,
                payloadSize
        );
        pubCow(forSnapshot, targetChunkIndex, rebuilt);
    }

    /**
     * Inserts a new entry at the correct ordered position via copy-on-write.
     * Locates the target chunk with {@link #insertionChunkIndex}, finds the
     * entry insertion point within it, rebuilds the chunk, and publishes
     * via {@link #pubCow}.
     *
     * @param forSnapshot   snapshot to operate on
     * @param order         ordering key of the new entry
     * @param payload       buffer containing the entry payload
     * @param payloadOffset offset within {@code payload}
     * @param payloadSize   size of the payload in bytes
     */
    private void cowInsert(final ChunkSnapshot forSnapshot,
                           final long order,
                           final AtomicBuffer payload,
                           final int payloadOffset,
                           final int payloadSize) {
        final int chunkIdx = insertionChunkIndex(forSnapshot, order);
        final Chunk oldChunk = forSnapshot.chunk(chunkIdx);
        final int oldEntryCount = oldChunk.getEntryCount();

        int entryOff = Chunk.HEADER_SIZE;
        int entryIdx;
        for (entryIdx = 0; entryIdx < oldEntryCount; entryIdx++) {
            if (oldChunk.entryOrder(entryOff) > order) {
                break;
            }
            entryOff += oldChunk.entryTotalSize(entryOff);
        }

        final Chunk[] rebuilt = rebuild(
                oldChunk,
                oldEntryCount,
                entryIdx,
                entryIdx,
                order,
                nextEntryVersion++,
                payload,
                payloadOffset,
                payloadSize
        );
        pubCow(forSnapshot, chunkIdx, rebuilt);
    }

    /**
     * Inserts a new entry immediately after the last entry in the group of
     * entries sharing the same order. If the group end cannot be found, falls
     * back to {@link #cowInsert}.
     *
     * @param forSnapshot     snapshot to operate on
     * @param order           ordering key of the new entry
     * @param payload         buffer containing the entry payload
     * @param payloadOffset   offset within {@code payload}
     * @param payloadSize     size of the payload in bytes
     * @param startChunkIndex chunk index hint from the prior search
     */
    private void cowInsertAfterGroup(final ChunkSnapshot forSnapshot,
                                     final long order,
                                     final AtomicBuffer payload,
                                     final int payloadOffset,
                                     final int payloadSize,
                                     final int startChunkIndex) {
        final GroupEndSearch end = groupEndSearch.find(forSnapshot, order, startChunkIndex);
        if (end == null) {
            cowInsert(forSnapshot, order, payload, payloadOffset, payloadSize);
            return;
        }

        final Chunk oldChunk = forSnapshot.chunk(end.chunkIndex);
        final int entryCount = oldChunk.getEntryCount();

        final Chunk[] rebuilt = rebuild(
                oldChunk,
                entryCount,
                end.entryIndex,
                end.entryIndex,
                order,
                nextEntryVersion++,
                payload,
                payloadOffset,
                payloadSize
        );
        pubCow(forSnapshot, end.chunkIndex, rebuilt);
    }

    /**
     * Builds one or more new chunks that are a modified copy of {@code oldChunk}.
     *
     * <p>Entries outside the {@code [rebuildStartIndexIncluded, rebuildEndIndexExcluded)}
     * range are copied verbatim. The new entry ({@code order}/{@code payload}) is emitted
     * at position {@code rebuildStartIndexIncluded}, and old entries inside the range are
     * dropped. This yields two modes of operation depending on how the range is specified:
     *
     * <ul>
     *   <li><b>Insert</b> — {@code rebuildStartIndexIncluded == rebuildEndIndexExcluded}:
     *       no existing entries are removed; the new entry is spliced in before
     *       the entry at {@code rebuildStartIndexIncluded}.</li>
     *   <li><b>Replace</b> — {@code rebuildStartIndexIncluded < rebuildEndIndexExcluded}:
     *       entries in the range are removed and replaced by the single new entry.</li>
     * </ul>
     *
     * <p>If the resulting data exceeds the capacity of a single chunk, it is
     * automatically split across multiple output chunks via the rebuild-writer
     * spill mechanism.
     *
     * @param oldChunk                  the source chunk whose entries are being rebuilt
     * @param oldEntryCount             number of entries in {@code oldChunk}
     * @param rebuildStartIndexIncluded first entry index (inclusive) to replace/insert-before
     * @param rebuildEndIndexExcluded   last entry index (exclusive) to replace; equal to
     *                                  {@code rebuildStartIndexIncluded} for pure insert
     * @param order                     order value of the new entry
     * @param version                   monotonic version assigned to the new entry
     * @param payload                   buffer containing the new entry's payload
     * @param payloadOffset             offset within {@code payload}
     * @param payloadSize               size of the payload in bytes
     * @return array of newly built chunks; the caller must publish them via
     * {@link #pubCow} and must not reuse them in subsequent rebuilds
     */
    private Chunk[] rebuild(final Chunk oldChunk,
                            final int oldEntryCount,
                            final int rebuildStartIndexIncluded,
                            final int rebuildEndIndexExcluded,
                            final long order,
                            final long version,
                            final AtomicBuffer payload,
                            final int payloadOffset,
                            final int payloadSize) {
        final int newEntrySize = Chunk.entrySize(payloadSize);
        final int oldData = oldChunk.getDataWriteOffset() - Chunk.HEADER_SIZE;

        // Single walk to compute prefix/removed/suffix byte boundaries
        int off = Chunk.HEADER_SIZE;
        int lastPrefixOffset = Chunk.HEADER_SIZE;
        for (int i = 0; i < rebuildStartIndexIncluded; i++) {
            lastPrefixOffset = off;
            off += oldChunk.entryTotalSize(off);
        }
        final int prefixEndOffset = off;
        final int prefixBytes = prefixEndOffset - Chunk.HEADER_SIZE;

        int removed = 0;
        for (int i = rebuildStartIndexIncluded; i < rebuildEndIndexExcluded; i++) {
            final int sz = oldChunk.entryTotalSize(off);
            removed += sz;
            off += sz;
        }
        final int suffixStartOff = off;
        final int suffixBytes = oldChunk.getDataWriteOffset() - suffixStartOff;

        final int est = Math.max(1, (oldData - removed + newEntrySize + dataCap - 1) / dataCap);
        rebuildWriter.reset(est);

        if (rebuildStartIndexIncluded > 0) {
            rebuildWriter.emitBulkCopy(oldChunk,
                    Chunk.HEADER_SIZE, prefixBytes, rebuildStartIndexIncluded,
                    oldChunk.getMinOrder(),
                    oldChunk.entryOrder(lastPrefixOffset));
        }

        rebuildWriter.emitNew(order, version, payload, payloadOffset, payloadSize);

        final int suffixCount = oldEntryCount - rebuildEndIndexExcluded;
        if (suffixCount > 0) {
            rebuildWriter.emitBulkCopy(
                    oldChunk,
                    suffixStartOff,
                    suffixBytes,
                    suffixCount,
                    oldChunk.entryOrder(suffixStartOff),
                    oldChunk.getMaxOrder()
            );
        }

        return rebuildWriter.done();
    }

    /**
     * Creates and publishes a new {@link ChunkSnapshot} from the current
     * writer state.  All snapshot creation goes through this method to
     * ensure head-offset bookkeeping is consistent.
     */
    private void publishSnapshot() {
        snapshot = new ChunkSnapshot(
                chunkSpine,
                epochSpine,
                chunksCount - writerHeadOffset,
                writerHeadOffset,
                snapshotVersion++
        );
    }

    /**
     * Appends a chunk to the writer-owned arrays and publishes a new snapshot.
     * O(1) amortised — extends the arrays in-place. Old snapshot readers only
     * access indices below the previous length, so the new slot is invisible
     * to them until the snapshot is published.
     *
     * @param chunk the chunk to append
     */
    private void pubAppend(final Chunk chunk) {
        ensureSegmentCapacity(chunksCount + 1);
        setChunkAt(chunksCount, chunk);
        setEpochAt(chunksCount, chunk.getChunkEpoch());
        chunksCount++;
        publishSnapshot();
    }

    /**
     * Replaces the chunk at {@code chunkIndex} with the rebuilt chunks in
     * {@code chunksToPub}, publishes a new snapshot, and retires the old chunk.
     * Segments entirely before the affected one are reused; segments from the
     * affected position onward are freshly allocated.
     *
     * <p><b>Sealing policy:</b> every chunk except the new tail is sealed and
     * enqueued for eviction. This is aggressive — freshly allocated heap chunks
     * produced by the COW may be evicted to mmap before they are read — but it
     * keeps the invariant simple: only the very last chunk in a sequence is
     * ever unsealed.
     *
     * @param forSnapshot snapshot that was current when the COW began
     * @param chunkIndex  index of the old chunk being replaced
     * @param chunksToPub rebuilt chunks to splice in
     */
    private void pubCow(final ChunkSnapshot forSnapshot,
                        final int chunkIndex,
                        final Chunk[] chunksToPub) {
        invalidateWriterTail();
        drainSwapSearchStart = 0;
        final Chunk updatingChunk = forSnapshot.chunk(chunkIndex);
        final int oldLiveSize = forSnapshot.size();
        final int newLiveSize = oldLiveSize - 1 + chunksToPub.length;

        final int newSegCount = newLiveSize == 0 ? 0
                : ((newLiveSize - 1) >> SEG_SHIFT) + 1;
        final Chunk[][] newCS = new Chunk[newSegCount][];
        final long[][] newES = new long[newSegCount][];
        for (int s = 0; s < newSegCount; s++) {
            newCS[s] = new Chunk[SEG_SIZE];
            newES[s] = new long[SEG_SIZE];
        }

        copySegmentedElements(chunkSpine, epochSpine, 0,
                newCS, newES, 0, chunkIndex);

        for (int i = 0; i < chunksToPub.length; i++) {
            final int di = chunkIndex + i;
            newCS[di >> SEG_SHIFT][di & SEG_MASK] = chunksToPub[i];
            newES[di >> SEG_SHIFT][di & SEG_MASK] = chunksToPub[i].getChunkEpoch();
        }

        copySegmentedElements(chunkSpine, epochSpine, chunkIndex + 1,
                newCS, newES, chunkIndex + chunksToPub.length,
                oldLiveSize - chunkIndex - 1);

        chunkSpine = newCS;
        epochSpine = newES;
        spineUsed = newSegCount;
        chunksCount = newLiveSize;

        for (int i = 0; i < newLiveSize - 1; i++) {
            final Chunk c = newCS[i >> SEG_SHIFT][i & SEG_MASK];
            if (!c.isSealed()) {
                sealEnqueue(c);
            }
        }
        publishSnapshot();

        if (!updatingChunk.casEvictionState(
                Chunk.EVICTION_NONE, Chunk.EVICTION_IN_PROGRESS
        )) {
            updatingChunk.casEvictionState(
                    Chunk.EVICTION_CANDIDATE,
                    Chunk.EVICTION_IN_PROGRESS
            );
        }
        if (updatingChunk.isMmapBased()) {
            mmap.submitPendingReclamation(updatingChunk);
        } else {
            heap.submitPendingReclamation(updatingChunk);
        }
    }

    /**
     * Ensures enough segments are allocated to hold at least {@code needed}
     * elements. Grows the spine pointer array (small) if necessary and
     * allocates new fixed-size segments on demand — never copies existing
     * segment data.
     *
     * @param needed minimum required element capacity
     */
    private void ensureSegmentCapacity(final int needed) {
        if (needed <= 0) {
            return;
        }
        final int neededSegs = ((needed - 1) >> SEG_SHIFT) + 1;
        if (neededSegs <= spineUsed) {
            return;
        }
        if (neededSegs > chunkSpine.length) {
            final int newSpineLen = Math.max(neededSegs, chunkSpine.length << 1);
            chunkSpine = Arrays.copyOf(chunkSpine, newSpineLen);
            epochSpine = Arrays.copyOf(epochSpine, newSpineLen);
        }
        for (int s = spineUsed; s < neededSegs; s++) {
            chunkSpine[s] = new Chunk[SEG_SIZE];
            epochSpine[s] = new long[SEG_SIZE];
        }
        spineUsed = neededSegs;
    }

    private Chunk chunkAt(final int index) {
        return chunkSpine[index >> SEG_SHIFT][index & SEG_MASK];
    }

    private void setChunkAt(final int index, final Chunk chunk) {
        chunkSpine[index >> SEG_SHIFT][index & SEG_MASK] = chunk;
    }

    private long epochAt(final int index) {
        return epochSpine[index >> SEG_SHIFT][index & SEG_MASK];
    }

    private void setEpochAt(final int index, final long epoch) {
        epochSpine[index >> SEG_SHIFT][index & SEG_MASK] = epoch;
    }

    /**
     * Copies elements between segmented arrays, using {@code System.arraycopy}
     * for bulk transfers within each segment boundary.
     *
     * @param srcC       source chunk spine segments
     * @param srcE       source epoch spine segments
     * @param srcPosInit source linear index (global position)
     * @param dstC       destination chunk spine segments
     * @param dstE       destination epoch spine segments
     * @param dstPosInit destination linear index
     * @param countInit  number of elements to copy
     */
    private static void copySegmentedElements(
            final Chunk[][] srcC, final long[][] srcE, final int srcPosInit,
            final Chunk[][] dstC, final long[][] dstE, final int dstPosInit,
            final int countInit) {
        int srcPos = srcPosInit;
        int dstPos = dstPosInit;
        int count = countInit;
        while (count > 0) {
            final int srcSeg = srcPos >> SEG_SHIFT;
            final int srcOff = srcPos & SEG_MASK;
            final int dstSeg = dstPos >> SEG_SHIFT;
            final int dstOff = dstPos & SEG_MASK;
            final int batch = Math.min(count,
                    Math.min(SEG_SIZE - srcOff, SEG_SIZE - dstOff));
            System.arraycopy(srcC[srcSeg], srcOff, dstC[dstSeg], dstOff, batch);
            System.arraycopy(srcE[srcSeg], srcOff, dstE[dstSeg], dstOff, batch);
            srcPos += batch;
            dstPos += batch;
            count -= batch;
        }
    }

    /**
     * Allocates a chunk, trying heap first. If the heap is exhausted, triggers
     * one eviction cycle and retries. Falls back to mmap allocation if heap
     * space is still unavailable.
     *
     * @return a freshly allocated chunk (heap- or mmap-backed)
     */
    private Chunk allocate() {
        Chunk chunk = heap.tryAllocate();
        if (chunk != null) {
            return chunk;
        }
        evictOne();
        drainSwaps();
        chunk = heap.tryAllocate();
        if (chunk != null) {
            return chunk;
        }
        return mmap.allocate();
    }

    /**
     * Attempts to evict a single sealed heap chunk to mmap storage. Polls the
     * shared eviction queue up to {@link #SWAP_CHECK_INTERVAL} times, skipping
     * stale or already-evicted candidates. On success, copies the chunk data to
     * a new mmap chunk and posts a {@link PendingSwap} so the owning sequence's
     * writer can integrate it on its next drain cycle.
     */
    private void evictOne() {
        for (int a = 0; a < SWAP_CHECK_INTERVAL; a++) {
            final EvictionQueue.Item e = evictQ.poll();
            if (e == null) {
                return;
            }
            final Chunk heapChunk = e.heapChunk;
            if (heapChunk.isMmapBased()) {
                continue;
            }
            if (heapChunk.getChunkEpoch() != e.epoch) {
                continue;
            }
            if (!heapChunk.casEvictionState(
                    Chunk.EVICTION_CANDIDATE, Chunk.EVICTION_IN_PROGRESS)) {
                continue;
            }

            if (!directPin(heapChunk, e.epoch)) {
                continue;
            }
            try {
                final Chunk mmapChunk = mmap.allocate();
                mmapChunk.copyChunkDataFrom(heapChunk);
                mmapChunk.putEntryCountOrdered(heapChunk.getEntryCount());
                e.owner.pendingSwaps.offer(new PendingSwap(heapChunk, mmapChunk));
            } finally {
                CursorSupport.decRef(heapChunk);
            }
            return;
        }
    }

    /**
     * Seals the chunk (marks it read-only) and, if it is heap-backed and not
     * already an eviction candidate, enqueues it for potential eviction to mmap.
     *
     * @param chunk the chunk to seal
     */
    private void sealEnqueue(final Chunk chunk) {
        chunk.seal();
        if (chunk.isHeapBased()
                && chunk.casEvictionState(Chunk.EVICTION_NONE, Chunk.EVICTION_CANDIDATE)) {
            evictQ.enqueue(new EvictionQueue.Item(this, chunk));
        }
    }

    /**
     * Submits a chunk for deferred reclamation via the appropriate allocator
     * (heap or mmap), depending on the chunk's backing storage.
     *
     * @param chunk the chunk to free
     */
    private void freeChunk(final Chunk chunk) {
        if (chunk.isMmapBased()) {
            mmap.free(chunk);
        } else {
            heap.submitPendingReclamation(chunk);
        }
    }

    /**
     * Attempts to increment the chunk's reference count (pin it) while
     * verifying the epoch has not changed. Uses a CAS spin loop with
     * {@link Thread#onSpinWait()} back-off. If the epoch drifts after a
     * successful CAS, the ref-count is decremented and the pin fails.
     *
     * @param chunk         the chunk to pin
     * @param expectedEpoch the epoch the chunk must still have
     * @return {@code true} if the chunk was successfully pinned with the
     * expected epoch; the caller must eventually call
     * {@link CursorSupport#decRef} to release it
     */
    private static boolean directPin(final Chunk chunk,
                                     final long expectedEpoch) {
        for (int s = 0; s < SWAP_CHECK_INTERVAL; s++) {
            final int refCount = chunk.getRefCount();
            if (refCount < 0) {
                return false;
            }
            if (chunk.casRefCount(refCount, refCount + 1)) {
                if (chunk.getChunkEpoch() == expectedEpoch) {
                    return true;
                }
                CursorSupport.decRef(chunk);
                return false;
            }
            Thread.onSpinWait();
        }
        return false;
    }

    /**
     * Initializes a freshly allocated chunk's header: sets the data write
     * offset to just past the header and zeroes the entry count.
     *
     * @param chunk the chunk to initialize
     */
    private static void initChunk(final Chunk chunk) {
        chunk.putDataWriteOffset(Chunk.HEADER_SIZE);
        chunk.putEntryCountPlain(0);
    }

    /**
     * Re-initializes a chunk that is already in the rebuild-writer scratch
     * array (skips allocation). Zeroes the writer metadata, resets the data
     * write offset, and clears the entry count.
     *
     * @param chunk the chunk to re-initialize
     */
    private static void initChunkReuse(final Chunk chunk) {
        chunk.zeroWriterMeta();
        chunk.putDataWriteOffset(Chunk.HEADER_SIZE);
        chunk.putEntryCountPlain(0);
    }

    /**
     * Encapsulates mutable search-result state and the entry-lookup
     * methods that populate it. Writer-thread only.
     */
    private static final class EntrySearch {
        private int chunkIndex;
        private int entryOffset;
        private int entryIndex;

        /**
         * Locates the first entry with the given order, starting from the
         * leftmost chunk whose order range includes it (found via binary
         * search). Scans forward through chunks until a match is found or
         * all candidates are exhausted.
         *
         * @param forSnapshot snapshot to search
         * @param order       the order to locate
         * @return {@code this} with populated fields if found, {@code null} otherwise
         */
        EntrySearch findFirst(final ChunkSnapshot forSnapshot,
                              final long order) {
            int chunkIdx = searchChunk(forSnapshot, order);
            if (chunkIdx < 0) {
                return null;
            }
            while (chunkIdx < forSnapshot.size()) {
                final Chunk chunk = forSnapshot.chunk(chunkIdx);
                final int entryCount = chunk.getEntryCount();
                int entryOff = Chunk.HEADER_SIZE;
                for (int i = 0; i < entryCount; i++) {
                    final long entryOrder = chunk.entryOrder(entryOff);
                    if (entryOrder == order) {
                        chunkIndex = chunkIdx;
                        entryOffset = entryOff;
                        entryIndex = i;
                        return this;
                    }
                    if (entryOrder > order) {
                        return null;
                    }
                    entryOff += chunk.entryTotalSize(entryOff);
                }
                chunkIdx++;
            }
            return null;
        }

        /**
         * Starting from a known position within a group of entries sharing
         * the same order, scans forward for an entry whose payload matches
         * according to the supplied equality predicate.
         *
         * @param forSnapshot     snapshot to search
         * @param order           the shared order of the group
         * @param payload         buffer containing the payload to match against
         * @param payloadOffset   offset within {@code payload}
         * @param payloadSize     size of the payload in bytes
         * @param startChunkIndex chunk index to start scanning from
         * @param startEntryOffset byte offset of the first entry to examine
         * @param startEntryIndex entry index within the starting chunk
         * @param eq              predicate used to compare payloads for equality
         * @return {@code this} with populated fields if a match is found, {@code null} otherwise
         */
        EntrySearch findEqualInGroup(final ChunkSnapshot forSnapshot,
                                     final long order,
                                     final AtomicBuffer payload,
                                     final int payloadOffset,
                                     final int payloadSize,
                                     final int startChunkIndex,
                                     final int startEntryOffset,
                                     final int startEntryIndex,
                                     final PayloadEquals eq) {
            int chunkIdx = startChunkIndex;
            int entryOff = startEntryOffset;
            int entryIdx = startEntryIndex;
            while (chunkIdx < forSnapshot.size()) {
                final Chunk chunk = forSnapshot.chunk(chunkIdx);
                final int entryCount = chunk.getEntryCount();
                while (entryIdx < entryCount) {
                    if (chunk.entryOrder(entryOff) != order) {
                        return null;
                    }
                    final int entryPayloadSize = chunk.entryPayloadSize(entryOff);
                    if (eq.eq(
                            chunk.buffer(),
                            entryOff + Chunk.ENTRY_HEADER_SIZE,
                            entryPayloadSize,
                            payload,
                            payloadOffset,
                            payloadSize
                    )) {
                        chunkIndex = chunkIdx;
                        entryOffset = entryOff;
                        entryIndex = entryIdx;
                        return this;
                    }
                    entryOff += chunk.entryTotalSize(entryOff);
                    entryIdx++;
                }
                chunkIdx++;
                entryOff = Chunk.HEADER_SIZE;
                entryIdx = 0;
            }
            return null;
        }
    }

    /**
     * Encapsulates mutable group-end search-result state and the
     * lookup method that populates it. Writer-thread only.
     */
    private static final class GroupEndSearch {
        private int chunkIndex;
        private int entryIndex;

        /**
         * Finds the position immediately after the last entry with the given
         * order. Starts scanning from {@code startChunkIndex} to avoid a
         * redundant binary search when the caller already knows which chunk
         * contains the group.
         *
         * @param forSnapshot     snapshot to search
         * @param order           the order whose group end to locate
         * @param startChunkIndex chunk index to start scanning from
         * @return {@code this} with populated fields if found, {@code null} otherwise
         */
        private GroupEndSearch find(final ChunkSnapshot forSnapshot,
                                    final long order,
                                    final int startChunkIndex) {
            int chunkIdx = startChunkIndex;
            while (chunkIdx < forSnapshot.size()) {
                final Chunk chunk = forSnapshot.chunk(chunkIdx);
                final int entryCount = chunk.getEntryCount();
                int entryOff = Chunk.HEADER_SIZE;
                boolean found = false;
                for (int i = 0; i < entryCount; i++) {
                    final long mt = chunk.entryOrder(entryOff);
                    if (mt == order) {
                        found = true;
                    } else if (mt > order) {
                        chunkIndex = chunkIdx;
                        entryIndex = i;
                        return this;
                    }
                    entryOff += chunk.entryTotalSize(entryOff);
                }
                if (found) {
                    if (chunkIdx + 1 < forSnapshot.size()
                            && forSnapshot.chunk(chunkIdx + 1).getMinOrder() == order) {
                        chunkIdx++;
                        continue;
                    }
                    chunkIndex = chunkIdx;
                    entryIndex = entryCount;
                    return this;
                }
                break;
            }
            return null;
        }
    }

    /**
     * Encapsulates the mutable state and logic for rebuilding a chunk's
     * entries into one or more new chunks. Reused across COW operations
     * to amortise scratch-array allocations. Writer-thread only.
     */
    private final class RebuildWriter {
        private Chunk[] scratch = new Chunk[4];
        private int chunkIndex;
        private int entryOffset;
        private int entryCount;
        private long minOrder;
        private long maxOrder;

        /**
         * Prepares the rebuild writer for a new session. Ensures the scratch
         * array has at least {@code estimatedChunkCount} slots, allocating or
         * re-initialising chunks as needed, and resets all cursor state
         * (chunk index, entry offset, entry count, min/max order).
         *
         * @param estimatedChunkCount minimum number of scratch chunks to provision
         */
        private void reset(final int estimatedChunkCount) {
            if (scratch.length < estimatedChunkCount) {
                scratch = new Chunk[estimatedChunkCount];
            }
            for (int i = 0; i < estimatedChunkCount; i++) {
                if (scratch[i] == null) {
                    scratch[i] = allocate();
                    initChunk(scratch[i]);
                } else {
                    initChunkReuse(scratch[i]);
                }
            }
            chunkIndex = 0;
            entryOffset = Chunk.HEADER_SIZE;
            entryCount = 0;
            minOrder = Long.MAX_VALUE;
            maxOrder = Long.MIN_VALUE;
        }

        /**
         * Writes a brand-new entry into the current scratch chunk, spilling
         * to the next chunk if insufficient space remains.
         *
         * @param order         ordering key of the new entry
         * @param version       monotonic version assigned to the new entry
         * @param payload       buffer containing the entry payload
         * @param payloadOffset offset within {@code payload}
         * @param payloadSize   size of the payload in bytes
         */
        private void emitNew(final long order,
                             final long version,
                             final AtomicBuffer payload,
                             final int payloadOffset,
                             final int payloadSize) {
            final int entrySize = Chunk.entrySize(payloadSize);
            spill(entrySize);
            scratch[chunkIndex].writeEntry(entryOffset, order, version, payload, payloadOffset, payloadSize);
            advise(entrySize, order);
        }

        /**
         * Copies a single entry verbatim from {@code srcChunk} into the current
         * scratch chunk, spilling to the next chunk if insufficient space remains.
         *
         * @param srcChunk       source chunk containing the entry
         * @param srcEntryOffset byte offset of the entry within {@code srcChunk}
         */
        private void emitCopy(final Chunk srcChunk,
                              final int srcEntryOffset) {
            final int entrySize = srcChunk.entryTotalSize(srcEntryOffset);
            final long order = srcChunk.entryOrder(srcEntryOffset);
            spill(entrySize);
            scratch[chunkIndex].buffer().putBytes(entryOffset, srcChunk.buffer(), srcEntryOffset, entrySize);
            advise(entrySize, order);
        }

        /**
         * Copies a contiguous block of entries from {@code srcChunk} in one shot
         * when the block fits in the current scratch chunk. Falls back to
         * per-entry {@link #emitCopy} when a spill would be needed.
         *
         * @param srcChunk       source chunk containing the entries to copy
         * @param srcEntryOffset byte offset within {@code srcChunk} where the block starts
         * @param byteLen        total byte length of the contiguous block
         * @param count          number of entries in the block
         * @param minOrd         minimum order among the block's entries
         * @param maxOrd         maximum order among the block's entries
         */
        private void emitBulkCopy(final Chunk srcChunk,
                                  final int srcEntryOffset,
                                  final int byteLen,
                                  final int count,
                                  final long minOrd,
                                  final long maxOrd) {
            if (count == 0) {
                return;
            }
            if (entryOffset + byteLen <= chunkSize) {
                scratch[chunkIndex].buffer().putBytes(entryOffset, srcChunk.buffer(), srcEntryOffset, byteLen);
                entryOffset += byteLen;
                entryCount += count;
                if (minOrd < minOrder) {
                    minOrder = minOrd;
                }
                if (maxOrd > maxOrder) {
                    maxOrder = maxOrd;
                }
            } else {
                int off = srcEntryOffset;
                for (int i = 0; i < count; i++) {
                    emitCopy(srcChunk, off);
                    off += srcChunk.entryTotalSize(off);
                }
            }
        }

        /**
         * Finalizes the current rebuild session and returns the completed chunks.
         *
         * <p>Performs three steps:
         * <ol>
         *   <li>Flushes the last in-progress chunk by writing its metadata
         *       (entry count, min/max order, data write offset) if it contains
         *       any entries.</li>
         *   <li>Frees any pre-allocated but unused scratch chunks back to the
         *       allocator and nulls their scratch slots.</li>
         *   <li>Copies the used chunks into a right-sized result array and
         *       <b>nulls out their scratch slots</b> so they are never mistakenly
         *       reused by a future rebuild — once published via {@link #pubCow},
         *       these chunks may be submitted for reclamation, and the allocator
         *       could reassign them to a different sequence.</li>
         * </ol>
         *
         * @return array of newly built chunks ready for publishing
         */
        private Chunk[] done() {
            if (entryCount > 0) {
                finish(
                        scratch[chunkIndex],
                        entryOffset,
                        entryCount,
                        minOrder,
                        maxOrder
                );
            }
            final int used = chunkIndex + 1;
            for (int i = used; i < scratch.length && scratch[i] != null; i++) {
                freeChunk(scratch[i]);
                scratch[i] = null;
            }
            final Chunk[] out = new Chunk[used];
            System.arraycopy(scratch, 0, out, 0, used);
            for (int i = 0; i < used; i++) {
                scratch[i] = null;
            }
            return out;
        }

        private void spill(final int size) {
            if (entryOffset + size > chunkSize && entryCount > 0) {
                finish(scratch[chunkIndex], entryOffset, entryCount, minOrder, maxOrder);
                chunkIndex++;
                if (chunkIndex >= scratch.length) {
                    scratch = Arrays.copyOf(scratch, scratch.length + 1);
                }
                if (scratch[chunkIndex] == null) {
                    scratch[chunkIndex] = allocate();
                }
                initChunk(scratch[chunkIndex]);
                entryOffset = Chunk.HEADER_SIZE;
                entryCount = 0;
                minOrder = Long.MAX_VALUE;
                maxOrder = Long.MIN_VALUE;
            }
        }

        private void advise(final int size,
                            final long order) {
            entryOffset += size;
            entryCount++;
            if (order < minOrder) {
                minOrder = order;
            }
            if (order > maxOrder) {
                maxOrder = order;
            }
        }

        private void finish(final Chunk chunk,
                            final int dataWriteOffset,
                            final int entryCount,
                            final long minOrder,
                            final long maxOrder) {
            chunk.putDataWriteOffset(dataWriteOffset);
            chunk.putMinOrder(minOrder);
            chunk.putMaxOrder(maxOrder);
            chunk.putEntryCountOrdered(entryCount);
        }
    }
}
