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
import io.github.green4j.d4m.common.BitSupport;

/**
 * Low-level accessor for a fixed-size chunk of sequence data. A chunk
 * stores a contiguous block of ordered entries with a two-cache-line
 * header: cache-line 0 holds identity/lifecycle fields (ref-count,
 * eviction state, epoch) that are never bulk-copied, and cache-line 1
 * holds writer metadata (entry count, sealed flag, min/max order,
 * data write offset) that is copyable between chunks.
 */
public class Chunk {
    /**
     * Cache-line width in bytes. 128 on some architects like Apple M-series;
     * safe for x86-64 (two 64-byte lines) because the two hot regions still
     * never share a single 64-byte line.
     */
    public static final int CACHE_LINE = 128;

    // CACHE-LINE 0 (bytes 0–127) - identity, NEVER bulk-copied
    public static final int REF_COUNT_OFFSET = 0; // int (CAS)
    public static final int EVICTION_STATE_OFFSET = REF_COUNT_OFFSET + 4; // int (CAS / ordered)
    public static final int CHUNK_EPOCH_OFFSET = EVICTION_STATE_OFFSET + 4; // long (volatile)
    // 16–127 Reserved/padding

    public static final int IDENTITY_SIZE = CACHE_LINE; // 128

    // CACHE-LINE 1 (bytes 128–255) - writer metadata, copyable
    public static final int ENTRY_COUNT_OFFSET = IDENTITY_SIZE; // 128
    public static final int SEALED_OFFSET = ENTRY_COUNT_OFFSET + 4; // 132
    public static final int MIN_ORDER_OFFSET = SEALED_OFFSET + 4; // 136
    public static final int MAX_ORDER_OFFSET = MIN_ORDER_OFFSET + 8; // 144
    public static final int DATA_WRITE_OFFSET = MAX_ORDER_OFFSET + 8; // 152
    // 156–255 Padding

    public static final int HEADER_SIZE = 2 * CACHE_LINE; // 256

    // Eviction lifecycle states
    public static final int EVICTION_NONE = 0;
    public static final int EVICTION_CANDIDATE = 1;
    public static final int EVICTION_IN_PROGRESS = 2;

    // Per-entry header (24 bytes, total 8-aligned)
    public static final int ENTRY_ORDER_OFFSET = 0; // long
    public static final int ENTRY_VERSION_OFFSET = ENTRY_ORDER_OFFSET + 8; // long
    public static final int ENTRY_PAYLOAD_LENGTH_OFFSET = ENTRY_VERSION_OFFSET + 8; // int (+4 implicit pad)

    public static final int ENTRY_HEADER_SIZE = 24;

    /**
     * Computes the 8-byte-aligned total entry size including the entry header.
     *
     * @param payloadLen payload length in bytes
     * @return aligned total entry size
     */
    public static int entrySize(final int payloadLen) {
        return BitSupport.alignToLong(ENTRY_HEADER_SIZE + payloadLen);
    }

    protected final AtomicBuffer buffer;

    /**
     * Creates a chunk backed by the given buffer.
     *
     * @param buffer the atomic buffer holding chunk data
     */
    public Chunk(final AtomicBuffer buffer) {
        this.buffer = buffer;
    }

    /**
     * Returns the underlying atomic buffer.
     *
     * @return the buffer backing this chunk
     */
    public AtomicBuffer buffer() {
        return buffer;
    }

    /**
     * Reads the reference count with volatile semantics.
     *
     * @return current reference count
     */
    public int getRefCount() {
        return buffer.getIntVolatile(REF_COUNT_OFFSET);
    }

    /**
     * Atomically sets the reference count if it currently equals {@code exp}.
     *
     * @param exp expected current value
     * @param upd new value to set
     * @return {@code true} if the CAS succeeded
     */
    public boolean casRefCount(final int exp, final int upd) {
        return buffer.compareAndSetInt(REF_COUNT_OFFSET, exp, upd);
    }

    /**
     * Stores the reference count with ordered (release) semantics.
     *
     * @param v value to store
     */
    public void putRefCountOrdered(final int v) {
        buffer.putIntOrdered(REF_COUNT_OFFSET, v);
    }

    /**
     * Reads the eviction state with volatile semantics.
     *
     * @return current eviction state ({@link #EVICTION_NONE},
     * {@link #EVICTION_CANDIDATE}, or {@link #EVICTION_IN_PROGRESS})
     */
    public int getEvictionState() {
        return buffer.getIntVolatile(EVICTION_STATE_OFFSET);
    }

    /**
     * Stores the eviction state with ordered (release) semantics.
     *
     * @param v new eviction state
     */
    public void putEvictionStateOrdered(final int v) {
        buffer.putIntOrdered(EVICTION_STATE_OFFSET, v);
    }

    /**
     * Atomically sets the eviction state if it currently equals {@code exp}.
     *
     * @param exp expected current value
     * @param upd new value to set
     * @return {@code true} if the CAS succeeded
     */
    public boolean casEvictionState(final int exp, final int upd) {
        return buffer.compareAndSetInt(EVICTION_STATE_OFFSET, exp, upd);
    }

    /**
     * Reads the chunk epoch with volatile semantics.
     *
     * @return current chunk epoch
     */
    public long getChunkEpoch() {
        return buffer.getLongVolatile(CHUNK_EPOCH_OFFSET);
    }

    /**
     * Stores the chunk epoch with volatile semantics.
     *
     * @param v new chunk epoch
     */
    public void putChunkEpoch(final long v) {
        buffer.putLongVolatile(CHUNK_EPOCH_OFFSET, v);
    }

    /**
     * Reads the entry count with volatile semantics, establishing an
     * acquire fence for subsequent plain reads of entry data.
     *
     * @return current entry count
     */
    public int getEntryCount() {
        return buffer.getIntVolatile(ENTRY_COUNT_OFFSET);
    }

    /**
     * Stores the entry count with ordered (release) semantics, acting
     * as a release fence for all preceding entry writes.
     *
     * @param v new entry count
     */
    public void putEntryCountOrdered(final int v) {
        buffer.putIntOrdered(ENTRY_COUNT_OFFSET, v);
    }

    /**
     * Stores the entry count with plain (non-volatile) semantics.
     * Suitable only when no concurrent readers are expected.
     *
     * @param v new entry count
     */
    public void putEntryCountPlain(final int v) {
        buffer.putInt(ENTRY_COUNT_OFFSET, v);
    }

    /**
     * Returns {@code true} if this chunk has been sealed (marked read-only).
     *
     * @return whether the chunk is sealed
     */
    public boolean isSealed() {
        return buffer.getIntVolatile(SEALED_OFFSET) == 1;
    }

    /**
     * Marks this chunk as sealed (read-only) using an ordered store.
     */
    public void seal() {
        buffer.putIntOrdered(SEALED_OFFSET, 1);
    }

    /**
     * Reads the minimum order value with plain semantics. Safe to call
     * after a volatile read of entry count establishes happens-before.
     *
     * @return minimum order among entries in this chunk
     */
    public long getMinOrder() {
        return buffer.getLong(MIN_ORDER_OFFSET);
    }

    /**
     * Stores the minimum order value with plain semantics.
     *
     * @param v minimum order value
     */
    public void putMinOrder(final long v) {
        buffer.putLong(MIN_ORDER_OFFSET, v);
    }

    /**
     * Reads the maximum order value with plain semantics. Safe to call
     * after a volatile read of entry count establishes happens-before.
     *
     * @return maximum order among entries in this chunk
     */
    public long getMaxOrder() {
        return buffer.getLong(MAX_ORDER_OFFSET);
    }

    /**
     * Stores the maximum order value with plain semantics.
     *
     * @param v maximum order value
     */
    public void putMaxOrder(final long v) {
        buffer.putLong(MAX_ORDER_OFFSET, v);
    }

    /**
     * Reads the minimum order value with volatile semantics for use in
     * concurrent reposition and search operations.
     *
     * @return minimum order among entries in this chunk
     */
    public long getMinOrderVolatile() {
        return buffer.getLongVolatile(MIN_ORDER_OFFSET);
    }

    /**
     * Reads the maximum order value with volatile semantics for use in
     * concurrent reposition and search operations.
     *
     * @return maximum order among entries in this chunk
     */
    public long getMaxOrderVolatile() {
        return buffer.getLongVolatile(MAX_ORDER_OFFSET);
    }

    /**
     * Reads the current data write offset with plain semantics.
     *
     * @return byte offset just past the last written entry
     */
    public int getDataWriteOffset() {
        return buffer.getInt(DATA_WRITE_OFFSET);
    }

    /**
     * Stores the data write offset with plain semantics.
     *
     * @param v byte offset just past the last written entry
     */
    public void putDataWriteOffset(final int v) {
        buffer.putInt(DATA_WRITE_OFFSET, v);
    }

    /**
     * Reads the order value of the entry at the given byte offset.
     *
     * @param o byte offset of the entry within the chunk
     * @return the entry's order value
     */
    public long entryOrder(final int o) {
        return buffer.getLong(o + ENTRY_ORDER_OFFSET);
    }

    /**
     * Reads the version of the entry at the given byte offset.
     *
     * @param o byte offset of the entry within the chunk
     * @return the entry's monotonic version
     */
    public long entryVersion(final int o) {
        return buffer.getLong(o + ENTRY_VERSION_OFFSET);
    }

    /**
     * Reads the payload size of the entry at the given byte offset.
     *
     * @param o byte offset of the entry within the chunk
     * @return the entry's payload size in bytes
     */
    public int entryPayloadSize(final int o) {
        return buffer.getInt(o + ENTRY_PAYLOAD_LENGTH_OFFSET);
    }

    /**
     * Returns the total 8-byte-aligned size of the entry at the given byte offset.
     *
     * @param o byte offset of the entry within the chunk
     * @return total aligned entry size (header + payload)
     */
    public int entryTotalSize(final int o) {
        return entrySize(entryPayloadSize(o));
    }

    /**
     * Writes a complete entry (order, version, and payload) at the given byte offset.
     *
     * @param offset        byte offset within the chunk to write at
     * @param order         ordering key of the entry
     * @param version       monotonic version of the entry
     * @param payload       buffer containing the payload data
     * @param payloadOffset offset within {@code payload}
     * @param payloadSize   size of the payload in bytes
     */
    public void writeEntry(final int offset,
                           final long order,
                           final long version,
                           final AtomicBuffer payload,
                           final int payloadOffset,
                           final int payloadSize) {
        buffer.putLong(offset + ENTRY_ORDER_OFFSET, order);
        buffer.putLong(offset + ENTRY_VERSION_OFFSET, version);
        buffer.putInt(offset + ENTRY_PAYLOAD_LENGTH_OFFSET, payloadSize);
        buffer.putBytes(offset + ENTRY_HEADER_SIZE, payload, payloadOffset, payloadSize);
    }

    /**
     * Copies writer-metadata + data from source chunk.
     * Bytes [0, IDENTITY_SIZE) are <b>never touched</b>.
     *
     * <p>The volatile read of {@code entryCount} on the source chunk
     * acts as an acquire fence, ensuring the subsequent plain read of
     * {@code dataWriteOffset} (and all entry data written before
     * the release-store of entryCount by the writer) is visible.</p>
     *
     * @param src the source chunk to copy writer-metadata and entry data from
     */
    public void copyChunkDataFrom(final Chunk src) {
        src.getEntryCount(); // volatile read - acquire fence
        final int dataEnd = src.getDataWriteOffset(); // plain read, now safe
        buffer.putBytes(IDENTITY_SIZE, src.buffer, IDENTITY_SIZE, dataEnd - IDENTITY_SIZE);
    }

    /**
     * Zeros only cache-line 1 (writer metadata). Identity fields in
     * cache-line 0 are left untouched so the ref-count protocol is safe.
     */
    public void zeroWriterMeta() {
        for (int i = IDENTITY_SIZE; i < HEADER_SIZE; i += 8) {
            buffer.putLong(i, 0L);
        }
    }

    /**
     * Returns {@code true} if this chunk is backed by heap memory.
     *
     * @return whether the chunk uses a heap-allocated buffer
     */
    public boolean isHeapBased() {
        return !isMmapBased();
    }

    /**
     * Returns {@code true} if this chunk is backed by a memory-mapped file.
     *
     * @return whether the chunk uses a direct (mmap) buffer
     */
    public boolean isMmapBased() {
        return buffer.byteBuffer().isDirect();
    }
}
