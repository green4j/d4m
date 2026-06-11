# d4m-sequence

Ordered, append-optimised sequence of entries stored across a chain of fixed-size chunks. Supports concurrent single-writer/multi-reader access with lock-free snapshot publication and cooperative heap-to-mmap chunk eviction.

## Architecture

```
+-----------------------------------------------------------------+
|  Writer Thread                                                  |
|                                                                 |
| append/insertBatch/... -> Chunk Spine (segmented array)         |
|                               |                                 |
|                               V                                 |
|                      +----------------+                         |
|                      | publishSnapshot| (volatile store)        |
|                      +----------------+                         |
|                               |                                 |
+-------------------------------+---------------------------------|
|  Reader Threads               V                                 |
|                                                                 |
|  ForwardCursor/BackwardCursor                                   |
|      reads ChunkSnapshot (immutable, all fields final)          |
|      pins chunks via ref-count for safe concurrent access       |
+-----------------------------------------------------------------+
```

### Storage Tiers

Entries are written into fixed-size **chunks**. The sequence manages two tiers:

- **Heap tier** (`HeapChunkAllocator`) -- fast writes, bounded memory budget. Chunks are pre-allocated in contiguous slabs. Lock-free Treiber stack for free-chunk management.
- **Mmap tier** (`MmapChunkAllocator`) -- overflow/cold storage backed by memory-mapped files. Virtually unbounded capacity.

When the heap is exhausted, sealed chunks are cooperatively evicted to mmap by the writer thread via a shared `EvictionQueue`. Readers transparently access both heap and mmap chunks through the same cursor API.

### Copy-on-Write (COW) Inserts

Inserts into the middle of the sequence trigger a COW rebuild of the affected chunk(s). The old chunk is replaced atomically (single snapshot publish), and reclaimed once all readers release their pins.

Batch inserts (`insertBatch`) merge multiple entries per affected chunk in a single rebuild, and publish one snapshot for the entire batch.

### Snapshots

Every mutation publishes a new `ChunkSnapshot` via a volatile store. Readers acquire a snapshot reference and iterate over a consistent, immutable view of the sequence. No locks on the read path.

Snapshot internals worth knowing:

- **Segmented spine.** A snapshot stores chunks in a fixed-size `Chunk[][]` (and a parallel `long[][]` of epochs) rather than a flat array. On `append`, the writer just writes into the next slot of the current segment — no array copy is needed. COW paths allocate fresh segments only for the affected region. Heap→mmap swaps mutate segment elements in place (safe — see "Pin protocol" below). Only the small spine pointer array is ever resized.
- **Monotonic version.** Every published snapshot carries a strictly increasing `version()`. Cursors use it to detect layout changes between calls and reposition without re-scanning.
- **`headOffset`.** A logical-index → physical-index translation kept on the snapshot. Reserved for future compaction of dead leading chunks; readers holding stale snapshots stay safe because each snapshot keeps its own head offset.

## Implementation details

### Chunk layout

Each chunk's `AtomicBuffer` starts with a fixed 256-byte header split across two cache lines:

| Bytes | Field group | Why separated |
|---|---|---|
| 0–127 (line 0) | ref-count, eviction state, epoch | Identity / lifecycle — **never bulk-copied** between chunks. CAS-updated. |
| 128–255 (line 1) | entry count, sealed flag, min/max order, data write offset | Writer metadata — **copyable** between chunks during heap→mmap swap and COW rebuilds. |

The split lets `copyChunkDataFrom` bulk-copy line 1 + entry data without touching line 0 (which the receiving chunk owns).

Per-entry header is 24 bytes (`order`, monotonic `version`, payload length); payload is 8-byte aligned.

`CACHE_LINE = 128` matches Apple M-series; on x86-64 (64-byte lines) the two hot regions still never share a single line.

### Pin protocol (reader safety vs. eviction)

Readers must hold a reference to chunk data across many operations (sometimes thousands of calls). The writer can swap a heap chunk's buffer for an mmap one underneath them, and freed heap chunks can be returned to a Treiber stack and re-used. The contract that keeps readers safe:

1. **Ref-count.** A cursor calls `CursorSupport.acquirePin(snapshot, chunkIndex, state)` which CAS-bumps the chunk's ref-count. While ref-count > 0, the allocator's `drainPendingReclamation` won't recycle the chunk (it CAS-sets `0 → -1` to claim it; non-zero ref-count fails the CAS).
2. **Epoch verification.** Every allocation stamps a fresh `chunkEpoch`. The pin records the expected epoch at acquire time and re-verifies it after the CAS. If the epoch drifted, the pin is released and the cursor re-pins.
3. **Eviction state machine.** A heap chunk transitions `NONE → CANDIDATE → IN_PROGRESS`. Only `CANDIDATE` chunks are picked from the shared `EvictionQueue`; the CAS to `IN_PROGRESS` ensures one-shot processing. Writers about to read from a chunk pre-emptively CAS it to `IN_PROGRESS` so concurrent eviction can't recycle it underneath them.
4. **Cooperative drain.** Sealing a heap chunk enqueues it for eviction. The next writer thread that runs out of free heap chunks calls `evictOne`, which copies the chunk's bytes to a freshly allocated mmap chunk and posts a `PendingSwap`. The owning sequence's writer drains pending swaps on its next call and replaces the heap reference with the mmap one in place. Old snapshots remain valid because the buffer identity / epoch check on the next `acquirePin` triggers a `forceRefresh()`.

### Writer-local tail cache

The `append` hot path reads neither the volatile `snapshot` nor `getEntryCount` / `isSealed` / `getDataWriteOffset` from the chunk. The writer keeps a thread-local snapshot of the tail chunk (`writerTail`, `writerTailWriteOffset`, `writerTailEntryCount`, `writerTailMaxOrder`) and uses plain stores for chunk metadata. A `putEntryCountOrdered` at the end acts as the single release fence for readers. The cache is invalidated whenever the tail rolls over, COW publishes, or a swap drains.

### Eviction cadence

The writer doesn't drain pending swaps after every single `append`. It checks every `SWAP_CHECK_INTERVAL` (64) appends — a power-of-two so the check is `(appendsSinceSwapCheck & (INTERVAL-1)) == 0`. The `drainSwapSearchStart` index tracks the last-touched spine position so the lookup is amortized O(1) for append-only workloads.

### Mmap files are never unmapped

`MmapChunkAllocator` writes into memory-mapped files that grow monotonically and are **never unmapped**. The OS reclaims pages through its own virtual-memory lifecycle. This is intentional: readers may hold pinned chunks pointing into a mapped region indefinitely; unmapping would risk a SIGBUS. Files are created with `deleteOnExit`, and stale files from previous runs are cleaned up at allocator construction.

## API

### Creating a Sequence

```java
AtomicLong epochCounter = new AtomicLong();

HeapChunkAllocator heap = new HeapChunkAllocator(
    chunkSize,       // bytes per chunk (e.g. 64 * 1024)
    maxHeapBytes,    // total heap budget
    slabSize,        // slab allocation granularity
    epochCounter
);

MmapChunkAllocator mmap = new MmapChunkAllocator(
    chunkSize,
    mmapDirectory,   // folder for .tmp mmap files
    false,           // preAllocate
    epochCounter
);

EvictionQueue evictQ = new EvictionQueue(); // we share an EvictionQueue among Sequences because of cooperative eviction implemented

Sequence sequence = new Sequence("my-sequence", chunkSize, heap, mmap, evictQ);
```

### Writing Entries

```java
// Single append (hot path -- zero allocations when chunk has space)
boolean ok = sequence.append(order, payload, offset, size);

// Batch append (pre-sorted, non-decreasing orders)
int appended = sequence.appendBatch(orders, payloads, offsets, sizes, count);

// Batch insert (pre-sorted, may insert into middle via COW)
int inserted = sequence.insertBatch(orders, payloads, offsets, sizes, count);

// Upsert by order (unique keys)
sequence.insertOrUpdateUnique(order, payload, offset, size);

// Upsert with payload equality predicate
sequence.insertOrUpdateEqual(order, payload, offset, size, payloadEquals);
```

### Reading Entries

```java
// Forward iteration
ForwardCursor cursor = new ForwardCursor(sequence);
cursor.seekTo(startOrder);
cursor.next(batchSize, (owner, order, buffer, offset, size) -> {
    // process entry
});
cursor.close();

// Backward iteration
BackwardCursor back = new BackwardCursor(sequence);
back.seekToEnd();
back.next(batchSize, (owner, order, buffer, offset, size) -> {
    // process entry newest-first
});
back.close();

// Bounded range scan
cursor.nextUntil(batchSize, upperBoundOrder, consumer);

// Merge multiple sequences into one ordered stream
MergedForwardCursor merged = MergedForwardCursor.create(sequenceA, sequenceB);
merged.next(batchSize, (sourceIndex, owner, order, buffer, offset, size) -> {
    // entries delivered in global order across sources
});

// Same shape exists for reverse iteration: MergedBackwardCursor
```

`MergedForwardCursor` / `MergedBackwardCursor` run a min/max-heap over the underlying cursors so the global next entry is delivered in O(log N) per step, where N is the number of sources. The consumer signature carries an extra `sourceIndex` so callers know which sequence the entry came from.

### Snapshots

`sequence.snapshot()` is itself a non-blocking volatile read and is safe from any thread, but the **direct `Chunk` handles** it returns are only safe to dereference from the writer thread (or while holding a pin via `CursorSupport`). The supported reader path is through cursors, which acquire pins for you.

```java
ChunkSnapshot snap = sequence.snapshot();
int chunkCount = snap.size();
long version = snap.version();
Chunk chunk = snap.chunk(0); // direct dereference: writer-thread only
```

## Concurrency Model

- **Single writer** -- all mutation methods (`append`, `insert`, `insertBatch`, etc.) must be called from a single thread.
- **Multiple readers** -- `ForwardCursor`, `BackwardCursor`, and `snapshot()` are safe to call from any thread. Cursors auto-reposition when the underlying chunk layout changes due to COW or eviction.

## Configuration

| Parameter | Description |
|-----------|-------------|
| `chunkSize` | Size of each chunk in bytes. Determines max entry size and memory granularity. |
| `maxHeapBytes` | Total heap budget for the heap allocator. Controls when eviction starts. |
| `slabSize` | Heap slab allocation size (must be >= `chunkSize`). |
| `mmapDirectory` | Directory for memory-mapped overflow files. |

## License

MIT
