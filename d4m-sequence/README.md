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

EvictionQueue evictQ = new EvictionQueue();

Sequence sequence = new Sequence("my-sequence", chunkSize, heap, mmap, evictQ);
```

### Writing Entries

```java
// Single append (hot path -- zero allocations when chunk has space)
boolean ok = sequence.append(order, payload, offset, size);

// Batch append (pre-sorted, non-decreasing orders)
int written = sequence.appendBatch(orders, payloads, offsets, sizes, count);

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
```

### Snapshots

To be used in Writer only, because of possible eviction of a Chunk:
```java
ChunkSnapshot snap = sequence.snapshot();
int chunkCount = snap.size();
long version = snap.version();
Chunk chunk = snap.chunk(0);
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
