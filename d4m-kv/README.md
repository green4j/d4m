# d4m-kv

Tiered, high-performance key-value store with hash-sharded segments and cascading eviction from heap memory through memory-mapped file tiers.

## Architecture

```
+--------------------------------------------------------------+
|  KeyValueStorage                                             |
|      |                                                       |
|      V                                                       |
|  KeyValueRing  (hash-sharded, lock-striped)                  |
|      |                                                       |
|      +-- Segment 0 --- Tier 0 (heap) -> Tier 1 (mmap) -> ... |
|      +-- Segment 1 --- Tier 0 (heap) -> Tier 1 (mmap) -> ... |
|      +-- ...                                                 |
|      +-- Segment N --- Tier 0 (heap) -> Tier 1 (mmap) -> ... |
|                                                              |
+--------------------------------------------------------------+
```

### Key Components

**KeyValueStorage** -- public entry point with a fluent `Builder`. Wraps a `KeyValueRing` and manages lifecycle.

**KeyValueRing** -- distributes keys across N power-of-two segments via hash sharding. Each segment has its own `StampedLock` (write lock for puts, read lock for gets), providing lock striping for concurrent access.

**KeyValueSegment** -- manages a chain of tiers (hot to cold). Writes always target tier 0. When a tier's circular buffer wraps, evicted entries cascade to the next tier. If no more tiers can be created, the terminal `EvictionListener` is notified.

**Tier** -- a single-level open-addressing hash map (linear probing, 0.75 load factor) backed by a circular `KeyValueBuffer`. Supports in-place update for values of the same size.

**MmapTierFactory** -- creates tier 0 on heap (or off-heap) and subsequent tiers backed by memory-mapped files. Configurable max tier count.

### Data Flow

**Write path:**
1. `put(key, value)` hashes the key and selects a segment.
2. Write lock acquired on that segment.
3. Entry written to tier 0 (hottest).
4. If tier 0's circular buffer wraps, evicted entries cascade down (tier 0 -> tier 1 -> ... -> tier N).
5. When the coldest tier evicts and no further tier can be created, the segment's `EvictionListener` is called (if set; the default `KeyValueStorage` builder configures an unbounded tier chain and no listener).

**Read path:**
1. `get(key)` hashes the key and selects a segment.
2. Read lock acquired on that segment.
3. Tiers are scanned hot-to-cold; first match returned.

### Eviction Model

Eviction is **implicit** -- driven by the circular buffer wrapping inside each tier. Older entries are pushed down the tier chain as newer entries arrive. This provides natural LRU-like behavior without explicit eviction policies or background threads.

## Implementation details

### Tier internals

Each `Tier` is an open-addressing hash map with **linear probing** and a 0.75 load factor. Metadata is packed into a single `long` per slot for cache friendliness:

| Bit range | Field | Purpose |
|---|---|---|
| 63 | occupied | Sign bit -- slot in use |
| 32–62 | hash (31 bit) | Saved hash for fast non-key comparisons during probes |
| 0–31 | index | Offset of the entry's header in the underlying `KeyValueBuffer` |

When the metadata array exceeds the load factor it doubles; existing keys are rehashed into the new array but their stored data stays put (the `index` field keeps pointing into the same `KeyValueBuffer` slot).

On eviction, the slot is cleared and the tail of its probe cluster is **compacted** so subsequent lookups don't see false "missing" tombstones (`compactCluster`).

### `KeyValueBuffer` is a forward-only ring

The hot data area is a `KeyValueBuffer` -- a ring buffer that writes entries contiguously head-to-tail. When the head meets the tail, the oldest entries are evicted **in insertion order** (FIFO). Combined with hash-sharded segments, this gives a per-shard FIFO eviction that approximates LRU under typical traffic without tracking access times.

Two write paths:

- **Insert**: appends a new header + key + value at the head, evicting older entries from the tail as needed to free space.
- **In-place update**: when the same key is re-`put` with a value of the same size, the bytes are overwritten where they are. Position in the ring does not change, so the entry doesn't get "promoted" to the head.

Each entry header is 16 bytes (two longs) and carries the key/value sizes plus the in-buffer index of the next entry, forming a linked list used by the eviction walker.

### Cascading tier chain

When `KeyValueBuffer`'s eviction fires inside tier *i*, the segment's `tierLinker` forwards the evicted entry to tier *i+1*. If tier *i+1* doesn't exist yet, the `TierFactory` creates it on demand. Coldest tier's evictions either flow to the user-supplied `EvictionListener` or are silently dropped, depending on configuration.

This means **only the segments that need cold storage allocate it**. A segment whose hot region never wraps stays single-tier forever.

### Hash sharding and shuffle multiplier

`KeyValueRing` distributes keys across `ringSize` real segments but allocates `ringSize * ringShuffleMultiplier` array slots, each pointing to one of the real segments. The mapping is `slot[i + j * ringSize] = segment[i]`. This **decouples the hash's low bits from segment identity** -- a workload whose keys cluster on the low bits doesn't fight over a single segment's `StampedLock`. Both sizes are rounded up to powers of two so segment selection stays a single AND.

### Mmap file lifecycle

`MmapTierFactory` namespaces each segment's mmap files by segment id and the `MMAP_FILE_PREFIX` (`mmap-kv-`). At construction it **deletes any stale files** from a previous run (matched by prefix/extension) -- KV state is **not persistent across restarts**. Mapped regions grow monotonically and are never unmapped while the process is alive; OS pages are reclaimed via the virtual-memory lifecycle.

`MmapTierFactory.Listener` exposes hooks for the three lifecycle events (folder cleanup, in-memory tier creation, mmap tier creation) so applications can observe storage growth without instrumenting internals.

## API

### Creating a Store

```java
KeyValueStorage storage = KeyValueStorage.builder()
    .withTotalMainMemory(128 * 1024 * 1024)        // 128 MB heap tier
    .withHeapMainMemory()                          // or .withOffHeapMainMemory()
    .withMmapFileSize(1024 * 1024 * 1024)          // 1 GB per mmap file
    .withRingSize(8)                               // 8 sharded segments
    .withRingShuffleMultiplier(32)                 // segment-array spread factor
    .withMemoryMappedFilesFolder(new File("/tmp/d4m"))
    .withPrepareMmapFilesOnStart()                 // create the first mmap tier eagerly
    .build();
```

### Put and Get

```java
// Keys and values are byte arrays accessed via AtomicBuffer
AtomicBuffer keyBuf = new UnsafeBuffer(keyBytes);
AtomicBuffer valBuf = new UnsafeBuffer(valueBytes);

// Put (insert or update in-place if value size matches)
storage.put(keyBuf, 0, keyBytes.length, valBuf, 0, valueBytes.length);

// Get with a value consumer callback
storage.get(keyBuf, 0, keyBytes.length, (buffer, offset, size) -> {
    // process the value bytes
});
```

### Eviction Listener

`KeyValueStorage.Builder` configures an **unbounded tier chain** (`maxNumberOfTiers = Integer.MAX_VALUE`) and consequently does not expose an `EvictionListener` -- evictions just spill into a new mmap tier on demand. To receive eviction callbacks (e.g. to persist to an external store), drop down to the lower-level API and assemble a `KeyValueRing` yourself:

```java
EvictionListener evictionListener =
    (notifier, hash, kv, keyOff, keySize, valOff, valSize) -> {
        // entry leaving the coldest tier -- persist externally or log
    };

MmapTierFactory factory = new MmapTierFactory(
        segmentId,
        memoryTierSize, /* offHeap */ false, /* initialCapacity */ 65536,
        mmapFileTierSize, /* initialCapacity */ 65536,
        mmapFilesFolder,
        /* maxNumberOfTiers */ 3, // bounded -> evictions actually fire
        listener);

KeyValueSegment segment = new KeyValueSegment(1, factory, evictionListener);
```

## Configuration Reference

| Builder Option | Default | Description |
|----------------|---------|-------------|
| `withTotalMainMemory` | 128 MB | Heap budget for tier 0 across all segments |
| `withHeapMainMemory` / `withOffHeapMainMemory` | heap | Backing memory for the hot tier |
| `withMmapFileSize` | 1 GB | Size of each memory-mapped file |
| `withRingSize` | 8 | Number of sharded segments (rounded to power of 2) |
| `withRingShuffleMultiplier` | 32 | Segment-array spread factor (rounded to power of 2) |
| `withPrepareMmapFilesOnStart` | `true` | Eagerly create the first mmap tier so spills don't pay for it later |
| `withMemoryMappedFilesFolder` | `java.io.tmpdir` | Directory for mmap backing files |
| `withMmapTierListener` | `System.out` logger | Hook for cleanup / tier-creation events |

## Concurrency Model

- **Thread-safe** for concurrent `put` and `get` from multiple threads.
- Lock striping via `StampedLock` per segment minimizes contention.
- Exclusive write lock for `put`, shared read lock (`StampedLock.readLock()`) for `get`.

## License

MIT
