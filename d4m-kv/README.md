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

## Memory Consumption

This section lets you predict how much memory a workload needs before you run it. All sizes are exact byte counts; `1M` below means the binary mebibyte, `1M = 1024 * 1024 = 1_048_576` bytes.

Two knobs drive every entry's footprint:

- Every entry is stored 8-byte aligned. Define `align8(x) = (x + 7) & ~7`.
- Every entry also occupies exactly **one 8-byte metadata slot** in its tier's open-addressing table (the packed `[occupied | hash | index]` long).

### Storing `N` key-values

For a single entry with key length `k` and value length `v` bytes:

- **Payload bytes** (in the `KeyValueBuffer`): `E = align8(16 + k + v)`, where `16` is the two-long entry header (`KeyValueBuffer.HEADER_SIZE`).
- **Metadata bytes**: `8` (one slot).

So the total **used** memory for `N` entries is:

```
used ~ Σ (align8(16 + kᵢ + vᵢ))   +   8 * N
        └─────── payload ───────┘     └ metadata ┘
```

When keys/values are fixed size this simplifies to `used ~ N * (align8(16 + k + v) + 8)`.

**Footprint** — for `N = 1,000,000` with key `k = 12 + digits` and value `v = 200 + digits` (the `TieredKeyValueStorageUsage` scenario, `-Dd4m.kv.count=1000000`; ~90% of indices are 6 digits, so the dominant bucket is `E = align8(16 + 18 + 206) = 240`): `~ 236.5M`.

**Overhead per entry** — footprint bytes beyond the raw key+value: `align8(16 + k + v) + 8 - (k + v)`. This is a **fixed** `16` (entry header) + `8` (metadata slot) = `24 B`, plus a **variable** `0..7` alignment padding — so **24–31 bytes** total. For the scenario above the payload is already 8-aligned, so the overhead is exactly `24 B`.

**Total fixed overhead** — `~ 24 * N` = `24,000,000 B ~ 22.9M`, about 10% of the `236.5M` footprint.

**Used vs. allocated.** The formulas above are *used* memory (live bytes). *Allocated* memory is always larger and is driven by three rounding rules:

- The metadata table only grows in powers of two and stays under a **0.75 load factor** (`Tier.LOAD_FACTOR`), so allocated slots ~ `N / 0.75` rounded up to a power of two per tier.
- The tier-0 heap buffer is a fixed budget per segment (`totalMainMemory / ringSize`), pre-allocated whole regardless of fill.
- Each mmap overflow tier maps a large file (up to ~1 GB) even though only the written prefix is touched — the OS keeps the rest as sparse/untouched pages. Compare *used*, not *allocated*, mmap bytes for real consumption.

### Storing `N` lists of `M` entries (`KeyListStorage`)

`KeyListStorage` layers a multimap over `KeyValueStorage` using two kinds of underlying key-values (see `USER_KEY_PREFIX = 1`, `SYNTHETIC_KEY_SIZE = 8`, `METADATA_VALUE_SIZE = 8`):

- **One metadata key-value per list**: key = `1 + k` (a 1-byte prefix + the user key), value = `8` → `Emeta = align8(16 + (1 + k) + 8) = align8(25 + k)`.
- **One entry key-value per value**: key = `8` (synthetic), value = user value `v` → `Eentry = align8(16 + 8 + v) = align8(24 + v)`.

Each of these is still a full key-value, so each also costs one 8-byte metadata slot:

```
used ~ N * (Emeta + 8)   +   N * M * (Eentry + 8)
       └─ per-list meta ┘     └──── per value ────┘

underlying key-values = N * (1 + M)
```

**Footprint** — for `N = 100,000` lists of `M = 10` entries with user key `k = 5 + digits` and value `v ~ 203 + digits` (the `KeyListStorageUsage` scenario, `-Dd4m.klist.lists=100000 -Dd4m.klist.entries=10`; `Eentry = 232` for every entry): `~ 233.46M` across `N * (1 + M) = 1,100,000` underlying key-values.

**Overhead** — two kinds:

- Per value entry (footprint beyond the raw value `v`): `align8(24 + v) + 8 - v` = a **fixed** `16` (entry header) + `8` (synthetic key) + `8` (metadata slot) = `32 B`, plus a **variable** `0..7` alignment padding = **32–39 bytes**.
- Per list (a **fixed** overhead regardless of `M`, and it also stores the user key once): the metadata key-value = `align8(25 + k) + 8 B`.

**Total overhead** — `N * (align8(25 + k) + 8) + N * M * (align8(24 + v) + 8 - v)`. For the scenario: `~ 4.58M` list-metadata overhead + `~ 34.3M` per-value overhead = `~ 38.9M`, about 17% of the `233.46M` footprint.

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

### Atomic Compound Operations: `compute(...)`

`KeyValues` exposes two `compute` overloads that run a user `action` atomically under whatever locking the implementation owns. On `KeyValueRing` that's the per-segment `StampedLock`; on `SingleThreadedKeyValueRing` it's a no-op call. Either way, the action object is long-lived and owns its `ComputeContext` instance(s), so the steady-state hot loop allocates **nothing**.

- `compute(key, action)` -- single-key form. The action's `ComputeContext` is bound to that key and the segment's write lock is held for the duration of `execute()`; the action can read the current value, decide, and write a new one atomically.
- `compute(key1, key2, action)` -- two-key form. The ring acquires both segment write locks in **canonical (segment-index) order**, so concurrent calls with the keys swapped cannot deadlock. This is the primitive used internally by `KeyListStorage.append` to make the metadata read-modify-write and the entry put atomic.

Inside the action, the caller may use **only** the provided contexts to touch the store; calling other `KeyValues` methods would acquire further locks in an undefined order and risk deadlock.

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

## Observability / Listeners

The store exposes optional listeners so applications can drive logging and metrics from structural events without instrumenting internals. Listeners are opt-in and zero-allocation -- each callback receives the raising component as its first `notifier` argument plus primitives / buffers (no boxing).

- **`EvictionListener`** -- `onEviction` fires when an entry leaves the coldest tier and no further tier can be created (i.e. only with a bounded tier chain; the default `KeyValueStorage` builder uses an unbounded chain, so it is never called). See the **Eviction Listener** API section above for how to wire a bounded `KeyValueSegment` that actually fires it.
- **`MmapTierFactory.Listener`** -- storage-growth lifecycle: `onMemoryMappedFileFolderCleanup` (stale file deleted at startup), `onMemoryTierCreated` (in-memory hot tier created), `onMemoryMappedFileTierCreated` (mmap tier created). Configurable through `KeyValueStorage.Builder.withMmapTierListener` (defaults to a `System.out` logger).

### Threading model

Listeners are invoked inline (synchronously) from the code path that raises the event, so implementations must be fast, non-blocking, must not call back into the store (doing so risks re-entrant locking or deadlock), and must not retain any supplied buffer beyond the call.

| Callback | Invoking thread | Lock held | Notes |
|----------|-----------------|-----------|-------|
| `EvictionListener.onEviction` | Caller's `put` / `compute` thread | that segment's exclusive `StampedLock` write lock (two locks for two-key `compute`) | On `SingleThreadedKeyValueRing` there is no lock -- it runs on the single caller thread |
| `MmapTierFactory.Listener.onMemoryMappedFileFolderCleanup` | Factory-constructing thread | none | Fires during construction, before the store is returned |
| `MmapTierFactory.Listener.onMemoryTierCreated` / `onMemoryMappedFileTierCreated` | Caller's `put` / `compute` thread (or build thread for eager prep) | that segment's exclusive write lock (none when fired at build time) | Tiers are created on demand while spilling |

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

- **Thread-safe** for concurrent `put`, `get` and `compute` from multiple threads.
- Lock striping via `StampedLock` per segment minimizes contention.
- Exclusive write lock for `put` and `compute`, shared read lock (`StampedLock.readLock()`) for `get`.

### Concurrency Variants

`KeyValueStorage.Builder` exposes two build modes; the `KeyValues` interface contract is the same either way:

- `KeyValueStorage.builder().build()` -- thread-safe ring (`KeyValueRing`). This is the default; the description above and the `Builder` reference apply unchanged.
- `KeyValueStorage.builder().buildSingleThreaded()` -- lock-free single-thread variant (`SingleThreadedKeyValueRing`). All `put`/`get`/`compute` calls must come from one thread; in return there is **no per-call lock cost**. Suited for replay pipelines and embedded use where concurrency is the caller's responsibility.

## Key-list Collection on Top: `KeyListStorage`

`KeyListStorage` is an append-only **multi-value-per-key** collection built directly on top of `KeyValues`. Each user key maps to an ordered sequence of values that can only grow; random access by entry index is provided through a reusable cursor (`ListAccessor`). Delete is not supported.

### Threading

`KeyLists` inherits the threading contract of the backing `KeyValues` verbatim, with no extra locks above what the backing store already provides:

- **Backing store is single-threaded** -> all calls (writer's `append`, reader's `list` + accessor iteration) must come from the same thread. Zero overhead.
- **Backing store is thread-safe** -> any number of writer threads may `append` concurrently on their own `KeyListsWriter` instances (obtained from `KeyLists.newWriter()`), including against the same user key -- the two-key `compute` makes the metadata RMW + entry put atomic. Concurrently, any number of reader threads may call `list(...)` and iterate via their own `ListAccessor` instances.

One rule per thread: a `KeyListsWriter` or `ListAccessor` instance must not be shared across threads.

### Encoding

Two byte-distinguishable key namespaces share the underlying `KeyValues`:

- **User-metadata key**: `[0x01 | userKey]`. The value at this key is the fixed 8-byte `[userKeyIndex (40 bits BE) | entryCount (24 bits BE)]` blob; same-size in-place updates let the entry count grow without re-allocating slots.
- **Synthetic entry key**: 8 bytes. Byte 0 has bit 7 set (marker, so the byte is `>= 0x80`); the remaining 7 bits plus bytes 1..7 carry a 39-bit `userKeyIndex` and a 24-bit `entryIndex`, written byte-by-byte so the layout is endianness-independent.

Because byte 0 distinguishes the two forms (`0x01` for user-metadata vs `>= 0x80` for synthetic), no byte collisions are possible regardless of the user key's content or length.

### Capacity

- Distinct user keys: `2^39 - 1` (~549 billion).
- Entries per list: `2^24 - 1` = 16,777,215.

The backing `KeyValues` store must be empty at construction time (the in-memory `userKeyIndexSequence` restarts at 1 and would otherwise collide with previously issued indices).

### Code sample

```java
KeyValueStorage kv = KeyValueStorage.builder().build();
KeyListStorage  lists  = new KeyListStorage(kv);

KeyListsWriter writer = lists.newWriter();
writer.append(keyBuf, 0, keyLen, valueBuf, 0, valueLen);

ListAccessor accessor = new ListAccessor();
if (lists.list(accessor, keyBuf, 0, keyLen)) {
    for (int i = 0; i < accessor.size(); i++) {
        accessor.get(i, valueConsumer);
    }
    // or accessor.forEach(valueConsumer);
}
```

### Allocation profile

Steady-state `append` and `list` + `forEach` allocate **nothing**: the writer is its own `TwoKeyComputeAction` (no lambda capture), the contexts and metadata consumer are reusable fields, and the reader's load buffers live on the `ListAccessor`. `KeyListsAllocationTest` enforces this.

## License

MIT
