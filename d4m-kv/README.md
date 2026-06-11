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
5. When the coldest tier evicts, the `EvictionListener` is called.

**Read path:**
1. `get(key)` hashes the key and selects a segment.
2. Read lock acquired on that segment.
3. Tiers are scanned hot-to-cold; first match returned.

### Eviction Model

Eviction is **implicit** -- driven by the circular buffer wrapping inside each tier. Older entries are pushed down the tier chain as newer entries arrive. This provides natural LRU-like behavior without explicit eviction policies or background threads.

## API

### Creating a Store

```java
KeyValueStorage storage = KeyValueStorage.builder()
    .totalMainMemory(128 * 1024 * 1024)    // 128 MB heap tier
    .useOffHeapMainMemory(false)            // heap ByteBuffer (or true for direct)
    .mmapFileSize(1024 * 1024 * 1024)       // 1 GB per mmap file
    .ringSize(8)                            // 8 sharded segments
    .mmapFilesFolder(new File("/tmp/d4m"))  // mmap file directory
    .prepareMmapFilesOnStart(true)          // pre-create mmap files
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

```java
KeyValueStorage storage = KeyValueStorage.builder()
    .evictionListener((notifier, hash, kv, keyOff, keySize, valOff, valSize) -> {
        // entry evicted from the coldest tier -- persist externally or log
    })
    .build();
```

## Configuration Reference

| Builder Option | Default | Description |
|----------------|---------|-------------|
| `totalMainMemory` | 128 MB | Heap budget for tier 0 across all segments |
| `useOffHeapMainMemory` | `false` | Use direct ByteBuffer instead of heap |
| `mmapFileSize` | 1 GB | Size of each memory-mapped file |
| `ringSize` | 8 | Number of sharded segments (rounded to power of 2) |
| `ringShuffleMultiplier` | 32 | Internal array spread factor for cache-line distribution |
| `maxNumberOfTiers` | unlimited | Max tier depth per segment |
| `prepareMmapFilesOnStart` | `true` | Pre-create mmap files at construction |
| `mmapFilesFolder` | `java.io.tmpdir` | Directory for mmap backing files |

## Concurrency Model

- **Thread-safe** for concurrent `put` and `get` from multiple threads.
- Lock striping via `StampedLock` per segment minimizes contention.
- Write lock for `put`, optimistic/read lock for `get`.

## License

MIT
