# d4m-benchmark

JMH benchmarks for the d4m KV stack (`KeyValues` + `KeyLists`) and the sequence module.

> **DISCLAIMER: The tests and their results below are indicative only.** 
> 1. The headline tables below come from a moderate JMH run
> (`-wi 3 -w 5 -i 3 -r 10 -f 1`) -- 3 warmup iterations of 5 s, 3 measurement
> iterations of 10 s, one fork per benchmark class. The single-thread KV/KL
> benchmarks run in `Mode.SingleShotTime`, so the `-w` / `-r` time flags do not
> apply to them (each iteration is one bounded loop invocation); they get 3
> warmup shots + 3 measurement shots. All other classes get the full 3x5s /
> 3x10s shape. Numbers are for signal shape, not for citation.
> 2. The test cases are mostly synthetic and illustrative. For example, 
> the concurrent read/write tests saturate throughput to the maximum available level,
> which results in high contention and noticeable performance degradation for both
> writers and readers in KeyValue and KeyList collections. At the same time,
> the detailed analysis of provided results helps explain user-observed behavior and 
> demonstrates patterns that can be used to predict performance in future use cases.

## Indicative test environment
A developer's workstation:
- Hardware: Apple M1 Pro, 10 cores, 32 GB RAM
- OS: macOS 26.4.1 (build 25E253)
- JVM: Amazon Corretto 21.0.3 (OpenJDK 21.0.3+9-LTS); `-Xmx4g`/`-Xms4g` for kv benchmarks, `-Xmx8g`/`-Xms8g` for sequence benchmarks

## How to run

Full canonical configuration (\~ tens of minutes per benchmark class):

```
./gradlew :d4m-benchmark:jmh:jmh -PjmhArgs="KeyValuesWriteBenchmark"
./gradlew :d4m-benchmark:jmh:jmh -PjmhArgs="KeyValuesReadBenchmark"
./gradlew :d4m-benchmark:jmh:jmh -PjmhArgs="KeyValuesConcurrentReadWriteBenchmark"
./gradlew :d4m-benchmark:jmh:jmh -PjmhArgs="KeyListsWriteBenchmark"
./gradlew :d4m-benchmark:jmh:jmh -PjmhArgs="KeyListsReadBenchmark"
./gradlew :d4m-benchmark:jmh:jmh -PjmhArgs="KeyListsConcurrentReadWriteBenchmark"
./gradlew :d4m-benchmark:jmh:jmh -PjmhArgs="benchmark.jmh.sequence"
```

Short indicative configuration (\~2 min per kv class, \~5 min per sequence class at `chunkSize=65536`):

```
./gradlew :d4m-benchmark:jmh:jmh \
  -PjmhArgs="-wi 3 -w 5 -i 3 -r 10 -f 1 -tu s -rf json -rff /tmp/d4m-jmh.json"
```

---

## `d4m-kv` -- `KeyValues` vs `KeyLists`

### Parameters

The single-thread write / read benchmarks are parametrised by an **eviction
profile** (`noEviction`, `evict30`) and, for the write benchmarks, an
operation **`mode`** (`insert`, `update`); the concurrent benchmarks are
parametrised by ring **`segments`** (8 or 16).

`evict30` is **defined by its eviction ratio**: **70 % of the
working set lives in the heap-resident hot tier, 30 % spills to
mmap.** The number `30` in the name is the spill percentage.

**Both profiles share the same 32 MB-per-segment hot tier.** The
difference between profiles is **how many entries are written**
within a single bounded JMH iteration:

- **`noEviction`** -- writes a count that fits in the hot tier.
  - KV: `KV_KEYS_NO_EVICTION ~ 1 118 481` entries (8 x 32 MB / 240 B).
  - KeyLists: `KL_LISTS_NO_EVICTION ~ 116 863` lists (each \~2.3 KB).
  - Working set fits 1 : 1 in the hot tier -> **0 % mmap**.
- **`evict30`** -- writes `1 / 0.70 = 1.43 x` more entries.
  - KV: `KV_KEYS_EVICT_30 ~ 1 597 830` entries.
  - KeyLists: `KL_LISTS_EVICT_30 ~ 166 947` lists.
  - 30 % overflows the hot tier -> **30 % mmap**.

This is only enforceable when each benchmark invocation does a
**fixed** number of operations, which is why the single-thread
write / read benchmarks run in **`Mode.SingleShotTime`**: each
invocation does `range` ops in a tight loop and JMH reports the
elapsed time. Per-op throughput is `range / time`. The concurrent
benchmarks stay in `Mode.Throughput` (group mode doesn't compose
with single-shot) and rely on cyclic updates over a pre-populated
range to give comparable numbers across profiles.

The hot tier (32 MB) still exceeds the M1 Pro 24 MB SLC, so the hot
working set stays cache-cold under both profiles.

### Memory model

`KeyValues` is a `KeyValueRing` -- a hash-sharded store split into
`RING_SIZE` segments (8 by default, 16 for the concurrent benchmarks).
Each segment owns a **heap-resident hot tier** (a 32 MB binary
`kvBuffer` for entry bytes plus a per-segment `metadata[]` long array
for open-addressed slot lookup) guarded by its own
`StampedLock`. A key is routed to a segment by hash, so independent
segments take independent locks and writers on different segments do
not contend.

When a segment's hot tier fills, the oldest entries **cascade to an
mmap-backed cold tier** on the same segment; a `get` / `put` that
misses the hot tier walks down into mmap. The two eviction profiles
above differ only in how much of the working set is pushed past the
32 MB hot tier into mmap.

`KeyLists` is layered on top of `KeyValues`: each list is a metadata
entry plus one `KeyValues` entry per list element, so every `append`
is a metadata read-modify-write plus an entry put -- the per-op cost
is structurally higher than a raw `put` / `get` (decomposed in
[Cost analysis](#cost-analysis)).

### What each benchmark measures

The single-thread write / read benchmarks measure **ms per invocation**
(SingleShotTime); per-op throughput is `range / (ms / 1000)`. The
concurrent benchmarks measure **ops/sec** (Throughput, group mode).

| Benchmark                                         | One invocation =                                                                                    | Notes                                                                                                                                                                                 |
|---------------------------------------------------|-----------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `KeyValuesWriteBenchmark.put`<br/>`mode=insert`   | `range` puts of fresh unique keys (`putLong(seq)` then `ring.put`) on an empty ring                 | `noEviction`: all puts fit in hot tier, 0 % mmap. `evict30`: last 30 % of puts cascade to mmap.                                                                                       |
| `KeyValuesWriteBenchmark.put`<br/>`mode=update`   | `range` cyclic puts (`putLong(seq)` then `ring.put`) over a pre-populated ring                      | Ring is pre-populated with `range` entries in `@Setup(Iteration)`. `noEviction`: every put updates in-place in tier[0]. `evict30`: \~30 % of puts hit mmap-resident keys and cascade. |
| `KeyListsWriteBenchmark.append`<br/>`mode=insert` | `range` appends (`putLong(seq)` then `writer.append`) on an empty store                             | One append per cycled list id. `noEviction`: fits. `evict30`: 30 % overflow to mmap.                                                                                                  |
| `KeyListsWriteBenchmark.append`<br/>`mode=update` | `range` appends extending pre-populated single-entry lists                                          | Pre-populated with one entry per list. The timed loop appends a second entry per list id. `noEviction`: all metadata reads hit tier[0]. `evict30`: \~30 % hit mmap.                   |
| `KeyValuesReadBenchmark.get`                      | `range` gets cycling pre-populated keys                                                             | Pre-populated in `@Setup(Iteration)`. Hot loop is `putLong(seq)` + `ring.get`. `noEviction`: all gets hit tier[0]. `evict30`: \~30 % hit mmap.                                        |
| `KeyListsReadBenchmark.list`                      | `range` list-loads (`putLong(seq)` + `lists.list` + `forEach(consumer)`) delivering 10 entries each | Pre-populated with `ENTRIES_PER_LIST = 10` entries per list. Per-invocation entry deliveries = `range x 10`. `noEviction`: all in tier[0]. `evict30`: \~30 % in mmap.                 |
| `...ConcurrentReadWriteBenchmark.rw1`             | 1 writer + 1 reader, group-mode throughput                                                          | Both threads cycle `seq % range` over the pre-populated set. Writer puts (in-place updates / cascades). Reader gets. `segments` is the ring shard count (8 or 16).                    |
| `...ConcurrentReadWriteBenchmark.rw10`            | 1 writer + 10 readers, group-mode throughput                                                        | Same cyclic-update shape; reader throughput dominates the group score.                                                                                                                |

### Working-set arithmetic

`BenchmarkSupport` constants:

- `HOT_TIER = 32 MB` per segment (single power-of-two value, shared by both profiles)
- `EVICT30_HEAP_FRACTION = 0.70` -- fraction of evict30's working set held by `HOT_TIER`
- `RING_SIZE = 8` segments
- `KV_KEYS_NO_EVICTION ~ 1 118 481` (computed: `8 x 32 MB / 240 B`) -- KV op count for `noEviction`; the resulting working set just fits in the hot tier
- `KV_KEYS_EVICT_30 ~ 1 597 830` (computed: `KV_KEYS_NO_EVICTION / 0.70`) -- KV op count for `evict30`, 30 % overflow
- `KL_LISTS_NO_EVICTION ~ 116 863` (computed: `8 x 32 MB / 2 297 B`) -- KeyLists list count for `noEviction`
- `KL_LISTS_EVICT_30 ~ 166 947` (computed: `KL_LISTS_NO_EVICTION / 0.70`) -- KeyLists list count for `evict30`
- `ENTRIES_PER_LIST = 10` entries appended per list during `KeyListsReadBenchmark` pre-population
- `LIST_FOOTPRINT_ESTIMATE = 2 297 B` per list (one \~57 B metadata entry + 10 x \~224 B data entries)

Per stored KV (in the segment's binary buffer):

- KeyValues entry \~ 32-byte key + 200-byte payload + 16-byte header = **240 B** (`ENTRY_SIZE_ESTIMATE`)
- KeyLists metadata entry \~ 33-byte key + 8-byte value + 16-byte header = **57 B**
- KeyLists data entry \~ 8-byte synthetic key + 200-byte payload + 16-byte header = **224 B**

`KeyValuesReadBenchmark` populated footprint per profile:
- `noEviction`: 1 118 481 x 240 B \~ **268 MB across the ring** \~ **32 MB / segment** -- exactly fills the hot tier, no spill.
- `evict30`: 1 597 830 x 240 B \~ **383 MB across the ring** \~ **48 MB / segment** -- 30 % overflows to mmap.

`KeyListsReadBenchmark` populated footprint per profile:
- `noEviction`: 116 863 x 2 297 B \~ **268 MB across the ring** \~ **32 MB / segment** -- fills the hot tier.
- `evict30`: 166 947 x 2 297 B \~ **383 MB across the ring** \~ **48 MB / segment** -- 30 % spills to mmap.

`KeyValuesWriteBenchmark` and `KeyListsWriteBenchmark` write the
same per-profile counts each invocation, so steady-state footprints
match the read footprints above.

### Cache budget on the test machine

Apple M1 Pro: L1d = 128 KB per core, L2 = 12 MB per P-cluster, system-level
cache = 24 MB shared.

|                           | `noEviction`                                     | `evict30`                                         |
|---------------------------|--------------------------------------------------|---------------------------------------------------|
| Hot tier per segment      | 32MB                                             | 32MB (same)                                       |
| Op count per invocation   | fits in hot tier                                 | 1 / 0.70 ~ 1.43 x noEviction                     |
| Working set per segment   | \~32MB                                           | \~48MB                                            |
| Hot vs mmap               | all in hot tier (0 % spill)                      | 70% hot-resident, 30% spilled to mmap             |
| Working set vs CPU caches | 32MB exceeds the 24MB SLC -- cache-cold hot path | 32MB exceeds the 24 MB SLC -- cache-cold hot path |
| Mmap involvement          | none                                             | 30% of accesses touch mmap                        |

### Results

Single-thread rows report **ops/sec** = `range / (ms-per-invocation /
1000)`; concurrent rows report group-mode **ops/sec** directly.

| Operation                                             | Mode     | `noEviction` | `evict30` |
|-------------------------------------------------------|----------|-------------:|----------:|
| `KeyValuesWriteBenchmark.put`                         | `insert` |        5.14M |     3.13M |
| `KeyValuesWriteBenchmark.put`                         | `update` |        1.57M |     1.77M |
| `KeyValuesReadBenchmark.get`                          | --       |        7.61M |     7.05M |
| `KeyListsWriteBenchmark.append`                       | `insert` |        3.52M |     2.94M |
| `KeyListsWriteBenchmark.append`                       | `update` |        2.67M |     2.21M |
| `KeyListsReadBenchmark.list`                          | --       |         609K |    493K ¹ |
| `KeyValuesConcurrentReadWriteBenchmark.rw1` (16 seg)  | --       |        9.20M |     9.48M |
| `KeyValuesConcurrentReadWriteBenchmark.rw10` (16 seg) | --       |       15.42M |    14.92M |
| `KeyListsConcurrentReadWriteBenchmark.rw1` (16 seg)   | --       |         528K |      480K |
| `KeyListsConcurrentReadWriteBenchmark.rw10` (16 seg)  | --       |         271K |      340K |

¹ Each `KeyListsReadBenchmark.list` op delivers 10 entries -- entry-level throughput is \~6.09 M / \~4.93 M entries/sec.

### Cost analysis

Both profiles share the same 32 MB hot tier, so the hot loop's
per-op CPU work is identical. The throughput delta below is
attributable to (a) the **larger op count** evict30 carries per
invocation, and (b) the **mmap-cascade cost** the 30 % overflow
forces on top of the hot-tier path.

| Benchmark                       | Mode     | `noEviction` | `evict30` |      Delta |
|---------------------------------|----------|-------------:|----------:|-----------:|
| `KeyValuesWriteBenchmark.put`   | `insert` |        5.14M |     3.13M |   **-39%** |
| `KeyValuesWriteBenchmark.put`   | `update` |        1.57M |     1.77M | **+13%** ¹ |
| `KeyValuesReadBenchmark.get`    | --       |        7.61M |     7.05M |    **-7%** |
| `KeyListsWriteBenchmark.append` | `insert` |        3.52M |     2.94M |   **-16%** |
| `KeyListsWriteBenchmark.append` | `update` |        2.67M |     2.21M |   **-17%** |
| `KeyListsReadBenchmark.list`    | --       |         609K |      493K |   **-19%** |
| `KeyValues rw1` (16 seg)        | --       |        9.20M |     9.48M |  **+3%** ¹ |
| `KeyValues rw10` (16 seg)       | --       |       15.42M |    14.92M |    **-3%** |
| `KeyLists rw1` (16 seg)         | --       |         528K |      480K |    **-9%** |
| `KeyLists rw10` (16 seg)        | --       |         271K |      340K | **+25%** ¹ |

¹ Rows with positive deltas sit inside the residual noise band of the
3 x 5 s + 3 x 10 s indicative config. The `KeyValuesWrite.update`
row (**+13 %**) is dominated by JMH iteration-to-iteration variance
of \~0.7-0.9 s/op measurements at 3 samples per profile; the
`KeyValues rw1` row (**+3 %**) is within one standard deviation of the
per-iteration score. The `KeyLists rw10` row (**+25 %**) is the
noisiest: the KeyLists concurrent group score drops sharply between
warmup and measurement (2.4M -> 0.34M ops/s) as the writer starts
cascading metadata updates through the two-segment `compute` path;
three measurement samples do not fully stabilise it.

The `KeyValuesWriteBenchmark.put insert` row (**-39 %**) is the
clearest signal: same hot tier, evict30 puts 30 % more entries, the
last 30 % of those cascade to mmap, and the resulting throughput
lands well below noEviction. The `KeyListsReadBenchmark.list` row
(**-19 %**) is the read-side analogue: 30 % of list-load probes
touch mmap-resident metadata or entries.

#### What the writer/reader actually touches per op

`KeyValueRing.put(key, value)` (single-thread baseline -- the cheap side
of the gap):

1. **Hash** the 32-byte user key (1 polynomial hash, \~80ns).
2. **Acquire** the segment's `StampedLock.writeLock()` -- one acquire +
   one release per call.
3. **`Tier.put`**: one `findSlot` walk (probes the per-segment
   `metadata[]` long array; on hash match, calls
   `kvBuffer.keyEquals(index, key, ...)` to widened-memcmp against the
   stored key bytes). Then `kvBuffer.putKeyValue(...)` writes the
   entry in place. Under `mode = insert` every put is a fresh
   insert and the last 30 % cascade to mmap on `evict30`; under
   `mode = update` puts are in-place updates of tier[0] entries on
   `noEviction`, and 70 % in-place / 30 % cascading on `evict30`.

`KeyValueRing.put` cost summary: **1 hash, 1 lock pair, 1 `findSlot`,
1 segment touched.**

`KeyListsWriter.append(key, value)` (the expensive side):

1. **Pre-lock GET of metadata** (in `KeyListStorage.append`):
   `kvStore.get(prefixedKey, ...)` -> first hash of the prefixed user
   key, *read-lock* acquire+release on segment A, one `findSlot` on A.
2. **Synthetic entry key encoding** (`KeyListStorage.writeSyntheticKey`):
   8-byte byte-by-byte write into a reusable buffer.
3. **`kvStore.compute(prefixedKey, syntheticKey, action)`**
   (`AbstractKeyValueRing.compute`): hashes **both** key1 *and* key2
   (so the prefixed key is hashed a *second* time here in addition to
   step 1, plus the synthetic-key hash). Computes the two segment
   indices, sorts them canonically, then
   `runUnderTwoSegmentLocks(firstIdx, secondIdx, action)` acquires the
   first write-lock and -- when the two keys land on different
   segments -- the second write-lock.
4. Inside `TwoKeyComputeAction.execute()`:
   - **Verify-GET** of metadata: `metaCtx.get(...)` -- a second
     `findSlot` on segment A.
   - **Entry put** in segment B: `synCtx.put(value, ...)` -- third
     `findSlot` (on B), then `kvBuffer.putKeyValue(...)` to write the
     synthetic-key + payload bytes, then pack the metadata-array slot.
   - **Metadata put** in segment A: `metaCtx.put(metadataValue, ...)` --
     fourth `findSlot` (on A), then update the 8-byte metadata payload
     in place, then pack the slot.
5. Release both write-locks.

`KeyListsWriter.append` cost summary: **3 hashes** (prefixed key twice _(there is a room for micro-optimization)_ + synthetic key once), **3 `StampedLock` state-change pairs** (one
read-lock for the pre-GET, two write-locks for the dual-segment compute
-- six acquires/releases total), **4 `findSlot` walks** (three on
segment A: pre-GET, verify-GET, metadata put; one on segment B: entry
put), and **two segments' `kvBuffer` + metadata arrays touched per
call** (vs. one for `put`).

#### Why `put` is faster than `append`

Both benchmarks run on a cache-cold pre-population, so per-line miss
cost is the same on both sides; the ratio is entirely the work
multiplier in `append`'s critical path. The extra cost decomposes into:

- **Lock state changes.** `put` does 1 write-lock pair (acquire +
  release \~60ns uncontended). `append` does 1 read-lock pair plus
  *two* write-lock pairs (different segments) \~180ns. Net extra for
  `append`: **\~120ns**.
- **`findSlot` walks.** `put` does 1; `append` does 4. Each walk is a
  metadata-array probe + a `kvBuffer.keyEquals` widened memcmp at a
  hash-distributed offset. The metadata probe itself is cheap (8-byte
  long reads). The `keyEquals` reads two cache lines' worth of stored
  key bytes from `kvBuffer` at a slot-specific offset inside the
  per-segment kvBuffer region; with the working set at \~63MB /
  segment (KV) and \~39MB / segment (KeyLists), every walk drags those
  cache lines from DRAM. **Three extra walks \~ 600-900ns**, mostly
  DRAM-fetch cost.
- **Hash compute.** `put` does 1 polynomial hash; `append` does 3
  (prefixed user key twice _(a room for micro-optimization)_ + synthetic key once). Net extra
  **\~160ns**.
- **Segment fan-out.** `append` touches *two* segments' arrays /
  buffers per call (metadata segment + entry segment) where `put`
  touches one, so the cache-line footprint per call is roughly
  doubled. **\~400-700ns** of extra DRAM traffic.
- **Misc.** Synthetic-key 8-byte encode, the
  `ComputeContext`/`TwoKeyComputeAction` interface dispatch, the
  retry-on-userKeyIndex-race outer loop, second `kvBuffer.putKeyValue`
  for the entry put, the metadata in-place write. **\~150-250ns**.

The whole gap is API design (extra hashes, extra lock pairs, extra
`findSlot` walks, two-segment fan-out) -- with both sides cache-cold,
cache residency contributes nothing to the *ratio*.

### Performance tips

Techniques to get the most out of `KeyValues` / `KeyLists` for typical
workloads:

- **Drop the lock when single-threaded.** Build via
  `KeyValueStorage.Builder.buildSingleThreaded()`
  (`SingleThreadedKeyValueRing`) to skip the per-op `StampedLock`
  acquire/release the concurrent ring pays on every `put` / `get`.
- **Scale segments with concurrency.** Raise the ring `segments`
  (8 -> 16) so per-segment `StampedLock`s spread across more shards --
  this is what lets the `rw10` read throughput scale in
  [Results](#results).
- **Keep the working set in the hot tier.** Fresh-key `insert`
  that overflows cascades to mmap (the **-39 %** `put insert` row in
  [Cost analysis](#cost-analysis)). Prefer cyclic in-place `update`
  over churny inserts when the key set is stable.
- **Reuse buffers; the hot path is zero-alloc.** Write keys/values into
  reusable `AtomicBuffer`s (as the benchmarks do with `putLong(seq)`)
  rather than allocating per call.
- **Use `KeyValues` directly when you don't need multi-value-per-key.**
  A `KeyListsWriter.append` costs roughly 2x a `put` (3 hashes, 3 lock
  pairs, 4 `findSlot` walks, 2 segments -- see
  [Cost analysis](#cost-analysis)).
- **Atomic read-modify-write via `compute`.** Use
  `KeyValueStorage.compute` / the two-key `compute` (`TwoKeyComputeAction`)
  instead of get-then-put to avoid the race and an extra lock round trip.
- **Fan out list reads with one lookup.** Load a whole list via
  `KeyListStorage.list` + `ListAccessor` / `forEach` (one metadata
  lookup delivering all N entries) instead of N point `get`s. When
  scanning for a match, pass a `StoppableValueConsumer` to `forEach` so
  it stops as soon as `stopped()` returns true, skipping the rest of the
  list.

---

## `d4m-sequence`

### Parameters

- **`chunkSize`** -- bytes per fixed-size chunk (the unit of heap allocation
  and mmap mapping). Bigger chunks mean fewer chunk transitions and more
  amortised cost per chunk.
- **`sequenceCount`** -- how many independent sequences share one
  allocator/eviction queue. `1` is a single large sequence (\~100 000 entries
  pre-populated); `1024` is many smaller sequences (\~5 000 entries each, with
  a shared mmap pool). Power of two so the writer's round-robin
  `opCount & (sequenceCount - 1)` is a single AND.
- **`writeProfile`** -- operation mix used by the writer:
  - `APPEND_100`: 100 % monotonic appends (best case for the append-optimised path).
  - `APPEND_90_INSERT_10` / `APPEND_50_INSERT_50`: mix in COW inserts into random gaps (slower; COW touches whole tier-snapshots).
  - `APPEND_90_UPDATE_10` / `APPEND_50_UPDATE_50`: replace existing entries in-place (no COW, no growth).
- **`cursorType`** -- `FORWARD`, `BACKWARD`, `MERGED_FORWARD`, `MERGED_BACKWARD`. The non-merged ones iterate a single sequence; the merged ones N-way-merge across all sequences in the sweep.

### Memory model

Each `Sequence` writes into fixed-size **chunks** drawn from a two-tier
allocator wired by
[`SequenceBenchmarkSupport.createSequences`](jmh/src/main/java/io/github/green4j/d4m/benchmark/jmh/sequence/SequenceBenchmarkSupport.java):

- **Heap tier** -- a `HeapChunkAllocator` (Treiber stack of pre-allocated
  chunks; see [`HeapChunkAllocator.tryAllocate`](../d4m-sequence/src/main/java/io/github/green4j/d4m/sequence/HeapChunkAllocator.java)).
  Fixed size; a `tryAllocate` miss returns `null`.
- **Mmap tier** -- an `MmapChunkAllocator` created with `preAlloc = false`,
  so mmap files grow on first write. Effectively unbounded until the
  filesystem gives up.
- **`EvictionQueue`** -- one queue shared by all sequences on a given
  allocator pair. When a heap chunk fills and is sealed
  (`Sequence.sealEnqueue`), it enters this queue as an eviction candidate.

The eviction hot path lives in
[`Sequence.allocate` and `Sequence.evictOne`](../d4m-sequence/src/main/java/io/github/green4j/d4m/sequence/Sequence.java):

1. Writer needs a fresh chunk -> `heap.tryAllocate()`.
2. On miss, call `evictOne()` -- polls the eviction queue up to
   `SWAP_CHECK_INTERVAL = 16` times, copies one heap chunk's bytes into a
   freshly `mmap.allocate()`-ed chunk, and posts a `PendingSwap`.
3. Retry `heap.tryAllocate()` (now the swapped-out chunk is reclaimable).
4. If still empty, allocate directly on mmap.
5. On the next write, the sequence's `drainSwaps` rewrites its spine array
   in place so cursors observe the new mmap-backed chunk seamlessly.

One `allocate()` miss = one `sealed heap chunk -> mmap` migration.

### What each benchmark measures

For `APPEND_100` the writer benchmarks take an `appendOnlyEntry` fast path
that strips the profile-decision branch -- the hot loop is just
`sequences[opCount & (sequenceCount - 1)].append(orderCounters[si]++, payload, 0, PAYLOAD_SIZE)`.
The mixed write profiles (`APPEND_X_INSERT_Y`, `APPEND_X_UPDATE_Y`) keep the
full `writeEntry` dispatch since their storage cost dwarfs any
decision overhead. The read benchmarks are clean
(`cursor.next(batch=256, NO_OP)` + counter accumulation).

One op = one writer step for `SequenceWriteBenchmark.write`; one
`cursor.next(batch=256, ...)` for `SequenceHistoricalReadBenchmark.read`; one writer
step + concurrent reader drain (group throughput) for the broadcast
benchmarks.

All four sequence benchmarks use `@Setup(Level.Trial)`, so the same
sequence array carries across every warmup and measurement iteration of a
`(chunkSize, sequenceCount)` combo -- state (and eviction pressure)
accumulates across iterations.

- **`SequenceWriteBenchmark.write` (APPEND_100).** Trial setup creates
  empty sequences (in [`SequenceWriteBenchmark.setup`](jmh/src/main/java/io/github/green4j/d4m/benchmark/jmh/sequence/SequenceWriteBenchmark.java)).
  The timed loop appends one entry per invocation and never reads, so
  allocation is monotonic. At the observed throughput, 10 s writes 30-60 M
  entries -- 100 K-200 K chunks -- vastly exceeding every pool row of the
  capacity table. The reported score is the **average of a
  monotonically-declining slope** as eviction saturates the writer's
  critical path, not a steady-state hot-tier number. Intra-iteration the
  drop is visible directly in the raw JMH output (10 M -> 2 M ops/s across
  three 10 s samples at `chunkSize=65 536, sequenceCount=1`).
- **`SequenceHistoricalReadBenchmark.read`.** Trial setup pre-populates
  every sequence (in [`SequenceHistoricalReadBenchmark.setup`](jmh/src/main/java/io/github/green4j/d4m/benchmark/jmh/sequence/SequenceHistoricalReadBenchmark.java)),
  then the timed loop iterates the cursor and rewinds on exhaustion. No
  new chunks are allocated in the timed loop -- hot/mmap split is fixed by
  the fit table below. The 65 K row (~44 % of data on mmap) reads ~30 %
  slower than 131 K (all-hot); 524 K (also all-hot) sits between the two
  because the 4x larger chunk size gives worse cache streaming per entry.
- **`SequenceRealtimeBroadcastBenchmark.broadcast`** (`sequenceCount = 1`).
  Trial setup creates empty sequences with no pre-population
  (in [`SequenceRealtimeBroadcastBenchmark.setup`](jmh/src/main/java/io/github/green4j/d4m/benchmark/jmh/sequence/SequenceRealtimeBroadcastBenchmark.java)).
  Writer allocates continuously; reader tails one `ForwardCursor` with
  `next(READ_BATCH=256, NO_OP)`. Same monotonic-allocation shape as the
  write bench, but the reader's ref-count pin keeps the tail chunk (and
  anything the cursor is currently walking) out of the eviction queue.
  The auxiliary `entries` counter in the raw output shows the writer
  produces 4-18 M/s while the reader iterates at 170-286 M/s -- so most
  of the reader's ops hit already-consumed hot chunks; eviction pressure
  from the writer is diluted across a much larger reader op-count.
- **`SequenceScaledBroadcastBenchmark.twoReaders`** (`sequenceCount = 1024`).
  Trial setup creates 1024 empty sequences, no pre-population; each reader
  thread holds `CURSOR_COUNT = 100` `ForwardCursor`s all seeked to 0
  (in [`SequenceScaledBroadcastBenchmark.setup`](jmh/src/main/java/io/github/green4j/d4m/benchmark/jmh/sequence/SequenceScaledBroadcastBenchmark.java)).
  With 100 cursors per reader and the writer round-robin appending across
  1024 sequences, chunk pins scatter widely and the eviction queue drains
  faster than any one sequence can fill its private pool. The score is
  reader-dominated and stays close to the historical read row.

### Working-set arithmetic

`SequenceBenchmarkSupport` constants:
- `PAYLOAD_SIZE = 200` B
- `ENTRIES_1_SEQUENCE = 100 000`, `ENTRIES_PER_1000_SEQUENCES = 5 000`
- `defaultMaxHeap(seq, chunkSize)` = `chunkSize * 200` for 1 seq, else
  `min(3 GiB, seq * chunkSize * 10)`

Per-entry footprint from [`Chunk.entrySize`](../d4m-sequence/src/main/java/io/github/green4j/d4m/sequence/Chunk.java)`(200)` = `alignToLong(24 + 200)` = **224 B**.
Per-chunk data capacity = `chunkSize - HEADER_SIZE` where `HEADER_SIZE = 256`.

**Entries per chunk** = `(chunkSize - 256) / 224`:

| chunkSize | entries per chunk |
|-----------|------------------:|
|    65 536 |               291 |
|   131 072 |               584 |
|   524 288 |             2 339 |

**Heap pool capacity in chunks** (`HeapChunkAllocator` pre-allocates
`floor(maxHeapBytes / chunkSize)` slabs because the benchmark support passes
`chunksPerSlab = 1`):

| `sequenceCount` |  65 536 | 131 072 | 524 288 |
|-----------------|--------:|--------:|--------:|
|               1 |     200 |     200 |     200 |
|            1024 |  10 240 |  10 240 |   6 144 |

At `sequenceCount = 1024`, the `min(3 GiB, ...)` cap in `defaultMaxHeap`
only bites at `chunkSize = 524 288` (`1024 * 524288 * 10 = 5 GiB > 3 GiB`,
so pool = 3 GiB / 524 288 = 6 144 chunks); at 65 536 and 131 072 the
formula yields 640 MiB and 1.25 GiB and the cap is not reached.

**Pre-populated chunk count** for the read benchmarks (each sequence owns
its own chunk run, so total = `sequenceCount * ceil(entriesPerSeries /
entriesPerChunk)`):

| Config                    |  65 536 | 131 072 | 524 288 |
|---------------------------|--------:|--------:|--------:|
| 1 seq, 100 000 entries    |     344 |     172 |      43 |
| 1024 seq, 5 000 per seq   |  18 432 |   9 216 |   3 072 |

**Heap fit** (pre-populated chunks / pool capacity):

| Config                    |          65 536 |     131 072 |     524 288 |
|---------------------------|----------------:|------------:|------------:|
| 1 seq                     | **172 %** spill |    86 % fit |    22 % fit |
| 1024 seq                  | **180 %** spill |    90 % fit |    50 % fit |

The 65 K column is the one where **pre-population itself already spills to
mmap** under both sequence counts -- roughly 40 % of the sequence's data is
on mmap before the read timed loop starts. This is the read-side analogue
of the KV `evict30` profile.

### Cache budget on the test machine

Apple M1 Pro: L1d = 128 KB per core, L2 = 12 MB per P-cluster, system-level
cache = 24 MB shared.

A single chunk always fits comfortably in an on-core cache -- 65 KB in L1,
131 KB nearly in L1, 524 KB in L2. Chunk size matters for **streaming
prefetch** during sequential reads (bigger chunks amortise the per-chunk
transition), not for cache residency of any one chunk. The pressure comes
from the **working-set total** = `chunk count * chunkSize`, which for the
read/broadcast benchmarks looks like:

| Config      |  65 536 | 131 072 | 524 288 |
|-------------|--------:|--------:|--------:|
| 1 seq       |   22 MB |   22 MB |   22 MB |
| 1024 seq    | 1.12 GB | 1.12 GB | 1.50 GB |

The 22 MB per-1-seq footprint fits the 24 MB SLC at 131 K / 524 K but
crosses into mmap territory at 65 K. The 1 GB+ 1024-seq footprints exceed
every cache level; there the read numbers reflect DRAM streaming, not
cache residency, and the 65 K -> 131 K -> 524 K spread comes from a mix
of (a) mmap pressure at 65 K, (b) TLB / prefetch friction at 524 K on
1024 concurrent streams.

### Results

Scope: `writeProfile=APPEND_100`, forward cursor. The three columns are
chunk sizes. Only `SequenceHistoricalReadBenchmark.read` has a fixed
hot-vs-mmap split -- its pre-populated 65 K set spills to mmap while the
131 K and 524 K sets fit the heap pool (see the **Heap fit** table
above). The `¹` rows have no such split: they start from an empty pool
and allocate monotonically throughout the timed loop.

| Benchmark                                        | Sequences | `65 K` | `131 K` | `524 K` |
|--------------------------------------------------|-----------|-------:|--------:|--------:|
| `SequenceWriteBenchmark.write` ¹                 | 1         |   6.11M |    3.00M |    2.89M |
| `SequenceWriteBenchmark.write` ¹                 | 1024      |   5.92M |    5.27M |    2.26M |
| `SequenceHistoricalReadBenchmark.read`           | 1024      | 112.66M |  166.75M |  144.72M |
| `SequenceRealtimeBroadcastBenchmark.broadcast` ¹ | 1         | 172.48M |  285.71M |  252.98M |
| `SequenceScaledBroadcastBenchmark.twoReaders` ¹  | 1024      | 152.19M |  147.10M |  132.84M |

¹ Rows marked ¹ start from an empty pool and allocate monotonically
throughout the timed loop, so the reported score is the average of a
declining throughput slope as the pool saturates and eviction kicks in --
not a steady hot- or eviction-tier number. All three columns spill to
mmap eventually for these rows; the 65 K column just reaches saturation
sooner. `SequenceHistoricalReadBenchmark.read` (the only unmarked row)
is the one row with a fixed hot/mmap split, because it only reads
pre-populated data. See the per-benchmark interpretation above for each
row's exact behavior.

Mixed-profile and per-cursor-type rows are excluded from this headline table
to keep it readable -- re-run with the full JMH args above to get the complete
matrix.

### Cost analysis

Only `SequenceHistoricalReadBenchmark.read` compares eviction against
hot like-for-like (it reads a fixed pre-populated set; the ¹ rows
allocate monotonically and saturate all three columns eventually). This
section is scoped to that comparison, so here the `eviction, 65 K` /
`hot, 131 K` column labels are meaningful. For the `read` row the delta
below isolates the mmap-spill cost against the in-heap baseline:

| Benchmark                              | Sequences | `eviction, 65 K` | `hot, 131 K` |       Delta |
|----------------------------------------|-----------|-----------------:|-------------:|------------:|
| `SequenceHistoricalReadBenchmark.read` | 1024      |          112.66M |      166.75M |    **-32%** |
| `SequenceWriteBenchmark.write` ¹       | 1         |            6.11M |        3.00M | **+104%** ¹ |
| `SequenceWriteBenchmark.write` ¹       | 1024      |            5.92M |        5.27M |  **+12%** ¹ |

¹ The write rows allocate monotonically from an empty pool, so their
65 K column is *not* a steady eviction state -- the smaller chunk simply
reaches saturation later in wall-clock terms per fixed op count, so its
averaged score sits above the larger-chunk columns. Read the write delta
as an allocation-cadence artefact, not a hot-vs-mmap cost. The
`read` row (**-32 %**) is the clean signal: \~44 % of its data is
mmap-resident at 65 K, and the mmap probes drag throughput down against
the all-hot 131 K baseline -- the read-side analogue of the KV
`evict30` `-19 %` list-load row.

#### What the writer/reader actually touches per op

[`Sequence.append`](../d4m-sequence/src/main/java/io/github/green4j/d4m/sequence/Sequence.java)`(order, payload, ...)` (the write hot path):

1. **Fast path** (tail chunk cached, room remains): a single
   `tail.writeEntry(...)` sequential copy of the 200-byte payload +
   24-byte entry header into the chunk buffer, then in-place header
   stores (`putDataWriteOffset`, `putMaxOrder`, `putEntryCountOrdered`).
   No volatile reads, no lock -- pure sequential writes.
2. **Swap drain cadence**: every `SWAP_CHECK_INTERVAL = 16` appends the
   writer calls `drainSwaps()` to fold any completed mmap migrations
   into its spine and publish a new snapshot.
3. **Chunk-boundary slow path** (tail full): `sealEnqueue(tail)` marks
   the chunk read-only and enqueues it as an eviction candidate, then
   `allocate()` fetches a fresh chunk.
4. **Allocation with eviction** (`Sequence.allocate` / `Sequence.evictOne`):
   `heap.tryAllocate()`; on a pool miss, `evictOne()` polls the shared
   eviction queue, `mmap.allocate()`s a chunk, and does a **whole-chunk
   `copyChunkDataFrom` memcpy** (`chunkSize` bytes) to migrate a sealed
   heap chunk out to mmap, freeing a heap slot to retry.

`Sequence.append` cost summary: **steady state = one sequential entry
copy + three header stores, no lock**; **at each chunk boundary = one
seal + one allocate**, and **under pool pressure = one full-chunk
memcpy migration** on top.

[`ForwardCursor.next`](../d4m-sequence/src/main/java/io/github/green4j/d4m/sequence/ForwardCursor.java)`(batch=256, consumer)` (the read hot path):

1. **`refresh()`** compares the sequence's snapshot version; the common
   case (`tryAdvance`) is a single epoch compare and skips the O(N)
   reposition scan.
2. **One `acquirePin` per chunk** (`CursorSupport.acquirePin`):
   a ref-count pin that holds the chunk alive and orders its state via a
   volatile epoch read. The pin cost is paid once and **amortised across
   up to `batch = 256` entries** streamed from that chunk.
3. **Tight decode loop**: per entry, read `entryOrder` / `entryVersion`
   from the chunk buffer, hand the payload slice to `consumer.onEntry`
   (a `NO_OP` in the benchmark), and advance the offset by the entry
   size. Sequential reads, zero allocation.

`ForwardCursor.next` cost summary: **1 snapshot check + 1 pin per chunk,
then a per-entry sequential read + callback**, with the pin's
synchronisation cost spread over the whole batch.

#### Why reads are faster than writes

The read and write throughputs live in different regimes -- reads run at
100-280 M ops/s, writes at 2-6 M ops/s -- and the gap is the same kind of
work-multiplier story as KV's `put` vs `append`:

- **No allocation on the read path.** `next` never allocates a chunk; it
  streams entries already resident in pinned chunks. `append` pays a
  chunk seal + allocate at every chunk boundary, and once the heap pool
  saturates, a **whole-chunk memcpy eviction** (`chunkSize` bytes copied
  heap -> mmap) on the allocation path.
- **Pin cost amortised over the batch.** The reader takes one pin per
  chunk and delivers up to 256 entries under it, so the per-entry
  synchronisation cost is \~1/256 of a pin. The writer's per-entry work
  (entry copy + ordered header stores + periodic `drainSwaps`) is paid
  every single op.
- **Steady state vs declining slope.** The historical-read timed loop
  touches a fixed pre-populated set, so its score is a genuine
  steady-state number. The write score is the **average of a
  monotonically-declining slope** (10 M -> 2 M ops/s across a 10 s
  sample) as monotonic allocation drives the eviction queue and the
  per-op migration cost climbs.

With reads streaming pinned, cache-resident (or mmap-resident but
sequential) chunks and writes paying allocation + eviction on top of
every entry copy, the throughput ratio is dominated by the write path's
allocation/eviction overhead, not by per-entry decode cost.

### Performance tips

Techniques to get the most out of `Sequence` for typical workloads:

- **Batch your writes.** Prefer `Sequence.appendBatch` /
  `Sequence.appendBatchPacked` over per-entry `append`; a batch
  amortizes the `SWAP_CHECK_INTERVAL` swap-check / `drainSwaps` cadence
  and the snapshot publish across many entries.
- **Batch your reads.** Call `ForwardCursor.next(maxEntryCount, consumer)`
  with a large batch (e.g. 256); the one `acquirePin` per chunk is
  amortized over the whole batch (see [Cost analysis](#cost-analysis-1)).
- **Prefer monotonic `append` over `insert` into gaps.** Appends take
  the lock-free sequential fast path; a single `Sequence.insert` into an
  existing region pays a full-chunk COW rebuild plus a full spine
  reconstruction and a snapshot publish.
- **Batch out-of-order inserts.** When you must insert into gaps, prefer
  `Sequence.insertBatch` (pre-sorted, non-decreasing orders) over
  repeated `insert`: it groups entries by target chunk and does one COW
  rebuild per affected chunk plus a single atomic snapshot publish for
  the whole batch, instead of a full-chunk rebuild + spine
  reconstruction + publish per entry -- an O(N) -> O(1) reduction in
  rebuilds and publishes when many entries land in the same chunk(s).
- **Overwrite in place.** Use `Sequence.insertOrUpdateEqual` /
  `insertOrUpdateUnique` to replace existing entries without COW growth.
- **Reuse buffers; the hot path is zero-alloc.** Write data into
  reusable `AtomicBuffer`s rather than allocating per call.
- **Size the heap pool to the hot working set.** Once the pool
  saturates, each allocation pays a whole-chunk `copyChunkDataFrom`
  memcpy to mmap (the declining write slope in
  [Cost analysis](#cost-analysis-1)).
- **Tune `chunkSize`.** Bigger chunks cut chunk-transition overhead and
  help streaming reads but hurt single-stream cache residency; the
  131 K column is the sweet spot in [Results](#results-1).
- **Merge with the built-in merged cursors.** Use `MergedForwardCursor`
  / `MergedBackwardCursor` for N-way merges across sequences instead of
  hand-rolling one.
- **Keep cursors advancing.** A reader's ref-count pin keeps its
  current / tail chunk out of eviction (what keeps broadcast readers
  hot) -- but a stalled cursor pins heap chunks and starves the pool.
