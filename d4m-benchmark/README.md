# d4m-benchmark

JMH benchmarks for the d4m KV stack (`KeyValues` + `KeyLists`) and the sequence module.

> **Indicative only.** The headline tables below come from a deliberately short JMH run
> (`-wi 1 -i 1 -r 1-2s -w 1-2s -f 1`). The class-level
> `@Warmup(5x5s) / @Measurement(5x10s) / @Fork(1)` annotations are the canonical
> configuration -- drop the `-PjmhArgs` overrides to use them.

## Indicative test environment
A developer's workstation:
- Hardware: Apple M1 Pro, 10 cores, 32 GB RAM
- OS: macOS 26.4.1 (build 25E253)
- JVM: Amazon Corretto 21.0.3 (OpenJDK 21.0.3+9-LTS); `-Xmx4g`/`-Xms4g` for kv benchmarks, `-Xmx8g`/`-Xms8g` for sequence benchmarks

## How to run

Full canonical configuration (~= tens of minutes per benchmark class):

```
./gradlew :d4m-benchmark:jmh:jmh -PjmhArgs="KeyValuesWriteBenchmark"
./gradlew :d4m-benchmark:jmh:jmh -PjmhArgs="KeyValuesReadBenchmark"
./gradlew :d4m-benchmark:jmh:jmh -PjmhArgs="KeyValuesConcurrentReadWriteBenchmark"
./gradlew :d4m-benchmark:jmh:jmh -PjmhArgs="KeyListsWriteBenchmark"
./gradlew :d4m-benchmark:jmh:jmh -PjmhArgs="KeyListsReadBenchmark"
./gradlew :d4m-benchmark:jmh:jmh -PjmhArgs="KeyListsConcurrentReadWriteBenchmark"
./gradlew :d4m-benchmark:jmh:jmh -PjmhArgs="benchmark.jmh.sequence"
```

Short indicative configuration (~2 min for the kv suite, ~5 min for sequence at `chunkSize=65536`):

```
./gradlew :d4m-benchmark:jmh:jmh \
  -PjmhArgs="-wi 1 -i 1 -r 2 -w 2 -f 1 -tu s -rf json -rff /tmp/d4m-jmh.json"
```

---

## `d4m-kv` -- `KeyValues` vs `KeyLists`

### Eviction profiles

`@Param eviction` selects how the underlying ring is sized relative to the working set:

- **`noEviction`** -- hot tier **128 MB per segment**, pre-populated with the
  full working set. No data ever spills to mmap. On Apple M1 Pro the
  per-segment working set is ~ 4 MB for the KeyValues pre-population
  (fits in L2) and ~ 39 MB for KeyLists (exceeds the 24 MB system-level
  cache -- this is the realistic case where the working set is bigger
  than CPU cache and the hot path is DRAM-bound).
- **`evict30`** -- hot tier **32 MB per segment**, sized to **exceed the
  24 MB SLC** so the hot working set is cache-cold; the workload size is
  big enough that the hot tier overflows and ~30 % of the data lives in
  mmap-backed files. The writer / reader therefore takes **real CPU-cache
  misses on top of mmap accesses** -- the realistic shape of a workload
  that has outgrown the hot tier. For KeyValues reads the pre-population
  is bumped from `KEY_ARRAY_SIZE = 131 072` to
  `EVICT_KEY_ARRAY_SIZE = 2 097 152` keys so the per-segment working set
  (~ 60 MB) is unambiguously cache-cold; KeyLists already pre-populates
  131 072 x 10 entries (~ 39 MB / segment), so no bump is needed.

> An earlier version of these benchmarks also exposed a small-hot-tier
> `evict30` (2 MB / segment for writes, hot-tier-sized-to-population
> for reads) whose working set was L2-resident. That hid the eviction
> cost behind cache residency. It has been removed -- the eviction
> baseline is now a single realistic, cache-cold mmap-eviction workload.

### What each row measures (1 JMH op = ...)

| Benchmark | Single op = | Notes |
|---|---|---|
| `KeyValuesWriteBenchmark.put` | one `ring.put(key, value)` | Keys come from a 131 072-entry array of 32-byte ASCII keys pre-built at `@Setup` (power-of-two so the hot loop indexes with AND). One shared 200-byte value buffer. In `noEviction` the store is pre-populated with the full key range, so each put is an in-place update. In `evict30` the writer instead writes a fresh unique key per iteration (cheap trailing-digit ASCII encoding) so the 32 MB hot tier overflows continuously and entries cascade to mmap. |
| `KeyListsWriteBenchmark.append` | one `writer.append(key, value)` | Same pre-built key array. In `noEviction` the store is pre-populated with 131 072 lists x 10 entries; each iteration appends to one of those lists (lists grow over the run -- see length math below). In `evict30` the store starts empty and lists grow on the fly, so the 32 MB hot tier overflows and entries cascade to mmap. |
| `KeyValuesReadBenchmark.get` | one `ring.get(key, consumer)` | Pre-populated working set; hot loop is array index + the storage call. `noEviction` uses the 131 072-entry array; `evict30` uses the 2 097 152-entry array so the working set per segment is cache-cold. |
| `KeyListsReadBenchmark.list` | one `lists.list(accessor, key)` **plus** an `accessor.forEach(consumer)` that delivers **10 entries** | The "ops/sec" counts list-loads; x 10 gives entry-deliveries/sec. |
| `...ConcurrentReadWriteBenchmark.rw1` | 1 writer thread + 1 reader thread, group-mode throughput | `segments` is the ring shard count (8 or 16). |
| `...ConcurrentReadWriteBenchmark.rw10` | 1 writer thread + 10 reader threads | "ops" is the sum of writer + reader iterations. |

The hot loop is just an array index (`keys[seq & MASK]`) plus the storage call -- no per-op ASCII conversion or modulo (except on the eviction-write path that *must* keep producing unique keys to drive eviction, where a stripped-down trailing-digit encoder is used).

### Full results

| Operation | Eviction | `KeyValues` | `KeyLists` |
|---|---|---:|---:|
| write (single-thread `put` / `append`) | `noEviction` | 12.45 M | 412 K |
| write (single-thread `put` / `append`) | `evict30` | 2.23 M | 881 K |
| read (single-thread `get` / `list`+`forEach`) | `noEviction` | 13.04 M | 777 K¹ |
| read (single-thread `get` / `list`+`forEach`) | `evict30` | 8.57 M | 740 K¹ |
| concurrent `rw1` (1 W + 1 R, 16 segments) | `noEviction` | 15.38 M | 1.21 M |
| concurrent `rw1` (1 W + 1 R, 16 segments) | `evict30` | 3.69 M | 744 K |
| concurrent `rw10` (1 W + 10 R, 16 segments) | `noEviction` | 22.68 M | 856 K |
| concurrent `rw10` (1 W + 10 R, 16 segments) | `evict30` | 18.24 M | 805 K |

¹ Each `KeyListsReadBenchmark.list` op delivers 10 entries -- entry-level throughput is ~7.8 M / 7.4 M entries/sec across the two profiles.

`KeyLists` is layered on `KeyValues`; each append is a metadata read-modify-write plus an entry put, so the per-op cost is structurally higher than a raw `put`/`get`.

### KeyLists `noEviction` list-length growth

The store starts with 131 072 lists x 10 entries each. The hot loop cycles
`seq & 131071`, appending one entry per iteration; after `K` invocations each
list has roughly `10 + K / 131 072` entries.

- For the short config above (~= 400 K ops/sec over 4 s of measurement ~= 1.6 M
  appends) that is **~= 22 entries per list** at exit.
- For the canonical 5x5 s warm-up + 5x10 s measurement run (~= 30 M appends
  total) it grows to **~= 240 entries per list**.

`MAX_ENTRY_COUNT` is 2^24 − 1 ~= 16.8 M, well above any realistic run. In
`evict30` mode of the single-thread write benchmark the store starts empty
and each iteration's append grows a (fresh-or-existing) list, so the
steady-state list length is similarly bounded by `K / 131 072`.

---

## What `evict30` actually costs

`evict30` is sized to be a realistic mmap-eviction workload: the 32 MB hot
tier per segment exceeds the M1 Pro 24 MB SLC, so the writer/reader is
**already DRAM-bound** (real CPU-cache misses) and **also** has ~30 % of the
data in mmap-backed files. The deltas vs `noEviction` therefore include
both effects -- they answer "what does eviction cost on a workload that's
big enough to need it" rather than isolating any one axis.

| Benchmark | `noEviction` | `evict30` | Delta |
|---|---:|---:|---:|
| `KeyValuesWriteBenchmark.put` | 12.45 M | 2.23 M | **-82 %**¹ |
| `KeyValuesReadBenchmark.get` | 13.04 M | 8.57 M | **-34 %** |
| `KeyListsWriteBenchmark.append` | 412 K | 881 K | **+114 %**² |
| `KeyListsReadBenchmark.list` | 777 K | 740 K | **-5 %** |
| `KeyValues rw1` (16 seg) | 15.38 M | 3.69 M | **-76 %** |
| `KeyValues rw10` (16 seg) | 22.68 M | 18.24 M | **-20 %** |
| `KeyLists rw1` (16 seg) | 1.21 M | 744 K | **-38 %** |
| `KeyLists rw10` (16 seg) | 856 K | 805 K | **-6 %** |

¹ `noEviction` pre-populates the full KV key space and runs in-place updates;
`evict30` writes a fresh unique key per iteration that forces a new metadata
slot, an entry put, and (once the hot tier fills) eviction to mmap on every
call. Most of the delta is workload (in-place update vs new-entry allocation),
not mmap cost. The single-thread `put` would also be heavily dominated by the
write-lock + hash-table allocation path.

² `noEviction` starts with 131 072 lists x 10 entries pre-populated, so every
append must read a metadata slot whose surrounding cache lines are cold
(~ 39 MB / segment, exceeds SLC). `evict30` starts empty: each fresh list's
metadata write lands on a cache line the writer was just touching, so the
metadata-RMW hits in L1/L2 while it still matters. The eviction cost (mmap
spill of older entries) is more than paid back by the cache locality of the
metadata path.

### What the writer/reader actually touches per op

`KeyValueRing.put(key, value)` (single-thread baseline -- the cheap side
of the gap):

1. **Hash** the 32-byte user key (1 polynomial hash, ~80 ns).
2. **Acquire** the segment's `StampedLock.writeLock()` -- one acquire +
   one release per call.
3. **`Tier.put`**: one `findSlot` walk (probes the per-segment
   `metadata[]` long array; on hash match, calls
   `kvBuffer.keyEquals(index, key, ...)` to widened-memcmp against the
   stored key bytes). Then `kvBuffer.putKeyValue(...)` writes the entry
   in place (in-place update on `noEviction` because the key already
   exists in the pre-populated range).

`KeyValueRing.put` cost summary: **1 hash, 1 lock pair, 1 `findSlot`,
1 segment touched.**

`KeyListsWriter.append(key, value)` (the expensive side):

1. **Pre-lock GET of metadata** (`KeyListStorage.java:239`):
   `kvStore.get(prefixedKey, ...)` -> first hash of the prefixed user
   key, *read-lock* acquire+release on segment A, one `findSlot` on A.
2. **Synthetic entry key encoding** (`KeyListStorage.writeSyntheticKey`):
   8-byte byte-by-byte write into a reusable buffer.
3. **`kvStore.compute(prefixedKey, syntheticKey, action)`**
   (`AbstractKeyValueRing.java:141`): hashes **both** key1 *and* key2
   (so the prefixed key is hashed a *second* time here in addition to
   step 1, plus the synthetic-key hash). Computes the two segment
   indices, sorts them canonically, then
   `runUnderTwoSegmentLocks(firstIdx, secondIdx, action)` acquires the
   first write-lock and -- when the two keys land on different
   segments -- the second write-lock.
4. Inside `TwoKeyComputeAction.execute()` (`KeyListStorage.java:282`):
   - **Verify-GET** of metadata: `metaCtx.get(...)` -- a second
     `findSlot` on segment A.
   - **Entry put** in segment B: `synCtx.put(value, ...)` -- third
     `findSlot` (on B), then `kvBuffer.putKeyValue(...)` to write the
     synthetic-key + payload bytes, then pack the metadata-array slot.
   - **Metadata put** in segment A: `metaCtx.put(metadataValue, ...)` --
     fourth `findSlot` (on A), then update the 8-byte metadata payload
     in place, then pack the slot.
5. Release both write-locks.

`KeyListsWriter.append` cost summary: **3 hashes** (prefixed key twice
+ synthetic key once), **3 `StampedLock` state-change pairs** (one
read-lock for the pre-GET, two write-locks for the dual-segment compute
-- six acquires/releases total), **4 `findSlot` walks** (three on
segment A: pre-GET, verify-GET, metadata put; one on segment B: entry
put), and **two segments' `kvBuffer` + metadata arrays touched per
call** (vs. one for `put`).

### Why `put` is ~30x faster than `append`

Per-op latency on this machine:
`1 / 12.45 M ~= 80 ns` for `put`, `1 / 412 K ~= 2.43 µs` for `append`.
The 30x ratio is the wall-clock difference, not the op-count
difference -- the API does ~3-4x more *work* per call, and the
remaining factor lives in cache traffic. Decomposing the ~2.43 µs
budget:

- **Lock state changes.** `put` does 1 write-lock pair (acquire +
  release ~= 60 ns uncontended). `append` does 1 read-lock pair plus
  *two* write-lock pairs (different segments) ~= 180 ns. Net extra
  for `append`: **~120 ns**.
- **`findSlot` walks.** `put` does 1; `append` does 4. Each extra walk
  is a metadata-array probe + a `kvBuffer.keyEquals` widened memcmp at
  a hash-distributed offset. The probe itself is cheap (8-byte long
  reads, almost always L1). The `keyEquals` reads two cache lines'
  worth of stored key bytes from `kvBuffer` at a slot-specific offset
  inside the per-segment kvBuffer region. **Three extra walks ~=
  300-450 ns**, of which most is cache miss cost (see below).
- **Hash compute.** `put` does 1 polynomial hash; `append` does 3
  (prefixed user key twice _(a room for micro-optimization)_ + synthetic key once). Net extra ~=
  **~160 ns**.
- **Cache traffic.** This is the dominant term and the reason the
  ratio is 30x rather than ~6-10x. `KeyValuesWriteBenchmark.put` runs
  on a pre-populated 131 072-key range (~ 4 MB / segment) that fits
  comfortably in the M1 Pro 12 MB L2 -- the writer's single `findSlot`
  + `keyEquals` almost always hits in cache.
  `KeyListsWriteBenchmark.append` runs on a 131 072 x 10-entry
  pre-population (~ 39 MB / segment, exceeds the 24 MB SLC) **and**
  touches *two* segments' arrays/buffers per call. Each call's hot
  path drags ~8-15 cache lines from DRAM at ~80 ns each ->
  **~600 ns - 1.2 µs**, the single biggest contributor.
- **Misc.** Synthetic-key 8-byte encode, the
  `ComputeContext`/`TwoKeyComputeAction` interface dispatch, the
  retry-on-userKeyIndex-race outer loop, second `kvBuffer.putKeyValue`
  for the entry put, the metadata in-place write. **~150-250 ns**.

Sum: **~1.4 - 2.3 µs of extra cost on top of `put`'s ~80 ns**, which
lands the `append` cost in the observed ~2.4 µs band. No single term
explains the 30x ratio; the design accounts for ~6-10x and cache
residency accounts for the rest.

A useful sanity check: if `KeyListsWriteBenchmark.append` ran against
a small enough pre-pop to fit in L2 (so the cache term collapses to
~0), the predicted per-op cost would be ~700-900 ns and the
`put` / `append` ratio would be **~9-11x**, not 30x. That's exactly
the pattern the eviction-vs-cache analysis already calls out -- the
`noEviction` KeyLists profile happens to size into the DRAM-bound
regime, so the headline gap looks bigger than the API-design gap on
its own.

### Working-set arithmetic

`BenchmarkSupport` constants:

- `HOT_TIER_NO_EVICTION = 128 MB` per segment
- `HOT_TIER_EVICT = 32 MB` per segment (`evict30`, all benchmarks)
- `RING_SIZE = 8` segments
- `KEY_ARRAY_SIZE = 131 072` (noEviction); `EVICT_KEY_ARRAY_SIZE = 2 097 152` (evict30 KV reads)
- `ENTRIES_PER_LIST = 10` pre-populated in noEviction / KeyLists concurrent

Per stored KV (in the segment's binary buffer):

- Metadata entry ~= 33-byte key + 8-byte value + 16-byte header = **57 B**
- Data entry ~= 8-byte synthetic key + 200-byte payload + 16-byte header = **224 B**

Per list footprint ~= 57 + 10 x 224 = **2.3 KB**.

KeyLists noEviction pre-populated footprint = 131 072 x 2.3 KB ~= **300 MB
across the ring** ~= **~37 MB of kvBuffer used per segment**, plus ~1.9 MB
metadata array per segment.

### Cache budget on the test machine

Apple M1 Pro: L1d = 128 KB per core, L2 = 12 MB per P-cluster, system-level
cache = 24 MB shared.

| | `noEviction` | `evict30` |
|---|---|---|
| Hot tier per segment | 128 MB | 32 MB |
| Steady-state used kvBuffer per segment | KV ~4 MB / KeyLists ~37 MB | KV ~32 MB / KeyLists ~32 MB (wraps) |
| Working set vs CPU caches | KV: fits in L2; KeyLists: **exceeds 24 MB SLC** | **exceeds 24 MB SLC for both** |
| Mmap involvement | none | continuous spill (writer also reads spilled data on reads / concurrent) |

The `noEviction` KV pre-population (~4 MB / segment) still fits in L2 even
on the cache-controlled side; eliminating that gap would mean bumping the
noEviction KV pre-pop to match `EVICT_KEY_ARRAY_SIZE`. It hasn't been done
here because the user asked specifically to keep one realistic *eviction*
test -- the noEviction asymmetry just means the `KeyValuesReadBenchmark.get`
delta (-34 %) bundles "KV cache misses" with "mmap accesses", while the
KeyLists deltas are pure eviction-cost (both sides are already cache-cold).

---

## `d4m-sequence`

### Parameters

- **`chunkSize`** -- bytes per fixed-size chunk (the unit of heap allocation
  and mmap mapping). Bigger chunks mean fewer chunk transitions and more
  amortised cost per chunk.
- **`sequenceCount`** -- how many independent sequences share one
  allocator/eviction queue. `1` is a single large sequence (~100 000 entries
  pre-populated); `1024` is many smaller sequences (~5 000 entries each, with
  a shared mmap pool). Power of two so the writer's round-robin
  `opCount & (sequenceCount - 1)` is a single AND.
- **`writeProfile`** -- operation mix used by the writer:
  - `APPEND_100`: 100 % monotonic appends (best case for the append-optimised path).
  - `APPEND_90_INSERT_10` / `APPEND_50_INSERT_50`: mix in COW inserts into random gaps (slower; COW touches whole tier-snapshots).
  - `APPEND_90_UPDATE_10` / `APPEND_50_UPDATE_50`: replace existing entries in-place (no COW, no growth).
- **`cursorType`** -- `FORWARD`, `BACKWARD`, `MERGED_FORWARD`, `MERGED_BACKWARD`. The non-merged ones iterate a single sequence; the merged ones N-way-merge across all sequences in the sweep.

### Hot-loop shape

For `APPEND_100` the writer benchmarks take an `appendOnlyEntry` fast path
that strips the profile-decision branch -- the hot loop is just
`sequences[opCount & (sequenceCount - 1)].append(orderCounters[si]++, payload, 0, PAYLOAD_SIZE)`.
The mixed write profiles (`APPEND_X_INSERT_Y`, `APPEND_X_UPDATE_Y`) keep the
full `writeEntry` dispatch since their storage cost dwarfs any
decision overhead. The read benchmarks were already clean
(`cursor.next(batch=256, NO_OP)` + counter accumulation).

One op = one writer step for `WriteBenchmark.write`; one
`cursor.next(batch=256, ...)` for `HistoricalReadBenchmark.read`; one writer
step + concurrent reader drain (group throughput) for the broadcast
benchmarks.

### Results (`writeProfile=APPEND_100`, forward cursor)

| Benchmark | Sequences | `chunkSize=65 536` | `chunkSize=131 072` | `chunkSize=524 288` |
|---|---|---:|---:|---:|
| `WriteBenchmark.write` | 1 | 21.83 M | 22.55 M | 22.81 M |
| `WriteBenchmark.write` | 1024 | 10.87 M | 10.55 M | 10.68 M |
| `HistoricalReadBenchmark.read` | 1024 | 112.91 M | 166.78 M | 145.01 M |
| `RealtimeBroadcastBenchmark.broadcast` | 1 | 111.85 M | 128.91 M | 114.90 M |
| `ScaledBroadcastBenchmark.twoReaders` | 1024 | 160.17 M | 149.65 M | 128.17 M |

Mixed-profile and per-cursor-type rows are excluded from this headline table
to keep it readable -- re-run with the full JMH args above to get the complete
matrix.
