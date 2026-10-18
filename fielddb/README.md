# fielddb: the Field engine with fixes, an MVCC layer, and a benchmark against Postgres

> **Read this first.** Everything measured here comes from a **quickly assembled test build** of
> fielddb, a production-style database layer (MVCC, ten secondary indexes, ten triggers,
> asynchronous commit) over the Field storage engine. It is **not a production-hardened
> release**. The numbers show how the design behaves under one specific workload; treat them as
> indicative, not as a product claim.

Release 2026-10-14. The base is the latest copy of the engine: the single-header amalgam of
[`dist/field_storage.hpp`](../dist/field_storage.hpp) plus two downstream patches (`retarget`,
`raise_next_id`).

```
include/field_storage.hpp      fixed engine, drop-in replacement (same API)
field_storage.fixes.diff       every change to the engine relative to the original
tests/regress.cpp              5 regression tests: all fail on the original, all pass on the release
tests/test_field_storage.patch a fix to the upstream test that had pinned bug No. 2 as normal behaviour
db/field_mvcc.cpp              DBMS layer over the engine: MVCC, shards, concurrent writers,
                               10 indexes, 10 triggers, async commit, vacuum, compaction
bench/                         orchestrator (run.py), Postgres configs and schema, first version of the layer
results/                       raw results (jsonl) of every run
docs/                          benchmark report and code review
```

## How to use it

```cpp
#include "field_storage.hpp"   // from fielddb/include
```

The API did not change. One behaviour did: an UPDATE for which the override nodes ran out is now
applied (the record is rewritten with a single CREATE) instead of being silently dropped. See
bug No. 2 below.

Check:

```
clang++ -std=c++23 -O1 -g -fsanitize=address,undefined -I include tests/regress.cpp -o regress && ./regress
```

## Engine bugs fixed

All five were found by the benchmark. Each one is reproduced by a test in `tests/regress.cpp`
on the original engine. The same bugs exist in the modular sources under `storage/` and in the
generated `dist/field_storage.hpp` of this repository.

### 1. `flat_hash_map::erase`: secondary indexes silently lose rows
`indexes.hpp`. The backward shift after a deletion (linear probing) has the branches of its
condition swapped. In the ordinary (non-wrapped) case the condition `home <= i && home > j` for
`i < j` is never true, so elements after the hole are not shifted and become unreachable for
`find`. In the benchmark, up to 43% of index entries are lost after a minute of UPDATEs.
**Fix:** `wrapped ? (home <= i && home > j) : (home <= i || home > j)`.

### 2. `set_override`: an UPDATE was silently dropped when nodes ran out
`field_storage.hpp`. When the override node arena (`max_overrides`) is exhausted, the code did a
`return` with the comment "the record keeps its previous value". By that point the UPDATE is
already in the log and the observer has already updated the indexes. The result: an acknowledged
write disappears, the indexes diverge from the data, and after a replay the UPDATE "comes back
to life". Up to `(max_depth+1)` nodes are needed per record, so with typical settings
(`max_overrides` ≈ 3×rows, `max_depth` 8) this happens under ordinary load.
**Fix:** a new `rewrite_with()` rewrites the record with one CREATE that includes the update.
A test in the upstream suite had pinned the old behaviour; the correction is in
`tests/test_field_storage.patch`.

### 3. `apply_wal_range`: a collapse corrupts the receiver's log
Used by replication and by background compaction. The function copied the whole WAL chunk into
the log, scanned it in place and advanced `write_offset` only at the end. If a record in the
chunk triggers a collapse, that collapse appends a CREATE at the not-yet-advanced `write_offset`,
that is, on top of the copied chunk. The in-memory state may even stay correct, but the log on
disk is torn: the receiver's replay reads 5.4 MB of 6.0 MB and the rest is lost on restart. In
the benchmark, compaction failed with the error "tail".
**Fix:** record by record: copy, advance `write_offset`, then `apply`. Whatever `apply` appends
lands after it.

### 4. `recover()`: replay after a crash corrupts the log and the data
The same mechanism, but during recovery. While replaying, `write_offset` is not yet set, so a
collapse (on `depth > max_depth` or `max_span`) writes over the log directly on disk. After a
restart some rows come back with other rows' values (test 5: row 198).
**Fix:** a `replaying` flag. While a replay is running, collapse and `rewrite_with` append
nothing. The original run had already logged each of its collapses as a CREATE, and the replay
applies it as the next record.

### 5. `index_set()` keeps the whole index on the heap
The constructor did `adopt({}, true)`, that is, it allocated and zeroed a copy of the whole index
arena on the heap before the engine mapped the arena file. The engine then switched the index to
mmap, but the heap was never released. For 3M rows and 10 indexes that is ~1.9 GB of anonymous
memory: an OOM under a cgroup limit, which makes the "out of RAM" mode with indexes impossible.
**Fix:** the constructor allocates nothing. The heap is released on the switch to mmap. If the
arena file cannot be mapped, the engine falls back to the heap by itself.

## The DBMS layer (`db/field_mvcc.cpp`)

It exists so that a DBMS is compared with a DBMS, not a bare engine with Postgres:

| Postgres feature | how it is done over Field |
|---|---|
| MVCC, reads do not wait for writers | every commit has its own timestamp; the current version is in the engine, older ones in an undo chain (as in InnoDB); a snapshot sees exactly the commits made before it |
| concurrent writers | row lock (first updater wins, READ COMMITTED); the row read and 10 triggers run in parallel under a shared latch; only the append runs under the exclusive one |
| partitions, parallel I/O | 8 shards, each with its own log, indexes, latch and compaction |
| 10 secondary indexes | `multi_index` over k0..k9; only the index whose key changed is updated |
| 10 BEFORE UPDATE triggers | called through an array of function pointers |
| `synchronous_commit=off` | the commit returns after the append, `msync` of the log tails every 10 ms |
| autovacuum | vacuum of undo versions plus compaction of a shard's log by threshold |
| random access | `MADV_RANDOM` on all mmap files (without it, readahead read ~100 KB per 4 KB page) |

Limitations: the row schema is hard-wired into the code (this is a benchmark, not a general-purpose
library); secondary indexes keep only the current key, so reading by a secondary key from an old
snapshot is not supported.

## Results

Hardware: VPS, 2 vCPU Xeon E5-2680 v2, steal 8-13%, ~1.9 GB RAM, ext4, Postgres 17.11. 8 clients,
60 s measurement after a 10 s warm-up. Both engines run in a cgroup with a memory limit; before the
"out of RAM" profiles the file cache is dropped. Metrics come from the engine's cgroup, while
`/proc/stat` and `/proc/diskstats` come from the whole system.

### In RAM (200k rows, 600 MB limit)

| UPDATE | Field, ops/s | PG, ops/s | × | UPDATE p50/p99, µs (F / PG) | read p50/p99, µs (F / PG) | CPU per op, µs (F / PG) | disk write, MB (F / PG) | iowait, s (F / PG) |
|---|---|---|---|---|---|---|---|---|
| 0% | 2 789 885 | 10 649 | 262 | n/a | 0/1 · 642/3011 | 0.7 / 124 | 1 / 1 | 0 / 0 |
| 20% | 119 036 | 5 569 | 21 | 12/5606 · 1530/11116 | 1/402 · 695/7582 | 8.6 / 268 | 5012 / 132 | 24 / 0 |
| 80% | 44 295 | 3 197 | 14 | 7/5447 · 1817/12025 | 2/982 · 1154/9173 | 22.6 / 524 | 7680 / 195 | 23 / 0 |
| 100% | 50 147 | 2 963 | 17 | 5/4359 · 1999/11454 | n/a | 20.3 / 577 | 7657 / 204 | 25 / 0 |

### Out of RAM (1.5M rows, 600 MB limit; reads: 256 MB limit)

| UPDATE | Field, ops/s | PG, ops/s | × | UPDATE p50/p99, µs (F / PG) | read p50/p99, µs (F / PG) | CPU per op, µs (F / PG) | disk read, MB (F / PG) | disk write, MB (F / PG) |
|---|---|---|---|---|---|---|---|---|
| 0% (256M) | 11 592 | 4 002 | 2.9 | n/a | 419/5181 · 1435/11006 | 72 / 279 | 2780 / 2110 | 1 / 1 |
| 20% | 3 556 | 1 668 | 2.1 | 2670/23742 · 11004/44192 | 402/8458 · 1843/14721 | 149 / 771 | 850 / 2353 | 666 / 1983 |
| 80% | 1 699 | 715 | 2.4 | 2832/26133 · 9336/58270 | 686/9574 · 3274/47737 | 347 / 1799 | 958 / 2118 | 1240 / 2950 |
| 100% | 1 541 | 574 | 2.7 | 2749/25444 · 10459/49253 | n/a | 401 / 2313 | 936 / 2225 | 1114 / 3167 |

Major page faults in PG are always 0: it reads through `read()`, not mmap, so its misses show up
as disk reads rather than faults. Field takes ~1 major fault per miss, ~4 KB each.

### How to read this

- **In RAM, PG is CPU-bound:** 124-577 µs per operation against 0.7-23 for Field. Most of the
  difference is the client-server design (socket, protocol, executor, PL/pgSQL) that an embedded
  Field does not have. The ×262 on reads is "DBMS versus embedded engine", not storage speed.
- **In RAM, Field is bound by the disk:** ~3.5 KB written per UPDATE against 1.2-2 KB for PG.
  This is the kernel flushing the pages of the hash indexes from mmap that every UPDATE dirtied,
  not the log: the log takes ~100 B per UPDATE. On this VPS the disk copes; on a slower one it
  would be the ceiling. The fix is to move the indexes into anonymous memory and rebuild them
  from the log.
- **Out of RAM both are bound by random I/O, but Field spends it more economically:**
  ~4 KB read per miss, and writes without full-page writes. Hence ×2.1-2.9.

### History of the runs (what each step fixed)

| out of RAM, 100% UPDATE | ops/s |
|---|---|
| v1: one exclusive latch for the whole table, misses taken under it | 261 |
| v2: 8 shards, preparation under a shared latch, index warm-up | 938 |
| v3: + `MADV_RANDOM` (15 GB of excess readahead per minute → 0.9 GB) | 1 541 |
| Postgres 17 | 574 |

The metrics also exposed a mistake in the layer itself: ordered publication of commits on a spin
wait gave 14M idle cycles and 5 s of system CPU on 2 vCPUs. It was replaced with a short mutex.

## Final run with CPU and memory (2026-10-18)

The same tests on the release version, plus CPU, memory (heap / file cache / dirty pages) sampled
every 0.5 s from each engine's cgroup, and CPU load, iowait, steal and free memory from the whole
system. For PG the pgbench client was sampled separately. Tables:
[`results/final_resources/RESULTS_RESOURCES.md`](results/final_resources/RESULTS_RESOURCES.md).

| profile | Field / PG | CPU Field / PG | peak memory Field / PG |
|---|---|---|---|
| RAM, 0% UPDATE | ×388 | 192% / 190% | 130 / 215 MB |
| RAM, 20% | ×32 | 98% / 181% | 341 / 242 MB |
| RAM, 80% | ×13 | 80% / 187% | 475 / 257 MB |
| RAM, 100% | ×10 | 79% / 189% | 421 / 256 MB |
| out of RAM, 20% | ×1.6 | 51% / 154% | 600 / 600 MB |
| out of RAM, 80% | ×2.9 | 78% / 155% | 600 / 600 MB |
| out of RAM, 100% | ×2.9 | 79% / 160% | 600 / 600 MB |
| out of RAM, reads (256 MB) | ×3.3 | 98% / 155% | 256 / 256 MB |

On that day the VPS was slower: PG lost 30-40% with no change at all compared with the run of
2026-10-14. So compare Field with PG within one run.

More runs (2026-11-24: the same VPS, and a local machine on btrfs) and a code review of this
layer are in [`docs/`](docs/).

## Reproduce

```
cd bench && ./run.py all --seconds 60 --warmup 10          # RAM + out of RAM, both engines
./run.py disk256 --profiles 0 --tag cap256                 # reads under a 256 MB limit
```

Needs Postgres 17 (`/usr/lib/postgresql/17/bin`), g++ 14 or clang 20 with C++23, and systemd
`--user` with a delegated memory controller.
