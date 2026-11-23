# Code review of the fielddb layer and its benchmark

> Review of a **quickly assembled test build**, not of a production release. Findings are meant as a
> to-do list for turning it into one.

**Scope.** Read in full: `db/field_mvcc.cpp` (657 lines), `bench/run.py`, `field_storage.fixes.diff`, the
Postgres configs. `include/field_storage.hpp` (4,873 lines) was read selectively: `patch`, `append`,
recovery and replay, and the comments about `msync`. This is **not** a line-by-line review of the whole
engine. Everything was built with clang 20.1.8, C++23.

Status of each item: **[CONFIRMED]** reproduced by a run, **[CODE]** follows from reading the code,
**[HYPOTHESIS]** needs a measurement.

## What was checked automatically (clean)

| check | result |
|---|---|
| `tests/regress.cpp`, clang, ASan + UBSan | 5/5 pass |
| `field_mvcc.cpp`, clang, **ThreadSanitizer**, 20k rows, 4 clients, 50% and 100% UPDATE, compaction active (24-29 compactions) | 0 warnings |
| the same under **ASan + UBSan** (32 compactions) | 0 warnings |
| index check at the end of every run (2,000 rows × 10 indexes) | all ok in all runs |

Manual walk through MVCC (`take_snapshot`, `vacuum_round`, the undo chain): vacuum frees only versions
older than the oldest snapshot, a reader stops at the first version with `ts <= s`, and `s >= oldest`.
No use-after-free or lost version found.

## Bugs

### 1. [CONFIRMED, high] The benchmark silently loses writes when the log fills up
`patch()` returns `void`. When the log is full, `append()` increments `refused_appends` and returns
`nullptr`, but `mvcc_update` never learns about it: it still installs the version in `meta`, creates the
undo record and counts the transaction as successful. `refused_append_count()` is not printed by the
benchmark. The compactor does `return` on its first error and never runs again.

Reproduction: `-DROWS=20000 -DCAP_MB=12`, `NOCOMP=1`, 100% UPDATE, 4 clients:
`tps = 2,035,755, upd_avg = 1.9 µs, err = "", index_check = ok, log_mb_after = 12.0 of 12`.
With a full log the benchmark reports a record speed and not a single error.

Fix:
- `patch()` returns `bool`; on `false` the transaction rolls back (do not set `m.ts` / `m.prev`, `delete u`);
- print `refused_append_count()` for all shards and mark the run invalid when it is `> 0`;
- when the compactor fails, mark the whole run with `err` instead of only printing to stderr;
- at the end, compare the number of committed updates on the clients with `sum(version)/10` over the table.

### 2. [CODE, high for methodology] The latency histograms take 144 MB of the Field memory limit
`histo` holds a `std::vector<uint32_t>(2'000'001)`, that is 8 MB. Per client there are two histograms
(`hu`, `hr`) plus two merged ones, so with 8 clients it is 8 × 16 + 16 = **144 MB of anonymous memory**
inside the engine's cgroup. It shows in the earlier results too: "peak memory Field 130 MB" at 0% UPDATE.
In the `disk256` mode (a 256 MB limit) more than half of the limit is occupied by the benchmark itself,
and what is left for the page cache goes to the data. Random writes into 8 MB per thread also cause cache
misses in the hottest loop.
Fix: a log-linear (HDR-style) histogram of ~4-8 KB, or sampling one operation in 32.

### 3. [CODE, medium] Asymmetric latency measurement
For Field `steady_clock::now()` is called twice per operation (≈40 ns) while a read costs ≈380 ns
(18.5M ops/s, 8 threads), about 10%. For PG `pgbench -l --sampling-rate=0.02` logs only 2% of the
transactions. Both sides should sample.

### 4. [CODE, medium] Weak correctness checks
Only 2,000 random rows are checked against the indexes. Missing:
- a data invariant. The trigger gives `chk = XOR(1..version)`; a reader can verify that on every read for
  free and catch torn reads and wrong MVCC versions;
- a check for dropped writes (see item 1);
- a recovery test: `kill -9` in the middle of a run, reopen, check the indexes and the invariant, measure
  the loss window (expected ≤ `FLUSH_MS`);
- a snapshot isolation test: a long reader must not see commits made after its `s`.

### 5. [CODE, low] Temporary files leak when compaction fails
The branches `"cannot open .next"`, `"fresh index arena not mapped"` and `"fresh full"` do `return`
without `remove_next()`. Up to `2 × shard size` of garbage on disk, and the compactor is not restarted.

### 6. [CODE, low] `return (int)(sink.load() == 42)`
A trick against dead-code elimination. It returns 1 on a rare coincidence, `run.py` accepts both 0 and 1,
but it pollutes diagnostics. `asm volatile` / `benchmark::DoNotOptimize` is cleaner.

### 7. [CONFIRMED, medium] Methodology on btrfs
The engine is written for ext4 and warns about btrfs. On btrfs (copy-on-write) Field's write results fall
by tens of times; see [`BENCHMARKS.md`](BENCHMARKS.md). Moving to NOCOW (`chattr +C`) returns 6.5-10×.

## Performance ideas

All items except P0 are **hypotheses**: they follow from the structure of the code but were not measured.

**P0. Do not hold `fresh->sync()` under the shard's exclusive latch** [CODE]. In `compact_shard`, inside
`unique_lock w(sh.latch)`, there is `fresh->sync()` (an msync of the whole new file) and two `rename`s plus
`fsync(dir)`. The shard accepts no reads or writes for that time. Better: loop until the tail is small
(more than 3 rounds), `sync()` before taking the latch, and under the latch sync only the last small tail.
(Compaction did not trigger in the 30 s runs, so there is no measurement of this.)

**P1. False sharing in `db_t`** [CODE]. Without `alignas(64)` the following sit side by side: `gts`,
`horizon`, `commit_mu`, `vq_mu` and ten statistics counters (`undo_live`, `undo_freed`, `row_lock_waits`,
`excl_ns`...). `gts` is written at every commit by all writers and read by every reader in
`take_snapshot`. Give each its own cache line and keep per-thread counters summed at the end.

**P2. A global `vq_mu` on every UPDATE** [CODE]. Every update takes a shared mutex and does `push_back`
into a shared vector that also grows with duplicates. Replace with per-client queues handed to the
vacuum in batches.

**P3. Two `patch()` calls per UPDATE** [CODE]. `mvcc_update` writes two log records: the key value and
`version/chk` (16 bytes). That is two headers, two checksums and two override nodes per write. With
`max_depth = 4` it makes `collapse` (a rewrite of the whole 264-byte record) come around quickly. Idea:
one `patch` with a scatter list of two ranges, or reorder the fields so that `version/chk` sit next to the
key being changed.

**P4. The undo record holds the whole 264-byte row** [CODE]. One `new undo` (≈288 bytes) per update and a
`delete` in the vacuum thread (cross-thread free, allocator pressure). A delta (8 bytes of key + 16 bytes
of `version/chk`) and a pool are enough.

**P5. The reader pays for `shared_mutex` and a `seq_cst` store into the slot** [CODE]. Every read does an
atomic read-modify-write on the shard latch and a `seq_cst` store into the snapshot slot. At 0% UPDATE it
scales well (18.5M ops/s in RAM), but under a mixed load it is a cache-line ping-pong point. An
alternative is epochs/RCU on the reader side.

**P6. `msync` per shard, sequentially** [CODE]. The flusher walks 8 shards one by one, each time with
`MS_SYNC` under `sync_mu`. Issue `msync(MS_ASYNC)` / `sync_file_range` on all shards and wait once.

**P7. Prefetch instead of a full `touch`** [HYPOTHESIS]. `touch()` does a full `count(key)` for two keys
just to get the pages into memory. `__builtin_prefetch` / `madvise(WILLNEED)` on the found pages is enough.

**P8. Default build flags of this toolchain** [CODE]. clang here builds with `-fstack-protector-strong` and
`-fcf-protection=full` (visible in `-cc1`), plus fortify. For a benchmark of the hot path, compare with
`-fno-stack-protector -fcf-protection=none`.
