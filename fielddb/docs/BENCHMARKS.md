# Benchmark report

> **Scope of these numbers.** They come from a **quickly assembled test build** of fielddb, a
> production-style database layer over the Field engine: MVCC, ten secondary indexes, ten triggers,
> asynchronous commit, vacuum and compaction. It is **not a production-hardened release**, it was
> built to answer one question quickly, and the comparison with Postgres is a comparison of an
> embedded engine with a client-server database on one specific workload. Read the figures as
> indicative.

## What is measured

- **Workload:** one table of 1.5M rows (200k in the small "in RAM" mode of the original runs), 264-byte
  rows, 10 indexed `bigint` columns, `version`/`chk` columns maintained by 10 identical `BEFORE UPDATE`
  triggers, a 168-byte payload. 8 clients, random keys, profiles of 0/20/80/100% UPDATE, each as an
  independent measurement after a warm-up. Page cache dropped before every "out of RAM" profile.
- **Engines:** Field (`fielddb/db/field_mvcc.cpp`, 8 shards, asynchronous commit, `msync` every 10 ms) and
  PostgreSQL (`synchronous_commit=off`, `fsync=on`, `wal_level=minimal`, aggressive autovacuum;
  configs in `fielddb/bench/pg/`). The loss window on a crash is the same for both.
- **Memory:** every engine runs in its own systemd cgroup scope with `MemoryMax` and `MemorySwapMax=0`.
- **Harness:** `fielddb/bench/run.py`. One run per measurement point, no repeats.

Load time is deliberately not reported: the test build's loader is not tuned or compared.

## Runs of 2026-11-24

Common settings: 8 clients, 8 s warm-up + 30 s measurement per profile, data of every
engine-and-mode pair deleted afterwards. "In RAM" and "out of RAM" mean different things on each stand:

| stand | "in RAM" | "out of RAM" |
|---|---|---|
| VPS, ext4 | 200k rows, 600 MB limit | 1.5M rows, 600 MB limit |
| local, btrfs | 1.5M rows, 8 GB limit (data fits), PG `shared_buffers=2GB` | 1.5M rows, 600 MB limit |

### VPS (ext4): 2 vCPU Xeon E5-2680 v2, 1.9 GB RAM, Postgres 17.12, g++ 14.2

| mode | UPDATE | Field ops/s | PG ops/s | Field / PG | UPDATE p50/p99 µs (F · PG) | read p50/p99 µs (F · PG) | msync avg, ms | disk write MB (F · PG) |
|---|---|---|---|---|---|---|---|---|
| in RAM | 0% | 1,244,743 | 3,328 | ×374.02 | n/a | 0/1 · 1660/16090 | 0.0 | 7.7 · 4.5 |
| in RAM | 20% | 92,745 | 1,874 | ×49.49 | 6/7172 · 4234/28067 | 1/442 · 2115/23930 | 8.7 | 1,007.1 · 54.4 |
| in RAM | 80% | 19,069 | 1,045 | ×18.25 | 7/10069 · 4947/44924 | 2/2601 · 2726/35411 | 8.6 | 1,054.8 · 37.8 |
| in RAM | 100% | 7,653 | 1,224 | ×6.25 | 23/16780 · 4184/45640 | n/a | 6.7 | 890.1 · 53.1 |
| out of RAM | 0% | 617,241 | 2,258 | ×273.36 | n/a | 1/2 · 2276/23050 | 0.0 | 8.1 · 6.4 |
| out of RAM | 20% | 2,512 | 736 | ×3.41 | 4988/44777 · 21601/99044 | 765/16144 · 4303/30572 | 3.0 | 211.2 · 537.2 |
| out of RAM | 80% | 1,077 | 253 | ×4.26 | 5121/52346 · 29163/113240 | 1192/19707 · 5311/40390 | 4.4 | 419.3 · 551.8 |
| out of RAM | 100% | 858 | 238 | ×3.61 | 4965/52311 · 22612/124480 | n/a | 4.0 | 361.2 · 723.4 |

### Local machine, btrfs (copy-on-write): 16 threads, 31 GB RAM, consumer NVMe, Postgres 18.3, clang 20.1.8

| mode | UPDATE | Field ops/s | PG ops/s | Field / PG | UPDATE p50/p99 µs (F · PG) | read p50/p99 µs (F · PG) | msync avg, ms | disk write MB (F · PG) |
|---|---|---|---|---|---|---|---|---|
| in RAM | 0% | 18,510,495 | 304,310 | ×60.83 | n/a | 0/0 · 22/65 | 0.0 | 1,408.7 · 727.3 |
| in RAM | 20% | 8,528 | 184,532 | ×0.05 | 380/27687 · 83/185 | 0/1461 · 24/74 | 36.2 | 6,017.1 · 2,458.6 |
| in RAM | 80% | 1,620 | 80,363 | ×0.02 | 2384/34426 · 91/288 | 0/5256 · 31/123 | 105.3 | 6,551.1 · 3,203.8 |
| in RAM | 100% | 1,608 | 66,384 | ×0.02 | 118/22497 · 91/282 | n/a | 159.3 | 5,639.4 · 3,008.2 |
| out of RAM | 0% | 222,146 | 40,279 | ×5.52 | n/a | 0/376 · 39/1975 | 0.0 | 489.7 · 555.3 |
| out of RAM | 20% | 4,150 | 2,400 | ×1.73 | 2645/27723 · 3544/48671 | 110/8858 · 500/13160 | 22.7 | 2,934.5 · 2,193.9 |
| out of RAM | 80% | 1,074 | 681 | ×1.58 | 6181/38912 · 8017/52132 | 178/9145 · 1259/16755 | 35.1 | 5,038.4 · 6,248.3 |
| out of RAM | 100% | 941 | 539 | ×1.75 | 4332/43460 · 11308/67772 | n/a | 39.0 | 4,765.8 · 5,293.9 |

### Local machine, btrfs, data directory with `chattr +C` (NOCOW)

| mode | UPDATE | Field ops/s | PG ops/s | Field / PG | UPDATE p50/p99 µs (F · PG) | read p50/p99 µs (F · PG) | msync avg, ms | disk write MB (F · PG) |
|---|---|---|---|---|---|---|---|---|
| in RAM | 0% | 18,885,156 | 292,494 | ×64.57 | n/a | 0/0 · 23/66 | 0.0 | 238.2 · 1,239.0 |
| in RAM | 20% | 55,748 | 178,965 | ×0.31 | 103/6611 · 82/186 | 0/1496 · 24/72 | 7.0 | 6,091.5 · 2,302.0 |
| in RAM | 80% | 16,473 | 81,327 | ×0.20 | 123/8335 · 91/281 | 0/2524 · 32/104 | 7.0 | 6,296.8 · 3,102.4 |
| in RAM | 100% | 6,087 | 69,276 | ×0.09 | 109/12428 · 89/270 | n/a | 16.9 | 2,695.4 · 3,090.2 |
| out of RAM | 0% | 231,829 | 42,466 | ×5.46 | n/a | 0/323 · 34/1833 | 0.0 | 457.7 · 641.9 |
| out of RAM | 20% | 4,215 | 2,379 | ×1.77 | 2506/25611 · 2867/42210 | 100/8786 · 383/13450 | 16.1 | 788.8 · 1,907.7 |
| out of RAM | 80% | 1,397 | 1,126 | ×1.24 | 3558/29883 · 3828/30168 | 127/8985 · 941/19554 | 21.6 | 850.1 · 2,734.9 |
| out of RAM | 100% | 1,468 | 810 | ×1.81 | 2108/23356 · 6603/35247 | n/a | 13.8 | 953.2 · 2,667.9 |

## Reading the three stands together

| stand | reads | writes, data in memory | writes, data not in memory |
|---|---|---|---|
| VPS, ext4 | Field ×270-370 | Field ×6-49 | Field ×3.4-4.3 |
| local, btrfs CoW | Field ×61 (in RAM), ×5.5 (out of RAM) | **Postgres ×20-50** | Field ×1.6-1.8 |
| local, btrfs NOCOW | Field ×65 (in RAM), ×5.5 (out of RAM) | **Postgres ×3-11** | Field ×1.2-1.8 |

1. **Reads.** Field reaches 18.5-18.9M ops/s in RAM against ~0.3M for Postgres. This is an embedded engine
   against a client-server database (socket, protocol, executor), not a statement about storage speed.
2. **Writes with the data in memory are decided by the file system.** On ext4 the same code and the same
   harness give the same order of magnitude as in the October runs (Field ahead). On btrfs Field is
   slower than Postgres: the average `msync` takes 7-160 ms against 3-9 ms on ext4. The engine's own
   source warns about btrfs (`dist/field_storage.hpp` and `storage/storage.hpp`: "A filesystem commit is per-INODE") and the
   upstream `bench/probe_sync_syscalls.cpp` and `bench/probe_split_log_and_data.cpp` measure that write
   amplification. `chattr +C` brings back 6.5-10×, but not the ext4 level. The mechanism (writers waiting
   for the log tail page that a flush is writing) was not verified and is a hypothesis.
3. **Writes with the data out of memory.** Field stays ahead of Postgres on every stand (×1.2-4.3): it
   spends less random I/O per miss.
4. **Integrity.** `index_check` and `err` were clean in every Field run. That indicator does not detect
   writes dropped because the log filled up (see [`REVIEW.md`](REVIEW.md), item 1); the log did not fill
   in any of these runs.
5. **Noise.** The VPS is a live host, the local machine was a normal workstation under use, and each
   point is a single 30 s run. The VPS was also about 30-40% slower than in October (Field 92.7k ops/s
   at 20% UPDATE in RAM against 119k, Postgres 1.9k against 5.6k). Compare engines within one run.

Raw results: [`../results/`](../results/).

## Engine benchmarks from the upstream project

These are the engine-level measurements that ship with the engine's history (`bench/` and
[`storage/FIELD_STORAGE.md`](../../storage/FIELD_STORAGE.md), sections 8a, 8b, 11 and 14). They use a
different setup from the database comparison above: a 1-core, 960 MB VPS, 1 KB records, one process per
cell, page cache dropped between cells, and a stated crash window.

| 1 KB records | Field | `storage.hpp` | LMDB |
|---|---|---|---|
| random update, in RAM | **3,208,666** | 12,611 | 9,905 |
| random update, out of RAM | **592,926** | 10,641 | 3,560 |
| major faults per update | **41** | 7,986 | 11,501 |
| CPU / iowait during updates | **93.8% / 0.00 s** | 3.7% / 13.52 s | 7.3% / 16.58 s |
| random read, in RAM | 1,541,045 | **6,550,781** | 827,178 |
| random read, out of RAM | **5,731** | 4,747 | 3,812 |
| fill, no durability | 236,231 | 143,559 | 237,283 |
| durable fill, 500-record crash window | **217,000** | n/a | 162,000 |
| durable fill, 5,000-record crash window | 230,000 | n/a | **240,000** |
| on disk per 1 GB of data | **1.05 GB** | 2.1 GB | 1.34 GB |

Against ObjectBox and LMDB at an identical 10,000-record crash window (1 GB of data on 960 MB of RAM,
300,000 operations):

| | Field | LMDB | ObjectBox |
|---|---|---|---|
| random update | **543,070** | 2,308 | 1,758 |
| major faults over 300,000 updates | **164** | 180,305 | 229,770 |
| iowait during the update phase | **0.01 s** | 90.74 s | 116.59 s |
| random read | **5,461** | 4,131 | 2,753 |
| fill | 249,105 | **283,158** | 178,002 |
| on disk per 1 GB of data | **1.05 GB** | 1.34 GB | 2.11 GB |

The out-of-RAM update rate depends on how much of the set a run sweeps: three runs of that cell gave
377,015, 435,691 and 543,070, so read it as a range. One writer per instance is the unit of scale:
sixteen independent instances on a desktop reach 125.8M updates/s (`bench/shard_scaling.cpp`).

Where each number comes from:

| program | what it answers |
|---|---|
| `bench/vps_matrix.cpp` | the same workload on three engines, two sizes, with and without a durability barrier placed by time |
| `bench/kv_comparison.cpp` | the bare engine against LMDB (and RocksDB) on an identical workload |
| `bench/schema_comparison.cpp` | the three engines doing the same schema work |
| `bench/big_dataset.cpp` | what happens when the data stops fitting in RAM |
| `bench/shard_scaling.cpp` | whether one writer per shard scales with cores |
| `bench/collapse_sweep.cpp` | what the collapse threshold costs and buys |
| `bench/uring_read.cpp` | what reading through an io_uring ring costs and buys |
| `bench/app_layer_bench.cpp`, `bench/app_schema_bench.cpp` | what a real application schema and the declared index layer cost |
| `bench/probe_commit_batching.cpp` | why the same engine reports ~1,200 durable ops/s in one benchmark and ~1,000,000 in another |
| `bench/probe_inplace_update_cost.cpp`, `bench/probe_split_log_and_data.cpp`, `bench/probe_sync_syscalls.cpp`, `bench/msync_probe.c` | the cost of one durability barrier, and the btrfs/CoW write amplification |
| `bench/probe_compaction_leak.cpp`, `bench/probe_restart_cost.cpp`, `bench/probe_snapshot_cost.cpp` | compaction leaks, restart time, snapshot memory |
| `bench/stress_heap.cpp`, `bench/business_demo_heap.cpp` | multi-phase stress scenario and a fiber-based business demo |
