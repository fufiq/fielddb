// field_mvcc.cpp: a DBMS layer over Field storage, functionally equivalent to
// what Postgres does in the benchmark (pg/), so that a DBMS is compared with a
// DBMS and not a bare engine with Postgres.
//
//   partitions              A table is SHARDS independent Field engines (id -> shard by
//                           modulo), each with its own log, its own 10 indexes, its own
//                           latch and its own compaction. Like a partitioned table with
//                           local indexes in PG. The engine is single-writer but there
//                           are many shards, so a page fault in one does not stall others.
//   MVCC (snapshot reads)   The current row version lives in the engine, older ones in an
//                           undo chain (as in InnoDB). A commit gets a ts; ts values are
//                           published strictly in order, so snapshot S sees exactly the
//                           commits <= S. A reader never waits for a writing transaction,
//                           only for its shard's latch during the append (the analogue of
//                           a buffer content lock).
//   concurrent writers      A transaction runs in the client's thread: row lock
//                           (first-updater-wins, READ COMMITTED), row read, SET, 10
//                           triggers, warming of index pages, all under the shard's
//                           shared latch, i.e. in parallel. Only the append and the index
//                           fix-up on already-hot pages run under the exclusive latch.
//   10 secondary indexes    multi_index over k0..k9; only the index whose field really
//                           changed is updated (the stock index_set updates all 10 on
//                           every append).
//   10 triggers             BEFORE UPDATE: version += 1; chk ^= version.
//   durability              asynchronous commit = synchronous_commit=off: the commit
//                           returns after the append, a flusher msyncs the log tails
//                           every FLUSH_MS.
//   background cleanup      vacuum (undo versions invisible to every snapshot) plus log
//                           compaction per shard by threshold (fill>=50% && garbage>=30%).
//
//   field_mvcc <dir> load
//   field_mvcc <dir> run <update_pct> <clients> <warmup_s> <seconds> <label>

#include "field_storage.hpp"   // fielddb/include

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef ROWS
#define ROWS 1000000
#endif
#ifndef CAP_MB
#define CAP_MB 2048          // total log capacity, split across shards
#endif
#ifndef SHARDS
#define SHARDS 8
#endif
#ifndef FLUSH_MS
#define FLUSH_MS 10
#endif

constexpr std::size_t rows = ROWS;
constexpr std::size_t shards = SHARDS;
constexpr std::size_t shard_rows = (rows + shards - 1) / shards;
constexpr std::size_t shard_cap = ((std::size_t)CAP_MB << 20) / shards;

struct row {
    static constexpr storage::tag_t tag = 1;
    int64_t k0, k1, k2, k3, k4, k5, k6, k7, k8, k9;
    int64_t version;
    int64_t chk;
    char payload[168];
};
static_assert(sizeof(row) == 264);

static constexpr uint32_t col_offset(uint32_t c) { return (uint32_t)(offsetof(row, k0) + 8 * c); }

// ---- indexes: only those whose field changed are updated ---------------------
using base_indexes = storage::index_set<shard_rows + shard_rows / 4,
    storage::multi_index<&row::k0>, storage::multi_index<&row::k1>, storage::multi_index<&row::k2>,
    storage::multi_index<&row::k3>, storage::multi_index<&row::k4>, storage::multi_index<&row::k5>,
    storage::multi_index<&row::k6>, storage::multi_index<&row::k7>, storage::multi_index<&row::k8>,
    storage::multi_index<&row::k9>>;

struct delta_indexes : base_indexes {
    void on_record(auto& table, const storage::record_header& hdr, const std::byte* payload) {
        if (hdr.op != storage::record_op::update) return base_indexes::on_record(table, hdr, payload);
        const uint32_t lo = hdr.offset, hi = hdr.offset + hdr.len;
        if (hi <= col_offset(0) || lo >= col_offset(10)) return;            // no indexed field touched
        row before;
        if (!table.get(hdr.id, before)) return;
        row after = before;
        if (hi <= sizeof(row)) std::memcpy((std::byte*)&after + lo, payload, hdr.len);
        each_touched(lo, hi, [&](auto& ix) { ix.remove(hdr.id, before); ix.add(hdr.id, after); });
    }

    // Walks the lookup path for `key` in column `col`'s index, so the pages a
    // later remove/add will touch are resident before the exclusive latch.
    std::size_t touch(uint32_t col, int64_t key) const {
        std::size_t n = 0;
        auto one = [&]<auto member>(uint32_t c) { if (c == col) n = of<member>().count(key); };
        one.template operator()<&row::k0>(0); one.template operator()<&row::k1>(1);
        one.template operator()<&row::k2>(2); one.template operator()<&row::k3>(3);
        one.template operator()<&row::k4>(4); one.template operator()<&row::k5>(5);
        one.template operator()<&row::k6>(6); one.template operator()<&row::k7>(7);
        one.template operator()<&row::k8>(8); one.template operator()<&row::k9>(9);
        return n;
    }

    template <typename F>
    void each_touched(uint32_t lo, uint32_t hi, F&& f) {
        auto one = [&]<auto member>(uint32_t c) { if (lo < col_offset(c) + 8 && hi > col_offset(c)) f(of<member>()); };
        one.template operator()<&row::k0>(0); one.template operator()<&row::k1>(1);
        one.template operator()<&row::k2>(2); one.template operator()<&row::k3>(3);
        one.template operator()<&row::k4>(4); one.template operator()<&row::k5>(5);
        one.template operator()<&row::k6>(6); one.template operator()<&row::k7>(7);
        one.template operator()<&row::k8>(8); one.template operator()<&row::k9>(9);
    }
};

// The engine silently DROPS an update when it runs out of override nodes
// (set_override: "out of nodes: the record keeps its previous value"), so the
// node arena is sized so that cannot happen: (max_depth + 1) per record.
constexpr std::size_t max_depth = 4;
using spec = storage::field_spec<"f.log", shard_cap, "f.idx", shard_rows + 16, shard_rows * (max_depth + 1) + 16, max_depth, sizeof(row), "f.ckp">;
static_assert(shard_rows * (max_depth + 1) + 16 < 0x00FFFFFFu);
using table_t = storage::field_storage<spec, delta_indexes>;

// ---- triggers ----------------------------------------------------------------
static void trg(row& n) { n.version += 1; n.chk ^= n.version; }
using trigger_fn = void (*)(row&);
static trigger_fn volatile triggers[10] = {trg, trg, trg, trg, trg, trg, trg, trg, trg, trg};

static uint64_t mix(uint64_t x) {
    x += 0x9e3779b97f4a7c15ull;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ull;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebull;
    return x ^ (x >> 31);
}
static int64_t key_of(uint64_t seed) { return (int64_t)(mix(seed) >> 1); }
static double now_s() { return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count(); }

// global id (1..rows) <-> (shard, local id 1..shard_rows)
static std::size_t shard_of(storage::record_id g) { return (g - 1) % shards; }
static storage::record_id local_of(storage::record_id g) { return (g - 1) / shards + 1; }

struct files_t {
    std::string dir;
    std::string log() const { return dir + "/f.log"; }
    std::string idx() const { return dir + "/f.idx"; }
    std::string ckp() const { return dir + "/f.ckp"; }
    storage::storage_paths live() const { return {log(), idx(), ckp()}; }
    storage::storage_paths next() const { return {log() + ".next", idx() + ".next", ckp() + ".next"}; }
    void remove_next() const {
        for (auto f : {log() + ".next", idx() + ".next", idx() + ".next.sx", ckp() + ".next", ckp() + ".next.tmp"}) ::unlink(f.c_str());
    }
};
static void sync_dir(const std::string& d) {
    int fd = ::open(d.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd >= 0) { ::fsync(fd); ::close(fd); }
}

// ---- latency histogram: 1 us buckets up to 2 s ----------------------------------
struct histo {
    static constexpr std::size_t n = 2000000;
    std::vector<uint32_t> b = std::vector<uint32_t>(n + 1, 0);
    uint64_t count = 0;
    double sum_us = 0, max_us = 0;
    void add(double us) {
        std::size_t i = (std::size_t)us;
        ++b[i < n ? i : n];
        ++count; sum_us += us; if (us > max_us) max_us = us;
    }
    void merge(const histo& o) {
        for (std::size_t i = 0; i <= n; ++i) b[i] += o.b[i];
        count += o.count; sum_us += o.sum_us; if (o.max_us > max_us) max_us = o.max_us;
    }
    double pct(double p) const {
        if (!count) return 0;
        uint64_t want = (uint64_t)(p * count), acc = 0;
        for (std::size_t i = 0; i <= n; ++i) { acc += b[i]; if (acc > want) return (double)i; }
        return max_us;
    }
};

// ---- MVCC + shards ----------------------------------------------------------------
struct undo {
    uint64_t ts;
    std::atomic<undo*> prev{nullptr};
    row image;
};
struct row_meta {
    std::atomic<uint64_t> ts{0};
    std::atomic<undo*> prev{nullptr};
};

constexpr std::size_t max_clients = 256;
constexpr std::size_t lock_stripes = 1 << 16;
struct alignas(64) snap_slot { std::atomic<uint64_t> s{0}; };
struct alignas(64) stripe { std::mutex m; };

struct shard_t {
    files_t files;
    std::unique_ptr<table_t> cur;
    std::shared_mutex latch;     // shared: reads + txn prep; exclusive: append / swap
    std::mutex sync_mu;          // flusher vs compaction swap
    std::size_t synced = 0;      // under sync_mu
};

struct db_t {
    std::string dir;
    shard_t sh[shards];
    std::vector<row_meta> meta = std::vector<row_meta>(rows + 1);
    std::unique_ptr<stripe[]> locks = std::make_unique<stripe[]>(lock_stripes);
    std::mutex commit_mu;                // ts allocation + version install + publish, in that order
    std::atomic<uint64_t> gts{1};        // everything <= gts is published
    std::atomic<uint64_t> horizon{0};
    snap_slot slots[max_clients];
    std::mutex vq_mu;
    std::vector<storage::record_id> vacuum_queue;
    std::atomic<bool> stop{false};
    // stats
    std::atomic<uint64_t> undo_live{0}, undo_freed{0}, vacuum_rounds{0}, old_version_reads{0}, row_lock_waits{0};
    std::atomic<uint64_t> flushes{0}, flush_ns{0}, excl_ns{0}, excl_n{0};
    std::atomic<uint64_t> compactions{0}, compact_pause_us_max{0}, compact_ms_sum{0};
    std::string compact_error;
};

static uint64_t take_snapshot(db_t& db, int me) {
    while (true) {
        const uint64_t s = db.gts.load(std::memory_order_seq_cst);
        db.slots[me].s.store(s + 1, std::memory_order_seq_cst);
        if (s >= db.horizon.load(std::memory_order_seq_cst)) return s;
    }
}
static void drop_snapshot(db_t& db, int me) { db.slots[me].s.store(0, std::memory_order_release); }

static bool mvcc_read(db_t& db, storage::record_id g, uint64_t s, row& out) {
    shard_t& sh = db.sh[shard_of(g)];
    std::shared_lock l(sh.latch);
    row_meta& m = db.meta[g];
    if (m.ts.load(std::memory_order_acquire) <= s) return sh.cur->get(local_of(g), out);
    for (undo* u = m.prev.load(std::memory_order_acquire); u; u = u->prev.load(std::memory_order_acquire))
        if (u->ts <= s) { out = u->image; db.old_version_reads.fetch_add(1, std::memory_order_relaxed); return true; }
    return false;
}

// UPDATE t SET k<col> = value WHERE id = g  (+ 10 BEFORE UPDATE triggers), one transaction.
static void mvcc_update(db_t& db, storage::record_id g, uint32_t col, int64_t value) {
    std::mutex& rl = db.locks[g & (lock_stripes - 1)].m;
    if (!rl.try_lock()) { db.row_lock_waits.fetch_add(1, std::memory_order_relaxed); rl.lock(); }
    std::lock_guard row_lock(rl, std::adopt_lock);

    shard_t& sh = db.sh[shard_of(g)];
    const storage::record_id id = local_of(g);
    row_meta& m = db.meta[g];
    auto* u = new undo;
    int64_t old_key;
    {   // prepare under SHARED latch: every page fault of this txn happens here, in parallel
        std::shared_lock l(sh.latch);
        if (!sh.cur->get(id, u->image)) { delete u; return; }
        std::memcpy(&old_key, (std::byte*)&u->image + col_offset(col), 8);
        const auto& ix = sh.cur->observer_state();
        (void)ix.touch(col, old_key);
        (void)ix.touch(col, value);
    }
    row n = u->image;
    std::memcpy((std::byte*)&n + col_offset(col), &value, 8);
    for (int i = 0; i < 10; ++i) triggers[i](n);
    u->ts = m.ts.load(std::memory_order_relaxed);
    u->prev.store(m.prev.load(std::memory_order_relaxed), std::memory_order_relaxed);

    uint64_t ts = 0;
    {   // commit: append + version install, the only serialized part (per shard)
        std::unique_lock w(sh.latch);
        const auto e0 = std::chrono::steady_clock::now();
        table_t& t = *sh.cur;
        t.patch(id, col_offset(col), std::as_bytes(std::span{&value, 1}));
        t.patch(id, (uint32_t)offsetof(row, version), std::span<const std::byte>{(const std::byte*)&n.version, 16});
        {   // a few ns; orders publication across shards so snapshot S sees exactly commits <= S
            std::lock_guard c(db.commit_mu);
            ts = db.gts.load(std::memory_order_relaxed) + 1;
            m.prev.store(u, std::memory_order_release);
            m.ts.store(ts, std::memory_order_release);
            db.gts.store(ts, std::memory_order_release);
        }
        db.excl_ns.fetch_add((uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - e0).count(), std::memory_order_relaxed);
        db.excl_n.fetch_add(1, std::memory_order_relaxed);
    }
    db.undo_live.fetch_add(1, std::memory_order_relaxed);
    std::lock_guard q(db.vq_mu);
    db.vacuum_queue.push_back(g);
}

// ---- vacuum ------------------------------------------------------------------------
static void free_chain(db_t& db, undo* u) {
    while (u) { undo* p = u->prev.load(std::memory_order_relaxed); delete u; u = p; db.undo_live.fetch_sub(1, std::memory_order_relaxed); db.undo_freed.fetch_add(1, std::memory_order_relaxed); }
}
static void vacuum_round(db_t& db) {
    const uint64_t H = db.gts.load(std::memory_order_seq_cst);
    db.horizon.store(H, std::memory_order_seq_cst);
    uint64_t oldest = H;
    for (auto& sl : db.slots) {
        const uint64_t v = sl.s.load(std::memory_order_seq_cst);
        if (v && v - 1 < oldest) oldest = v - 1;
    }
    std::vector<storage::record_id> work;
    { std::lock_guard q(db.vq_mu); work.swap(db.vacuum_queue); }
    std::sort(work.begin(), work.end());
    work.erase(std::unique(work.begin(), work.end()), work.end());
    std::vector<storage::record_id> again;
    for (storage::record_id g : work) {
        std::lock_guard rl(db.locks[g & (lock_stripes - 1)].m);
        row_meta& m = db.meta[g];
        if (m.ts.load(std::memory_order_acquire) <= oldest) { free_chain(db, m.prev.exchange(nullptr, std::memory_order_acq_rel)); continue; }
        for (undo* u = m.prev.load(std::memory_order_acquire); u; u = u->prev.load(std::memory_order_acquire))
            if (u->ts <= oldest) { free_chain(db, u->prev.exchange(nullptr, std::memory_order_acq_rel)); break; }
        again.push_back(g);
    }
    if (!again.empty()) { std::lock_guard q(db.vq_mu); db.vacuum_queue.insert(db.vacuum_queue.end(), again.begin(), again.end()); }
    db.vacuum_rounds.fetch_add(1, std::memory_order_relaxed);
}
static void vacuumer(db_t& db) {
    while (!db.stop.load()) { std::this_thread::sleep_for(std::chrono::milliseconds(10)); vacuum_round(db); }
    vacuum_round(db);
}

// ---- async durability --------------------------------------------------------------
static void flush_shard(db_t& db, shard_t& sh) {
    std::lock_guard s(sh.sync_mu);
    std::byte* base; std::size_t end;
    { std::shared_lock l(sh.latch); base = sh.cur->raw_base(); end = sh.cur->current_offset(); }
    if (end <= sh.synced) return;
    const std::size_t page = storage::platform::page_size();
    const std::size_t start = sh.synced & ~(page - 1);
    const double t0 = now_s();
    ::msync(base + start, end - start, MS_SYNC);
    db.flush_ns.fetch_add((uint64_t)((now_s() - t0) * 1e9), std::memory_order_relaxed);
    db.flushes.fetch_add(1, std::memory_order_relaxed);
    sh.synced = end;
}
static void flusher(db_t& db) {
    while (!db.stop.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(FLUSH_MS));
        for (auto& sh : db.sh) flush_shard(db, sh);
    }
    for (auto& sh : db.sh) flush_shard(db, sh);
}

// Random access is the whole workload, and mmap's default readahead turns every
// 4 KB fault into ~128 KB of reads (measured: 15 GB read for 156k major faults).
// Every mapping of a file under `dir` gets MADV_RANDOM — set after open and
// again after each compaction swap, since those are new mappings.
static std::size_t advise_random(const std::string& dir) {
    std::ifstream maps("/proc/self/maps");
    std::string line;
    std::size_t n = 0;
    while (std::getline(maps, line)) {
        const auto slash = line.find('/');
        if (slash == std::string::npos || line.compare(slash, dir.size(), dir) != 0) continue;
        unsigned long lo = 0, hi = 0;
        if (std::sscanf(line.c_str(), "%lx-%lx", &lo, &hi) != 2) continue;
        if (::madvise((void*)lo, hi - lo, MADV_RANDOM) == 0) ++n;
    }
    return n;
}
static bool g_advise = std::getenv("NOADVISE") == nullptr;

// ---- background compaction, one shard at a time ------------------------------------
static bool replay_tail(table_t& from, table_t& to, std::size_t lo, std::size_t hi) {
    // Not apply_wal_range(): it copies the run and only THEN advances
    // write_offset, so a collapse() triggered inside the run appends over the
    // records not yet scanned.
    bool ok = true;
    const std::size_t end = from.for_each_record(lo, hi, [&](const storage::record_header& h, std::span<const std::byte> p) {
        switch (h.op) {
            case storage::record_op::create:  ok &= to.create_raw(h.id, h.tag, p) != 0; break;
            case storage::record_op::update:  to.patch(h.id, h.offset, p); break;
            case storage::record_op::destroy: to.destroy(h.id); break;
        }
    });
    return ok && end == hi;
}

static void compact_shard(db_t& db, shard_t& sh) {
    const double t0 = now_s();
    std::size_t w0;
    storage::record_id next_id;
    { std::shared_lock r(sh.latch); w0 = sh.cur->current_offset(); next_id = sh.cur->next_record_id(); }
    sh.files.remove_next();
    auto fresh = std::make_unique<table_t>(sh.files.next());
    if (!fresh->ok()) { db.compact_error = "cannot open .next"; return; }
    fresh->restart(spec::capacity);
    if (!fresh->index_arena_mapped()) { db.compact_error = "fresh index arena not mapped"; return; }
    alignas(8) std::byte buf[sizeof(row)];
    constexpr storage::record_id chunk = 1024;
    for (storage::record_id lo = 1; lo < next_id; lo += chunk) {
        std::shared_lock r(sh.latch);
        const storage::record_id hi = std::min<storage::record_id>(lo + chunk, next_id);
        for (storage::record_id id = lo; id < hi; ++id) {
            if (!sh.cur->alive(id)) continue;
            const storage::tag_t tag = sh.cur->tag_of(id);
            if (!sh.cur->read_raw(id, buf)) continue;
            if (!fresh->create_raw(id, tag, std::span<const std::byte>{buf, sizeof(row)})) { db.compact_error = "fresh full"; return; }
        }
    }
    fresh->raise_next_id(next_id);
    fresh->sync();
    std::size_t done = w0;
    for (int round = 0; round < 3; ++round) {
        std::size_t upto;
        { std::shared_lock r(sh.latch); upto = sh.cur->current_offset(); }
        if (upto <= done) break;
        bool ok;
        { std::shared_lock r(sh.latch); ok = replay_tail(*sh.cur, *fresh, done, upto); }
        if (!ok) { db.compact_error = "tail"; sh.files.remove_next(); return; }
        done = upto;
    }
    fresh->sync();
    std::unique_ptr<table_t> old;
    const double p0 = now_s();
    {
        std::lock_guard s(sh.sync_mu);
        std::unique_lock w(sh.latch);
        const std::size_t upto = sh.cur->current_offset();
        if (upto > done && !replay_tail(*sh.cur, *fresh, done, upto)) { db.compact_error = "final tail"; sh.files.remove_next(); return; }
        fresh->sync();
        ::unlink(sh.files.ckp().c_str());
        bool ok = ::rename((sh.files.log() + ".next").c_str(), sh.files.log().c_str()) == 0 &&
                  ::rename((sh.files.idx() + ".next").c_str(), sh.files.idx().c_str()) == 0;
        ::rename((sh.files.idx() + ".next.sx").c_str(), (sh.files.idx() + ".sx").c_str());
        sync_dir(sh.files.dir);
        if (!ok) { db.compact_error = "rename"; return; }
        fresh->retarget(sh.files.live());
        old = std::move(sh.cur);
        sh.cur = std::move(fresh);
        sh.synced = sh.cur->current_offset();
    }
    const uint64_t pus = (uint64_t)((now_s() - p0) * 1e6);
    old.reset();
    if (g_advise) advise_random(db.dir);
    db.compactions.fetch_add(1);
    uint64_t mx = db.compact_pause_us_max.load();
    while (pus > mx && !db.compact_pause_us_max.compare_exchange_weak(mx, pus)) {}
    db.compact_ms_sum.fetch_add((uint64_t)((now_s() - t0) * 1e3));
}
static void compactor(db_t& db) {
    while (!db.stop.load()) {
        for (int i = 0; i < 5 && !db.stop.load(); ++i) std::this_thread::sleep_for(std::chrono::milliseconds(100));
        for (auto& sh : db.sh) {
            if (db.stop.load()) return;
            double fill, garbage;
            { std::shared_lock r(sh.latch); fill = (double)sh.cur->current_offset() / spec::capacity; garbage = sh.cur->garbage_ratio(); }
            if (fill >= 0.5 && garbage >= 0.3) compact_shard(db, sh);
            if (!db.compact_error.empty()) { std::fprintf(stderr, "compaction failed: %s\n", db.compact_error.c_str()); return; }
        }
    }
}

// ---- process metrics ------------------------------------------------------------------
struct proc_stats {
    double utime = 0, stime = 0;
    long minflt = 0, majflt = 0, nvcsw = 0, nivcsw = 0;
    long long read_bytes = 0, write_bytes = 0;
    static proc_stats now() {
        proc_stats p;
        rusage u{}; getrusage(RUSAGE_SELF, &u);
        p.utime = u.ru_utime.tv_sec + u.ru_utime.tv_usec / 1e6;
        p.stime = u.ru_stime.tv_sec + u.ru_stime.tv_usec / 1e6;
        p.minflt = u.ru_minflt; p.majflt = u.ru_majflt; p.nvcsw = u.ru_nvcsw; p.nivcsw = u.ru_nivcsw;
        std::ifstream io("/proc/self/io");
        std::string k; long long v;
        while (io >> k >> v) { if (k == "read_bytes:") p.read_bytes = v; else if (k == "write_bytes:") p.write_bytes = v; }
        return p;
    }
};

static long rss_mb() { rusage u{}; getrusage(RUSAGE_SELF, &u); return u.ru_maxrss / 1024; }

static int do_load(const std::string& dir) {
    std::vector<std::thread> ts;
    std::atomic<int> failed{0};
    std::atomic<std::size_t> log_bytes{0}, ix_fail{0};
    const double t0 = now_s();
    for (std::size_t s = 0; s < shards; ++s) {
        ts.emplace_back([&, s] {
            files_t f{dir + "/s" + std::to_string(s)};
            ::mkdir(f.dir.c_str(), 0755);
            for (auto p : {f.log(), f.idx(), f.idx() + ".sx", f.ckp()}) ::unlink(p.c_str());
            f.remove_next();
            auto t = std::make_unique<table_t>(f.live());
            if (!t->ok()) { failed = 1; return; }
            t->restart(spec::capacity);
            if (!t->index_arena_mapped()) { failed = 2; return; }
            row r{};
            std::memset(r.payload, 'x', sizeof r.payload);
            for (storage::record_id g = s + 1; g <= rows; g += shards) {
                int64_t* k = &r.k0;
                for (int c = 0; c < 10; ++c) k[c] = key_of(g * 16 + c);
                if (t->create(r) != local_of(g)) { failed = 3; return; }
            }
            t->sync();
            t->checkpoint();
            log_bytes += t->current_offset();
            ix_fail += t->observer_state().failed_inserts();
        });
    }
    for (auto& t : ts) t.join();
    if (failed) { std::fprintf(stderr, "load failed: %d\n", failed.load()); return 1; }
    std::printf("{\"engine\":\"field\",\"phase\":\"load\",\"rows\":%zu,\"shards\":%zu,\"load_s\":%.2f,\"log_mb\":%.1f,\"index_failures\":%zu}\n",
                rows, shards, now_s() - t0, log_bytes / 1048576.0, ix_fail.load());
    return 0;
}

int main(int argc, char** argv) {
    if (argc < 3) { std::fprintf(stderr, "usage: %s dir load|run ...\n", argv[0]); return 2; }
    const std::string dir = argv[1];
    if (std::strcmp(argv[2], "load") == 0) return do_load(dir);
    if (argc < 8) { std::fprintf(stderr, "run <update_pct> <clients> <warmup> <seconds> <label>\n"); return 2; }
    const int upd = std::atoi(argv[3]), clients = std::atoi(argv[4]);
    const double warm = std::atof(argv[5]), secs = std::atof(argv[6]);
    const std::string label = argv[7];
    if (clients < 1 || (std::size_t)clients > max_clients) return 2;

    auto dbp = std::make_unique<db_t>();
    db_t& db = *dbp;
    db.dir = dir;
    const double o0 = now_s();
    std::size_t log_before = 0;
    for (std::size_t s = 0; s < shards; ++s) {
        shard_t& sh = db.sh[s];
        sh.files = files_t{dir + "/s" + std::to_string(s)};
        sh.files.remove_next();
        sh.cur = std::make_unique<table_t>(sh.files.live());
        if (!sh.cur->ok()) { std::fprintf(stderr, "open failed\n"); return 1; }
        sh.cur->restart(spec::capacity);
        if (!sh.cur->index_arena_mapped()) { std::fprintf(stderr, "index arena not mapped\n"); return 1; }
        sh.synced = sh.cur->current_offset();
        log_before += sh.synced;
    }
    const double open_s = now_s() - o0;
    char real[4096];
    if (::realpath(dir.c_str(), real)) db.dir = real;
    const std::size_t advised = g_advise ? advise_random(db.dir) : 0;

    auto off = [](const char* e) { return std::getenv(e) != nullptr; };
    std::thread fl([&] { if (!off("NOFLUSH")) flusher(db); }), vac([&] { if (!off("NOVAC")) vacuumer(db); }),
                comp([&] { if (!off("NOCOMP")) compactor(db); });

    std::atomic<int> phase{0};
    std::vector<histo> hu(clients), hr(clients);
    std::vector<std::thread> ws;
    std::atomic<uint64_t> sink{0};
    for (int ti = 0; ti < clients; ++ti) {
        ws.emplace_back([&, ti] {
            std::mt19937_64 rng(1234567 + ti);
            uint64_t local = 0;
            while (true) {
                const int ph = phase.load(std::memory_order_relaxed);
                if (ph == 2) break;
                const uint64_t x = rng();
                const storage::record_id g = 1 + (x % rows);
                const bool is_update = (int)((x >> 40) % 100) < upd;
                const double a = now_s();
                if (is_update) {
                    mvcc_update(db, g, (uint32_t)((x >> 20) % 10), (int64_t)(rng() >> 1));
                } else {
                    const uint64_t s = take_snapshot(db, ti);
                    row r;
                    if (mvcc_read(db, g, s, r)) local += (uint64_t)r.version + (uint64_t)r.k3;
                    drop_snapshot(db, ti);
                }
                if (ph == 1) (is_update ? hu[ti] : hr[ti]).add((now_s() - a) * 1e6);
            }
            sink += local;
        });
    }
    std::this_thread::sleep_for(std::chrono::duration<double>(warm));
    const uint64_t c0 = db.compactions.load(), f0 = db.flushes.load(), fn0 = db.flush_ns.load(), w0 = db.row_lock_waits.load(),
                   ov0 = db.old_version_reads.load(), ex0 = db.excl_ns.load(), exn0 = db.excl_n.load();
    const proc_stats p0 = proc_stats::now();
    phase = 1;
    const double m0 = now_s();
    std::this_thread::sleep_for(std::chrono::duration<double>(secs));
    phase = 2;
    const double elapsed = now_s() - m0;
    const proc_stats p1 = proc_stats::now();
    for (auto& w : ws) w.join();
    db.stop = true;
    fl.join(); vac.join(); comp.join();

    histo U, R;
    for (int i = 0; i < clients; ++i) { U.merge(hu[i]); R.merge(hr[i]); }

    // the 10 indexes of every shard must agree with the data
    std::size_t checked = 0, bad = 0, ov_used = 0, ix_fail = 0, log_after = 0, live = 0;
    {
        std::mt19937_64 rng(42);
        for (int s = 0; s < 2000; ++s) {
            const storage::record_id g = 1 + rng() % rows;
            table_t& t = *db.sh[shard_of(g)].cur;
            const storage::record_id id = local_of(g);
            row r; if (!t.get(id, r)) { ++bad; continue; }
            auto& ix = t.observer_state();
            auto has = [&](auto& index, int64_t key) { bool hit = false; index.each(key, [&](storage::record_id x) { hit |= x == id; }); return hit; };
            bad += !has(ix.of<&row::k0>(), r.k0) + !has(ix.of<&row::k1>(), r.k1) + !has(ix.of<&row::k2>(), r.k2) +
                   !has(ix.of<&row::k3>(), r.k3) + !has(ix.of<&row::k4>(), r.k4) + !has(ix.of<&row::k5>(), r.k5) +
                   !has(ix.of<&row::k6>(), r.k6) + !has(ix.of<&row::k7>(), r.k7) + !has(ix.of<&row::k8>(), r.k8) +
                   !has(ix.of<&row::k9>(), r.k9);
            checked += 10;
        }
        for (auto& sh : db.sh) {
            ov_used = std::max(ov_used, sh.cur->overrides_used());
            ix_fail += sh.cur->observer_state().failed_inserts();
            log_after += sh.cur->current_offset();
            live += sh.cur->live_bytes();
            sh.cur->checkpoint();
        }
    }
    for (auto& m : db.meta) free_chain(db, m.prev.exchange(nullptr));

    const double nf = (double)(db.flushes.load() - f0);
    const double exn = (double)(db.excl_n.load() - exn0);
    std::printf("{\"engine\":\"field\",\"label\":\"%s\",\"update_pct\":%d,\"threads\":%d,\"shards\":%zu,\"seconds\":%.1f,"
                "\"tps\":%.0f,\"update_tps\":%.0f,\"read_tps\":%.0f,"
                "\"upd_avg_us\":%.1f,\"upd_p50_us\":%.0f,\"upd_p99_us\":%.0f,\"upd_max_us\":%.0f,"
                "\"read_avg_us\":%.2f,\"read_p50_us\":%.0f,\"read_p99_us\":%.0f,\"read_max_us\":%.0f,"
                "\"proc_user_s\":%.2f,\"proc_sys_s\":%.2f,\"proc_minflt\":%ld,\"proc_majflt\":%ld,\"proc_vcsw\":%ld,\"proc_ivcsw\":%ld,"
                "\"proc_read_mb\":%.1f,\"proc_write_mb\":%.1f,"
                "\"excl_avg_us\":%.2f,"
                "\"flushes\":%.0f,\"flush_avg_us\":%.0f,\"row_lock_waits\":%llu,\"old_version_reads\":%llu,"
                "\"undo_freed\":%llu,\"vacuum_rounds\":%llu,"
                "\"compactions\":%llu,\"compact_pause_max_ms\":%.1f,\"compact_total_ms\":%llu,"
                "\"log_mb_before\":%.1f,\"log_mb_after\":%.1f,\"garbage_after\":%.3f,"
                "\"advised_mappings\":%zu,\"overrides_used_max_shard\":%zu,\"overrides_max\":%zu,\"open_s\":%.2f,\"index_failures\":%zu,"
                "\"index_check\":\"%zu/%zu ok\",\"maxrss_mb\":%ld,\"err\":\"%s\"}\n",
                label.c_str(), upd, clients, shards, elapsed,
                (U.count + R.count) / elapsed, U.count / elapsed, R.count / elapsed,
                U.count ? U.sum_us / U.count : 0, U.pct(0.5), U.pct(0.99), U.max_us,
                R.count ? R.sum_us / R.count : 0, R.pct(0.5), R.pct(0.99), R.max_us,
                p1.utime - p0.utime, p1.stime - p0.stime, p1.minflt - p0.minflt, p1.majflt - p0.majflt, p1.nvcsw - p0.nvcsw, p1.nivcsw - p0.nivcsw,
                (p1.read_bytes - p0.read_bytes) / 1048576.0, (p1.write_bytes - p0.write_bytes) / 1048576.0,
                exn ? (db.excl_ns.load() - ex0) / 1e3 / exn : 0,
                nf, nf ? (db.flush_ns.load() - fn0) / 1e3 / nf : 0,
                (unsigned long long)(db.row_lock_waits.load() - w0), (unsigned long long)(db.old_version_reads.load() - ov0),
                (unsigned long long)db.undo_freed.load(), (unsigned long long)db.vacuum_rounds.load(),
                (unsigned long long)(db.compactions.load() - c0), db.compact_pause_us_max.load() / 1e3, (unsigned long long)db.compact_ms_sum.load(),
                log_before / 1048576.0, log_after / 1048576.0, log_after ? 1.0 - (double)live / log_after : 0.0,
                advised, ov_used, spec::max_overrides, open_s, ix_fail, checked - bad, checked, rss_mb(), db.compact_error.c_str());
    return (int)(sink.load() == 42);
}
