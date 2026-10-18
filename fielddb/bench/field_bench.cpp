// field_bench.cpp: Field storage (as vendored by the downstream backend, 2026-10-14) under the
// same workload as Postgres in pg/:
//
//   table row: 10 indexed int64 (k0..k9), version, chk, 168-byte payload
//   10 secondary indexes  - storage::multi_index over k0..k9 (maintained by the engine
//                           through an observer: retract+add of all 10 on every append)
//   10 identical triggers - BEFORE UPDATE: version += 1; chk ^= version
//   N concurrent clients  - an UPDATE goes to a group-commit queue, one committer applies
//                           the batch and does ONE msync(MS_SYNC) per batch; the client
//                           gets its answer only after the msync (= synchronous_commit=on)
//   reads                 - std::shared_mutex (shared), in parallel with each other and
//                           with msync. This is NOT MVCC: a reader waits while the
//                           committer applies a batch (microseconds) but not for the disk.
//   background cleanup    - a compactor thread as in the backend maintenance code:
//                           fill>=50% && garbage>=30% -> new engine on *.next, live
//                           records copied in chunks under a shared lock, WAL tail
//                           catch-up, a short exclusive pause for the swap.
//
//   field_bench <dir> load
//   field_bench <dir> run <update_pct> <threads> <warmup_s> <seconds> <label>

#include "field_storage.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <sys/resource.h>

#ifndef ROWS
#define ROWS 1000000
#endif
#ifndef CAP_MB
#define CAP_MB 2048
#endif

constexpr std::size_t rows = ROWS;
constexpr std::size_t cap = (std::size_t)CAP_MB << 20;

struct row {
    static constexpr storage::tag_t tag = 1;
    int64_t k0, k1, k2, k3, k4, k5, k6, k7, k8, k9;
    int64_t version;
    int64_t chk;
    char payload[168];
};
static_assert(sizeof(row) == 264);

using indexes = storage::index_set<rows + rows / 4,
    storage::multi_index<&row::k0>, storage::multi_index<&row::k1>, storage::multi_index<&row::k2>,
    storage::multi_index<&row::k3>, storage::multi_index<&row::k4>, storage::multi_index<&row::k5>,
    storage::multi_index<&row::k6>, storage::multi_index<&row::k7>, storage::multi_index<&row::k8>,
    storage::multi_index<&row::k9>>;

using spec = storage::field_spec<"f.log", cap, "f.idx", rows + 16, rows * 3, 8, sizeof(row), "f.ckp">;
using table_t = storage::field_storage<spec, indexes>;

// ---- triggers -------------------------------------------------------------
// Ten identical ones, called through an array of pointers as a DBMS calls its
// trigger list, so the compiler cannot fold them into a single version += 10.
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

static double now_s() {
    return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

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

// ---- latency histogram: 100 ns buckets up to 50 ms -------------------------
struct histo {
    static constexpr std::size_t n = 500000;
    std::vector<uint32_t> b = std::vector<uint32_t>(n + 1, 0);
    uint64_t count = 0;
    double sum_us = 0, max_us = 0;
    void add(double us) {
        std::size_t i = (std::size_t)(us * 10);
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
        for (std::size_t i = 0; i <= n; ++i) { acc += b[i]; if (acc > want) return i / 10.0; }
        return max_us;
    }
};

// ---- the shared state -------------------------------------------------------
struct req {
    storage::record_id id;
    uint32_t col;
    int64_t value;
    std::atomic<uint32_t> done{0};
};

struct db_t {
    files_t files;
    std::unique_ptr<table_t> cur;
    std::shared_mutex rw;      // reads: shared; apply/swap: exclusive
    std::mutex sync_mu;        // held across the msync; lock order: rw -> sync_mu

    std::mutex qmu;
    std::condition_variable qcv;
    std::vector<req*> queue;
    std::atomic<bool> stop{false};

    // stats
    std::atomic<uint64_t> batches{0}, batched{0}, apply_ns{0}, apply_ns_max{0}, sync_ns{0};
    std::atomic<uint64_t> compactions{0}, compact_pause_us_max{0}, compact_pause_us_sum{0}, compact_ms_sum{0};
    std::atomic<uint64_t> compact_bytes_before{0}, compact_bytes_after{0};
    std::string compact_error;
};

static constexpr uint32_t col_offset(uint32_t c) { return (uint32_t)(offsetof(row, k0) + 8 * c); }

static void committer(db_t& db) {
    std::vector<req*> batch;
    batch.reserve(256);
    while (true) {
        {
            std::unique_lock lk(db.qmu);
            db.qcv.wait(lk, [&] { return !db.queue.empty() || db.stop.load(); });
            if (db.queue.empty() && db.stop.load()) return;
            batch.swap(db.queue);
        }
        std::unique_lock w(db.rw);
        const auto a0 = std::chrono::steady_clock::now();
        table_t& t = *db.cur;
        for (req* r : batch) {
            row n;
            if (!t.get(r->id, n)) continue;
            std::memcpy((std::byte*)&n + col_offset(r->col), &r->value, 8);   // SET k_col = value
            for (int i = 0; i < 10; ++i) triggers[i](n);                      // 10 BEFORE UPDATE triggers
            t.patch(r->id, col_offset(r->col), std::as_bytes(std::span{&r->value, 1}));
            t.patch(r->id, (uint32_t)offsetof(row, version), std::span<const std::byte>{(const std::byte*)&n.version, 16});
        }
        std::unique_lock s(db.sync_mu);
        const auto a1 = std::chrono::steady_clock::now();
        w.unlock();
        t.sync();                                                              // one msync(MS_SYNC) per group
        s.unlock();
        const uint64_t ans = (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(a1 - a0).count();
        db.apply_ns.fetch_add(ans, std::memory_order_relaxed);
        if (ans > db.apply_ns_max.load(std::memory_order_relaxed)) db.apply_ns_max.store(ans, std::memory_order_relaxed);
        db.sync_ns.fetch_add((uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - a1).count(), std::memory_order_relaxed);
        db.batches.fetch_add(1, std::memory_order_relaxed);
        db.batched.fetch_add(batch.size(), std::memory_order_relaxed);
        for (req* r : batch) { r->done.store(1, std::memory_order_release); r->done.notify_one(); }
        batch.clear();
    }
}

// Background compaction, the backend way, but streaming instead of copying the
// live set into the heap (which would not fit under the out-of-RAM cap).
static void compact_once(db_t& db) {
    const double t0 = now_s();
    std::size_t w0, before;
    storage::record_id next_id;
    {
        std::shared_lock r(db.rw);
        w0 = db.cur->current_offset();
        before = w0;
        next_id = db.cur->next_record_id();
    }
    db.files.remove_next();
    auto fresh = std::make_unique<table_t>(db.files.next());
    if (!fresh->ok()) { db.compact_error = "cannot open .next"; return; }
    fresh->restart(spec::capacity);

    alignas(8) std::byte buf[sizeof(row)];
    constexpr storage::record_id chunk = 2048;
    for (storage::record_id lo = 1; lo < next_id; lo += chunk) {
        std::shared_lock r(db.rw);
        const storage::record_id hi = std::min<storage::record_id>(lo + chunk, next_id);
        for (storage::record_id id = lo; id < hi; ++id) {
            if (!db.cur->alive(id)) continue;
            const storage::tag_t tag = db.cur->tag_of(id);
            if (!db.cur->read_raw(id, buf)) continue;
            if (!fresh->create_raw(id, tag, std::span<const std::byte>{buf, sizeof(row)})) { db.compact_error = "fresh full"; return; }
        }
    }
    fresh->raise_next_id(next_id);
    fresh->sync();

    // catch up outside the lock: everything below `upto` is immutable in the old log
    std::size_t done = w0;
    for (int round = 0; round < 3; ++round) {
        std::size_t upto;
        { std::shared_lock r(db.rw); upto = db.cur->current_offset(); }
        if (upto <= done) break;
        if (fresh->apply_wal_range(db.cur->raw_base() + done, upto - done) != upto - done) { db.compact_error = "tail"; db.files.remove_next(); return; }
        done = upto;
    }
    fresh->sync();

    std::unique_ptr<table_t> old;
    const double p0 = now_s();
    {
        std::unique_lock w(db.rw);
        std::unique_lock s(db.sync_mu);
        const std::size_t upto = db.cur->current_offset();
        if (upto > done && fresh->apply_wal_range(db.cur->raw_base() + done, upto - done) != upto - done) {
            db.compact_error = "final tail"; db.files.remove_next(); return;
        }
        fresh->sync();
        ::unlink(db.files.ckp().c_str());
        bool ok = ::rename((db.files.log() + ".next").c_str(), db.files.log().c_str()) == 0 &&
                  ::rename((db.files.idx() + ".next").c_str(), db.files.idx().c_str()) == 0;
        ::rename((db.files.idx() + ".next.sx").c_str(), (db.files.idx() + ".sx").c_str());
        sync_dir(db.files.dir);
        if (!ok) { db.compact_error = "rename"; return; }
        fresh->retarget(db.files.live());
        old = std::move(db.cur);
        db.cur = std::move(fresh);
        db.compact_bytes_after.store(db.cur->current_offset());
    }
    const double pause = now_s() - p0;
    old.reset();   // munmap of the old log, outside every lock
    db.compactions.fetch_add(1);
    db.compact_bytes_before.store(before);
    const uint64_t pus = (uint64_t)(pause * 1e6);
    db.compact_pause_us_sum.fetch_add(pus);
    uint64_t m = db.compact_pause_us_max.load();
    while (pus > m && !db.compact_pause_us_max.compare_exchange_weak(m, pus)) {}
    db.compact_ms_sum.fetch_add((uint64_t)((now_s() - t0) * 1e3));
}

static void compactor(db_t& db) {
    while (!db.stop.load()) {
        for (int i = 0; i < 5 && !db.stop.load(); ++i) std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (db.stop.load()) return;
        double fill, garbage;
        {
            std::shared_lock r(db.rw);
            fill = (double)db.cur->current_offset() / spec::capacity;
            garbage = db.cur->garbage_ratio();
        }
        if (fill >= 0.5 && garbage >= 0.3) compact_once(db);
        if (!db.compact_error.empty()) { std::fprintf(stderr, "compaction failed: %s\n", db.compact_error.c_str()); return; }
    }
}

static long rss_mb() {
    rusage u{}; getrusage(RUSAGE_SELF, &u); return u.ru_maxrss / 1024;
}

static int do_load(const files_t& f) {
    for (auto p : {f.log(), f.idx(), f.idx() + ".sx", f.ckp()}) ::unlink(p.c_str());
    f.remove_next();
    auto t = std::make_unique<table_t>(f.live());
    if (!t->ok()) { std::fprintf(stderr, "open failed\n"); return 1; }
    t->restart(spec::capacity);
    const double t0 = now_s();
    row r{};
    std::memset(r.payload, 'x', sizeof r.payload);
    for (std::size_t i = 1; i <= rows; ++i) {
        int64_t* k = &r.k0;
        for (int c = 0; c < 10; ++c) k[c] = key_of(i * 16 + c);
        if (t->create(r) == 0) { std::fprintf(stderr, "create failed at %zu\n", i); return 1; }
    }
    t->sync();
    const double t1 = now_s();
    t->checkpoint();
    std::printf("{\"engine\":\"field\",\"phase\":\"load\",\"rows\":%zu,\"load_s\":%.2f,\"log_mb\":%.1f,\"index_failures\":%zu}\n",
                rows, t1 - t0, t->current_offset() / 1048576.0, (std::size_t)t->observer_state().failed_inserts());
    return 0;
}

int main(int argc, char** argv) {
    if (argc < 3) { std::fprintf(stderr, "usage: %s dir load|run ...\n", argv[0]); return 2; }
    files_t f{argv[1]};
    if (std::strcmp(argv[2], "load") == 0) return do_load(f);
    if (argc < 8) { std::fprintf(stderr, "run <update_pct> <threads> <warmup> <seconds> <label>\n"); return 2; }
    const int upd = std::atoi(argv[3]), threads = std::atoi(argv[4]);
    const double warm = std::atof(argv[5]), secs = std::atof(argv[6]);
    const std::string label = argv[7];

    db_t db;
    db.files = f;
    f.remove_next();
    const double o0 = now_s();
    db.cur = std::make_unique<table_t>(f.live());
    if (!db.cur->ok()) { std::fprintf(stderr, "open failed\n"); return 1; }
    db.cur->restart(spec::capacity);
    const double open_s = now_s() - o0;
    const std::size_t log_before = db.cur->current_offset();

    std::thread comm(committer, std::ref(db));
    std::thread comp(compactor, std::ref(db));

    std::atomic<int> phase{0};   // 0 warmup, 1 measure, 2 stop
    std::vector<histo> hu(threads), hr(threads);
    std::vector<std::thread> ws;
    std::atomic<uint64_t> sink{0};
    for (int ti = 0; ti < threads; ++ti) {
        ws.emplace_back([&, ti] {
            std::mt19937_64 rng(1234567 + ti);
            uint64_t local = 0;
            req rq;
            while (true) {
                const int ph = phase.load(std::memory_order_relaxed);
                if (ph == 2) break;
                const uint64_t x = rng();
                const storage::record_id id = 1 + (x % rows);
                const bool is_update = (int)((x >> 40) % 100) < upd;
                const double a = now_s();
                if (is_update) {
                    rq.id = id; rq.col = (uint32_t)((x >> 20) % 10); rq.value = (int64_t)(rng() >> 1);
                    rq.done.store(0, std::memory_order_relaxed);
                    { std::lock_guard lk(db.qmu); db.queue.push_back(&rq); }
                    db.qcv.notify_one();
                    rq.done.wait(0, std::memory_order_acquire);
                } else {
                    row r;
                    std::shared_lock s(db.rw);
                    if (db.cur->get(id, r)) local += (uint64_t)r.version + (uint64_t)r.k3;
                }
                if (ph == 1) (is_update ? hu[ti] : hr[ti]).add((now_s() - a) * 1e6);
            }
            sink += local;
        });
    }
    std::this_thread::sleep_for(std::chrono::duration<double>(warm));
    phase = 1;
    const uint64_t c0 = db.compactions.load(), b0 = db.batches.load(), bb0 = db.batched.load();
    const double m0 = now_s();
    std::this_thread::sleep_for(std::chrono::duration<double>(secs));
    phase = 2;
    const double elapsed = now_s() - m0;
    for (auto& w : ws) w.join();
    db.stop = true;
    db.qcv.notify_all();
    comm.join();
    comp.join();

    histo U, R;
    for (int i = 0; i < threads; ++i) { U.merge(hu[i]); R.merge(hr[i]); }

    // verify the 10 indexes agree with the data on a sample of rows
    std::size_t checked = 0, bad = 0;
    {
        std::mt19937_64 rng(42);
        for (int s = 0; s < 2000; ++s) {
            const storage::record_id id = 1 + rng() % rows;
            row r; if (!db.cur->get(id, r)) { ++bad; continue; }
            auto& ix = db.cur->observer_state();
            auto has = [&](auto& index, int64_t key) { bool hit = false; index.each(key, [&](storage::record_id x) { hit |= x == id; }); return hit; };
            bad += !has(ix.of<&row::k0>(), r.k0) + !has(ix.of<&row::k1>(), r.k1) + !has(ix.of<&row::k2>(), r.k2) +
                   !has(ix.of<&row::k3>(), r.k3) + !has(ix.of<&row::k4>(), r.k4) + !has(ix.of<&row::k5>(), r.k5) +
                   !has(ix.of<&row::k6>(), r.k6) + !has(ix.of<&row::k7>(), r.k7) + !has(ix.of<&row::k8>(), r.k8) +
                   !has(ix.of<&row::k9>(), r.k9);
            checked += 10;
        }
    }
    const std::size_t log_after = db.cur->current_offset();
    const double garbage = db.cur->garbage_ratio();
    db.cur->checkpoint();

    const uint64_t comps = db.compactions.load() - c0;
    const double nb = (double)(db.batches.load() - b0), nbb = (double)(db.batched.load() - bb0);
    std::printf("{\"engine\":\"field\",\"label\":\"%s\",\"update_pct\":%d,\"threads\":%d,\"seconds\":%.1f,"
                "\"tps\":%.0f,\"update_tps\":%.0f,\"read_tps\":%.0f,"
                "\"upd_avg_us\":%.1f,\"upd_p50_us\":%.1f,\"upd_p99_us\":%.1f,\"upd_max_us\":%.0f,"
                "\"read_avg_us\":%.2f,\"read_p50_us\":%.1f,\"read_p99_us\":%.1f,\"read_max_us\":%.0f,"
                "\"commit_groups\":%.0f,\"avg_group\":%.2f,\"apply_avg_us\":%.1f,\"apply_max_us\":%.0f,\"msync_avg_us\":%.1f,"
                "\"compactions\":%llu,\"compact_pause_max_ms\":%.1f,\"compact_total_ms\":%llu,"
                "\"log_mb_before\":%.1f,\"log_mb_after\":%.1f,\"garbage_after\":%.3f,"
                "\"open_s\":%.2f,\"index_failures\":%zu,\"index_check\":\"%zu/%zu ok\",\"maxrss_mb\":%ld,\"err\":\"%s\"}\n",
                label.c_str(), upd, threads, elapsed,
                (U.count + R.count) / elapsed, U.count / elapsed, R.count / elapsed,
                U.count ? U.sum_us / U.count : 0, U.pct(0.5), U.pct(0.99), U.max_us,
                R.count ? R.sum_us / R.count : 0, R.pct(0.5), R.pct(0.99), R.max_us,
                nb, nb ? nbb / nb : 0, db.batches ? db.apply_ns / 1e3 / db.batches : 0, db.apply_ns_max / 1e3, db.batches ? db.sync_ns / 1e3 / db.batches : 0,
                (unsigned long long)comps, db.compact_pause_us_max.load() / 1e3, (unsigned long long)db.compact_ms_sum.load(),
                log_before / 1048576.0, log_after / 1048576.0, garbage,
                open_s, (std::size_t)db.cur->observer_state().failed_inserts(), checked - bad, checked, rss_mb(), db.compact_error.c_str());
    return (int)(sink.load() == 42);
}
