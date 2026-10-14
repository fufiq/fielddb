// regress.cpp — one test per engine bug found while benchmarking against Postgres.
// Every test FAILS on the original (pre-fix) field_storage.hpp
// (2026-10-14) and passes on the fixed copy.
//
//   g++ -std=c++23 -O1 -g -fsanitize=address,undefined -I <dir with field_storage.hpp> regress.cpp -o regress && ./regress
#include "field_storage.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>
#include <sys/resource.h>
#include <unistd.h>

static int failures = 0;
#define CHECK(cond, ...) do { if (!(cond)) { std::printf("  FAIL: " __VA_ARGS__); std::printf("\n"); ++failures; return; } } while (0)

static std::string tmp(const char* name) {
    static std::string dir = [] { char t[] = "/tmp/fieldregress.XXXXXX"; return std::string(mkdtemp(t)); }();
    return dir + "/" + name;
}

struct row { static constexpr storage::tag_t tag = 1; int64_t k0, k1; int64_t f[10]; };
struct wide { static constexpr storage::tag_t tag = 2; int64_t f[64]; };   // 512 B: a collapse writes far more than an update

// 1. flat_hash_map::erase backward shift: branches were swapped, so entries past
//    the hole became unreachable and secondary indexes silently lost rows.
static void test_hash_erase() {
    std::printf("1. flat_hash_map::erase (index loses rows)\n");
    for (int seed = 0; seed < 100; ++seed) {
        std::mt19937_64 r(seed);
        const std::size_t N = 64 + r() % 2000;
        std::vector<std::byte> mem(1 << 24);
        storage::index_arena a(mem.data(), mem.size());
        storage::flat_multi_map m; m.place(a, N, N); m.clear();
        std::map<uint64_t, std::set<uint64_t>> ref;
        std::vector<std::pair<uint64_t, uint64_t>> live;
        for (int op = 0; op < 20000; ++op) {
            if (live.size() < N / 2 && (r() % 2 || live.empty())) {
                const uint64_t k = r() % (N * 4) + 1, v = r() % 1000000 + 1;
                if (ref[k].count(v) || !m.insert(k, v)) continue;
                ref[k].insert(v); live.push_back({k, v});
            } else {
                const std::size_t i = r() % live.size(); auto [k, v] = live[i];
                live[i] = live.back(); live.pop_back();
                CHECK(m.erase(k, v), "seed %d: erase of a present key missed", seed);
                ref[k].erase(v);
            }
        }
        for (auto& [k, s] : ref) {
            std::set<uint64_t> got; m.each(k, [&](uint64_t v) { got.insert(v); });
            CHECK(got == s, "seed %d: key %llu has %zu values, expected %zu", seed, (unsigned long long)k, got.size(), s.size());
        }
    }
    std::printf("  ok\n");
}

// 2. set_override out of override nodes: the update was dropped ("keeps its
//    previous value") although it was already logged and indexed.
static void test_out_of_nodes() {
    std::printf("2. set_override out of nodes (update silently dropped)\n");
    using ix = storage::index_set<1024, storage::multi_index<&row::k0>>;
    using spec = storage::field_spec<"oon.log", 1u << 24, "oon.idx", 1024, 64 /* tiny node arena */, 8, sizeof(row)>;
    storage::field_storage<spec, ix> t(storage::storage_paths{tmp("oon.log"), tmp("oon.idx"), ""});
    t.recover(0);
    std::mt19937_64 r(7);
    std::vector<row> truth(501);
    for (int i = 1; i <= 500; ++i) { row x{}; x.k0 = (int64_t)(r() >> 1); t.create(x); truth[i] = x; }
    for (int s = 0; s < 20000; ++s) {
        const int id = 1 + r() % 500; const int c = r() % 12; const int64_t v = (int64_t)(r() >> 1);
        t.patch(id, 8 * c, std::as_bytes(std::span{&v, 1}));
        std::memcpy((std::byte*)&truth[id] + 8 * c, &v, 8);
    }
    for (int i = 1; i <= 500; ++i) {
        row x; CHECK(t.get(i, x), "row %d missing", i);
        CHECK(std::memcmp(&x, &truth[i], sizeof x) == 0, "row %d lost an update (overrides used %zu of 64)", i, t.overrides_used());
        bool hit = false; t.observer_state().each<&row::k0>(x.k0, [&](storage::record_id y) { hit |= y == (storage::record_id)i; });
        CHECK(hit, "row %d: index does not hold its current k0", i);
    }
    std::printf("  ok\n");
}

// 3. apply_wal_range: write_offset advanced only after the whole run, so a
//    collapse() inside the run appended over records not yet scanned.
static void test_apply_wal_range() {
    std::printf("3. apply_wal_range (collapse tears the tail)\n");
    // Wide records and max_depth 1: every second update collapses, and each
    // collapse appends 512 B while the scan advances ~40 B per update record.
    using spec = storage::field_spec<"awr.log", 1u << 26, "awr.idx", 2048, 20000, 1, sizeof(wide)>;
    storage::field_storage<spec> src(storage::storage_paths{tmp("awr_src.log"), tmp("awr_src.idx"), ""});
    storage::field_storage<spec> dst(storage::storage_paths{tmp("awr_dst.log"), tmp("awr_dst.idx"), ""});
    src.recover(0); dst.recover(0);
    std::mt19937_64 r(11);
    std::vector<wide> truth(201);
    for (int i = 1; i <= 200; ++i) { wide x{}; for (auto& v : x.f) v = (int64_t)r(); src.create(x); truth[i] = x; }
    const std::size_t w0 = src.current_offset();
    for (int s = 0; s < 20000; ++s) {
        const int id = 1 + r() % 200, c = r() % 64; const int64_t v = (int64_t)r();
        src.patch(id, 8 * c, std::as_bytes(std::span{&v, 1}));
        std::memcpy((std::byte*)&truth[id] + 8 * c, &v, 8);
    }
    // the receiver already holds the creates (as a compaction's fresh engine does), then takes the tail
    for (int i = 1; i <= 200; ++i) { wide x; (void)src.get(i, x); (void)x; }
    {
        const std::size_t head = dst.apply_wal_range(src.raw_base(), w0);
        CHECK(head == w0, "head: applied %zu of %zu bytes", head, w0);
    }
    const std::size_t len = src.current_offset() - w0;
    const std::size_t applied = dst.apply_wal_range(src.raw_base() + w0, len);
    CHECK(applied == len, "tail: applied %zu of %zu bytes (receiver collapses %zu)", applied, len, dst.collapse_count());
    for (int i = 1; i <= 200; ++i) {
        wide x; CHECK(dst.get(i, x), "row %d missing on the receiver", i);
        CHECK(std::memcmp(&x, &truth[i], sizeof x) == 0, "row %d differs on the receiver", i);
    }
    // and the receiver's LOG must say the same thing: replay it from disk
    dst.sync();
    const std::size_t dst_end = dst.current_offset();
    storage::field_storage<spec> again(storage::storage_paths{tmp("awr_dst.log"), tmp("awr_dst.idx2"), ""});
    const std::size_t replayed = again.recover(dst_end);
    CHECK(replayed == dst_end, "receiver log replays to %zu of %zu bytes — torn", replayed, dst_end);
    for (int i = 1; i <= 200; ++i) {
        wide x; CHECK(again.get(i, x), "row %d missing after replaying the receiver log", i);
        CHECK(std::memcmp(&x, &truth[i], sizeof x) == 0, "row %d differs after replaying the receiver log", i);
    }
    std::printf("  ok (receiver collapses: %zu)\n", dst.collapse_count());
}

// 4. index_set() allocated a zero-filled heap copy of the whole arena before the
//    engine mapped the arena file: the full index in anonymous memory.
static void test_index_heap() {
    std::printf("4. index_set constructor (whole index allocated on the heap)\n");
    constexpr std::size_t n = 2'000'000;
    using ix = storage::index_set<n, storage::multi_index<&row::k0>, storage::multi_index<&row::k1>>;
    using spec = storage::field_spec<"ih.log", 1u << 22, "ih.idx", 1024, 1024, 8, sizeof(row), "ih.ckp">;
    // Anonymous (heap) memory while the table is open: the arena is a file
    // mapping, so a correct engine holds next to nothing of it on the heap.
    auto rss_anon_mb = [] {
        FILE* f = std::fopen("/proc/self/status", "r"); char line[256]; long kb = 0;
        while (f && std::fgets(line, sizeof line, f)) if (std::sscanf(line, "RssAnon: %ld kB", &kb) == 1) break;
        if (f) std::fclose(f);
        return kb / 1024;
    };
    const long anon0 = rss_anon_mb();
    long grew_mb = 0;
    {
        storage::field_storage<spec, ix> t(storage::storage_paths{tmp("ih.log"), tmp("ih.idx"), tmp("ih.ckp")});
        t.restart(spec::capacity);
        CHECK(t.index_arena_mapped(), "arena not mapped");
        row x{}; x.k0 = 42; t.create(x);
        CHECK(t.observer_state().count<&row::k0>(42) == 1, "index does not work");
        grew_mb = rss_anon_mb() - anon0;
    }
    const long arena_mb = (long)(ix::arena_bytes() >> 20);
    CHECK(grew_mb < arena_mb / 4, "heap holds %ld MB for a %ld MB file-backed arena", grew_mb, arena_mb);
    std::printf("  ok (arena %ld MB, heap grew %ld MB)\n", arena_mb, grew_mb);
}

// 5. Not a bug — checked because 3 suggested it: recover() with collapses in the log.
static void test_recover() {
    std::printf("5. recover() after a crash with collapses in the log\n");
    using spec = storage::field_spec<"rc.log", 1u << 24, "rc.idx", 2048, 20000, 4, sizeof(row)>;
    std::vector<row> truth(1001);
    std::mt19937_64 r(5);
    {
        storage::field_storage<spec> t(storage::storage_paths{tmp("rc.log"), tmp("rc.idx"), ""});
        t.recover(0);
        for (int i = 1; i <= 1000; ++i) { row x{}; for (auto& v : x.f) v = (int64_t)r(); t.create(x); truth[i] = x; }
        for (int s = 0; s < 20000; ++s) {
            const int id = 1 + r() % 1000, c = r() % 12; const int64_t v = (int64_t)r();
            t.patch(id, 8 * c, std::as_bytes(std::span{&v, 1}));
            std::memcpy((std::byte*)&truth[id] + 8 * c, &v, 8);
        }
        t.sync();
    }
    for (int pass = 0; pass < 2; ++pass) {   // twice: the first replay must not damage the log
        storage::field_storage<spec> t(storage::storage_paths{tmp("rc.log"), tmp("rc.idx"), ""});
        t.recover(spec::capacity);
        for (int i = 1; i <= 1000; ++i) {
            row x; CHECK(t.get(i, x), "pass %d: row %d missing", pass, i);
            CHECK(std::memcmp(&x, &truth[i], sizeof x) == 0, "pass %d: row %d differs", pass, i);
        }
    }
    std::printf("  ok\n");
}

int main() {
    test_hash_erase();
    test_out_of_nodes();
    test_apply_wal_range();
    test_index_heap();
    test_recover();
    std::printf(failures ? "\n%d FAILED\n" : "\nall passed\n", failures);
    return failures ? 1 : 0;
}
