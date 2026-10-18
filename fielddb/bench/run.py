#!/usr/bin/env python3
"""Field vs Postgres: 10 indexes, 10 triggers, N concurrent clients,
4 profiles (100/80/20/0 % UPDATE), in RAM and out of RAM (cgroup MemoryMax).

    ./run.py [ram|disk|all] [--seconds 60] [--warmup 10] [--clients 8] [--engines field,pg]

Results: results/results.jsonl (one line per run) and console output.
"""
import argparse, glob, itertools, json, os, resource, shutil, statistics, subprocess, sys, threading, time

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")
RES = os.path.join(HERE, "results")
PGDIR = os.path.join(DATA, "pg")
SOCK = "/tmp/pgsock-fb"   # unix socket path limit is 107 bytes; data stays on NVMe
PORT = "5499"

MODES = {
    # rows, field log capacity (MB), memory cap for the engine's whole cgroup.
    # VPS: 2 vCPU, ~1.9 GB RAM shared with live services, so both modes run capped.
    "ram":  dict(rows=200_000, cap_mb=1024, mem="600M", load_mem="900M", evict=False),
    "disk": dict(rows=1_500_000, cap_mb=2048, mem="600M", load_mem="900M", evict=True),
    # same data, a cap neither engine's read working set fits in
    "disk256": dict(rows=1_500_000, cap_mb=2048, mem="256M", load_mem="900M", evict=True),
}
os.environ["PATH"] = "/usr/lib/postgresql/17/bin:" + os.environ["PATH"]
PROFILES = [0, 20, 80, 100]   # % UPDATE; order is the same for both engines (bloat accumulates the same way)


def sh(cmd, check=True, **kw):
    print("  $", cmd if isinstance(cmd, str) else " ".join(cmd), flush=True)
    return subprocess.run(cmd, shell=isinstance(cmd, str), check=check, text=True, **kw)


_units = itertools.count()


def capped(cmd, mem, unit=None):
    if not mem and not unit:
        return cmd
    u = ["--unit", unit] if unit else []
    m = ["-p", f"MemoryMax={mem}", "-p", "MemorySwapMax=0"] if mem else []
    return ["systemd-run", "--user", "--scope", "--quiet"] + u + m + ["--"] + cmd


def new_unit(prefix):
    return f"{prefix}-{os.getpid()}-{next(_units)}"


# ---------------------------------------------------------------- metrics -----
# Per engine: its own cgroup (cpu.stat, memory.stat, memory.peak, PSI). System:
# /proc/stat (iowait, steal) and /proc/diskstats for the data disk. The io
# controller is not delegated to user units on this host, so disk bytes are
# system-wide (the VPS runs a mail server alongside; its share is small but not 0).
DISK = os.environ.get("BENCH_DISK", "sda")
HZ = os.sysconf("SC_CLK_TCK")


def cg_path(unit):
    p = subprocess.run(["systemctl", "--user", "show", "-p", "ControlGroup", "--value", unit + ".scope"],
                       capture_output=True, text=True).stdout.strip()
    return "/sys/fs/cgroup" + p if p else None


def _kv(path):
    out = {}
    try:
        with open(path) as f:
            for line in f:
                k, _, v = line.partition(" ")
                if v.strip().lstrip("-").isdigit():
                    out[k] = int(v)
    except OSError:
        pass
    return out


def _psi(path):
    out = {}
    try:
        with open(path) as f:
            for line in f:
                kind = line.split()[0]
                out[kind] = int(line.rsplit("total=", 1)[1])
    except OSError:
        pass
    return out


def cg_snap(path):
    if not path:
        return {}
    cpu, mem = _kv(path + "/cpu.stat"), _kv(path + "/memory.stat")
    io_p, mem_p, cpu_p = _psi(path + "/io.pressure"), _psi(path + "/memory.pressure"), _psi(path + "/cpu.pressure")
    try:
        peak = int(open(path + "/memory.peak").read())
    except (OSError, ValueError):
        peak = 0
    return dict(cpu_user=cpu.get("user_usec", 0), cpu_sys=cpu.get("system_usec", 0),
                pgfault=mem.get("pgfault", 0), pgmajfault=mem.get("pgmajfault", 0),
                refault=mem.get("workingset_refault_file", 0),
                io_some=io_p.get("some", 0), io_full=io_p.get("full", 0),
                mem_some=mem_p.get("some", 0), mem_full=mem_p.get("full", 0), cpu_some=cpu_p.get("some", 0),
                peak=peak)


def sys_snap():
    with open("/proc/stat") as f:
        c = [int(x) for x in f.readline().split()[1:]]
    d = [0] * 11
    with open("/proc/diskstats") as f:
        for line in f:
            p = line.split()
            if p[2] == DISK:
                d = [int(x) for x in p[3:14]]
    return dict(user=c[0] + c[1], system=c[2], idle=c[3], iowait=c[4], steal=c[7] if len(c) > 7 else 0,
                rios=d[0], rsect=d[2], wios=d[4], wsect=d[6], busy_ms=d[9])


def deltas(cg0, cg1, s0, s1, wall):
    out = {}
    if cg0 and cg1:
        out.update(cg_cpu_user_s=round((cg1["cpu_user"] - cg0["cpu_user"]) / 1e6, 2),
                   cg_cpu_sys_s=round((cg1["cpu_sys"] - cg0["cpu_sys"]) / 1e6, 2),
                   cg_minflt=(cg1["pgfault"] - cg0["pgfault"]) - (cg1["pgmajfault"] - cg0["pgmajfault"]),
                   cg_majflt=cg1["pgmajfault"] - cg0["pgmajfault"],
                   cg_refault_file=cg1["refault"] - cg0["refault"],
                   cg_psi_io_some_s=round((cg1["io_some"] - cg0["io_some"]) / 1e6, 2),
                   cg_psi_io_full_s=round((cg1["io_full"] - cg0["io_full"]) / 1e6, 2),
                   cg_psi_mem_some_s=round((cg1["mem_some"] - cg0["mem_some"]) / 1e6, 2),
                   cg_psi_mem_full_s=round((cg1["mem_full"] - cg0["mem_full"]) / 1e6, 2),
                   cg_psi_cpu_some_s=round((cg1["cpu_some"] - cg0["cpu_some"]) / 1e6, 2),
                   cg_mem_peak_mb=round(cg1["peak"] / 1048576, 1))
    out.update(sys_user_s=round((s1["user"] - s0["user"]) / HZ, 2), sys_system_s=round((s1["system"] - s0["system"]) / HZ, 2),
               sys_iowait_s=round((s1["iowait"] - s0["iowait"]) / HZ, 2), sys_steal_s=round((s1["steal"] - s0["steal"]) / HZ, 2),
               sys_idle_s=round((s1["idle"] - s0["idle"]) / HZ, 2),
               disk_read_mb=round((s1["rsect"] - s0["rsect"]) * 512 / 1048576, 1),
               disk_write_mb=round((s1["wsect"] - s0["wsect"]) * 512 / 1048576, 1),
               disk_read_iops=round((s1["rios"] - s0["rios"]) / wall), disk_write_iops=round((s1["wios"] - s0["wios"]) / wall),
               disk_busy_pct=round(100 * (s1["busy_ms"] - s0["busy_ms"]) / 1000 / wall, 1))
    return out


class Sampler:
    """Samples resource use every `interval` s while a measurement runs.

    Per cgroup (the engine; for PG also the pgbench client): CPU % (100 = one
    vCPU), memory.current split into anon (heap) and file (page cache), dirty
    and writeback. System-wide: CPU busy / iowait / steal %, MemAvailable,
    Dirty. `units` maps a name to a systemd unit, resolved lazily so a scope
    that starts after the sampler is still picked up.
    """

    def __init__(self, units, interval=0.5):
        self.units, self.interval = units, interval
        self.paths = {}
        self.series = []
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._run, daemon=True)

    @staticmethod
    def _cg(path):
        cpu = _kv(path + "/cpu.stat").get("usage_usec")
        if cpu is None:
            return None
        mem = _kv(path + "/memory.stat")
        try:
            cur = int(open(path + "/memory.current").read())
        except (OSError, ValueError):
            cur = 0
        return dict(cpu_us=cpu, cur=cur, anon=mem.get("anon", 0), file=mem.get("file", 0),
                    dirty=mem.get("file_dirty", 0), wb=mem.get("file_writeback", 0))

    @staticmethod
    def _sys():
        with open("/proc/stat") as f:
            c = [int(x) for x in f.readline().split()[1:]]
        mi = {}
        with open("/proc/meminfo") as f:
            for line in f:
                k, v = line.split(":", 1)
                mi[k] = int(v.split()[0])
        return dict(total=sum(c[:8]), idle=c[3], iowait=c[4], steal=c[7] if len(c) > 7 else 0,
                    avail=mi.get("MemAvailable", 0), dirty=mi.get("Dirty", 0))

    def _run(self):
        prev, prev_sys, prev_t = {}, self._sys(), time.time()
        while not self._stop.wait(self.interval):
            now, sy = time.time(), self._sys()
            dt = now - prev_t
            point = {"t": round(now, 2)}
            dtot = max(1, sy["total"] - prev_sys["total"])
            point["sys_busy_pct"] = round(100 * (1 - (sy["idle"] - prev_sys["idle"] + sy["iowait"] - prev_sys["iowait"]) / dtot), 1)
            point["sys_iowait_pct"] = round(100 * (sy["iowait"] - prev_sys["iowait"]) / dtot, 1)
            point["sys_steal_pct"] = round(100 * (sy["steal"] - prev_sys["steal"]) / dtot, 1)
            point["sys_avail_mb"] = round(sy["avail"] / 1024)
            point["sys_dirty_mb"] = round(sy["dirty"] / 1024)
            for name, unit in self.units.items():
                if name not in self.paths:
                    p = cg_path(unit)
                    if p:
                        self.paths[name] = p
                    continue
                g = self._cg(self.paths[name])
                if g is None:
                    continue
                if name in prev:
                    point[f"{name}_cpu_pct"] = round(100 * (g["cpu_us"] - prev[name]["cpu_us"]) / 1e6 / dt, 1)
                point[f"{name}_mem_mb"] = round(g["cur"] / 1048576, 1)
                point[f"{name}_anon_mb"] = round(g["anon"] / 1048576, 1)
                point[f"{name}_file_mb"] = round(g["file"] / 1048576, 1)
                point[f"{name}_dirty_mb"] = round((g["dirty"] + g["wb"]) / 1048576, 1)
                prev[name] = g
            prev_sys, prev_t = sy, now
            self.series.append(point)

    def start(self):
        self._t.start()
        return self

    def stop(self, label):
        self._stop.set()
        self._t.join()
        with open(os.path.join(RES, f"ts_{label}.json"), "w") as f:
            json.dump(self.series, f)
        out = {}
        keys = sorted({k for p in self.series for k in p if k != "t"})
        for k in keys:
            xs = [p[k] for p in self.series if k in p]
            if not xs:
                continue
            out[f"{k}_avg"] = round(sum(xs) / len(xs), 1)
            out[f"{k}_max" if not k.endswith("avail_mb") else f"{k}_min"] = max(xs) if not k.endswith("avail_mb") else min(xs)
        return out


def evict(paths):
    """Drop clean page-cache pages of every file (fsync first so they are clean)."""
    n = 0
    for p in paths:
        for f in (glob.glob(os.path.join(p, "**"), recursive=True) if os.path.isdir(p) else [p]):
            if not os.path.isfile(f):
                continue
            try:
                fd = os.open(f, os.O_RDONLY)
                try:
                    os.fsync(fd)
                    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
                    n += 1
                finally:
                    os.close(fd)
            except OSError:
                pass
    return n


def prewarm_files(path):
    for f in glob.glob(os.path.join(path, "**"), recursive=True):
        if os.path.isfile(f):
            with open(f, "rb") as fh:
                while fh.read(1 << 24):
                    pass


def du_mb(path):
    out = subprocess.run(["du", "-sm", path], capture_output=True, text=True).stdout.split()
    return int(out[0]) if out else 0


TAG = [""]


def emit(rec):
    if TAG[0]:
        rec["tag"] = TAG[0]
    print("  =>", json.dumps(rec, ensure_ascii=False), flush=True)
    with open(os.path.join(RES, "results.jsonl"), "a") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------- Field -----
def build_field(mode):
    m = MODES[mode]
    exe = os.path.join(HERE, f"field_mvcc_{m['rows']}_{m['cap_mb']}")
    sh(["g++", "-std=c++23", "-O3", "-march=native", "-DNDEBUG", f"-DROWS={m['rows']}", f"-DCAP_MB={m['cap_mb']}",
        "-I", os.path.join(HERE, "..", "include"), os.path.join(HERE, "..", "db", "field_mvcc.cpp"), "-o", exe, "-pthread"])
    return exe


def run_field(mode, a):
    m = MODES[mode]
    exe = build_field(mode)
    d = os.path.join(DATA, "field")
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    r = sh(capped([exe, d, "load"], m["load_mem"]), capture_output=True)   # load is not measured
    load = json.loads(r.stdout.strip().splitlines()[-1])
    load.update(mode=mode, disk_mb=du_mb(d))
    emit(load)
    for upd in a.profiles:
        if m["evict"]:
            evict([d])
        # RAM mode: no external prewarm (it would charge the page cache to another
        # cgroup); the in-process warmup touches the whole 200k-row table.
        unit = new_unit("fb-field")
        cmd = capped([exe, d, "run", str(upd), str(a.clients), str(a.warmup), str(a.seconds), f"{mode}-{upd}"], m["mem"], unit)
        print("  $", " ".join(cmd), flush=True)
        t_start = time.time()
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        path = None
        while path is None and time.time() - t_start < 5:
            time.sleep(0.2)
            path = cg_path(unit)
        time.sleep(max(0.0, t_start + a.warmup - time.time()))
        cg0, s0, w0 = cg_snap(path), sys_snap(), time.time()
        smp = Sampler({"engine": unit}).start()
        time.sleep(max(0.0, w0 + a.seconds - time.time()))
        cg1, s1, w1 = cg_snap(path), sys_snap(), time.time()
        res_use = smp.stop(f"field_{mode}_{upd}")
        out, err = proc.communicate()
        if proc.returncode not in (0, 1) or not out.strip():
            print(out, err)
            emit(dict(engine="field", mode=mode, update_pct=upd, error=f"exit {proc.returncode}: {err[-400:]}"))
            continue
        rec = json.loads(out.strip().splitlines()[-1])
        rec.update(mode=mode, disk_mb=du_mb(d), **deltas(cg0, cg1, s0, s1, w1 - w0), **res_use)
        emit(rec)
    shutil.rmtree(d, ignore_errors=True)


# ------------------------------------------------------------- Postgres -----
def pg_conf(mode):
    with open(os.path.join(HERE, "pg/postgresql.common.conf")) as f:
        common = f.read()
    with open(os.path.join(HERE, f"pg/postgresql.{mode}.conf")) as f:
        spec = f.read()
    with open(os.path.join(PGDIR, "postgresql.auto.conf"), "w") as f:
        f.write(common + "\n" + spec + f"\nunix_socket_directories = '{SOCK}'\n")


PG_UNIT = [None]


def pg_start(mem):
    PG_UNIT[0] = new_unit("fb-pg")
    sh(capped(["pg_ctl", "-D", PGDIR, "-l", os.path.join(DATA, "pg.log"), "-w", "-t", "600", "start"], mem, PG_UNIT[0]))


def pg_stop():
    sh(["pg_ctl", "-D", PGDIR, "-w", "-t", "600", "-m", "fast", "stop"], check=False)


def psql(sql, **kw):
    return sh(["psql", "-h", SOCK, "-p", PORT, "-d", "postgres", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-At", "-c", sql],
              capture_output=True, **kw).stdout.strip()


def pct(xs, p):
    if not xs:
        return 0
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))]


def run_pg(mode, a):
    m = MODES[mode]
    pg_stop()
    shutil.rmtree(PGDIR, ignore_errors=True)
    shutil.rmtree(SOCK, ignore_errors=True)
    os.makedirs(SOCK)
    sh(["initdb", "-D", PGDIR, "-A", "trust", "--no-sync", "-E", "UTF8", "--locale=C"], capture_output=True)
    pg_conf(mode)
    pg_start(m["load_mem"])   # load is not measured
    t0 = time.time()
    sh(["psql", "-h", SOCK, "-p", PORT, "-d", "postgres", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-v", f"rows={m['rows']}",
        "-f", os.path.join(HERE, "pg/schema.sql")], capture_output=True)
    load_s = time.time() - t0
    emit(dict(engine="pg", phase="load", mode=mode, rows=m["rows"], load_s=round(load_s, 2), disk_mb=du_mb(PGDIR),
              version=psql("show server_version")))
    pg_stop()
    pg_start(m["mem"])
    if not m["evict"]:
        psql("select pg_prewarm(c.oid) from pg_class c where c.relname like 't%' and c.relkind in ('r','i')")

    for upd in a.profiles:
        if m["evict"]:
            pg_stop()
            evict([PGDIR])
            pg_start(m["mem"])
        stats0 = psql("select n_tup_upd, n_dead_tup, autovacuum_count from pg_stat_user_tables where relname='t'")
        scripts = []
        if upd < 100:
            scripts += ["-f", os.path.join(HERE, "pg/read.sql") + f"@{10 * (100 - upd)}"]   # 10 update scripts below share `upd`
        if upd > 0:
            for c in range(10):
                scripts += ["-f", os.path.join(HERE, f"pg/upd_k{c}.sql") + f"@{upd}"]
        base = ["pgbench", "-h", SOCK, "-p", PORT, "-n", "-M", "prepared", "-c", str(a.clients), "-j", str(a.clients),
                "-D", f"rows={m['rows']}", "postgres"] + scripts
        sh(base + ["-T", str(int(a.warmup))], capture_output=True)
        prefix = os.path.join(RES, f"pglog_{mode}_{upd}")
        for f in glob.glob(prefix + "*"):
            os.unlink(f)
        path = cg_path(PG_UNIT[0])
        cg0, s0, w0 = cg_snap(path), sys_snap(), time.time()
        cli_unit = new_unit("fb-cli")
        smp = Sampler({"engine": PG_UNIT[0], "client": cli_unit}).start()
        ru0 = resource.getrusage(resource.RUSAGE_CHILDREN)
        r = sh(capped(base + ["-T", str(int(a.seconds)), "-l", "--sampling-rate=0.02", f"--log-prefix={prefix}"], None, cli_unit),
               capture_output=True)
        ru1 = resource.getrusage(resource.RUSAGE_CHILDREN)
        cg1, s1, w1 = cg_snap(path), sys_snap(), time.time()
        res_use = smp.stop(f"pg_{mode}_{upd}")
        out = r.stdout
        tps = float([l for l in out.splitlines() if l.startswith("tps =")][0].split()[2])
        # per-transaction log: client tx_no latency_us script_no epoch us — script 0 is read.sql when present
        lat = {"read": [], "upd": []}
        for f in glob.glob(prefix + "*"):
            with open(f) as fh:
                for line in fh:
                    p = line.split()
                    us, script = int(p[2]), int(p[3])
                    is_read = upd < 100 and script == 0
                    lat["read" if is_read else "upd"].append(us)
            os.unlink(f)
        n = len(lat["read"]) + len(lat["upd"])
        share_u = len(lat["upd"]) / n if n else 0
        stats1 = psql("select n_tup_upd, n_dead_tup, autovacuum_count from pg_stat_user_tables where relname='t'")
        u0, _, av0 = map(int, stats0.split("|"))
        u1, dead, av1 = map(int, stats1.split("|"))
        psql("checkpoint")
        rec = dict(engine="pg", mode=mode, label=f"{mode}-{upd}", update_pct=upd, threads=a.clients, seconds=a.seconds,
                   tps=round(tps), update_tps=round(tps * share_u), read_tps=round(tps * (1 - share_u)),
                   upd_avg_us=round(statistics.mean(lat["upd"]), 1) if lat["upd"] else 0,
                   upd_p50_us=pct(lat["upd"], .5), upd_p99_us=pct(lat["upd"], .99), upd_max_us=max(lat["upd"], default=0),
                   read_avg_us=round(statistics.mean(lat["read"]), 1) if lat["read"] else 0,
                   read_p50_us=pct(lat["read"], .5), read_p99_us=pct(lat["read"], .99), read_max_us=max(lat["read"], default=0),
                   rows_updated=u1 - u0, dead_tuples_after=dead, autovacuum_runs=av1 - av0, disk_mb=du_mb(PGDIR),
                   client_user_s=round(ru1.ru_utime - ru0.ru_utime, 2), client_sys_s=round(ru1.ru_stime - ru0.ru_stime, 2),
                   **deltas(cg0, cg1, s0, s1, w1 - w0), **res_use)
        emit(rec)
    pg_stop()
    shutil.rmtree(PGDIR, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("modes", nargs="?", default="all")
    ap.add_argument("--seconds", type=float, default=60)
    ap.add_argument("--warmup", type=float, default=10)
    ap.add_argument("--clients", type=int, default=8)
    ap.add_argument("--engines", default="field,pg")
    ap.add_argument("--profiles", default=",".join(map(str, PROFILES)))
    ap.add_argument("--tag", default="")
    a = ap.parse_args()
    a.profiles = [int(x) for x in a.profiles.split(",")]
    TAG[0] = a.tag
    os.makedirs(RES, exist_ok=True)
    modes = ["ram", "disk"] if a.modes == "all" else a.modes.split(",")
    for mode in modes:
        for eng in a.engines.split(","):
            print(f"\n=== {eng} / {mode} ===", flush=True)
            (run_field if eng == "field" else run_pg)(mode, a)


if __name__ == "__main__":
    main()
