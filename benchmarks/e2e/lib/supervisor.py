"""Arc process supervisor, resource sampler and scenario timeline.

Each Arc process runs natively with cwd=<run>/procs/<name> (Arc reads
./arc.toml). Restart semantics:

  restart(wipe=False)  container restart: WAL, stager and data dirs survive
                       (k8s emptyDir outlives a container restart)
  restart(wipe=True)   pod deletion / eviction: every local dir is wiped
                       (emptyDir is lost with the pod)
"""
from __future__ import annotations

import csv
import json
import os
import shutil
import signal
import subprocess
import threading
import time
from pathlib import Path

import psutil
import requests

from . import guard
from .config import ROLE_OF, build_arc_config, write_arc_toml

WIPE_DIRS = ["wal", "fresh-stager", "late-stager", "data", "duckdb-tmp", "compaction-tmp", "reorg-tmp"]


class Timeline:
    def __init__(self, path: Path):
        self.path = path
        self.t0 = time.time()
        self.events: list[dict] = []
        self._lock = threading.Lock()

    def mark(self, kind: str, **fields):
        ev = {"t": round(time.time() - self.t0, 2), "wall": time.strftime("%H:%M:%S"), "event": kind, **fields}
        with self._lock:
            self.events.append(ev)
            with open(self.path, "a") as f:
                f.write(json.dumps(ev) + "\n")
        print(f"[{ev['t']:8.1f}s] {kind} {json.dumps(fields) if fields else ''}", flush=True)
        return ev

    def first(self, kind: str):
        for e in self.events:
            if e["event"] == kind:
                return e
        return None


def dir_stats(path: Path) -> tuple[int, int]:
    """(bytes, parquet_files) under path, recursively. Missing dir = (0, 0)."""
    total = 0
    files = 0
    if not path.exists():
        return 0, 0
    for root, _dirs, fnames in os.walk(path):
        for fn in fnames:
            try:
                total += os.lstat(os.path.join(root, fn)).st_size
            except OSError:
                continue
            if fn.endswith(".parquet"):
                files += 1
    return total, files


class ArcProc:
    def __init__(self, name: str, binary: str, run_dir: Path, port: int, s3_endpoint: str,
                 bucket: str, profile: dict, overrides: list[str] | None, timeline: Timeline):
        self.name = name
        self.role = ROLE_OF[name]
        self.binary = binary
        self.port = port
        self.url = f"http://127.0.0.1:{port}"
        self.dir = run_dir / "procs" / name
        self.log_path = run_dir / "logs" / f"{name}.log"
        self.timeline = timeline
        self.proc: subprocess.Popen | None = None
        self.starts = 0
        self.pidfile = run_dir / "pids.json"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.cfg = build_arc_config(profile, name, self.dir, port, s3_endpoint, bucket, overrides)
        write_arc_toml(self.cfg, self.dir / "arc.toml")
        self.last_startup: dict = {}
        self.tz = "UTC"

    # ------------------------------------------------------------ paths
    @property
    def wal_dir(self) -> Path:
        return self.dir / "wal"

    @property
    def fresh_dir(self) -> Path:
        return self.dir / "fresh-stager"

    @property
    def late_dir(self) -> Path:
        return self.dir / "late-stager"

    # ------------------------------------------------------------ lifecycle
    def start(self):
        if self.alive():
            return
        self.starts += 1
        with open(self.log_path, "ab") as f:
            f.write(f'{{"harness":"start","n":{self.starts},"wall":"{time.strftime("%Y-%m-%dT%H:%M:%S")}"}}\n'.encode())
        log = open(self.log_path, "ab")
        argv = [self.binary]
        guard.check_command(argv)
        # TZ=UTC: prod containers run in UTC. On a non-UTC host the FreshStager's
        # DuckDB PARTITION_BY extract(hour FROM time) uses local time and files
        # land in the wrong hour directory (verified on the fork).
        self.proc = subprocess.Popen(argv, cwd=self.dir, stdout=log, stderr=subprocess.STDOUT,
                                     env=guard.child_env({"TZ": self.tz}), start_new_session=True)
        self._t_start = time.time()
        self._record_pid()
        self.timeline.mark("proc_start", proc=self.name, pid=self.proc.pid, n=self.starts)

    def _record_pid(self):
        try:
            pids = json.loads(self.pidfile.read_text()) if self.pidfile.exists() else {}
        except json.JSONDecodeError:
            pids = {}
        pids[self.name] = self.proc.pid if self.proc else None
        self.pidfile.write_text(json.dumps(pids))

    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def wait_healthy(self, timeout: float = 120, need_ready: bool = True) -> dict:
        """Poll /health then /ready; record time-to-health/ready since start."""
        t0 = getattr(self, "_t_start", time.time())
        res = {"proc": self.name, "health_s": None, "ready_s": None, "health_fail_polls": 0}
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not self.alive():
                res["exited"] = self.proc.returncode if self.proc else None
                break
            try:
                if res["health_s"] is None:
                    r = requests.get(self.url + "/health", timeout=1)
                    if r.status_code == 200:
                        res["health_s"] = round(time.time() - t0, 2)
                    else:
                        res["health_fail_polls"] += 1
                if res["health_s"] is not None:
                    if not need_ready:
                        break
                    r = requests.get(self.url + "/ready", timeout=1)
                    if r.status_code == 200:
                        res["ready_s"] = round(time.time() - t0, 2)
                        break
            except requests.RequestException:
                res["health_fail_polls"] += 1
            time.sleep(0.2)
        self.last_startup = res
        self.timeline.mark("proc_healthy" if res.get("health_s") is not None else "proc_unhealthy", **res)
        return res

    def sigterm(self, timeout: float | None = None) -> dict:
        """Graceful stop. Returns shutdown duration and exit code."""
        if not self.alive():
            return {"proc": self.name, "already_stopped": True}
        st = self.cfg.get("server", {}).get("shutdown_timeout", 30)
        timeout = timeout if timeout is not None else st + 30
        t0 = time.time()
        self.timeline.mark("sigterm", proc=self.name, pid=self.proc.pid)
        self.proc.send_signal(signal.SIGTERM)
        try:
            rc = self.proc.wait(timeout=timeout)
            killed = False
        except subprocess.TimeoutExpired:
            self.proc.kill()
            rc = self.proc.wait()
            killed = True
        res = {"proc": self.name, "shutdown_s": round(time.time() - t0, 2), "rc": rc, "escalated_to_kill": killed}
        self.timeline.mark("stopped", **res)
        return res

    def sigkill(self) -> dict:
        if not self.alive():
            return {"proc": self.name, "already_stopped": True}
        self.timeline.mark("sigkill", proc=self.name, pid=self.proc.pid)
        # Kill the whole session so compaction subprocesses die too (OOM-kill semantics).
        try:
            os.killpg(self.proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        rc = self.proc.wait()
        return {"proc": self.name, "rc": rc}

    def wipe(self):
        for d in WIPE_DIRS:
            shutil.rmtree(self.dir / d, ignore_errors=True)
        self.timeline.mark("wipe_dirs", proc=self.name)

    def restart(self, wipe: bool = False, graceful: bool = True, wait: bool = True) -> dict:
        stop = self.sigterm() if graceful else self.sigkill()
        if wipe:
            self.wipe()
        self.start()
        h = self.wait_healthy() if wait else {}
        return {"stop": stop, "startup": h}

    # ------------------------------------------------------------ introspection
    def buffer_stats(self) -> dict | None:
        try:
            r = requests.get(self.url + "/api/v1/write/msgpack/stats", timeout=3)
            if r.ok:
                return r.json().get("buffer", {})
        except requests.RequestException:
            return None
        return None

    def local_state(self) -> dict:
        wal_b, _ = dir_stats(self.wal_dir)
        fr_b, fr_n = dir_stats(self.fresh_dir)
        la_b, la_n = dir_stats(self.late_dir)
        q_n = 0
        for d in (self.fresh_dir / ".corrupt", self.late_dir / ".corrupt"):
            if d.exists():
                q_n += sum(1 for _ in d.iterdir())
        wal_files = len(list(self.wal_dir.glob("*.wal"))) if self.wal_dir.exists() else 0
        return {"wal_bytes": wal_b, "wal_files": wal_files, "fresh_bytes": fr_b, "fresh_files": fr_n,
                "late_bytes": la_b, "late_files": la_n, "quarantined": q_n}


class Sampler(threading.Thread):
    """Samples RSS (process + children) and local dir sizes every second."""

    FIELDS = ["t", "wall", "proc", "pid", "alive", "rss_mb", "children_rss_mb", "cpu_pct",
              "wal_bytes", "wal_files", "fresh_bytes", "fresh_files", "late_bytes", "late_files", "quarantined"]

    def __init__(self, procs: dict[str, ArcProc], path: Path, timeline: Timeline, interval: float = 1.0):
        super().__init__(daemon=True)
        self.procs = procs
        self.path = path
        self.timeline = timeline
        self.interval = interval
        self.stop_evt = threading.Event()
        self.max: dict[str, dict] = {}
        self._ps: dict[int, psutil.Process] = {}

    def run(self):
        with open(self.path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=self.FIELDS)
            w.writeheader()
            while not self.stop_evt.is_set():
                t = round(time.time() - self.timeline.t0, 1)
                for name, p in list(self.procs.items()):
                    row = {"t": t, "wall": time.strftime("%H:%M:%S"), "proc": name, "alive": int(p.alive())}
                    rss = crss = cpu = 0.0
                    if p.alive():
                        pid = p.proc.pid
                        row["pid"] = pid
                        try:
                            ps = self._ps.get(pid) or psutil.Process(pid)
                            self._ps[pid] = ps
                            rss = ps.memory_info().rss / 1e6
                            cpu = ps.cpu_percent(None)
                            for c in ps.children(recursive=True):
                                try:
                                    crss += c.memory_info().rss / 1e6
                                except psutil.Error:
                                    pass
                        except psutil.Error:
                            pass
                    row.update({"rss_mb": round(rss, 1), "children_rss_mb": round(crss, 1), "cpu_pct": round(cpu, 1)})
                    if p.role == "ingest":
                        row.update(p.local_state())
                    w.writerow(row)
                    m = self.max.setdefault(name, {})
                    for k in ("rss_mb", "children_rss_mb", "wal_bytes", "fresh_bytes", "late_bytes",
                              "fresh_files", "late_files", "wal_files", "quarantined"):
                        v = row.get(k)
                        if v is not None and v != "":
                            m[k] = max(m.get(k, 0), v)
                    if p.role == "ingest":
                        tot = (row.get("wal_bytes", 0) + row.get("fresh_bytes", 0) + row.get("late_bytes", 0))
                        m["local_total_bytes"] = max(m.get("local_total_bytes", 0), tot)
                f.flush()
                self.stop_evt.wait(self.interval)

    def stop(self):
        self.stop_evt.set()
        self.join(timeout=5)
