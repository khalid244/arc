#!/usr/bin/env python3
"""Arc E2E durability harness - scenario runner.

  python run.py S1 --binary /path/to/arc            # one scenario
  python run.py S1,S3,S4 --binary /path/to/arc      # several, sequentially
  python run.py list                                # describe scenarios
  python run.py cleanup [--remove-minio]            # kill strays, stop proxy, rm sync-* containers

Exit code: 0 PASS, 1 FAIL, 2 NOT_SUPPORTED, 3 harness error (worst of all
scenarios when several are run).
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from lib import scenarios  # noqa: E402
from lib.report import EXIT  # noqa: E402

DEFAULT_RUNS = HERE / "runs"


def common_args(p: argparse.ArgumentParser):
    p.add_argument("--binary", default=os.environ.get("ARC_E2E_BINARY"),
                   help="Arc binary under test (or env ARC_E2E_BINARY)")
    p.add_argument("--profile", default="scaled", help="profiles/<name>.toml or a path (default: scaled)")
    p.add_argument("--set", action="append", default=[],
                   help="override an arc.toml key for ALL roles, e.g. --set ingest.flush_timeout_seconds=30")
    p.add_argument("--rate", type=float, default=1500, help="offered rows/s across all ingest processes")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--runs-dir", default=str(DEFAULT_RUNS))
    p.add_argument("--startup-timeout", type=float, default=180)
    p.add_argument("--client-drain-timeout", type=float, default=300,
                   help="how long the load generator keeps retrying unacked batches after stop")
    p.add_argument("--drain-timeout", type=float, default=300,
                   help="T: max seconds after S3 recovery for Arc to drain local backlog (no restart)")
    p.add_argument("--max-rss-mb", type=float, default=4000, help="RSS bound per ingest process")
    p.add_argument("--warmup", type=float, default=None, help="seconds of clean load before the fault")
    p.add_argument("--outage-s", type=float, default=None, help="fault duration")
    p.add_argument("--post-restore-s", type=float, default=None, help="load after the fault ends")
    p.add_argument("--kill-at-s", type=float, default=None, help="S4/S5/S8/S11: seconds into the outage to kill")
    # fault knobs
    p.add_argument("--outage-mode", default="down", choices=["down", "hang", "http503"],
                   help="S1/S4/S5/S6/S7/S8/S9 outage shape: refused (down), black-holed (hang), 503 to everything")
    p.add_argument("--pct", type=float, default=None, help="S2/S10: %% of requests faulted")
    p.add_argument("--head-pct", type=float, default=100,
                   help="S10: %% of compactor HEADs answered 503 with '404' in the RequestId")
    p.add_argument("--latency-ms", type=int, default=None)
    p.add_argument("--jitter-ms", type=int, default=None)
    p.add_argument("--every-n", type=int, default=3, help="S3: commit_then_timeout every Nth PUT")
    p.add_argument("--ctt-response", default="504", choices=["504", "hang", "reset"])
    p.add_argument("--ctt-delay-ms", type=int, default=2000)
    p.add_argument("--sticky-ms", type=int, default=60000,
                   help="S3: retries of a hit key also commit+fail for this long (0 = off)")
    p.add_argument("--cap", action="append", default=[], help="S6: disk-cap key=value to set if supported")
    p.add_argument("--handoff", action="store_true",
                   help="ingest processes hand staged work to each other at shutdown "
                        "(ingest.stager_handoff_url + stager_import_enabled, like pods behind the ingest Service); "
                        "S5 then requires the wiped-dirs restart to be lossless")
    p.add_argument("--cap-keys", default=None, help="S6: comma list of config keys that indicate cap support")
    p.add_argument("--evict-at-mb", type=float, default=0,
                   help="S6: if caps unsupported, simulate kubelet emptyDir eviction at this local size")
    p.add_argument("--max-startup-s", type=float, default=60, help="S8: /health must answer within")
    p.add_argument("--burst-rate", type=float, default=5000, help="S8: load rate used to grow the WAL")
    p.add_argument("--stagger-ms", type=float, default=1700,
                   help="delay between ingest process starts (0 = phase-aligned stager tickers)")
    p.add_argument("--arc-tz", default="UTC",
                   help="TZ for Arc processes (prod containers: UTC; a local TZ exposes the FreshStager hour bug)")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("scenario", help="S0..S10, comma list, 'all', 'list' or 'cleanup'")
    p.add_argument("--remove-minio", action="store_true", help="cleanup: also remove the sync-minio container")
    common_args(p)
    a = p.parse_args()

    if a.scenario == "list":
        for k, (fn, doc) in scenarios.REGISTRY.items():
            print(f"{k:4} {doc}")
        return 0
    if a.scenario == "cleanup":
        return cleanup(a)
    if not a.binary:
        p.error("--binary (or ARC_E2E_BINARY) is required")

    names = list(scenarios.REGISTRY) if a.scenario == "all" else [s.strip().upper() for s in a.scenario.split(",")]
    worst = 0
    results = []
    for name in names:
        if name not in scenarios.REGISTRY:
            p.error(f"unknown scenario {name}")
        fn, _doc = scenarios.REGISTRY[name]
        t0 = time.time()
        try:
            code = fn(a)
        except KeyboardInterrupt:
            print("interrupted; cleaning up", flush=True)
            cleanup(a, quiet=True)
            return 130
        except Exception:  # noqa: BLE001
            traceback.print_exc()
            code = EXIT["ERROR"]
        results.append((name, code, round(time.time() - t0)))
        worst = max(worst, code)
    if len(results) > 1:
        inv = {v: k for k, v in EXIT.items()}
        print("\nscenario  status         seconds")
        for n, c, s in results:
            print(f"{n:9} {inv.get(c, c):14} {s}")
    return worst


def cleanup(a, quiet=False) -> int:
    """Kill Arc/proxy/loadgen processes started by the harness and remove sync-* containers."""
    runs = Path(a.runs_dir)
    killed = []
    for pidfile in runs.glob("*/pids.json"):
        try:
            pids = json.loads(pidfile.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        for name, pid in pids.items():
            if not pid:
                continue
            try:
                os.killpg(pid, signal.SIGKILL)
                killed.append((name, pid))
            except (ProcessLookupError, PermissionError):
                pass
    # Stray harness processes (match on the harness dir so nothing else is touched).
    r = subprocess.run(["pgrep", "-f", f"{HERE}/.build/faultproxy|e2e-harness={HERE}"], capture_output=True, text=True)
    for pid in r.stdout.split():
        try:
            os.kill(int(pid), signal.SIGKILL)
            killed.append(("harness", int(pid)))
        except (ProcessLookupError, PermissionError):
            pass
    r = subprocess.run(["docker", "ps", "-a", "--format", "{{.Names}}"], capture_output=True, text=True)
    removed = []
    for n in r.stdout.split():
        if n.startswith("sync-mc-") or (n == "sync-minio" and a.remove_minio):
            subprocess.run(["docker", "rm", "-f", n], capture_output=True)
            removed.append(n)
    if not quiet:
        print(f"killed: {killed}\nremoved containers: {removed}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
