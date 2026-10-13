"""Local infrastructure: MinIO container (sync-minio) and the Go fault proxy."""
from __future__ import annotations

import json
import socket
import subprocess
import time
import uuid
from pathlib import Path

import requests

from . import guard
from .config import HARNESS_DIR, MINIO_PASSWORD, MINIO_USER, Ports

MINIO_IMAGE = "minio/minio:latest"
MC_IMAGE = "quay.io/minio/mc:latest"
CONTAINER = "sync-minio"


def _run(argv, check=True, capture=True, timeout=120):
    guard.check_command(argv)
    return subprocess.run(argv, check=check, capture_output=capture, text=True, timeout=timeout)


class Minio:
    def __init__(self, ports: Ports):
        self.ports = ports
        self.endpoint = f"127.0.0.1:{ports.minio}"
        guard.check_endpoint(self.endpoint, "MinIO endpoint")

    def _state(self) -> str | None:
        r = _run(["docker", "inspect", "-f", "{{.State.Status}}", CONTAINER], check=False)
        return r.stdout.strip() if r.returncode == 0 else None

    def ensure_running(self):
        st = self._state()
        if st == "running":
            self._wait_live()
            return
        if st is not None:
            _run(["docker", "rm", "-f", CONTAINER], check=False)
        _run([
            "docker", "run", "-d", "--name", CONTAINER,
            "-p", f"127.0.0.1:{self.ports.minio}:9000",
            "-p", f"127.0.0.1:{self.ports.minio_console}:9001",
            "-e", f"MINIO_ROOT_USER={MINIO_USER}",
            "-e", f"MINIO_ROOT_PASSWORD={MINIO_PASSWORD}",
            "--label", "arc-e2e=1",
            MINIO_IMAGE, "server", "/data", "--console-address", ":9001",
        ])
        self._wait_live()

    def _wait_live(self, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                if requests.get(f"http://{self.endpoint}/minio/health/live", timeout=2).status_code == 200:
                    return
            except requests.RequestException:
                pass
            time.sleep(0.5)
        raise RuntimeError("MinIO did not become live")

    def mc(self, *args: str, timeout=300) -> str:
        """Run an mc command inside a throwaway sync-mc-* container that
        shares sync-minio's network namespace."""
        name = f"sync-mc-{uuid.uuid4().hex[:8]}"
        script = (f"mc alias set e2e http://127.0.0.1:9000 {MINIO_USER} {MINIO_PASSWORD} >/dev/null && "
                  + " ".join(args))
        r = _run(["docker", "run", "--rm", "--name", name, "--network", f"container:{CONTAINER}",
                  "--entrypoint", "sh", MC_IMAGE, "-c", script], check=False, timeout=timeout)
        if r.returncode != 0:
            raise RuntimeError(f"mc {' '.join(args)} failed: {r.stdout}\n{r.stderr}")
        return r.stdout

    def create_bucket(self, bucket: str):
        self.mc(f"mc mb --ignore-existing e2e/{bucket}")

    def remove(self):
        _run(["docker", "rm", "-f", CONTAINER], check=False)


def port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sk:
        sk.settimeout(0.3)
        return sk.connect_ex(("127.0.0.1", port)) == 0


def preflight_ports(ports: list[int]):
    """Refuse to start if a port the harness needs is already taken (a stale proxy
    or Arc from an earlier run would silently answer instead of ours)."""
    busy = [p for p in ports if port_in_use(p)]
    if busy:
        raise RuntimeError(f"ports already in use: {busy} - run `python run.py cleanup` "
                           f"(or check `lsof -nP -iTCP:{busy[0]} -sTCP:LISTEN`)")


def build_slowsync() -> Path:
    """slowdisk/slowsync.c -> .build/slowsync.dylib (HDD-like fsync for ingest, see the source)."""
    src = HARNESS_DIR / "slowdisk" / "slowsync.c"
    out = HARNESS_DIR / ".build" / "slowsync.dylib"
    if out.exists() and out.stat().st_mtime >= src.stat().st_mtime:
        return out
    out.parent.mkdir(parents=True, exist_ok=True)
    r = subprocess.run(["clang", "-dynamiclib", "-O2", "-o", str(out), str(src)], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"slowsync build failed:\n{r.stderr}")
    return out


class FaultProxy:
    SRC = HARNESS_DIR / "faultproxy"
    BIN = HARNESS_DIR / ".build" / "faultproxy"

    def __init__(self, ports: Ports, run_dir: Path):
        self.ports = ports
        self.run_dir = run_dir
        self.proc: subprocess.Popen | None = None
        self.ctl = f"http://127.0.0.1:{ports.proxy_control}"

    @classmethod
    def build(cls):
        srcs = list(cls.SRC.glob("*.go")) + [cls.SRC / "go.mod"]
        if cls.BIN.exists() and all(cls.BIN.stat().st_mtime >= s.stat().st_mtime for s in srcs):
            return cls.BIN
        cls.BIN.parent.mkdir(parents=True, exist_ok=True)
        r = subprocess.run(["go", "build", "-o", str(cls.BIN), "."], cwd=cls.SRC, capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError(f"faultproxy build failed:\n{r.stderr}")
        return cls.BIN

    def endpoint(self, scope: str) -> str:
        return f"127.0.0.1:{self.ports.proxy[scope]}"

    def start(self):
        preflight_ports([self.ports.proxy_control, *self.ports.proxy.values()])
        binp = self.build()
        argv = [str(binp), "-upstream", f"http://127.0.0.1:{self.ports.minio}",
                "-control", f"127.0.0.1:{self.ports.proxy_control}",
                "-events", str(self.run_dir / "proxy-events.jsonl")]
        for name, port in self.ports.proxy.items():
            argv += ["-listen", f"{name}=127.0.0.1:{port}"]
        guard.check_command(argv)
        log = open(self.run_dir / "logs" / "faultproxy.log", "ab")
        self.proc = subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT, env=guard.child_env(),
                                     start_new_session=True)
        deadline = time.time() + 15
        while time.time() < deadline:
            if self.proc.poll() is not None:
                break
            try:
                if requests.get(self.ctl + "/health", timeout=1).ok:
                    names = set(requests.get(self.ctl + "/mode", timeout=2).json())
                    if names != set(self.ports.proxy):
                        raise RuntimeError(f"proxy listeners {names} != expected {set(self.ports.proxy)}")
                    return
            except requests.RequestException:
                pass
            time.sleep(0.2)
        raise RuntimeError("fault proxy failed to start (see logs/faultproxy.log)")

    def set(self, mode: str = "normal", scope="all", **cfg) -> dict:
        body = {"scope": scope, "mode": mode, **cfg}
        r = requests.post(self.ctl + "/mode", data=json.dumps(body), timeout=10)
        if not r.ok:
            raise RuntimeError(f"proxy /mode failed: {r.status_code} {r.text}")
        return r.json()

    def normal(self, scope="all"):
        return self.set("normal", scope=scope)

    def stats(self) -> dict:
        try:
            return requests.get(self.ctl + "/stats", timeout=10).json()
        except requests.RequestException as e:
            return {"error": str(e)}

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
