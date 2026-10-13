"""A Kubernetes Service in front of the ingest processes, the way kube-proxy does it.

Each NEW client connection is assigned, round-robin, to one of the endpoints
currently listed in the endpoints file, and stays pinned to it until either
side closes (iptables DNAT + conntrack): removing an endpoint (pod terminating,
preStop running) stops new connections from going there, while established
keep-alive connections keep carrying requests to it - which is exactly how a
producer's pooled connections reach a pod after SIGTERM.

Bytes are forwarded unchanged. A FIN from one side is passed on as a FIN
(half-close); a reset (or a connect failure) is passed on as a reset.

Run:  python -m lib.svcproxy --listen 29150 --endpoints-file run/endpoints.json
      endpoints.json: ["http://127.0.0.1:29101", ...]   (re-read every 0.2 s)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import socket
import sys
from urllib.parse import urlparse

from .guard import check_endpoint


class Service:
    def __init__(self, endpoints_file: str):
        self.endpoints_file = endpoints_file
        self.endpoints: list[tuple[str, int]] = []
        self.rr = 0
        self.stats = {"connections": 0, "no_endpoint": 0, "connect_failed": 0, "resets": 0}
        self._mtime = None

    def reload(self):
        try:
            st = os.stat(self.endpoints_file)
        except FileNotFoundError:
            return
        if st.st_mtime_ns == self._mtime:
            return
        try:
            with open(self.endpoints_file) as f:
                urls = json.load(f)
        except (OSError, json.JSONDecodeError):
            return
        eps = []
        for u in urls:
            check_endpoint(u, "service endpoint")
            p = urlparse(u)
            eps.append((p.hostname, p.port))
        self.endpoints = eps
        self._mtime = st.st_mtime_ns

    async def watch(self):
        while True:
            self.reload()
            await asyncio.sleep(0.2)

    def pick(self):
        if not self.endpoints:
            return None
        ep = self.endpoints[self.rr % len(self.endpoints)]
        self.rr += 1
        return ep

    async def handle(self, creader: asyncio.StreamReader, cwriter: asyncio.StreamWriter):
        ep = self.pick()
        self.stats["connections"] += 1
        if ep is None:
            self.stats["no_endpoint"] += 1
            _abort(cwriter)
            return
        try:
            breader, bwriter = await asyncio.open_connection(ep[0], ep[1])
        except OSError:
            self.stats["connect_failed"] += 1
            _abort(cwriter)
            return
        for w in (cwriter, bwriter):
            sock = w.get_extra_info("socket")
            if sock is not None:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        reset = asyncio.Event()

        async def pump(r: asyncio.StreamReader, w: asyncio.StreamWriter):
            try:
                while True:
                    data = await r.read(1 << 16)
                    if not data:
                        if w.can_write_eof():
                            w.write_eof()  # FIN -> FIN
                        return
                    w.write(data)
                    await w.drain()
            except (ConnectionResetError, BrokenPipeError, OSError):
                reset.set()

        t1 = asyncio.create_task(pump(creader, bwriter))
        t2 = asyncio.create_task(pump(breader, cwriter))
        rt = asyncio.create_task(reset.wait())
        done, _ = await asyncio.wait({asyncio.gather(t1, t2), rt}, return_when=asyncio.FIRST_COMPLETED)
        if rt in done:
            self.stats["resets"] += 1
            _abort(cwriter)
            _abort(bwriter)
            t1.cancel()
            t2.cancel()
        else:
            rt.cancel()
            for w in (cwriter, bwriter):
                try:
                    w.close()
                except OSError:
                    pass


def _abort(w: asyncio.StreamWriter):
    try:
        w.transport.abort()  # RST
    except OSError:
        pass


async def main_async(a):
    check_endpoint(f"http://127.0.0.1:{a.listen}", "service listen")
    svc = Service(a.endpoints_file)
    svc.reload()
    server = await asyncio.start_server(svc.handle, "127.0.0.1", a.listen, backlog=1024)
    watcher = asyncio.create_task(svc.watch())
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    loop.add_signal_handler(signal.SIGINT, stop.set)
    async with server:
        await stop.wait()
    watcher.cancel()
    if a.stats_file:
        with open(a.stats_file, "w") as f:
            json.dump(svc.stats, f)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--listen", type=int, required=True)
    p.add_argument("--endpoints-file", required=True)
    p.add_argument("--stats-file", default="")
    p.add_argument("--tag", default="", help="ignored; lets the harness find its own processes")
    a = p.parse_args(argv)
    if not (29000 <= a.listen <= 29999):
        raise SystemExit("service port must be in 29000-29999")
    asyncio.run(main_async(a))


if __name__ == "__main__":
    sys.exit(main())
