"""Load generator: msgpack-columnar + line-protocol writes with a uuid per row.

Every row carries a unique `uuid`. A batch is recorded in the ledger as
ACKED only when Arc answered 2xx. Non-2xx answers and transport errors are
retried with exponential backoff (same payload, same uuids). An attempt whose
outcome is unknown (timeout / reset after the request was sent) is counted as
"ambiguous": if that batch later shows duplicates, the client may have caused
them, and the verifier reports such duplicates separately.

Measurements (prod-shaped):
  default.downloads   msgpack columnar, sort key site,response,tag
  posthog.events      msgpack columnar, wide + sparse: each batch is one event
                      type with its own property columns (schema variance)
  default.lp_metrics  line protocol, precision s/ms/us/ns
  posthog.lifecycle   (mix kind "phsparse") msgpack columnar shaped like
                      hammel-posthog-api write_buffer.rs: time (ms), uuid,
                      event, distinct_id, created_at, seq + a random subset of a
                      pool of --prop-pool sparse STRING property columns ("" for
                      an event without the property), so no two batches carry
                      the same column set

Timestamps: mostly fresh (inside late_window), some slightly in the future,
some older than late_window (late sidecar path), some days old, some beyond
future_skew. Units per batch: us (most), ms, s.

--no-retry: one attempt per batch, like hammel-posthog-api (any non-2xx or
transport error drops the batch); counted per status and as never_acked.

--tick-s T: producer replicas instead of a shared queue. Each of --concurrency
writers has its own client with ONE keep-alive connection (a replica's
reqwest pool) and sends one batch on every shared tick of T seconds, all
writers at once: the worst case of replicas flushing together against one
measurement. Batches are built between ticks, so the sends really coincide.

Run:  python -m lib.loadgen --targets http://127.0.0.1:29101,http://127.0.0.1:29102 \
        --ledger run/ledger.sqlite --stats-file run/loadgen-stats.json --stop-file run/STOP
Stop: create the stop file or send SIGTERM. In-flight batches keep retrying
until --drain-timeout, then remaining batches are recorded as never_acked.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import signal
import sys
import time
import uuid as uuidlib
from collections import Counter, deque
from pathlib import Path

import aiohttp
import msgpack

from .guard import check_endpoint
from .ledger import Ledger

SITES = [f"site{i:02d}.example" for i in range(40)]
RESPONSES = [200, 200, 200, 200, 206, 206, 304, 404, 500, 503]
TAGS = ["mp3", "video", "podcast", "ringtone", "album", "single", "playlist", "radio", "clip", "other"]
COUNTRIES = ["SA", "AE", "EG", "KW", "QA", "BH", "OM", "JO", "MA", "US"]
OSES = ["iOS", "Android", "iPadOS", "web"]
APP_VERSIONS = ["10.17.0", "10.16.2", "10.15.0", "9.9.1", "10.18.0-beta"]

EVENT_TYPES = (
    ["survey sent", "screen_view", "purchase", "song_played", "app_open", "app_background"]
    + [f"custom_event_{i:02d}" for i in range(19)]
)


def event_columns(ev: str, n: int, rnd: random.Random) -> dict:
    """Per-event-type property columns (posthog-like schema variance)."""
    if ev == "survey sent":
        return {"survey_name": [rnd.choice(["nps", "churn", "onboarding"]) for _ in range(n)],
                "survey_response": [rnd.choice(["1", "5", "9", "10", "great"]) for _ in range(n)]}
    if ev == "screen_view":
        return {"screen_name": [rnd.choice(["home", "player", "search", "library", "settings"]) for _ in range(n)]}
    if ev == "purchase":
        return {"price": [round(rnd.uniform(0.99, 49.99), 2) for _ in range(n)],
                "currency": [rnd.choice(["SAR", "USD", "AED"]) for _ in range(n)]}
    if ev == "song_played":
        return {"song_id": [rnd.randint(1, 10_000_000) for _ in range(n)],
                "duration_s": [round(rnd.uniform(5, 600), 1) for _ in range(n)],
                "playlist": [rnd.choice(["fav", "top50", None, "radio"]) for _ in range(n)]}
    if ev in ("app_open", "app_background"):
        return {"session_len_s": [rnd.randint(0, 7200) for _ in range(n)]}
    k = ev.rsplit("_", 1)[-1]
    return {f"prop_{k}_a": [rnd.randint(0, 1000) for _ in range(n)],
            f"prop_{k}_b": [rnd.choice(["x", "y", "z", "w"]) for _ in range(n)]}


class TimeModel:
    """Row timestamp classes, in microseconds."""

    def __init__(self, late_window: int, future_skew: int, weights: dict[str, float]):
        self.lw = late_window
        self.fs = future_skew
        self.classes = list(weights.keys())
        self.weights = list(weights.values())

    def pick(self, rnd: random.Random, now_us: int) -> tuple[int, str]:
        c = rnd.choices(self.classes, self.weights)[0]
        lw, fs = self.lw * 1_000_000, self.fs * 1_000_000
        if c == "fresh":             # real-time traffic
            off = -int(rnd.uniform(0, 60e6))
        elif c == "recent":          # inside late_window (may still hit an older hour bucket)
            off = -int(rnd.uniform(60e6, 0.9 * lw))
        elif c == "future":
            off = int(rnd.uniform(0.05, 0.8) * fs)
        elif c == "late":            # older than late_window, up to 6h
            off = -int(rnd.uniform(lw * 1.15, max(lw * 1.2, 6 * 3600e6)))
        elif c == "old":             # 1-3 days old
            off = -int(rnd.uniform(86400e6, 3 * 86400e6))
        else:                        # "farfuture": beyond future_skew
            off = int(rnd.uniform(fs * 1.5, fs * 1.5 + 3600e6))
        return now_us + off, c


def parse_mix(s: str) -> dict[str, float]:
    out = {}
    for part in s.split(","):
        k, v = part.split(":")
        out[k.strip()] = float(v)
    return out


class LoadGen:
    def __init__(self, a):
        self.a = a
        self.targets = [t.rstrip("/") for t in a.targets.split(",")]
        for t in self.targets:
            check_endpoint(t, "ingest target")
        self.rnd = random.Random(a.seed)
        self.mix = parse_mix(a.mix)
        self.units = parse_mix(a.unit_mix)
        self.tm = TimeModel(a.late_window, a.future_skew, parse_mix(a.time_mix))
        self.ledger = Ledger(Path(a.ledger))
        self.next_id = self.ledger.next_batch_id()
        self.rr = 0
        self.stopping = False
        self.drain_deadline = None
        self.done_batches: list[dict] = []
        self.stats = Counter()
        self.errors = Counter()
        self.per_target_acked = Counter()
        self.lat = deque(maxlen=5000)  # (t_done, latency_s)
        self.acks = deque(maxlen=200000)  # (t, rows)
        self.inflight = 0
        self.t0 = time.time()
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=a.concurrency * 2)
        self.all_lat: list[float] = []      # every request's latency (s), whole run
        self.status_counts = Counter()      # "204", "503", "500", "transport"
        self.err_samples: list[dict] = []
        self.prop_pool = [f"prop_{i:03d}" for i in range(a.prop_pool)]
        self.prop_values = [f"v{i}" for i in range(400)] + [""] * 260  # ~40 % of cells empty

    # ------------------------------------------------------------ payloads
    def make_batch(self) -> dict:
        rnd = self.rnd
        kind = rnd.choices(list(self.mix), list(self.mix.values()))[0]
        n = rnd.randint(self.a.batch_min, self.a.batch_max)
        now_us = int(time.time() * 1_000_000)
        rows_t = [self.tm.pick(rnd, now_us) for _ in range(n)]
        # uuid4() (os.urandom), never the seeded RNG: the seeder and earlier runs share seeds.
        uuids = [str(uuidlib.uuid4()) for _ in range(n)]
        bid = self.next_id
        self.next_id += 1
        target = self.targets[self.rr % len(self.targets)]
        self.rr += 1

        if kind == "lp":
            prec = rnd.choices(["ns", "us", "ms", "s"], [0.4, 0.3, 0.15, 0.15])[0]
            div = {"ns": None, "us": 1, "ms": 1000, "s": 1_000_000}[prec]
            lines, rows = [], []
            for (t_us, c), u in zip(rows_t, uuids):
                if prec == "ns":
                    ts = t_us * 1000
                    exp = t_us
                else:
                    ts = t_us // div
                    exp = ts * div
                host = f"h{rnd.randint(0, 30):02d}"
                lines.append(f'lp_metrics,host={host},region=r{rnd.randint(0, 3)} '
                             f'uuid="{u}",value={rnd.uniform(0, 100):.3f},count={rnd.randint(0, 99)}i {ts}')
                rows.append((u, "default", "lp_metrics", exp, c))
            return {"batch_id": bid, "db": "default", "measurement": "lp_metrics", "fmt": "lp", "unit": prec,
                    "n_rows": n, "body": ("\n".join(lines) + "\n").encode(), "rows": rows, "target": target,
                    "path": f"/api/v1/write/line-protocol?precision={prec}",
                    "headers": {"Content-Type": "text/plain", "x-arc-database": "default"}}

        if kind == "phsparse":
            return self._phsparse_batch(n, rows_t, uuids, bid, target)

        unit = rnd.choices(list(self.units), list(self.units.values()))[0]
        div = {"us": 1, "ms": 1000, "s": 1_000_000}[unit]
        tcol = [t // div for t, _ in rows_t]
        exp = [t * div for t in tcol]
        if kind == "downloads":
            db, m = "default", "downloads"
            cols = {
                "time": tcol, "uuid": uuids,
                "site": [rnd.choice(SITES) for _ in range(n)],
                "response": [rnd.choice(RESPONSES) for _ in range(n)],
                "tag": [rnd.choice(TAGS) for _ in range(n)],
                "bytes": [rnd.randint(1000, 50_000_000) for _ in range(n)],
                "duration_ms": [round(rnd.uniform(1, 30000), 2) for _ in range(n)],
                "country": [rnd.choice(COUNTRIES) for _ in range(n)],
            }
        else:
            db, m = "posthog", "events"
            ev = rnd.choice(EVENT_TYPES)
            cols = {
                "time": tcol, "uuid": uuids, "event": [ev] * n,
                "distinct_id": [f"user-{rnd.randint(1, 50000)}" for _ in range(n)],
                "os_name": [rnd.choice(OSES) for _ in range(n)],
                "app_version": [rnd.choice(APP_VERSIONS) for _ in range(n)],
            }
            cols.update(event_columns(ev, n, rnd))
        body = msgpack.packb({"m": m, "columns": cols}, use_bin_type=True)
        rows = [(u, db, m, e, c) for u, e, (_, c) in zip(uuids, exp, rows_t)]
        return {"batch_id": bid, "db": db, "measurement": m, "fmt": "msgpack", "unit": unit, "n_rows": n,
                "body": body, "rows": rows, "target": target, "path": "/api/v1/write/msgpack",
                "headers": {"Content-Type": "application/msgpack", "x-arc-database": db}}

    def _phsparse_batch(self, n, rows_t, uuids, bid, target) -> dict:
        """One hammel-posthog-api batch: fixed columns + the union of the
        properties its events carry, every property a string."""
        rnd = self.rnd
        db, m = "posthog", "lifecycle"
        tcol = [t // 1000 for t, _ in rows_t]            # the producer sends ms
        exp = [t * 1000 for t in tcol]
        created = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())
        cols = {
            "time": tcol, "uuid": uuids,
            "event": rnd.choices(EVENT_TYPES, k=n),
            "distinct_id": [f"user-{rnd.randint(1, 50000)}" for _ in range(n)],
            "created_at": [created] * n,
            "seq": list(range(n)),
        }
        k = rnd.randint(self.a.props_min, min(self.a.props_max, len(self.prop_pool)))
        for name in rnd.sample(self.prop_pool, k):
            cols[name] = rnd.choices(self.prop_values, k=n)
        body = msgpack.packb({"m": m, "columns": cols}, use_bin_type=True)
        rows = [(u, db, m, e, c) for u, e, (_, c) in zip(uuids, exp, rows_t)]
        return {"batch_id": bid, "db": db, "measurement": m, "fmt": "msgpack", "unit": "ms", "n_rows": n,
                "body": body, "rows": rows, "target": target, "path": "/api/v1/write/msgpack",
                "headers": {"Content-Type": "application/msgpack", "x-arc-database": db}}

    # ------------------------------------------------------------ sending
    async def send_once(self, session: aiohttp.ClientSession, b: dict):
        """--no-retry: a single attempt; anything but 2xx loses the batch."""
        first_sent = t_req = time.time()
        target = b["target"]
        err, ambiguous, body = None, 0, b""
        try:
            async with session.post(target + b["path"], data=b["body"], headers=b["headers"],
                                    timeout=aiohttp.ClientTimeout(total=self.a.timeout)) as resp:
                body = await resp.read()
                self.status_counts[str(resp.status)] += 1
                if 200 <= resp.status < 300:
                    lat = time.time() - t_req
                    self.all_lat.append(lat)
                    self.lat.append((time.time(), lat))
                    self._finish(b, "acked", 1, 0, first_sent, target, None)
                    return
                err = f"http_{resp.status}"
        except aiohttp.ClientConnectorError:
            err = "connect_refused"
        except (asyncio.TimeoutError, aiohttp.ServerDisconnectedError, aiohttp.ClientOSError,
                aiohttp.ClientPayloadError) as e:
            err, ambiguous = f"ambiguous_{type(e).__name__}", 1
        except aiohttp.ClientError as e:
            err, ambiguous = f"client_{type(e).__name__}", 1
        if not err.startswith("http_"):
            self.status_counts["transport"] += 1
        self.all_lat.append(time.time() - t_req)
        self.errors[err] += 1
        if len(self.err_samples) < 50:
            self.err_samples.append({"t": round(time.time() - self.t0, 2), "error": err,
                                     "latency_s": round(time.time() - t_req, 3), "rows": b["n_rows"],
                                     "body": body[:300].decode(errors="replace")})
        self._finish(b, "never_acked", 1, ambiguous, first_sent, None, err)

    async def send(self, session: aiohttp.ClientSession, b: dict):
        if self.a.no_retry:
            return await self.send_once(session, b)
        attempts = ambiguous = 0
        first_sent = time.time()
        target = b["target"]
        last_err = None
        ti = self.targets.index(target)
        while True:
            attempts += 1
            t_req = time.time()
            failover = False
            try:
                async with session.post(target + b["path"], data=b["body"], headers=b["headers"],
                                        timeout=aiohttp.ClientTimeout(total=self.a.timeout)) as resp:
                    await resp.read()
                    if 200 <= resp.status < 300:
                        self._finish(b, "acked", attempts, ambiguous, first_sent, target, None)
                        self.lat.append((time.time(), time.time() - t_req))
                        return
                    last_err = f"http_{resp.status}"
                    if resp.status in (502, 503, 504):
                        failover = True
                    if 400 <= resp.status < 500 and resp.status not in (408, 429) and attempts >= 3:
                        self.errors[last_err] += 1
                        self._finish(b, "never_acked", attempts, ambiguous, first_sent, None, last_err)
                        return
            except aiohttp.ClientConnectorError:
                last_err = "connect_refused"
                failover = True
            except (asyncio.TimeoutError, aiohttp.ServerDisconnectedError, aiohttp.ClientOSError,
                    aiohttp.ClientPayloadError) as e:
                last_err = f"ambiguous_{type(e).__name__}"
                ambiguous += 1
                failover = True
            except aiohttp.ClientError as e:
                last_err = f"client_{type(e).__name__}"
                ambiguous += 1
            self.errors[last_err] += 1
            if self.stopping and self.drain_deadline and time.time() > self.drain_deadline:
                self._finish(b, "never_acked", attempts, ambiguous, first_sent, None, last_err)
                return
            if failover and self.a.failover and len(self.targets) > 1:
                ti = (ti + 1) % len(self.targets)
                target = self.targets[ti]
            await asyncio.sleep(min(self.a.backoff_max, 0.2 * (2 ** min(attempts, 6))) * (0.5 + random.random()))

    def _finish(self, b, status, attempts, ambiguous, first_sent, ack_target, err):
        rec = {k: b[k] for k in ("batch_id", "db", "measurement", "fmt", "unit", "n_rows", "rows")}
        rec.update({"first_target": b["target"], "ack_target": ack_target, "attempts": attempts,
                    "ambiguous_attempts": ambiguous, "first_sent": first_sent,
                    "acked_at": time.time() if status == "acked" else None, "status": status, "last_error": err})
        self.done_batches.append(rec)
        self.stats[f"batches_{status}"] += 1
        self.stats[f"rows_{status}"] += b["n_rows"]
        if status == "acked":
            self.per_target_acked[ack_target] += b["n_rows"]
            self.acks.append((time.time(), b["n_rows"]))

    async def worker(self, session):
        while True:
            b = await self.queue.get()
            if b is None:
                self.queue.task_done()
                return
            self.inflight += 1
            try:
                await self.send(session, b)
            finally:
                self.inflight -= 1
                self.queue.task_done()

    async def producer(self):
        rate = self.a.rate
        next_t = time.time()
        end_t = self.t0 + self.a.duration if self.a.duration > 0 else None
        while not self.stopping:
            if end_t and time.time() >= end_t:
                break
            b = self.make_batch()
            self.stats["batches_created"] += 1
            self.stats["rows_created"] += b["n_rows"]
            await self.queue.put(b)
            next_t += b["n_rows"] / rate
            delay = next_t - time.time()
            if delay > 0:
                await asyncio.sleep(delay)
            elif delay < -5:
                next_t = time.time()  # fell behind (backpressure): don't burst to catch up
        self.stopping = True

    async def flusher(self):
        while True:
            await asyncio.sleep(0.5)
            self.flush_ledger()
            self.write_stats()

    def flush_ledger(self):
        if self.done_batches:
            batch, self.done_batches = self.done_batches, []
            self.ledger.record(batch)

    def write_stats(self, done=False):
        now = time.time()
        rows10 = sum(n for t, n in self.acks if now - t <= 10)
        lats = sorted(l for t, l in self.lat if now - t <= 10)
        p = lambda q: round(lats[min(len(lats) - 1, int(q * len(lats)))], 4) if lats else None
        st = {"t": round(now - self.t0, 1), "done": done, "stopping": self.stopping,
              **self.stats, "inflight": self.inflight, "queued": self.queue.qsize(),
              "acked_rows_per_s_10s": round(rows10 / 10, 1), "lat_p50_s": p(0.5), "lat_p99_s": p(0.99),
              "errors": dict(self.errors), "per_target_acked_rows": dict(self.per_target_acked),
              "status_counts": dict(self.status_counts), "err_samples": self.err_samples,
              "lat_all": self._lat_summary()}
        tmp = self.a.stats_file + ".tmp"
        with open(tmp, "w") as f:
            json.dump(st, f)
        os.replace(tmp, self.a.stats_file)

    def _lat_summary(self) -> dict:
        lats = sorted(self.all_lat)
        if not lats:
            return {"count": 0}
        q = lambda f: round(lats[min(len(lats) - 1, int(f * len(lats)))], 4)
        return {"count": len(lats), "p50_s": q(0.5), "p90_s": q(0.9), "p99_s": q(0.99), "max_s": round(lats[-1], 4)}

    async def ticking_writer(self, i: int):
        """--tick-s: one producer replica with its own keep-alive connection."""
        conn = aiohttp.TCPConnector(limit=1, force_close=False)
        tick = self.a.tick_s
        end_t = self.t0 + self.a.duration if self.a.duration > 0 else None
        async with aiohttp.ClientSession(connector=conn) as session:
            next_t = self.t0 + tick
            while not self.stopping and not (end_t and time.time() >= end_t):
                b = self.make_batch()
                self.stats["batches_created"] += 1
                self.stats["rows_created"] += b["n_rows"]
                delay = next_t - time.time()
                if delay > 0:
                    await asyncio.sleep(delay)
                if self.stopping:
                    self.stats["batches_created"] -= 1
                    self.stats["rows_created"] -= b["n_rows"]
                    break
                self.inflight += 1
                try:
                    await self.send(session, b)
                finally:
                    self.inflight -= 1
                next_t += tick
                if next_t < time.time():  # the send outlasted the tick: next one on the grid
                    next_t += tick * (int((time.time() - next_t) / tick) + 1)

    async def run_ticking(self):
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, lambda: setattr(self, "stopping", True))
        loop.add_signal_handler(signal.SIGINT, lambda: setattr(self, "stopping", True))
        fl = asyncio.create_task(self.flusher())
        sw = asyncio.create_task(self.stop_watch())
        await asyncio.gather(*(self.ticking_writer(i) for i in range(self.a.concurrency)))
        self.stopping = True
        fl.cancel()
        sw.cancel()
        self.flush_ledger()
        self.ledger.set_meta("loadgen_args", json.dumps(vars(self.a)))
        self.write_stats(done=True)
        self.ledger.close()

    async def stop_watch(self):
        while not self.stopping:
            if self.a.stop_file and os.path.exists(self.a.stop_file):
                self.stopping = True
                break
            await asyncio.sleep(0.2)

    async def run(self):
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, lambda: setattr(self, "stopping", True))
        loop.add_signal_handler(signal.SIGINT, lambda: setattr(self, "stopping", True))
        conn = aiohttp.TCPConnector(limit=self.a.concurrency * 2, force_close=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            workers = [asyncio.create_task(self.worker(session)) for _ in range(self.a.concurrency)]
            fl = asyncio.create_task(self.flusher())
            sw = asyncio.create_task(self.stop_watch())
            await self.producer()
            self.drain_deadline = time.time() + self.a.drain_timeout
            for _ in workers:
                await self.queue.put(None)
            await asyncio.gather(*workers)
            fl.cancel()
            sw.cancel()
        self.flush_ledger()
        self.ledger.set_meta("loadgen_args", json.dumps(vars(self.a)))
        self.write_stats(done=True)
        self.ledger.close()


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--targets", required=True)
    p.add_argument("--ledger", required=True)
    p.add_argument("--stats-file", required=True)
    p.add_argument("--stop-file", default="")
    p.add_argument("--rate", type=float, default=1500, help="rows/s offered")
    p.add_argument("--duration", type=float, default=0, help="seconds (0 = until stop)")
    p.add_argument("--batch-min", type=int, default=50)
    p.add_argument("--batch-max", type=int, default=400)
    p.add_argument("--mix", default="downloads:0.45,events:0.45,lp:0.10")
    p.add_argument("--unit-mix", default="us:0.8,ms:0.1,s:0.1")
    p.add_argument("--time-mix", default="fresh:0.70,recent:0.10,future:0.02,late:0.12,old:0.05,farfuture:0.01")
    p.add_argument("--late-window", type=int, default=7200)
    p.add_argument("--future-skew", type=int, default=300)
    p.add_argument("--concurrency", type=int, default=16)
    p.add_argument("--timeout", type=float, default=30)
    p.add_argument("--backoff-max", type=float, default=5)
    p.add_argument("--drain-timeout", type=float, default=300)
    p.add_argument("--no-failover", dest="failover", action="store_false")
    p.add_argument("--no-retry", action="store_true",
                   help="one attempt per batch (hammel-posthog-api): any non-2xx / transport error loses it")
    p.add_argument("--tick-s", type=float, default=0,
                   help="producer replicas: each writer, own connection, one batch per shared tick (0 = queue mode)")
    p.add_argument("--prop-pool", type=int, default=300, help="phsparse: property columns to draw from")
    p.add_argument("--props-min", type=int, default=20, help="phsparse: fewest properties in one batch")
    p.add_argument("--props-max", type=int, default=140, help="phsparse: most properties in one batch")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--tag", default="", help="ignored; lets the harness find its own processes")
    a = p.parse_args(argv)
    if a.seed is None:
        a.seed = int(time.time())
    lg = LoadGen(a)
    asyncio.run(lg.run_ticking() if a.tick_s > 0 else lg.run())


if __name__ == "__main__":
    sys.exit(main())
