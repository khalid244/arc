"""Seed a back-dated backlog straight into MinIO so compaction and reorg have
real work inside a short scenario.

What ingest would have produced in earlier hours/days, with Arc's exact
object layout and parquet schema (time = TIMESTAMP WITH TIME ZONE, micros):

  <db>/<m>/YYYY/MM/DD/HH/<m>_<YYYYMMDD_HHMMSS>_<nanos>.parquet
      several small files in hours 2-5h ago   -> hourly compaction candidates
      several small files in hours of 2 days ago -> daily compaction candidates
  <db>/<m>_late/<m>_late_<YYYYMMDD_HHMMSS>_<nanos>.parquet
      file names 3-5h old (closed reorg buckets), rows days/hours old
                                               -> reorg drains them

Every seeded row is recorded in the ledger (source='seed', status='acked'),
so the verifier treats it exactly like ingested data.
"""
from __future__ import annotations

import random
import time
import uuid as uuidlib
from datetime import datetime, timedelta, timezone

import pyarrow as pa

from .ledger import Ledger
from .loadgen import APP_VERSIONS, COUNTRIES, EVENT_TYPES, OSES, RESPONSES, SITES, TAGS, event_columns
from .verify import _connect

MEAS = [("default", "downloads"), ("posthog", "events")]


def _rows(db: str, m: str, times_us: list[int], rnd: random.Random) -> tuple[pa.Table, list[str]]:
    n = len(times_us)
    uuids = [str(uuidlib.uuid4()) for _ in range(n)]  # os.urandom: must not collide with loadgen
    tcol = pa.array(times_us, type=pa.timestamp("us", tz="UTC"))
    if m == "downloads":
        cols = {
            "time": tcol, "uuid": uuids,
            "site": [rnd.choice(SITES) for _ in range(n)],
            "response": pa.array([rnd.choice(RESPONSES) for _ in range(n)], type=pa.int64()),
            "tag": [rnd.choice(TAGS) for _ in range(n)],
            "bytes": pa.array([rnd.randint(1000, 50_000_000) for _ in range(n)], type=pa.int64()),
            "duration_ms": [round(rnd.uniform(1, 30000), 2) for _ in range(n)],
            "country": [rnd.choice(COUNTRIES) for _ in range(n)],
        }
    else:
        ev = rnd.choice(EVENT_TYPES)
        cols = {
            "time": tcol, "uuid": uuids, "event": [ev] * n,
            "distinct_id": [f"user-{rnd.randint(1, 50000)}" for _ in range(n)],
            "os_name": [rnd.choice(OSES) for _ in range(n)],
            "app_version": [rnd.choice(APP_VERSIONS) for _ in range(n)],
        }
        for k, v in event_columns(ev, n, rnd).items():
            if v and isinstance(v[0], int):
                cols[k] = pa.array(v, type=pa.int64())
            else:
                cols[k] = v
    # Arc sorts by (sort keys..., time); time-sorted is enough for the seed.
    t = pa.table(cols)
    t = t.sort_by([("time", "ascending")])
    return t, t.column("uuid").to_pylist()


def _name_ts(dt: datetime) -> str:
    return dt.strftime("%Y%m%d_%H%M%S")


def seed_backlog(bucket: str, minio_endpoint: str, ledger: Ledger, seed: int = 7,
                 rows_per_file: int = 300, hourly_files: int = 6, hourly_hours=(2, 3, 4),
                 daily_files: int = 3, daily_hours=(0, 6, 12, 18), late_files: int = 8) -> dict:
    rnd = random.Random(seed)
    con = _connect(minio_endpoint)
    now = datetime.now(timezone.utc)
    bid = ledger.next_batch_id()
    batches = []
    stats = {"objects": 0, "rows": 0, "hourly_partitions": [], "daily_partitions": [], "late_objects": 0}

    def put(db, m, key, times):
        nonlocal bid
        tbl, uuids = _rows(db, m, times, rnd)
        con.register("seed_t", tbl)
        con.execute(f"COPY (SELECT * FROM seed_t) TO 's3://{bucket}/{key}' (FORMAT parquet, COMPRESSION zstd)")
        con.unregister("seed_t")
        batches.append({"batch_id": bid, "db": db, "measurement": m, "fmt": "seed", "unit": "us",
                        "n_rows": len(uuids), "first_target": "seed", "ack_target": "seed", "attempts": 1,
                        "ambiguous_attempts": 0, "first_sent": time.time(), "acked_at": time.time(),
                        "status": "acked", "last_error": None, "source": "seed",
                        "rows": [(u, db, m, t, "seed") for u, t in zip(uuids, sorted(times))]})
        bid += 1
        stats["objects"] += 1
        stats["rows"] += len(uuids)

    for db, m in MEAS:
        # Hourly candidates: hours 2-4h ago, `hourly_files` small files each.
        for h in hourly_hours:
            hour = (now - timedelta(hours=h)).replace(minute=0, second=0, microsecond=0)
            for i in range(hourly_files):
                written = hour + timedelta(minutes=5 + i * 7, seconds=rnd.randint(0, 59))
                times = [int((hour + timedelta(seconds=rnd.uniform(0, 3599))).timestamp() * 1e6)
                         for _ in range(rows_per_file)]
                key = (f"{db}/{m}/{hour:%Y/%m/%d/%H}/{m}_{_name_ts(written)}_"
                       f"{rnd.randint(0, 999_999_999):09d}.parquet")
                put(db, m, key, times)
            stats["hourly_partitions"].append(f"{db}/{m}/{hour:%Y/%m/%d/%H}")
        # Daily candidates: a day 2 days ago, a few hours with a few files each.
        day = (now - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        for hh in daily_hours:
            hour = day + timedelta(hours=hh)
            for i in range(daily_files):
                written = hour + timedelta(minutes=10 + i * 9)
                times = [int((hour + timedelta(seconds=rnd.uniform(0, 3599))).timestamp() * 1e6)
                         for _ in range(rows_per_file)]
                key = (f"{db}/{m}/{hour:%Y/%m/%d/%H}/{m}_{_name_ts(written)}_"
                       f"{rnd.randint(0, 999_999_999):09d}.parquet")
                put(db, m, key, times)
        stats["daily_partitions"].append(f"{db}/{m}/{day:%Y/%m/%d}")
        # Late sidecar: names 3-5h old (closed buckets for min_age 7200), rows hours..days old.
        for i in range(late_files):
            written = now - timedelta(hours=3 + (i % 3), minutes=rnd.randint(0, 50))
            times = [int((now - timedelta(seconds=rnd.uniform(3 * 3600, 3 * 86400))).timestamp() * 1e6)
                     for _ in range(rows_per_file)]
            key = (f"{db}/{m}_late/{m}_late_{_name_ts(written)}_{rnd.randint(0, 999_999_999):09d}.parquet")
            put(db, m, key, times)
            stats["late_objects"] += 1
    ledger.record(batches)
    con.close()
    return stats


def object_counts(bucket: str, minio_endpoint: str, prefixes: list[str]) -> dict:
    con = _connect(minio_endpoint)
    out = {}
    for p in prefixes:
        try:
            out[p] = con.execute(f"SELECT count(*) FROM glob('s3://{bucket}/{p}/**')").fetchone()[0]
        except Exception:  # noqa: BLE001
            out[p] = 0
    con.close()
    return out
