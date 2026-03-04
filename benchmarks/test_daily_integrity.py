#!/usr/bin/env python3
"""
Daily write integrity test for Arc.

Simulates realistic production conditions:
  - Peak/off-peak traffic patterns (~2M rows/day)
  - Concurrent hourly compaction during writes (like cron in production)
  - Burst traffic spikes that pressure WAL buffer
  - Optional storage outage simulation
  - S3 fault injection (latency, timeout, reset, 503 errors)

Every record gets a unique sequence ID. After writing, queries Arc to verify:
  - Total count matches expected
  - No duplicate seq_ids
  - No missing seq_ids
  - Per-minute counts match expected

Detects two production bugs:
  1. DATA LOSS: sync flush path never sets hasFlushFailure, WAL purged before recovery
  2. DUPLICATION: async flush partial multi-hour write + WAL recovery replays all

Usage:
  # Full day simulation (24 min at 1s/sim-minute pacing)
  python benchmarks/test_daily_integrity.py

  # 7 days, fast pacing, with S3 faults — detects data loss
  python benchmarks/test_daily_integrity.py --sim-minutes 10080 --real-seconds-per-sim-min 0.05 \\
      --compact-during-writes --bursts --s3-fault

  # 7 days with small buffer — detects data loss AND duplication
  # Start Arc with: ARC_INGEST_MAX_BUFFER_SIZE=5000 ARC_INGEST_MAX_BUFFER_AGE_MS=500 ./arc
  python benchmarks/test_daily_integrity.py --sim-minutes 10080 --real-seconds-per-sim-min 0.05 \\
      --compact-during-writes --bursts --s3-fault --small-buffer

  # With storage outage simulation (blocks data dir for 5s at peak)
  python benchmarks/test_daily_integrity.py --sim-minutes 1440 --storage-outage

  # Quick smoke test
  python benchmarks/test_daily_integrity.py --sim-minutes 10 --workers 5
"""
import msgpack
import aiohttp
import asyncio
import time
import random
import argparse
import json
import os
import sys
import subprocess
import signal
from collections import defaultdict

S3_PROXY_CONTROL = "/tmp/s3proxy_mode"

try:
    import zstandard as zstd
    ZSTD_AVAILABLE = True
except ImportError:
    ZSTD_AVAILABLE = False

# ---------------------------------------------------------------------------
# Traffic profile: records-per-minute for each simulated hour (0-23)
# Peak at hours 9-17, trough at 2-5 AM
# Totals to ~2M records/day
# ---------------------------------------------------------------------------
HOURLY_PROFILE = {
    0: 700, 1: 500, 2: 350, 3: 300, 4: 300, 5: 400,
    6: 800, 7: 1200, 8: 1800, 9: 2500, 10: 2800, 11: 2900,
    12: 2600, 13: 2800, 14: 2900, 15: 2700, 16: 2400, 17: 2000,
    18: 1600, 19: 1300, 20: 1100, 21: 900, 22: 800, 23: 750,
}

# Burst multiplier: at certain minutes, traffic spikes 5x to pressure WAL buffer
BURST_MINUTES = {60, 120, 540, 600, 660, 900, 1380}  # start of hours 1,2,9,10,11,15,23

DATABASE = "integrity_test"
MEASUREMENT = "sensor_data"


def total_expected_records(sim_minutes: int, enable_bursts: bool) -> dict:
    """Pre-calculate expected records per simulated minute."""
    rng = random.Random(42)
    result = {}
    for m in range(sim_minutes):
        hour = (m // 60) % 24
        base = HOURLY_PROFILE[hour]
        jitter = rng.uniform(0.9, 1.1)
        count = max(1, int(base * jitter))
        # Apply burst at specific minutes (repeat pattern every day)
        if enable_bursts and (m % 1440) in BURST_MINUTES:
            count = int(count * 5)
        result[m] = count
    return result


class IntegrityTester:
    def __init__(self, url, query_url, compact_url, token, compress, workers):
        self.url = url
        self.query_url = query_url
        self.compact_url = compact_url
        self.token = token
        self.compress = compress
        self.workers = workers
        # Tracking
        self.seq_counter = 0
        self.seq_lock = asyncio.Lock()
        self.sent_per_minute = defaultdict(int)
        self.acked_per_minute = defaultdict(int)
        self.errors = 0
        self.error_details = []
        self.total_acked = 0
        self.total_attempted = 0
        self.compactions_triggered = 0
        self.storage_outages = 0

    def _compress(self, data: bytes) -> bytes:
        if self.compress == "zstd":
            return zstd.ZstdCompressor(level=3).compress(data)
        return data

    def _headers(self) -> dict:
        h = {
            "Content-Type": "application/msgpack",
            "x-arc-database": DATABASE,
        }
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        if self.compress == "zstd":
            h["Content-Encoding"] = "zstd"
        return h

    async def allocate_seq_range(self, count: int) -> int:
        async with self.seq_lock:
            start = self.seq_counter
            self.seq_counter += count
            return start

    async def send_batch(self, session, sim_minute: int, seq_start: int, count: int, base_time_us: int):
        payload = {
            "m": MEASUREMENT,
            "columns": {
                "time": [base_time_us + (seq_start + i) for i in range(count)],
                "seq_id": list(range(seq_start, seq_start + count)),
                "sim_minute": [sim_minute] * count,
                "host": [f"host-{(seq_start + i) % 50:03d}" for i in range(count)],
                "temperature": [20.0 + random.random() * 30 for _ in range(count)],
                "pressure": [950.0 + random.random() * 100 for _ in range(count)],
                "humidity": [30.0 + random.random() * 60 for _ in range(count)],
            }
        }
        data = self._compress(msgpack.packb(payload))

        try:
            async with session.post(self.url, data=data, headers=self._headers()) as resp:
                if resp.status == 204:
                    self.acked_per_minute[sim_minute] += count
                    self.total_acked += count
                else:
                    self.errors += 1
                    text = await resp.text()
                    if len(self.error_details) < 10:
                        self.error_details.append(f"[min={sim_minute}] HTTP {resp.status}: {text[:200]}")
        except Exception as e:
            self.errors += 1
            if len(self.error_details) < 10:
                self.error_details.append(f"[min={sim_minute}] {str(e)[:200]}")

    async def write_minute(self, session, sim_minute: int, target_count: int, base_time_us: int):
        batch_size = min(1000, target_count)
        remaining = target_count
        tasks = []

        while remaining > 0:
            count = min(batch_size, remaining)
            seq_start = await self.allocate_seq_range(count)
            self.sent_per_minute[sim_minute] += count
            self.total_attempted += count
            tasks.append(self.send_batch(session, sim_minute, seq_start, count, base_time_us))
            remaining -= count

        sem = asyncio.Semaphore(self.workers)
        async def bounded(coro):
            async with sem:
                await coro
        await asyncio.gather(*[bounded(t) for t in tasks])

    async def trigger_compaction(self, session, tier: str):
        """Fire-and-forget compaction trigger (non-blocking, like cron)."""
        try:
            url = f"{self.compact_url}?database={DATABASE}&tier={tier}"
            headers = {}
            if self.token:
                headers["Authorization"] = f"Bearer {self.token}"
            async with session.post(url, headers=headers) as resp:
                self.compactions_triggered += 1
                if resp.status != 200:
                    text = await resp.text()
                    if len(self.error_details) < 10:
                        self.error_details.append(f"[compact] HTTP {resp.status}: {text[:100]}")
        except Exception as e:
            if len(self.error_details) < 10:
                self.error_details.append(f"[compact] {str(e)[:100]}")

    async def simulate_storage_outage(self, data_path: str, duration_sec: int = 5):
        """Make data directory read-only briefly, simulating S3/storage outage."""
        target = os.path.join(data_path, DATABASE)
        if not os.path.exists(target):
            return
        self.storage_outages += 1
        print(f"    ** STORAGE OUTAGE: making {target} read-only for {duration_sec}s **")
        try:
            subprocess.run(["chmod", "-R", "a-w", target], check=True)
            await asyncio.sleep(duration_sec)
        finally:
            subprocess.run(["chmod", "-R", "u+w", target], check=True)
            print(f"    ** STORAGE RESTORED **")

    async def query_arc(self, session, sql: str, retries: int = 3) -> list:
        payload = json.dumps({"sql": sql}).encode()
        headers = {
            "Content-Type": "application/json",
            "x-arc-database": DATABASE,
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        last_err = None
        for attempt in range(retries):
            try:
                async with session.post(self.query_url, data=payload, headers=headers) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        last_err = RuntimeError(f"Query failed ({resp.status}): {text[:500]}")
                        if attempt < retries - 1:
                            await asyncio.sleep(3)
                            continue
                        raise last_err
                    body = await resp.json()
                    if not body.get("success"):
                        last_err = RuntimeError(f"Query error: {body.get('error', 'unknown')}")
                        if attempt < retries - 1:
                            await asyncio.sleep(3)
                            continue
                        raise last_err
                    columns = body.get("columns", [])
                    data = body.get("data", [])
                    return [dict(zip(columns, row)) for row in data]
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_err = RuntimeError(f"Query connection error: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(3)
                    continue
                raise last_err
        raise last_err

    async def verify(self, session, label: str) -> dict:
        """Query Arc and verify data integrity."""
        print(f"\n{'='*70}")
        print(f"INTEGRITY REPORT: {label}")
        print(f"{'='*70}")

        report = {"label": label}

        # 1. Total count
        result = await self.query_arc(
            session,
            f"SELECT COUNT(*) as cnt FROM {MEASUREMENT}"
        )
        actual_total = result[0]["cnt"] if result else 0
        report["expected_total"] = self.total_acked
        report["actual_total"] = actual_total
        report["total_match"] = actual_total == self.total_acked

        ok = report["total_match"]
        sym = "OK" if ok else "FAIL"
        diff = actual_total - self.total_acked
        diff_str = f"  (diff={diff:+,})" if diff != 0 else ""
        print(f"  [{sym:>4}] Total records: expected={self.total_acked:,}  actual={actual_total:,}{diff_str}")

        # 2. Duplicates
        result = await self.query_arc(
            session,
            f"SELECT COUNT(*) as cnt, COUNT(DISTINCT seq_id) as distinct_cnt FROM {MEASUREMENT}"
        )
        total_rows = result[0]["cnt"]
        distinct_rows = result[0]["distinct_cnt"]
        duplicates = total_rows - distinct_rows
        report["duplicates"] = duplicates

        ok = duplicates == 0
        sym = "OK" if ok else "FAIL"
        print(f"  [{sym:>4}] Duplicates: {duplicates:,}")

        if duplicates > 0:
            result = await self.query_arc(
                session,
                f"""SELECT seq_id, COUNT(*) as cnt
                    FROM {MEASUREMENT}
                    GROUP BY seq_id HAVING COUNT(*) > 1
                    ORDER BY cnt DESC LIMIT 20"""
            )
            report["duplicate_samples"] = result
            for row in result[:10]:
                print(f"         seq_id={row['seq_id']}  count={row['cnt']}")

        # 3. Gaps
        result = await self.query_arc(
            session,
            f"SELECT MIN(seq_id) as min_seq, MAX(seq_id) as max_seq FROM {MEASUREMENT}"
        )
        min_seq = result[0]["min_seq"]
        max_seq = result[0]["max_seq"]
        expected_range = max_seq - min_seq + 1 if max_seq is not None else 0
        missing = expected_range - distinct_rows
        report["seq_range"] = {"min": min_seq, "max": max_seq}
        report["missing_records"] = missing

        ok = missing == 0
        sym = "OK" if ok else "FAIL"
        print(f"  [{sym:>4}] Missing records: {missing:,}  (seq range {min_seq}..{max_seq})")

        # 4. Per-day summary
        result = await self.query_arc(
            session,
            f"""SELECT CAST(sim_minute / 1440 AS INTEGER) as day,
                       COUNT(*) as cnt,
                       COUNT(DISTINCT seq_id) as dist
                FROM {MEASUREMENT}
                GROUP BY day ORDER BY day"""
        )
        print(f"\n  Per-day breakdown:")
        for row in result:
            day, cnt, dist = row["day"], row["cnt"], row["dist"]
            dupes = cnt - dist
            flag = f"  ** {dupes:,} DUPES **" if dupes > 0 else ""
            print(f"    Day {day}: {cnt:>10,} rows  ({dist:>10,} distinct){flag}")

        # 5. Per-minute mismatches
        result = await self.query_arc(
            session,
            f"""SELECT sim_minute, COUNT(*) as cnt
                FROM {MEASUREMENT}
                GROUP BY sim_minute
                ORDER BY sim_minute"""
        )
        actual_per_minute = {row["sim_minute"]: row["cnt"] for row in result}

        minute_mismatches = []
        for m, expected in sorted(self.acked_per_minute.items()):
            actual = actual_per_minute.get(m, 0)
            if actual != expected:
                minute_mismatches.append({
                    "minute": m, "expected": expected, "actual": actual,
                    "diff": actual - expected,
                })

        report["minute_mismatches"] = minute_mismatches
        report["minutes_checked"] = len(self.acked_per_minute)
        report["minutes_ok"] = len(self.acked_per_minute) - len(minute_mismatches)

        ok = len(minute_mismatches) == 0
        sym = "OK" if ok else "FAIL"
        print(f"\n  [{sym:>4}] Per-minute: {report['minutes_ok']}/{report['minutes_checked']} minutes match")
        if minute_mismatches:
            for mm in minute_mismatches[:20]:
                direction = "EXTRA" if mm["diff"] > 0 else "MISSING"
                print(f"         min={mm['minute']:>5}  expected={mm['expected']:>5}  actual={mm['actual']:>5}  {direction} {abs(mm['diff'])}")
            if len(minute_mismatches) > 20:
                print(f"         ... and {len(minute_mismatches) - 20} more")

        # Final verdict
        all_ok = (
            report["total_match"]
            and duplicates == 0
            and missing == 0
            and len(minute_mismatches) == 0
        )
        report["pass"] = all_ok

        print(f"\n  {'PASS' if all_ok else '** FAIL **'}")
        if not all_ok:
            if duplicates > 0:
                print(f"    -> {duplicates:,} duplicate records (WAL replay / checkpoint race?)")
            if missing > 0:
                print(f"    -> {missing:,} missing records (WAL drop / flush queue overflow?)")
            if minute_mismatches:
                extra = sum(1 for m in minute_mismatches if m["diff"] > 0)
                miss = sum(1 for m in minute_mismatches if m["diff"] < 0)
                print(f"    -> {len(minute_mismatches)} minutes wrong ({extra} extra, {miss} missing)")
        print(f"{'='*70}")

        return report


def print_traffic_plan(plan: dict, sim_minutes: int):
    total = sum(plan.values())
    print(f"\nTraffic plan: {sim_minutes} simulated minutes, {total:,} total records")
    print(f"{'Hour':>6} {'Avg RPM':>10} {'Minutes':>8} {'Subtotal':>10}")
    print("-" * 40)
    by_hour = defaultdict(list)
    for m, cnt in plan.items():
        by_hour[(m // 60) % 24].append(cnt)
    for hour in sorted(by_hour):
        vals = by_hour[hour]
        avg = sum(vals) / len(vals)
        sub = sum(vals)
        bar = "#" * int(avg / 100)
        print(f"{hour:>6} {avg:>10.0f} {len(vals):>8} {sub:>10,}  {bar}")
    print("-" * 40)
    print(f"{'TOTAL':>6} {'':>10} {sim_minutes:>8} {total:>10,}")


async def run(args):
    random.seed(42)

    scheme = "https" if args.tls else "http"
    base_url = f"{scheme}://{args.host}:{args.port}"
    write_url = f"{base_url}/api/v1/write/msgpack"
    query_url = f"{base_url}/api/v1/query"
    compact_url = f"{base_url}/api/v1/compaction/trigger"
    token = args.token or os.environ.get("ARC_TOKEN")

    if args.compress == "zstd" and not ZSTD_AVAILABLE:
        print("ERROR: zstd requested but 'zstandard' not installed. pip install zstandard")
        sys.exit(1)

    plan = total_expected_records(args.sim_minutes, args.bursts)
    total_records = sum(plan.values())

    print("=" * 70)
    print("ARC DAILY WRITE INTEGRITY TEST")
    print("=" * 70)
    print(f"Target:       {write_url}")
    print(f"Database:     {DATABASE}")
    print(f"Measurement:  {MEASUREMENT}")
    print(f"Workers:      {args.workers}")
    print(f"Compression:  {args.compress}")
    print(f"Sim minutes:  {args.sim_minutes} ({args.sim_minutes / 60:.1f} hours)")
    print(f"Pacing:       {args.real_seconds_per_sim_min}s real per sim-minute")
    print(f"Bursts:       {'ON (5x at hour boundaries)' if args.bursts else 'OFF'}")
    print(f"Compact:      {'during writes (every sim-hour)' if args.compact_during_writes else 'after writes only'}")
    if args.s3_fault and args.small_buffer:
        print(f"S3 faults:    ON (aggressive: error500 at hour boundaries + latency/timeout/reset)")
        print(f"Small buffer: ON (requires ARC_INGEST_MAX_BUFFER_SIZE=5000)")
    elif args.s3_fault:
        print(f"S3 faults:    ON (latency/timeout/reset via proxy)")
    else:
        print(f"S3 faults:    OFF")
    print(f"Total plan:   {total_records:,} records")
    if token:
        print(f"Auth token:   {token[:8]}...")

    if args.show_plan:
        print_traffic_plan(plan, args.sim_minutes)

    print()

    tester = IntegrityTester(write_url, query_url, compact_url, token, args.compress, args.workers)

    base_day_us = int(time.time()) // 86400 * 86400 * 1_000_000

    ssl_ctx = None
    if args.tls:
        import ssl
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

    connector = aiohttp.TCPConnector(
        limit=args.workers + 20,
        limit_per_host=args.workers + 20,
        force_close=False,
        keepalive_timeout=30,
        ssl=ssl_ctx,
    )
    timeout = aiohttp.ClientTimeout(total=300, connect=10, sock_read=120)

    # S3 fault injection schedule (per-day minute offsets and modes)
    # Each entry: (minute_offset_in_day, mode, duration_in_sim_minutes)
    s3_fault_schedule = []
    if args.s3_fault:
        if args.small_buffer:
            # Aggressive schedule: targets hour boundaries during bursts
            # to trigger async flush partial writes → duplication
            # Also includes original faults for data loss detection
            # BURST_MINUTES = {60, 120, 540, 600, 660, 900, 1380}
            s3_fault_schedule = [
                # Night maintenance window — data loss trigger (sync path)
                (180, "latency", 10),    # 3:00 AM: slow for 10 min
                (190, "timeout", 3),     # 3:10 AM: timeout for 3 min
                (193, "normal", 0),      # 3:13 AM: recovery
                # Prolonged outage — tests WAL preservation during buffer cap overflow
                (300, "error500", 60),   # 5:00 AM: 60-min sustained S3 outage
                (360, "normal", 0),      # 6:00 AM: recovery
                # Hour 9 boundary — duplication trigger
                # Burst at min 540, buffer has hour-8 + hour-9 data
                (538, "error500", 4),    # 8:58 AM: 503s spanning hour boundary
                (542, "normal", 0),      # 9:02 AM: recovery
                # Peak hour degradation — data loss trigger
                (600, "latency", 20),    # 10:00 AM: S3 slow for 20 min
                (620, "timeout", 5),     # 10:20 AM: S3 timeout for 5 min
                (625, "reset", 5),       # 10:25 AM: connection resets for 5 min
                (630, "normal", 0),      # 10:30 AM: recovery
                # Hour 11 boundary — duplication trigger
                # Burst at min 660, buffer has hour-10 + hour-11 data
                (658, "error500", 4),    # 10:58 AM: 503s spanning hour boundary
                (662, "normal", 0),      # 11:02 AM: recovery
                # Extended outage at peak — duplication trigger (like Hetzner)
                (780, "error500", 15),   # 1:00 PM: 15-min 503 outage
                (795, "normal", 0),      # 1:15 PM: recovery
                # Hour 15 boundary — duplication trigger
                # Burst at min 900, buffer has hour-14 + hour-15 data
                (898, "error500", 4),    # 2:58 PM: 503s spanning hour boundary
                (902, "normal", 0),      # 3:02 PM: recovery
                # Evening fault
                (1080, "error500", 5),   # 6:00 PM: brief 503 outage
                (1085, "normal", 0),     # 6:05 PM: recovery
            ]
        else:
            s3_fault_schedule = [
                # Peak hour faults — simulates real S3 degradation
                (600, "latency", 20),    # 10:00 AM: S3 slow for 20 min
                (620, "timeout", 5),     # 10:20 AM: S3 timeout for 5 min
                (625, "reset", 5),       # 10:25 AM: connection resets for 5 min
                (630, "normal", 0),      # 10:30 AM: recovery
                # Night maintenance window
                (180, "latency", 10),    # 3:00 AM: slow for 10 min
                (190, "timeout", 3),     # 3:10 AM: timeout for 3 min
                (193, "normal", 0),      # 3:13 AM: recovery
            ]

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # --- Phase 1: Write ---
        print("PHASE 1: Writing data")
        print("-" * 70)
        write_start = time.time()
        current_s3_mode = "normal"

        for sim_min in range(args.sim_minutes):
            min_start = time.time()
            target = plan[sim_min]
            hour = (sim_min // 60) % 24
            day_minute = sim_min % 1440

            minute_base_us = base_day_us + sim_min * 60 * 1_000_000

            # Trigger hourly compaction during writes (like production cron)
            if args.compact_during_writes and sim_min > 0 and sim_min % 60 == 5:
                asyncio.create_task(tester.trigger_compaction(session, "hourly"))

            # S3 fault injection — check schedule
            if args.s3_fault:
                for offset, mode, duration in s3_fault_schedule:
                    if day_minute == offset:
                        current_s3_mode = mode
                        with open(S3_PROXY_CONTROL, "w") as f:
                            f.write(mode)
                        sym = "!!" if mode != "normal" else "OK"
                        print(f"    [{sym}] S3 FAULT: mode={mode} at sim_min={sim_min} (day_min={day_minute})")
                        sys.stdout.flush()

            await tester.write_minute(session, sim_min, target, minute_base_us)

            elapsed_min = time.time() - min_start
            total_elapsed = time.time() - write_start

            if sim_min % 100 == 0 or sim_min == args.sim_minutes - 1:
                pct = (sim_min + 1) / args.sim_minutes * 100
                rps = tester.total_acked / total_elapsed if total_elapsed > 0 else 0
                burst = " BURST" if plan[sim_min] > HOURLY_PROFILE.get(hour, 0) * 1.2 else ""
                print(
                    f"  sim_min={sim_min:>5} hour={hour:>2} "
                    f"wrote={target:>5} "
                    f"total={tester.total_acked:>10,} / {total_records:,} "
                    f"({pct:5.1f}%) "
                    f"errs={tester.errors} "
                    f"rps={rps:,.0f} "
                    f"[{elapsed_min*1000:.0f}ms]"
                    f"{burst}"
                )

            wait = args.real_seconds_per_sim_min - (time.time() - min_start)
            if wait > 0:
                await asyncio.sleep(wait)

        # Ensure S3 proxy is back to normal before verification
        if args.s3_fault:
            with open(S3_PROXY_CONTROL, "w") as f:
                f.write("normal")
            print("    [OK] S3 FAULT: reset to normal for verification")

        write_elapsed = time.time() - write_start

        print()
        print(f"Write complete: {tester.total_acked:,} acked, {tester.errors} errors in {write_elapsed:.1f}s")
        print(f"Compactions triggered during writes: {tester.compactions_triggered}")
        print(f"Storage outages simulated: {tester.storage_outages}")
        if tester.error_details:
            print("Error samples:")
            for e in tester.error_details:
                print(f"  - {e}")

        # --- Phase 2: Wait for flush ---
        flush_wait = args.flush_wait
        print(f"\nPHASE 2: Waiting {flush_wait}s for Arc to flush buffers...")
        await asyncio.sleep(flush_wait)

        # --- Phase 3: Verify before compaction ---
        report_pre = await tester.verify(session, "AFTER WRITES (pre-compaction)")

        # --- Phase 4: Hourly compaction ---
        print("\nPHASE 3: Triggering HOURLY compaction...")
        await tester.trigger_compaction(session, "hourly")
        # Wait for compaction to finish
        for i in range(120):
            await asyncio.sleep(1)
            try:
                async with session.get(f"{base_url}/api/v1/compaction/status") as resp:
                    body = await resp.json()
                    active = body.get("manager", {}).get("active_jobs", 0)
                    if active == 0:
                        print(f"  Hourly compaction done (waited {i+1}s)")
                        break
            except Exception:
                pass
        await asyncio.sleep(3)

        report_hourly = await tester.verify(session, "AFTER HOURLY COMPACTION")

        # --- Phase 5: Daily compaction ---
        print("\nPHASE 4: Triggering DAILY compaction...")
        await tester.trigger_compaction(session, "daily")
        for i in range(120):
            await asyncio.sleep(1)
            try:
                async with session.get(f"{base_url}/api/v1/compaction/status") as resp:
                    body = await resp.json()
                    active = body.get("manager", {}).get("active_jobs", 0)
                    if active == 0:
                        print(f"  Daily compaction done (waited {i+1}s)")
                        break
            except Exception:
                pass
        await asyncio.sleep(3)

        report_daily = await tester.verify(session, "AFTER DAILY COMPACTION")

        # --- Summary ---
        print(f"\n{'='*70}")
        print("SUMMARY")
        print(f"{'='*70}")
        print(f"  {'Phase':<30} {'Total':>12} {'Dupes':>8} {'Missing':>8} {'Result':>8}")
        print(f"  {'-'*30} {'-'*12} {'-'*8} {'-'*8} {'-'*8}")
        for r in [report_pre, report_hourly, report_daily]:
            lbl = r["label"][:30]
            print(f"  {lbl:<30} {r['actual_total']:>12,} {r['duplicates']:>8,} {r['missing_records']:>8,} {'PASS' if r['pass'] else 'FAIL':>8}")
        print(f"{'='*70}")

        all_pass = all(r["pass"] for r in [report_pre, report_hourly, report_daily])
        return 0 if all_pass else 1


def main():
    parser = argparse.ArgumentParser(
        description="Arc daily write integrity test — detects missing data and duplicates"
    )
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--tls", action="store_true")
    parser.add_argument("--token", type=str, default="")
    parser.add_argument("--compress", default="none", choices=["none", "zstd"])
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--sim-minutes", type=int, default=1440)
    parser.add_argument("--real-seconds-per-sim-min", type=float, default=1.0)
    parser.add_argument("--flush-wait", type=int, default=15)
    parser.add_argument("--show-plan", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bursts", action="store_true",
                        help="Enable 5x traffic bursts at hour boundaries (pressures WAL buffer)")
    parser.add_argument("--compact-during-writes", action="store_true",
                        help="Trigger hourly compaction every sim-hour during writes (like production cron)")
    parser.add_argument("--s3-fault", action="store_true",
                        help="Inject S3 faults via proxy (latency/timeout/reset at peak hours)")
    parser.add_argument("--small-buffer", action="store_true",
                        help="Use aggressive fault schedule targeting hour boundaries during bursts "
                             "to reproduce duplication bugs. Requires Arc started with: "
                             "ARC_INGEST_MAX_BUFFER_SIZE=5000 ARC_INGEST_MAX_BUFFER_AGE_MS=500")
    parser.add_argument("--storage-outage", action="store_true",
                        help="Simulate storage outage (read-only data dir) at peak on days 2 and 5")
    parser.add_argument("--data-path", type=str, default="./data/arc",
                        help="Path to Arc data directory (for storage outage sim)")
    args = parser.parse_args()

    if args.dry_run:
        random.seed(42)
        plan = total_expected_records(args.sim_minutes, args.bursts)
        print_traffic_plan(plan, args.sim_minutes)
        sys.exit(0)

    exit_code = asyncio.run(run(args))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
