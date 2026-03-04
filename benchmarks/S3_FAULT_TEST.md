# S3 Fault Injection Test

Validates Arc's data integrity under realistic S3 failure conditions. Simulates 7 days of production traffic through a fault-injecting proxy, then checks for missing records and duplicates.

## Known Bugs Detected

This test suite reproduces two production data integrity bugs:

### Bug 1: Data Loss (sync flush path)

When a periodic flush (buffer age timeout) fails to write to S3, the sync path at `arrow_writer.go:2038` logs "data preserved in WAL for recovery" but **never sets `hasFlushFailure`**. The WAL maintenance goroutine sees `hasFlushFailure == false`, purges WAL files after 30 seconds, and the data is permanently lost.

**Trigger:** `--s3-fault` with `timeout` or `reset` faults during off-peak hours (buffer fills slowly, ages out via periodic flush).

**Timeline:**
```
T=0s:   Data arrives → written to WAL + memory buffer
T=1s:   Buffer age exceeds MaxBufferAgeMS → periodic (sync) flush triggers
T=1s:   Buffer cleared from memory, S3 PutObject attempted → TIMEOUT
T=1s:   "preserved in WAL" logged, but hasFlushFailure NOT set
T=31s:  WAL purge runs, file is >30s old → DELETED. Data permanently lost.
```

### Bug 2: Data Duplication (async flush path + WAL replay)

When the buffer hits `MaxBufferSize`, the async flush path writes multi-hour data as separate Parquet files per hour. If S3 fails partway through (hour A succeeds, hour B fails), `hasFlushFailure` IS set, WAL recovery replays ALL entries including hour A's already-persisted data, creating duplicate Parquet files with new UUIDs.

**Trigger:** `--s3-fault --small-buffer` with `error500` faults at hour boundaries during burst traffic.

**Timeline:**
```
T=0:  1000 records arrive spanning hours 14:00 + 15:00, written to WAL
T=1:  Buffer hits MaxBufferSize → async flush, buffer cleared from memory
T=2:  flushPartitionedData writes hour 14:00 to S3 → SUCCESS (new UUID file)
T=3:  flushPartitionedData writes hour 15:00 to S3 → 503 FAILURE
T=3:  hasFlushFailure.Store(true) ← async path sets this
T=4:  Recovery goroutine sees hasFlushFailure=true
T=4:  Recovery replays ALL WAL entries (including hour 14:00 already on S3)
T=5:  Replayed data gets new UUID filename → DUPLICATE for hour 14:00
```

### Test Results (before fix)

**Data loss only** (default buffer, `--s3-fault`):
```
  Phase                                 Total    Dupes  Missing   Result
  AFTER WRITES (pre-compaction)    15,629,876        0   19,600     FAIL
  AFTER HOURLY COMPACTION          15,629,876        0   19,600     FAIL
  AFTER DAILY COMPACTION           15,629,876        0   19,600     FAIL
```

**Data loss + duplication** (small buffer, `--s3-fault --small-buffer`):
```
  Phase                                 Total    Dupes  Missing   Result
  AFTER WRITES (pre-compaction)    17,026,514 1,481,984  104,946     FAIL
  AFTER HOURLY COMPACTION          17,026,514 1,481,984  104,946     FAIL
  AFTER DAILY COMPACTION           17,026,514 1,481,984  104,946     FAIL
```

## Components

### `run_s3_fault_test.sh`

Orchestrator script that configures Arc for S3 via the fault proxy, starts all services, runs the integrity test, and collects results.

**Configuration:**

| Parameter | Value | Description |
|-----------|-------|-------------|
| SIM_MINUTES | 10080 | 7 simulated days |
| PACING | 0.05 | 50ms per sim-minute (~8.4 min per real day) |
| WORKERS | 15 | Concurrent write workers |
| FLUSH_WAIT | 20 | Seconds to wait before verification |
| ARC_INGEST_MAX_BUFFER_SIZE | 5000 | Small buffer to trigger async flush path |
| ARC_INGEST_MAX_BUFFER_AGE_MS | 500 | Short buffer age for frequent periodic flushes |

**Flow:**
1. Backs up `arc.toml`, configures S3 backend via proxy (port 9090), enables WAL, disables auth
2. Cleans MinIO test bucket and local WAL directory
3. Builds Arc, starts the fault proxy and Arc server with small buffer env vars
4. Runs `test_daily_integrity.py` with `--s3-fault --small-buffer`
5. Collects results to `/tmp/arc-s3fault-*.result`

### `s3_fault_proxy.py`

HTTP reverse proxy (aiohttp) between Arc and MinIO that injects failures on demand.

**Ports:** Arc → `localhost:9090` (proxy) → `localhost:9000` (MinIO)

**Fault modes** (controlled via `/tmp/s3proxy_mode` file):

| Mode | Behavior |
|------|----------|
| `normal` | Pass-through, no faults |
| `latency` | 200-800ms random delay per request |
| `timeout` | Hangs for 30s then returns 504 |
| `reset` | Closes connection immediately |
| `error500` | Returns HTTP 500 on PUT/POST (writes only) |

The proxy logs stats every 10 seconds (request count, fault count, current mode).

### `test_daily_integrity.py`

Core test harness. Simulates realistic 24-hour traffic patterns with peak/off-peak cycles and verifies data integrity.

**Traffic profile:**
- ~2M records per simulated day
- Peak hours (9-17): highest write rate
- Off-peak (2-5 AM): lowest write rate
- Burst mode: 5x multiplier at specific hours

**S3 fault schedules:**

Default (`--s3-fault`):
- 3:00 AM: latency → timeout (data loss trigger, sync flush path)
- 10:00 AM: latency → timeout → connection reset (data loss trigger)

Aggressive (`--s3-fault --small-buffer`): all of the above plus:
- 8:58 AM: error500 at hour-9 boundary during burst (duplication trigger)
- 10:58 AM: error500 at hour-10/11 boundary during burst (duplication trigger)
- 1:00 PM: 15-min error500 outage at peak (duplication trigger, matches Hetzner production)
- 2:58 PM: error500 at hour-15 boundary during burst (duplication trigger)
- 6:00 PM: brief error500 outage (duplication trigger)

**Verification checks:**
1. Total record count (expected vs actual)
2. Duplicate detection via `COUNT(DISTINCT seq_id)` (WAL replay / checkpoint races)
3. Missing record detection via seq range gaps (flush drops / WAL purge)
4. Per-minute breakdown accuracy
5. Per-day breakdown with duplicate counts per day

**Key flags:**

| Flag | Description |
|------|-------------|
| `--sim-minutes N` | Total simulated minutes (default 1440 = 1 day) |
| `--real-seconds-per-sim-min N` | Pacing factor (default 0.05) |
| `--workers N` | Concurrent batch writers (default 10) |
| `--compress zstd` | Enable zstd compression |
| `--flush-wait N` | Seconds before verification (default 15) |
| `--bursts` | Enable 5x traffic spikes at hour boundaries |
| `--compact-during-writes` | Trigger compaction every sim-hour |
| `--s3-fault` | Enable fault injection via proxy |
| `--small-buffer` | Aggressive faults at hour boundaries; requires Arc started with `ARC_INGEST_MAX_BUFFER_SIZE=5000 ARC_INGEST_MAX_BUFFER_AGE_MS=500` |

## Quick Start

### Test data loss only (default buffer)

```bash
# 1. Start MinIO
docker run -d --name minio-test -p 9000:9000 minio/minio server /data

# 2. Configure arc.toml: backend="s3", s3_endpoint="localhost:9090", auth=false, wal=true

# 3. Start proxy and Arc
echo "normal" > /tmp/s3proxy_mode
python3 benchmarks/s3_fault_proxy.py &
ARC_STORAGE_S3_ACCESS_KEY=minioadmin ARC_STORAGE_S3_SECRET_KEY=minioadmin ./arc &

# 4. Run test
python3 benchmarks/test_daily_integrity.py --sim-minutes 10080 \
    --real-seconds-per-sim-min 0.05 --workers 15 --compress zstd \
    --flush-wait 20 --bursts --compact-during-writes --s3-fault
```

### Test data loss + duplication (small buffer)

```bash
# Same as above but start Arc with small buffer:
ARC_STORAGE_S3_ACCESS_KEY=minioadmin ARC_STORAGE_S3_SECRET_KEY=minioadmin \
    ARC_INGEST_MAX_BUFFER_SIZE=5000 ARC_INGEST_MAX_BUFFER_AGE_MS=500 ./arc &

# Run with --small-buffer flag:
python3 benchmarks/test_daily_integrity.py --sim-minutes 10080 \
    --real-seconds-per-sim-min 0.05 --workers 15 --compress zstd \
    --flush-wait 20 --bursts --compact-during-writes --s3-fault --small-buffer
```

### Using the orchestrator script

```bash
./benchmarks/run_s3_fault_test.sh
```

This handles all configuration, startup, and teardown automatically.

## Prerequisites

- MinIO running on `localhost:9000` (Docker: `minio-test`)
- Arc built (`make build`)
- Python 3 with `aiohttp`, `msgpack`, `zstandard`

## Architecture

```
                    ┌─────────────────┐
                    │  test_daily_     │
                    │  integrity.py    │
                    │  (write workers) │
                    └────────┬────────┘
                             │ HTTP POST /api/v1/write/msgpack
                             ▼
                    ┌─────────────────┐
                    │    Arc Server    │
                    │   (port 8000)   │
                    │                 │
                    │  Buffer → WAL   │
                    │  Buffer → Flush │──► Parquet to S3
                    └────────┬────────┘
                             │ S3 PUT (flush)
                             ▼
                    ┌─────────────────┐
                    │  s3_fault_proxy  │    ◄── /tmp/s3proxy_mode
                    │   (port 9090)   │        (normal|latency|timeout|reset|error500)
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │     MinIO        │
                    │   (port 9000)   │
                    └─────────────────┘
```

