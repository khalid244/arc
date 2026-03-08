#!/bin/bash
# =============================================================================
# Data Duplication Reproduction Test
#
# Root cause from production logs (hammel-arc-ingest-logs2.txt):
#   1. "downloads" measurement: multi-hour backfill data → partial flush failures
#   2. "devices" measurement: current-hour data → always flushes successfully
#   3. Each successful devices flush decrements pendingFlushFailures counter
#   4. Counter hits 0 → WAL maintenance purges WAL files
#   5. But downloads partial flush already wrote some partitions to S3
#   6. Restored unwritten data retries, eventually succeeds → writes to S3 AGAIN
#   7. The already-written partitions from step 5 are now DUPLICATED
#
# Evidence from production:
#   08:28:08  Partial flush downloads (written=8787, unwritten=6223)  → counter++
#   08:28:11  Partial flush downloads (written=178, unwritten=19822)  → counter++
#   08:28:15  Partial flush downloads (written=5000, unwritten=30000) → counter++
#   08:28:37  Devices flush succeeds                                  → counter--
#   08:28:40  WAL purge: deleted=5 ← SHOULD NOT HAPPEN
# =============================================================================
set -euo pipefail

ARC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ARC_DIR"

ARC_HOST="localhost"
ARC_PORT=8000
ARC_URL="http://${ARC_HOST}:${ARC_PORT}"
MAX_RUNS=30
DATABASE="integrity_test"

cleanup() {
    pkill -9 -f "./arc" 2>/dev/null || true
    pkill -f "s3_fault_proxy" 2>/dev/null || true
    echo "normal" > /tmp/s3proxy_mode 2>/dev/null || true
    sleep 1
}

wait_healthy() {
    for i in $(seq 1 20); do
        if curl -s "${ARC_URL}/health" > /dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "ERROR: Arc failed to become healthy"
    return 1
}

query_arc() {
    local SQL="$1"
    curl -s -X POST "${ARC_URL}/api/v1/query" \
        -H "Content-Type: application/json" \
        -H "x-arc-database: ${DATABASE}" \
        -d "{\"sql\": \"${SQL}\"}"
}

run_test() {
    local RUN="$1"
    local LOG="/tmp/arc-duptest-run${RUN}.log"

    echo ""
    echo "============================================================"
    echo " DUPLICATION TEST  (Run #${RUN})"
    echo "============================================================"

    cleanup

    # Configure for S3 via proxy
    cp arc.toml.bak.fault arc.toml 2>/dev/null || true
    python3 -c "
import re
with open('arc.toml') as f:
    content = f.read()
content = re.sub(r'(^\[storage\]\s*\nbackend\s*=\s*)\"local\"', r'\1\"s3\"', content, flags=re.MULTILINE)
content = re.sub(r's3_endpoint\s*=\s*\"[^\"]*\"', 's3_endpoint = \"localhost:9090\"', content, count=1)
content = re.sub(r'(\[auth\]\s*\n\s*enabled\s*=\s*)(true|false)', r'\1false', content)
content = re.sub(r'(\[wal\]\s*\n\s*enabled\s*=\s*)(true|false)', r'\1true', content)
with open('arc.toml', 'w') as f:
    f.write(content)
"

    # Clean old test data
    docker exec minio-test rm -rf /data/arc-test/integrity_test 2>/dev/null || true
    rm -rf data/wal 2>/dev/null || true

    # Start fault proxy
    python3 benchmarks/s3_fault_proxy.py > "/tmp/s3proxy-duptest-run${RUN}.log" 2>&1 &
    PROXY_PID=$!
    sleep 1

    # Start Arc:
    # - buffer=5000, age=10s (frequent flushes)
    # - WAL maintenance every 10s (frequent purge checks)
    # - safeAge = 10s * 3 = 30s
    echo "  Starting Arc..."
    ARC_STORAGE_S3_ACCESS_KEY=minioadmin ARC_STORAGE_S3_SECRET_KEY=minioadmin \
        ARC_INGEST_MAX_BUFFER_SIZE=5000 ARC_INGEST_MAX_BUFFER_AGE_MS=10000 \
        ARC_WAL_RECOVERY_INTERVAL_SECONDS=10 \
        ./arc > "$LOG" 2>&1 &
    ARC_PID=$!
    wait_healthy || return 1

    # Write TWO measurements simultaneously:
    # 1. "backfill_data" — timestamps spread across many hours → multi-hour splits → partial failures
    # 2. "live_data" — current-hour timestamps → single-hour flush → always succeeds
    #
    # The live_data successful flushes decrement pendingFlushFailures,
    # draining the counter even while backfill_data has active failures
    echo "  Writing dual-measurement data with flaky S3..."
    echo "flaky" > /tmp/s3proxy_mode

    python3 -c "
import msgpack, requests, time, random, sys, threading, concurrent.futures
import zstandard as zstd

random.seed(${RUN})
url = '${ARC_URL}/api/v1/write/msgpack'
base_headers = {
    'Content-Type': 'application/msgpack',
    'x-arc-database': '${DATABASE}',
    'Content-Encoding': 'zstd',
}
compressor = zstd.ZstdCompressor(level=3)
base_us = int(time.time()) // 86400 * 86400 * 1_000_000

backfill_seq = 0
live_seq = 0
backfill_errors = 0
live_errors = 0
lock = threading.Lock()

def write_backfill(batch_id):
    \"\"\"Write backfill data spanning many hours — triggers multi-hour flush splits.\"\"\"
    global backfill_seq, backfill_errors
    count = random.randint(3000, 5000)

    with lock:
        seq_start = backfill_seq
        backfill_seq += count

    # Spread across 5-30 different hours (like production backfill)
    hour_spread = random.randint(5, 30)
    hour_offsets = [random.randint(0, 720) for _ in range(hour_spread)]
    times = []
    for i in range(count):
        hour_off = random.choice(hour_offsets)
        times.append(base_us - hour_off * 3600_000_000 + batch_id * 60_000_000 + i)

    payload = {
        'm': 'backfill_data',
        'columns': {
            'time': times,
            'seq_id': list(range(seq_start, seq_start + count)),
            'batch_id': [batch_id] * count,
            'value': [random.random() * 100 for _ in range(count)],
        }
    }
    data = compressor.compress(msgpack.packb(payload))
    try:
        r = requests.post(url, data=data, headers=base_headers, timeout=30)
        if r.status_code != 204:
            with lock:
                backfill_errors += 1
    except Exception:
        with lock:
            backfill_errors += 1

def write_live(batch_id):
    \"\"\"Write live data with current timestamps — always flushes as single hour.
    These successful flushes decrement pendingFlushFailures, draining the counter.\"\"\"
    global live_seq, live_errors
    count = random.randint(100, 500)

    with lock:
        seq_start = live_seq
        live_seq += count

    # All timestamps in CURRENT hour — single-hour flush, always succeeds
    now_us = int(time.time()) * 1_000_000
    times = [now_us + i for i in range(count)]

    payload = {
        'm': 'live_data',
        'columns': {
            'time': times,
            'seq_id': list(range(seq_start, seq_start + count)),
            'batch_id': [batch_id] * count,
            'value': [random.random() * 100 for _ in range(count)],
        }
    }
    data = compressor.compress(msgpack.packb(payload))
    try:
        r = requests.post(url, data=data, headers=base_headers, timeout=30)
        if r.status_code != 204:
            with lock:
                live_errors += 1
    except Exception:
        with lock:
            live_errors += 1

# Run both measurements in parallel for 120 seconds
# This gives enough time for multiple WAL purge cycles (every 10s)
duration = 120
start = time.time()
batch = 0

print(f'  Running dual-measurement writes for {duration}s...', flush=True)

while time.time() - start < duration:
    # Write backfill batch (triggers multi-hour partial failures)
    write_backfill(batch)

    # Write 3-5 live data batches (creates successful flushes to drain counter)
    for _ in range(random.randint(3, 5)):
        write_live(batch)

    batch += 1
    elapsed = int(time.time() - start)

    if batch % 20 == 0:
        print(f'    t={elapsed}s batch={batch} backfill_seq={backfill_seq:,} live_seq={live_seq:,} errs={backfill_errors}/{live_errors}', flush=True)

    time.sleep(0.1)

print(f'  Write done: backfill={backfill_seq:,} live={live_seq:,} batches={batch} errs={backfill_errors}/{live_errors}', flush=True)

with open('/tmp/duptest_backfill_seq', 'w') as f:
    f.write(str(backfill_seq))
with open('/tmp/duptest_live_seq', 'w') as f:
    f.write(str(live_seq))
" 2>&1

    BACKFILL_SEQ=$(cat /tmp/duptest_backfill_seq 2>/dev/null || echo "0")
    LIVE_SEQ=$(cat /tmp/duptest_live_seq 2>/dev/null || echo "0")

    # Switch to normal S3 and wait for all retries to flush
    echo "  S3 normal, waiting 60s for retries + WAL purge cycles..."
    echo "normal" > /tmp/s3proxy_mode
    sleep 60

    # Check for duplicates in backfill_data (the measurement that gets partial flushes)
    echo "  Checking backfill_data for duplicates..."
    RESULT=$(query_arc "SELECT COUNT(*) as cnt, COUNT(DISTINCT seq_id) as dist FROM backfill_data")
    echo "  Raw: $RESULT"

    TOTAL=$(echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['data'][0][0])" 2>/dev/null || echo "0")
    DISTINCT=$(echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['data'][0][1])" 2>/dev/null || echo "0")
    DUPES=$((TOTAL - DISTINCT))
    MISSING=$((BACKFILL_SEQ - DISTINCT))

    echo "  Expected: ${BACKFILL_SEQ}  Total: ${TOTAL}  Distinct: ${DISTINCT}  Duplicates: ${DUPES}  Missing: ${MISSING}"

    # Diagnostic stats
    PARTIAL_FLUSHES=$(grep -c "Partial multi-hour" "$LOG" 2>/dev/null || echo "0")
    FULL_FAILURES=$(grep -c "Flush failed - data restored" "$LOG" 2>/dev/null || echo "0")
    WAL_PURGES=$(grep -c "Periodic WAL cleanup" "$LOG" 2>/dev/null || echo "0")
    SKIPPED_PURGES=$(grep -c "Skipping WAL purge" "$LOG" 2>/dev/null || echo "0")
    BACKFILL_OK=$(grep -c "flush completed.*backfill" "$LOG" 2>/dev/null || echo "0")
    LIVE_OK=$(grep -c "flush completed.*live" "$LOG" 2>/dev/null || echo "0")
    echo "  Partial: ${PARTIAL_FLUSHES}  Full fail: ${FULL_FAILURES}  WAL purges: ${WAL_PURGES}  Skip purges: ${SKIPPED_PURGES}"
    echo "  Backfill OK: ${BACKFILL_OK}  Live OK: ${LIVE_OK}"

    # Stop
    kill "$ARC_PID" 2>/dev/null || true
    wait "$ARC_PID" 2>/dev/null || true
    kill "$PROXY_PID" 2>/dev/null || true
    wait "$PROXY_PID" 2>/dev/null || true

    if [ "$DUPES" -gt 0 ]; then
        echo ""
        echo "========================================================"
        echo " DUPLICATION REPRODUCED on Run #${RUN}!"
        echo " Total: ${TOTAL}  Distinct: ${DISTINCT}  Duplicates: ${DUPES}"
        echo "========================================================"

        # Restart to query details
        python3 benchmarks/s3_fault_proxy.py > /dev/null 2>&1 &
        sleep 1
        ARC_STORAGE_S3_ACCESS_KEY=minioadmin ARC_STORAGE_S3_SECRET_KEY=minioadmin \
            ./arc > /dev/null 2>&1 &
        QUERY_PID=$!
        sleep 3

        echo "  Top duplicate seq_ids:"
        query_arc "SELECT seq_id, COUNT(*) as cnt FROM backfill_data GROUP BY seq_id HAVING COUNT(*) > 1 ORDER BY cnt DESC LIMIT 20" | python3 -c "
import sys, json
d = json.load(sys.stdin)
for row in d.get('data', [])[:20]:
    print(f'    seq_id={row[0]}  count={row[1]}')
" 2>/dev/null || true

        kill "$QUERY_PID" 2>/dev/null || true
        pkill -f "s3_fault_proxy" 2>/dev/null || true

        echo ""
        echo "  Arc log: $LOG"
        echo ""
        echo "  Race evidence:"
        grep -E "Partial multi-hour|Periodic WAL cleanup|Skipping WAL" "$LOG" | head -40
        return 0
    else
        echo "  Run #${RUN}: No duplicates (${PARTIAL_FLUSHES} partials, ${WAL_PURGES} purges, ${SKIPPED_PURGES} skipped)"
        return 1
    fi
}

# Save original config
cp arc.toml arc.toml.bak.fault 2>/dev/null || true

# Build once
echo "Building Arc..."
cd "$ARC_DIR"
make build 2>&1 | tail -1

echo ""
echo "============================================================"
echo " Reproducing data duplication via counter drain race"
echo " Two measurements: backfill (fails) + live (succeeds, drains counter)"
echo " Max runs: $MAX_RUNS"
echo "============================================================"

for run in $(seq 1 "$MAX_RUNS"); do
    if run_test "$run"; then
        cp arc.toml.bak.fault arc.toml 2>/dev/null || true
        echo ""
        echo "Duplication reproduced after $run run(s)."
        exit 0
    fi
done

cp arc.toml.bak.fault arc.toml 2>/dev/null || true
echo ""
echo "No duplication found after $MAX_RUNS runs."
exit 1
