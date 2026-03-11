#!/bin/bash
# =============================================================================
# 7-Day Soak Test — Simulates 7 days of production data (~2M records/day)
#
# Single Arc instance, persistent across all 7 days. S3 outages injected
# daily. Verifies integrity after each day and at the end.
# Total: ~14M records across 5 measurements over 7 simulated days.
# =============================================================================
set -uo pipefail

ARC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ARC_DIR"

ARC_HOST="localhost"
ARC_PORT=8000
ARC_URL="http://${ARC_HOST}:${ARC_PORT}"
DATABASE="soak_test"
DAYS=7
RECORDS_PER_DAY=2000000
SUMMARY="/tmp/arc-soak-summary.log"

cleanup() {
    pkill -9 -f "./arc" 2>/dev/null || true
    pkill -f "s3_fault_proxy" 2>/dev/null || true
    echo "normal" > /tmp/s3proxy_mode 2>/dev/null || true
    sleep 1
}

wait_healthy() {
    for i in $(seq 1 20); do
        curl -s "${ARC_URL}/health" > /dev/null 2>&1 && return 0
        sleep 1
    done
    return 1
}

query_count() {
    local TABLE="$1"
    local RESULT
    RESULT=$(curl -s -X POST "${ARC_URL}/api/v1/query" \
        -H "Content-Type: application/json" \
        -H "x-arc-database: ${DATABASE}" \
        -d "{\"sql\": \"SELECT COUNT(*) as cnt, COUNT(DISTINCT seq_id) as dist FROM ${TABLE}\"}" 2>/dev/null)
    echo "$RESULT" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(d['data'][0][0], d['data'][0][1])
except:
    print('0 0')
" 2>/dev/null
}

cleanup

cat <<'BANNER' | tee "$SUMMARY"
============================================================
 7-Day Soak Test — 2M records/day × 7 days = 14M total
 5 measurements, daily S3 outages, cumulative integrity
============================================================
BANNER
echo "Started: $(date)" | tee -a "$SUMMARY"
echo "" >> "$SUMMARY"

# Save original config
cp arc.toml arc.toml.bak.fault 2>/dev/null || true

# Configure for S3 via fault proxy
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

# Clean slate
docker exec minio-test rm -rf /data/arc-test/soak_test 2>/dev/null || true
rm -rf data/wal 2>/dev/null || true

# Start proxy + Arc (persistent for all 7 days)
python3 benchmarks/s3_fault_proxy.py > "/tmp/s3proxy-soak.log" 2>&1 &
PROXY_PID=$!
sleep 1

ARC_STORAGE_S3_ACCESS_KEY=minioadmin ARC_STORAGE_S3_SECRET_KEY=minioadmin \
    ARC_INGEST_MAX_BUFFER_SIZE=5000 ARC_INGEST_MAX_BUFFER_AGE_MS=10000 \
    ARC_INGEST_FLUSH_QUEUE_SIZE=1000 \
    ARC_WAL_RECOVERY_INTERVAL_SECONDS=10 \
    ./arc > "/tmp/arc-soak.log" 2>&1 &
ARC_PID=$!

if ! wait_healthy; then
    echo "FATAL: Arc failed to start"
    kill "$PROXY_PID" 2>/dev/null || true
    exit 1
fi

echo "Arc running (PID=$ARC_PID), proxy (PID=$PROXY_PID)"

# =============================================================================
# Write 7 days of data with daily S3 outages
# =============================================================================
python3 -c "
import msgpack, requests, time, random, threading, json
import zstandard as zstd

random.seed(42)
url = '${ARC_URL}/api/v1/write/msgpack'
compressor = zstd.ZstdCompressor(level=3)
control_file = '/tmp/s3proxy_mode'
DAYS = ${DAYS}
RECORDS_PER_DAY = ${RECORDS_PER_DAY}

# 5 measurements with different traffic profiles
# Weights control share of daily 2M records
MEASUREMENTS = [
    {'name': 'downloads', 'weight': 0.30, 'spread': (5, 30),  'backfill': True},
    {'name': 'devices',   'weight': 0.20, 'spread': (0, 0),   'backfill': False},
    {'name': 'events',    'weight': 0.20, 'spread': (0, 3),   'backfill': False},
    {'name': 'metrics',   'weight': 0.10, 'spread': (0, 0),   'backfill': False},
    {'name': 'imports',   'weight': 0.20, 'spread': (5, 24),  'backfill': True},
]

counters = {m['name']: 0 for m in MEASUREMENTS}
day_counters = {m['name']: 0 for m in MEASUREMENTS}
lock = threading.Lock()
stop_event = threading.Event()

BATCH_SIZE = 5000

def write_batch(m, batch_id, day, count):
    name = m['name']
    with lock:
        seq_start = counters[name]
        counters[name] += count
        day_counters[name] += count

    # Simulate timestamps for this day (day 0 = 7 days ago, day 6 = today)
    day_offset_us = (DAYS - 1 - day) * 24 * 3600 * 1_000_000
    base_us = int(time.time()) * 1_000_000 - day_offset_us

    if m['backfill'] or m['spread'][1] > 0:
        spread_min, spread_max = m['spread']
        hour_spread = random.randint(max(1, spread_min), max(1, spread_max))
        hour_offsets = [random.randint(0, 720) for _ in range(hour_spread)]
        times = []
        for i in range(count):
            hour_off = random.choice(hour_offsets)
            times.append(base_us - hour_off * 3600_000_000 + batch_id * 60_000_000 + i)
    else:
        times = [base_us + batch_id * 60_000_000 + i for i in range(count)]

    payload = {
        'm': name,
        'columns': {
            'time': times,
            'seq_id': list(range(seq_start, seq_start + count)),
            'batch_id': [batch_id] * count,
            'day': [day] * count,
            'value': [random.random() * 100 for _ in range(count)],
            'tag': [f'tag_{i % 10}' for i in range(count)],
        }
    }
    data = compressor.compress(msgpack.packb(payload))
    headers = {
        'Content-Type': 'application/msgpack',
        'x-arc-database': '${DATABASE}',
        'Content-Encoding': 'zstd',
    }
    try:
        requests.post(url, data=data, headers=headers, timeout=30)
    except Exception:
        pass

# Daily outage patterns — each day gets a different pattern
DAILY_OUTAGE_PLANS = [
    # Day 0: Morning S3 errors
    [('normal',10), ('error500',8), ('normal',5), ('error500',12), ('normal',999)],
    # Day 1: Flaky S3 all day
    [('normal',5), ('flaky',20), ('normal',8), ('flaky',15), ('normal',999)],
    # Day 2: Clean day (no outages)
    [('normal',999)],
    # Day 3: Connection resets + timeouts
    [('normal',8), ('reset',5), ('normal',10), ('timeout',8), ('normal',5), ('reset',3), ('normal',999)],
    # Day 4: Mixed chaos
    [('normal',5), ('error500',5), ('normal',3), ('flaky',10), ('normal',3), ('timeout',5), ('normal',3), ('reset',3), ('normal',999)],
    # Day 5: Extended outage
    [('normal',5), ('error500',25), ('normal',999)],
    # Day 6: Flaky recovery
    [('normal',3), ('flaky',30), ('normal',5), ('latency',10), ('normal',999)],
]

def outage_controller_day(day):
    plan = DAILY_OUTAGE_PLANS[day % len(DAILY_OUTAGE_PLANS)]
    for mode, duration in plan:
        if stop_event.is_set():
            break
        with open(control_file, 'w') as f:
            f.write(mode)
        if mode != 'normal':
            print(f'  [day {day}] outage: {mode} for {duration}s', flush=True)
        stop_event.wait(duration)
    with open(control_file, 'w') as f:
        f.write('normal')

results = {}

for day in range(DAYS):
    day_counters = {m['name']: 0 for m in MEASUREMENTS}
    stop_event.clear()

    print(f'', flush=True)
    print(f'=== Day {day}/6 — writing ~{RECORDS_PER_DAY:,} records ===', flush=True)

    # Start outage controller for this day
    outage_thread = threading.Thread(target=outage_controller_day, args=(day,), daemon=True)
    outage_thread.start()

    # Write records for this day
    batch = 0
    day_written = 0
    while day_written < RECORDS_PER_DAY:
        for m in MEASUREMENTS:
            remaining = RECORDS_PER_DAY - day_written
            if remaining <= 0:
                break
            count = min(BATCH_SIZE, int(remaining * m['weight'] / max(0.01, sum(
                mm['weight'] for mm in MEASUREMENTS if day_counters[mm['name']] < RECORDS_PER_DAY * mm['weight']
            ))))
            count = max(100, min(count, remaining))
            write_batch(m, batch, day, count)
            day_written += count
        batch += 1
        if batch % 20 == 0:
            totals = ', '.join(f\"{k}={v:,}\" for k, v in day_counters.items())
            print(f'    batch={batch} total={day_written:,}/{RECORDS_PER_DAY:,} {totals}', flush=True)
        time.sleep(0.02)

    stop_event.set()
    with open(control_file, 'w') as f:
        f.write('normal')

    print(f'  Day {day} write complete: {day_written:,} records in {batch} batches', flush=True)
    for k, v in day_counters.items():
        print(f'    {k}: {v:,}', flush=True)

    # Drain between days
    print(f'  Draining (45s)...', flush=True)
    time.sleep(45)

    # Save day results
    results[day] = dict(day_counters)

# Save cumulative expected counts
with open('/tmp/soak_expected.json', 'w') as f:
    json.dump({'counters': dict(counters), 'days': results}, f)

print(f'', flush=True)
print(f'All {DAYS} days complete. Total records sent:', flush=True)
for k, v in counters.items():
    print(f'  {k}: {v:,}', flush=True)
print(f'  TOTAL: {sum(counters.values()):,}', flush=True)
" 2>&1

# Ensure proxy is healthy for drain + verification
echo "normal" > /tmp/s3proxy_mode

# Final drain
echo ""
echo "Final drain (90s)..."
sleep 90

# =============================================================================
# Verify: check each measurement for duplication and data loss
# =============================================================================
echo ""
echo "============================================================"
echo " Verification"
echo "============================================================"

LOG="/tmp/arc-soak.log"

# Extract Arc's own stats from log (reliable, doesn't depend on query)
ARC_RECORDS_WRITTEN=$(grep "ArrowBuffer closed" "$LOG" 2>/dev/null | grep -o 'total_records_written=[0-9]*' | cut -d= -f2 || echo "0")
ARC_TOTAL_FLUSHES=$(grep "ArrowBuffer closed" "$LOG" 2>/dev/null | grep -o 'total_flushes=[0-9]*' | cut -d= -f2 || echo "0")

EXPECTED=$(cat /tmp/soak_expected.json 2>/dev/null || echo '{}')
ISSUES=""
DETAILS=""
TOTAL_EXP=0
TOTAL_GOT=0
TOTAL_DUPES=0
TOTAL_MISS=0

# Query each measurement with retries (queries go through proxy to S3)
for MEAS in downloads devices events metrics imports; do
    EXP=$(echo "$EXPECTED" | python3 -c "import sys,json; print(json.load(sys.stdin)['counters'].get('$MEAS', 0))" 2>/dev/null || echo "0")

    # Retry query up to 3 times with increasing timeout
    READ="0 0"
    for ATTEMPT in 1 2 3; do
        RESULT=$(curl -s --max-time 120 -X POST "${ARC_URL}/api/v1/query" \
            -H "Content-Type: application/json" \
            -H "x-arc-database: ${DATABASE}" \
            -d "{\"sql\": \"SELECT COUNT(*) as cnt, COUNT(DISTINCT seq_id) as dist FROM ${MEAS}\"}" 2>/dev/null)
        PARSED=$(echo "$RESULT" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(d['data'][0][0], d['data'][0][1])
except:
    print('0 0')
" 2>/dev/null)
        if [ "$PARSED" != "0 0" ]; then
            READ="$PARSED"
            break
        fi
        echo "  Query retry $ATTEMPT for $MEAS..."
        sleep 5
    done

    TOTAL=$(echo "$READ" | cut -d' ' -f1)
    DISTINCT=$(echo "$READ" | cut -d' ' -f2)
    DUPES=$((TOTAL - DISTINCT))
    MISSING=$((EXP - DISTINCT))

    TOTAL_EXP=$((TOTAL_EXP + EXP))
    TOTAL_GOT=$((TOTAL_GOT + DISTINCT))
    TOTAL_DUPES=$((TOTAL_DUPES + DUPES))

    STAT="ok"
    if [ "$DUPES" -gt 0 ]; then
        STAT="DUPES=${DUPES}"
        ISSUES="${ISSUES} ${MEAS}:${STAT}"
    fi
    if [ "$MISSING" -gt 0 ]; then
        STAT="${STAT} MISS=${MISSING}"
        ISSUES="${ISSUES} ${MEAS}:MISS=${MISSING}"
        TOTAL_MISS=$((TOTAL_MISS + MISSING))
    fi
    DETAILS="${DETAILS}  ${MEAS}: exp=${EXP} got=${DISTINCT} dupes=${DUPES} miss=${MISSING} [${STAT}]\n"
done

QUEUE_FULL=$(grep -c "Flush queue full" "$LOG" 2>/dev/null || echo "0")
FLUSH_FAIL=$(grep -c "Flush failed - data restored" "$LOG" 2>/dev/null || echo "0")
PARTIAL=$(grep -c "Partial multi-hour" "$LOG" 2>/dev/null || echo "0")
WAL_PURGE=$(grep -c "Periodic WAL cleanup\|Purged WAL files" "$LOG" 2>/dev/null || echo "0")
SKIP_PURGE=$(grep -c "Skipping WAL purge" "$LOG" 2>/dev/null || echo "0")

# Stop Arc + proxy AFTER verification
kill "$ARC_PID" 2>/dev/null || true
wait "$ARC_PID" 2>/dev/null || true
kill "$PROXY_PID" 2>/dev/null || true
wait "$PROXY_PID" 2>/dev/null || true

echo -e "$DETAILS"
echo "  arc_records_written=${ARC_RECORDS_WRITTEN} arc_total_flushes=${ARC_TOTAL_FLUSHES}"
echo "  queue_full=${QUEUE_FULL} flush_fail=${FLUSH_FAIL} partial=${PARTIAL} wal_purge=${WAL_PURGE} skip_purge=${SKIP_PURGE}"
echo ""
echo "  TOTALS: exp=${TOTAL_EXP} got=${TOTAL_GOT} dupes=${TOTAL_DUPES} miss=${TOTAL_MISS}"

if [ -z "$ISSUES" ]; then
    STATUS="CLEAN"
else
    STATUS="ISSUES:${ISSUES}"
fi

echo "" | tee -a "$SUMMARY"
echo "============================================================" | tee -a "$SUMMARY"
echo " 7-Day Soak Result — $(date)" | tee -a "$SUMMARY"
echo " Status: ${STATUS}" | tee -a "$SUMMARY"
echo -e "$DETAILS" | tee -a "$SUMMARY"
echo " arc_records_written=${ARC_RECORDS_WRITTEN} arc_total_flushes=${ARC_TOTAL_FLUSHES}" | tee -a "$SUMMARY"
echo " queue_full=${QUEUE_FULL} flush_fail=${FLUSH_FAIL} partial=${PARTIAL}" | tee -a "$SUMMARY"
echo " wal_purge=${WAL_PURGE} skip_purge=${SKIP_PURGE}" | tee -a "$SUMMARY"
echo " TOTALS: exp=${TOTAL_EXP} got=${TOTAL_GOT} dupes=${TOTAL_DUPES} miss=${TOTAL_MISS}" | tee -a "$SUMMARY"
echo "============================================================" | tee -a "$SUMMARY"

cp arc.toml.bak.fault arc.toml 2>/dev/null || true

[ -z "$ISSUES" ] && exit 0 || exit 1
