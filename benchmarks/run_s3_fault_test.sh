#!/bin/bash
# =============================================================================
# S3 Fault Injection Test: Before vs After flushTracker fix
#
# Runs Arc with S3 backend through the fault proxy, injects failures,
# then checks for data loss. Runs twice: without fix, then with fix.
# =============================================================================
set -euo pipefail

ARC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ARC_DIR"

ARC_HOST="localhost"
ARC_PORT=8000
ARC_URL="http://${ARC_HOST}:${ARC_PORT}"
SIM_MINUTES=10080   # 7 days
PACING=0.05         # 50ms per sim-minute (~8.4min per day)
WORKERS=15
FLUSH_WAIT=20

cleanup() {
    pkill -f "./arc" 2>/dev/null || true
    pkill -f "s3_fault_proxy" 2>/dev/null || true
    echo "normal" > /tmp/s3proxy_mode 2>/dev/null || true
    sleep 1
}

run_test() {
    local LABEL="$1"
    local LOG="/tmp/arc-s3fault-${LABEL}.log"
    local RESULT="/tmp/arc-s3fault-${LABEL}.result"

    echo ""
    echo "============================================================"
    echo " S3 FAULT INJECTION TEST  (${LABEL})"
    echo "============================================================"
    echo ""

    cleanup

    # Restore clean config then configure for S3 via proxy
    cp arc.toml.bak.fault arc.toml 2>/dev/null || true
    python3 -c "
import re
with open('arc.toml') as f:
    content = f.read()
# Switch to S3 backend
content = re.sub(r'(^\[storage\]\s*\nbackend\s*=\s*)\"local\"', r'\1\"s3\"', content, flags=re.MULTILINE)
# Set S3 endpoint to proxy (handle both empty and already-set)
content = re.sub(r's3_endpoint\s*=\s*\"[^\"]*\"', 's3_endpoint = \"localhost:9090\"', content, count=1)
# Disable auth
content = re.sub(r'(\[auth\]\s*\n\s*enabled\s*=\s*)(true|false)', r'\1false', content)
# Enable WAL
content = re.sub(r'(\[wal\]\s*\n\s*enabled\s*=\s*)(true|false)', r'\1true', content)
with open('arc.toml', 'w') as f:
    f.write(content)
print('Storage: S3 via proxy (9090->9000), WAL: ON, Auth: OFF')
"

    # Clean old test data from MinIO and WAL
    docker exec minio-test rm -rf /data/arc-test/integrity_test 2>/dev/null || true
    rm -rf data/wal 2>/dev/null || true
    echo "Cleaned old data"

    # Build
    echo "Building Arc..."
    make build 2>&1 | tail -1

    # Start fault proxy
    echo "Starting S3 fault proxy..."
    python3 benchmarks/s3_fault_proxy.py > "/tmp/s3proxy-${LABEL}.log" 2>&1 &
    PROXY_PID=$!
    sleep 1
    echo "Proxy PID: $PROXY_PID"

    # Start Arc with small buffer to trigger async flush path (reproduces duplication)
    echo "Starting Arc (small buffer: 5000 records, 500ms age)..."
    ARC_STORAGE_S3_ACCESS_KEY=minioadmin ARC_STORAGE_S3_SECRET_KEY=minioadmin \
        ARC_INGEST_MAX_BUFFER_SIZE=5000 ARC_INGEST_MAX_BUFFER_AGE_MS=500 \
        ./arc > "$LOG" 2>&1 &
    ARC_PID=$!
    echo "Arc PID: $ARC_PID"

    # Wait for health
    for i in $(seq 1 15); do
        if curl -s "${ARC_URL}/health" > /dev/null 2>&1; then
            echo "Arc is healthy"
            break
        fi
        sleep 1
    done

    # Run with S3 fault injection
    echo ""
    echo "Running 1-day test with S3 faults..."
    echo ""

    python3 benchmarks/test_daily_integrity.py \
        --sim-minutes "$SIM_MINUTES" \
        --real-seconds-per-sim-min "$PACING" \
        --workers "$WORKERS" \
        --compress zstd \
        --flush-wait "$FLUSH_WAIT" \
        --bursts \
        --compact-during-writes \
        --s3-fault \
        --small-buffer \
        2>&1 | tee "$RESULT" || true

    echo ""
    echo "Test log: $LOG"
    echo "Proxy log: /tmp/s3proxy-${LABEL}.log"
    echo "Result:    $RESULT"

    # Stop
    kill "$ARC_PID" 2>/dev/null || true
    wait "$ARC_PID" 2>/dev/null || true
    kill "$PROXY_PID" 2>/dev/null || true
    wait "$PROXY_PID" 2>/dev/null || true
    echo "Arc + Proxy stopped"
}

# Save original config
cp arc.toml arc.toml.bak.fault 2>/dev/null || true

# --- Run WITHOUT fix ---
echo "Reverting tracked files to test WITHOUT flushTracker fix..."
# Save modified files, then checkout originals
cp internal/ingest/arrow_writer.go /tmp/arrow_writer_fixed.go
cp cmd/arc/main.go /tmp/main_fixed.go
git checkout -- internal/ingest/arrow_writer.go cmd/arc/main.go
run_test "before"

# --- Run WITH fix ---
echo ""
echo "Restoring flushTracker fix..."
cp /tmp/arrow_writer_fixed.go internal/ingest/arrow_writer.go
cp /tmp/main_fixed.go cmd/arc/main.go
run_test "after"

# Restore config
cp arc.toml.bak.fault arc.toml 2>/dev/null || true

# --- Compare ---
echo ""
echo "============================================================"
echo " COMPARISON: BEFORE vs AFTER flushTracker fix"
echo "============================================================"
echo ""
echo "BEFORE (no fix):"
grep -E "Total records|Duplicates|Missing|PASS|FAIL|SUMMARY" /tmp/arc-s3fault-before.result | tail -10
echo ""
echo "AFTER (with fix):"
grep -E "Total records|Duplicates|Missing|PASS|FAIL|SUMMARY" /tmp/arc-s3fault-after.result | tail -10
