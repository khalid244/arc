"""SQLite ledger of every row the harness sent.

A row counts as ACKED only when the HTTP request that carried it returned
2xx. Rows of batches that were never acknowledged (retries exhausted at the
end of the run) are kept with status 'never_acked': they are not counted as
missing, but if they show up in storage they are reported separately.
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches(
  batch_id INTEGER PRIMARY KEY,
  db TEXT, measurement TEXT, fmt TEXT, unit TEXT, n_rows INTEGER,
  first_target TEXT, ack_target TEXT,
  attempts INTEGER, ambiguous_attempts INTEGER,
  first_sent REAL, acked_at REAL,
  status TEXT, last_error TEXT, source TEXT
);
CREATE TABLE IF NOT EXISTS rows(
  uuid TEXT, batch_id INTEGER, db TEXT, measurement TEXT, time_us INTEGER, tclass TEXT
);
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
"""


class Ledger:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.conn = sqlite3.connect(str(self.path), timeout=30, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(SCHEMA)

    def set_meta(self, key: str, value: str):
        self.conn.execute("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)", (key, str(value)))

    def next_batch_id(self) -> int:
        r = self.conn.execute("SELECT COALESCE(MAX(batch_id), 0) FROM batches").fetchone()
        return int(r[0]) + 1

    def record(self, batches: list[dict]):
        """batches: dicts with batch fields + 'rows' = list of (uuid, db, m, time_us, tclass)."""
        if not batches:
            return
        cur = self.conn.cursor()
        cur.execute("BEGIN")
        cur.executemany(
            "INSERT INTO batches(batch_id,db,measurement,fmt,unit,n_rows,first_target,ack_target,attempts,"
            "ambiguous_attempts,first_sent,acked_at,status,last_error,source) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(b["batch_id"], b["db"], b["measurement"], b["fmt"], b["unit"], b["n_rows"], b["first_target"],
              b.get("ack_target"), b["attempts"], b["ambiguous_attempts"], b["first_sent"], b.get("acked_at"),
              b["status"], b.get("last_error"), b.get("source", "loadgen")) for b in batches])
        rows = []
        for b in batches:
            for (u, db, m, t, c) in b["rows"]:
                rows.append((u, b["batch_id"], db, m, t, c))
        cur.executemany("INSERT INTO rows(uuid,batch_id,db,measurement,time_us,tclass) VALUES(?,?,?,?,?,?)", rows)
        cur.execute("COMMIT")

    def summary(self) -> dict:
        q = self.conn.execute(
            "SELECT status, COUNT(*), COALESCE(SUM(n_rows),0) FROM batches GROUP BY status").fetchall()
        out = {"batches": {}, "rows": {}}
        for st, nb, nr in q:
            out["batches"][st] = nb
            out["rows"][st] = nr
        return out

    def close(self):
        self.conn.close()


def now() -> float:
    return time.time()
