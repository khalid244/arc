"""Verifier: independent truth from the bucket vs. the ledger of acked rows.

Reads every parquet object of each measurement (base `<db>/<m>/**` and the
late sidecar `<db>/<m>_late/**`, never `_arc/`) DIRECTLY from MinIO (not
through the fault proxy) with DuckDB httpfs, and compares the uuids with the
ledger:

  missing          acked uuid not found in storage            -> data loss
  dup_rows         extra copies of a uuid (count - 1 summed)  -> duplication
  dup_rows_client  ...of which the batch had an ambiguous client retry
                   (timeout/reset after send) - possibly client-caused
  unexpected       uuid in storage that the ledger never sent -> corruption
  unacked_present  never-acked uuid that did land (informational)
  time_mismatch    stored time != sent time (unit / replay bugs)

Then asks Arc's query process for count(*) / count(DISTINCT uuid) per
measurement and compares with the direct read.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import duckdb
import requests

from . import guard
from .config import MINIO_PASSWORD, MINIO_USER


def _connect(minio_endpoint: str, threads: int = 16) -> duckdb.DuckDBPyConnection:
    guard.check_endpoint(minio_endpoint, "verifier S3 endpoint")
    con = duckdb.connect()
    con.execute("LOAD httpfs")
    con.execute(f"SET threads={threads}")
    con.execute(f"""CREATE SECRET e2e (TYPE s3, KEY_ID '{MINIO_USER}', SECRET '{MINIO_PASSWORD}',
                    ENDPOINT '{minio_endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')""")
    return con


def _attach_ledger(con, ledger_path: Path):
    try:
        con.execute("LOAD sqlite")
    except duckdb.Error:
        con.execute("INSTALL sqlite")
        con.execute("LOAD sqlite")
    con.execute(f"ATTACH '{ledger_path}' AS L (TYPE sqlite, READ_ONLY)")
    con.execute("""CREATE TABLE led AS
        SELECT r.uuid, r.db, r.measurement AS m, r.time_us, r.tclass, r.batch_id,
               b.status, b.ambiguous_attempts, b.attempts, b.ack_target, b.first_target,
               b.acked_at, b.first_sent, b.source, b.fmt, b.unit
        FROM L.rows r JOIN L.batches b USING (batch_id)""")
    con.execute("DETACH L")


def list_files(con, bucket: str, prefix: str) -> list[str]:
    try:
        return [r[0] for r in con.execute(f"SELECT file FROM glob('s3://{bucket}/{prefix}**/*.parquet')").fetchall()]
    except duckdb.Error:
        return []


def load_storage(con, bucket: str, measurements: list[tuple[str, str]]) -> dict:
    con.execute("CREATE TABLE s3 (db VARCHAR, m VARCHAR, is_late BOOLEAN, uuid VARCHAR, t_us BIGINT, file VARCHAR, "
                "part_us BIGINT, part_grain VARCHAR)")
    files = {}
    for db, m in measurements:
        for late in (False, True):
            name = m + ("_late" if late else "")
            fl = list_files(con, bucket, f"{db}/{name}/")
            files[f"{db}.{name}"] = len(fl)
            if not fl:
                continue
            # Read in chunks so one listing/read hiccup doesn't lose the whole set.
            for i in range(0, len(fl), 2000):
                chunk = fl[i:i + 2000]
                lst = "[" + ",".join("'" + f.replace("'", "''") + "'" for f in chunk) + "]"
                for attempt in range(3):
                    try:
                        # Partition encoded in the key: <db>/<m>/YYYY/MM/DD[/HH]/file
                        rx = f"/{db}/{m}/([0-9]{{4}})/([0-9]{{2}})/([0-9]{{2}})(/([0-9]{{2}}))?/[^/]+$"
                        con.execute(f"""INSERT INTO s3 SELECT '{db}', '{m}', {str(late).lower()},
                                CAST(uuid AS VARCHAR), epoch_us(time), filename,
                                CASE WHEN regexp_matches(filename, '{rx}') THEN epoch_us(make_timestamp(
                                    CAST(regexp_extract(filename, '{rx}', 1) AS BIGINT),
                                    CAST(regexp_extract(filename, '{rx}', 2) AS BIGINT),
                                    CAST(regexp_extract(filename, '{rx}', 3) AS BIGINT),
                                    COALESCE(TRY_CAST(NULLIF(regexp_extract(filename, '{rx}', 5), '') AS BIGINT), 0),
                                    0, 0)) END,
                                CASE WHEN NOT regexp_matches(filename, '{rx}') THEN NULL
                                     WHEN regexp_extract(filename, '{rx}', 5) = '' THEN 'day' ELSE 'hour' END
                            FROM read_parquet({lst}, union_by_name=true, filename=true)""")
                        break
                    except duckdb.Error:
                        if attempt == 2:
                            raise
                        time.sleep(2)
    return files


def query_api_counts(query_url: str, measurements: list[tuple[str, str]], timeout=300) -> dict:
    guard.check_endpoint(query_url, "query API")
    out = {}
    for db, m in measurements:
        for name in (m, m + "_late"):
            sql = f"SELECT count(*) AS n, count(DISTINCT uuid) AS d FROM {db}.{name}"
            key = f"{db}.{name}"
            try:
                r = requests.post(query_url.rstrip("/") + "/api/v1/query", json={"sql": sql}, timeout=timeout)
                if not r.ok:
                    out[key] = {"error": f"http {r.status_code}: {r.text[:300]}"}
                    continue
                n, d = _first_row(r.json())
                out[key] = {"rows": n, "distinct": d}
            except Exception as e:  # noqa: BLE001
                out[key] = {"error": f"{type(e).__name__}: {e}"[:300]}
    return out


def _first_row(js):
    if isinstance(js, dict):
        if "data" in js and js["data"]:
            row = js["data"][0]
            if isinstance(row, dict):
                return int(row.get("n", 0)), int(row.get("d", 0))
            return int(row[0]), int(row[1])
        if "rows" in js and js["rows"]:
            row = js["rows"][0]
            if isinstance(row, dict):
                return int(row.get("n", 0)), int(row.get("d", 0))
            return int(row[0]), int(row[1])
        if js.get("row_count") == 0 or js.get("data") == []:
            return 0, 0
    if isinstance(js, list) and js:
        row = js[0]
        return int(row.get("n", 0)), int(row.get("d", 0))
    raise ValueError(f"unrecognised query response: {str(js)[:200]}")


def verify(bucket: str, minio_endpoint: str, ledger_path: Path, query_url: str | None = None,
           label: str = "final", sample: int = 15) -> dict:
    t0 = time.time()
    con = _connect(minio_endpoint)
    _attach_ledger(con, ledger_path)
    meas = [tuple(r) for r in con.execute("SELECT DISTINCT db, m FROM led ORDER BY 1, 2").fetchall()]
    files = load_storage(con, bucket, meas)
    con.execute("CREATE TABLE s3c AS SELECT db, m, uuid, count(*) AS c, any_value(t_us) AS t_us, "
                "bool_or(is_late) AS any_late, bool_or(NOT is_late) AS any_base FROM s3 GROUP BY ALL")

    def one(sql):
        return con.execute(sql).fetchone()

    per = {}
    for db, m in meas:
        w = f"db='{db}' AND m='{m}'"
        acked = one(f"SELECT count(*) FROM led WHERE {w} AND status='acked'")[0]
        never = one(f"SELECT count(*) FROM led WHERE {w} AND status='never_acked'")[0]
        s3rows, s3distinct = one(f"SELECT count(*), count(DISTINCT uuid) FROM s3 WHERE {w}")
        base_rows = one(f"SELECT count(*) FROM s3 WHERE {w} AND NOT is_late")[0]
        missing = one(f"""SELECT count(*) FROM led l WHERE l.db='{db}' AND l.m='{m}'
                          AND l.status='acked' AND NOT EXISTS
                          (SELECT 1 FROM s3c s WHERE s.db=l.db AND s.m=l.m AND s.uuid=l.uuid)""")[0]
        dup_uuids, dup_rows = one(f"SELECT count(*), COALESCE(sum(c-1),0) FROM s3c WHERE {w} AND c>1")
        dup_client = one(f"""SELECT COALESCE(sum(s.c-1),0) FROM s3c s JOIN led l USING (db, m, uuid)
                            WHERE s.db='{db}' AND s.m='{m}' AND s.c>1 AND l.ambiguous_attempts>0""")[0]
        unexpected = one(f"""SELECT count(*) FROM s3c s WHERE s.db='{db}' AND s.m='{m}' AND NOT EXISTS
                            (SELECT 1 FROM led l WHERE l.db=s.db AND l.m=s.m AND l.uuid=s.uuid)""")[0]
        unacked_present = one(f"""SELECT count(*) FROM s3c s JOIN led l USING (db, m, uuid)
                                 WHERE s.db='{db}' AND s.m='{m}' AND l.status='never_acked'""")[0]
        tmis = one(f"""SELECT count(*) FROM s3 s JOIN led l USING (db, m, uuid)
                      WHERE s.db='{db}' AND s.m='{m}' AND s.t_us <> l.time_us""")[0]
        misplaced = one(f"""SELECT count(*) FROM s3 WHERE {w} AND NOT is_late AND (
                part_us IS NULL
             OR (part_grain='hour' AND t_us // 3600000000 <> part_us // 3600000000)
             OR (part_grain='day' AND t_us // 86400000000 <> part_us // 86400000000))""")[0]
        per[f"{db}.{m}"] = {
            "acked_rows": acked, "never_acked_rows": never, "s3_rows": s3rows, "s3_distinct": s3distinct,
            "s3_base_rows": base_rows, "s3_late_rows": s3rows - base_rows,
            "files_base": files.get(f"{db}.{m}", 0), "files_late": files.get(f"{db}.{m}_late", 0),
            "missing": missing, "dup_uuids": dup_uuids, "dup_rows": dup_rows,
            "dup_rows_client_ambiguous": dup_client, "unexpected": unexpected,
            "unacked_present": unacked_present, "time_mismatch_rows": tmis,
            "misplaced_rows": misplaced,
        }

    tot = {k: sum(v[k] for v in per.values()) for k in next(iter(per.values()), {}).keys()} if per else {}

    # ---------------- evidence
    ev = {}
    ev["missing_by_target"] = con.execute("""
        SELECT ack_target, count(*) FROM led l WHERE status='acked' AND NOT EXISTS
          (SELECT 1 FROM s3c s WHERE s.db=l.db AND s.m=l.m AND s.uuid=l.uuid) GROUP BY 1 ORDER BY 2 DESC""").fetchall()
    ev["missing_by_class"] = con.execute("""
        SELECT tclass, fmt, unit, count(*) FROM led l WHERE status='acked' AND NOT EXISTS
          (SELECT 1 FROM s3c s WHERE s.db=l.db AND s.m=l.m AND s.uuid=l.uuid) GROUP BY ALL ORDER BY 4 DESC""").fetchall()
    ev["missing_by_ack_minute"] = con.execute("""
        SELECT strftime(to_timestamp(acked_at), '%H:%M') AS minute, count(*) FROM led l WHERE status='acked'
          AND NOT EXISTS (SELECT 1 FROM s3c s WHERE s.db=l.db AND s.m=l.m AND s.uuid=l.uuid)
          GROUP BY 1 ORDER BY 1""").fetchall()
    ev["missing_sample"] = con.execute(f"""
        SELECT db, m, uuid, batch_id, ack_target, tclass, unit, strftime(to_timestamp(acked_at), '%H:%M:%S')
        FROM led l WHERE status='acked' AND NOT EXISTS
          (SELECT 1 FROM s3c s WHERE s.db=l.db AND s.m=l.m AND s.uuid=l.uuid) LIMIT {sample}""").fetchall()
    ev["dup_by_target_class"] = con.execute("""
        SELECT l.ack_target, l.tclass, sum(s.c-1) FROM s3c s JOIN led l USING (db, m, uuid)
        WHERE s.c>1 GROUP BY ALL ORDER BY 3 DESC LIMIT 20""").fetchall()
    ev["dup_file_groups"] = con.execute("""
        WITH d AS (SELECT db, m, uuid FROM s3c WHERE c>1),
             f AS (SELECT s.db, s.m, s.uuid, list_sort(list(DISTINCT regexp_replace(s.file, '^s3://[^/]+/', ''))) AS files,
                          count(*) AS copies
                   FROM s3 s JOIN d USING (db, m, uuid) GROUP BY ALL)
        SELECT files, copies, count(*) AS uuids FROM f GROUP BY ALL ORDER BY uuids DESC LIMIT 12""").fetchall()
    ev["dup_sample"] = con.execute(f"""
        SELECT s.db, s.m, s.uuid, s.c, l.batch_id, l.ack_target, l.attempts, l.ambiguous_attempts, l.tclass
        FROM s3c s JOIN led l USING (db, m, uuid) WHERE s.c>1 LIMIT {sample}""").fetchall()
    ev["time_mismatch_sample"] = con.execute(f"""
        SELECT s.db, s.m, s.uuid, s.t_us, l.time_us, l.unit, l.fmt, regexp_replace(s.file, '^s3://[^/]+/', '')
        FROM s3 s JOIN led l USING (db, m, uuid) WHERE s.t_us <> l.time_us LIMIT {sample}""").fetchall()
    ev["misplaced_sample"] = con.execute(f"""
        SELECT db, m, uuid, t_us, part_us, part_grain, regexp_replace(file, '^s3://[^/]+/', '') FROM s3
        WHERE NOT is_late AND (part_us IS NULL
             OR (part_grain='hour' AND t_us // 3600000000 <> part_us // 3600000000)
             OR (part_grain='day' AND t_us // 86400000000 <> part_us // 86400000000)) LIMIT {sample}""").fetchall()
    ev["unexpected_sample"] = con.execute(f"""
        SELECT s.db, s.m, s.uuid FROM s3c s WHERE NOT EXISTS
          (SELECT 1 FROM led l WHERE l.db=s.db AND l.m=s.m AND l.uuid=s.uuid) LIMIT {sample}""").fetchall()

    # Harness self-check: every uuid must have been sent exactly once.
    led_dups = one("SELECT count(*) - count(DISTINCT (db, m, uuid)) FROM led")[0]
    tot["ledger_duplicate_uuids"] = led_dups
    res = {"label": label, "bucket": bucket, "verify_seconds": None, "totals": tot,
           "per_measurement": per, "evidence": _jsonable(ev)}

    if query_url:
        api = query_api_counts(query_url, meas)
        cmp = {}
        for db, m in meas:
            k = f"{db}.{m}"
            p = per[k]
            a_base = api.get(k, {})
            a_late = api.get(f"{k}_late", {})
            api_rows, errs = 0, []
            for part, nfiles in ((a_base, p["files_base"]), (a_late, p["files_late"])):
                if "error" in part:
                    # Arc errors on a table with no objects yet; that is 0 rows, not a mismatch.
                    if nfiles > 0:
                        errs.append(part["error"])
                else:
                    api_rows += part.get("rows") or 0
            cmp[k] = {"api_rows": api_rows, "s3_rows": p["s3_rows"],
                      "match": (api_rows == p["s3_rows"]) and not errs,
                      "errors": errs, "api_raw": {"base": a_base, "late": a_late}}
        res["query_api"] = cmp
        res["query_api_all_match"] = all(v["match"] for v in cmp.values())
    res["verify_seconds"] = round(time.time() - t0, 1)
    con.close()
    return res


def _jsonable(x):
    if isinstance(x, dict):
        return {k: _jsonable(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_jsonable(v) for v in x]
    if isinstance(x, (int, float, str, bool)) or x is None:
        return x
    return str(x)


def main(argv=None):
    import argparse
    p = argparse.ArgumentParser(description="Verify ledger vs bucket")
    p.add_argument("--bucket", required=True)
    p.add_argument("--ledger", required=True)
    p.add_argument("--minio", default="127.0.0.1:29000")
    p.add_argument("--query-url", default=None)
    p.add_argument("--out", default=None)
    a = p.parse_args(argv)
    res = verify(a.bucket, a.minio, Path(a.ledger), a.query_url)
    s = json.dumps(res, indent=2)
    if a.out:
        Path(a.out).write_text(s)
    print(s)


if __name__ == "__main__":
    main()
