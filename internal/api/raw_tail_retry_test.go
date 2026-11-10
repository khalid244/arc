package api

import (
	"context"
	"errors"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
)

func TestMissingRawSourceObject(t *testing.T) {
	raw := "s3://arc/posthog/events/2026/10/20/14/20261020_140501_ab.parquet"
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"GET 404 on raw file", errors.New(`IO Error: HTTP GET error reading '` + raw + `' in region 'us-east-1' (HTTP 404 Not Found)`), raw},
		// Observed verbatim shape (DuckDB 1.5.5 + cache_httpfs, MinIO, path-style), e2e 2026-10-20.
		{"real GET 404 via http endpoint", errors.New(`arrow query failed: HTTP Error: HTTP GET error reading 'http://127.0.0.1:19201/qcache-e2e/posthog/events/2026/10/20/19/events_20261020_190435_f8f659ac.parquet' in region 'us-east-1' (HTTP 404 Not Found)`), "http://127.0.0.1:19201/qcache-e2e/posthog/events/2026/10/20/19/events_20261020_190435_f8f659ac.parquet"},
		{"HEAD 404 as http URL", errors.New(`HTTP Error: Unable to connect to URL "http://minio:9000/arc/posthog/events/2026/10/20/14/a.parquet": 404 (Not Found)`), "http://minio:9000/arc/posthog/events/2026/10/20/14/a.parquet"},
		{"NoSuchKey", errors.New(`HTTP GET error on '` + raw + `' NoSuchKey`), raw},
		{"cube object 404 stays a stale cube", errors.New(`HTTP GET error on 's3://arc/_arc/rollup/posthog/events/c1/2026/10/19/cube.parquet' (HTTP 404)`), ""},
		{"403 is not a vanished file", errors.New(`HTTP GET error on '` + raw + `' (HTTP 403 Forbidden)`), ""},
		{"no files found is not this", errors.New(`IO Error: No files found that match the pattern "s3://arc/posthog/events/2026/10/20/14/*.parquet"`), ""},
		{"binder error", errors.New(`Binder Error: Referenced column "x" not found`), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := missingRawSourceObject(tc.err); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTailGlobsCovering(t *testing.T) {
	sql := `SELECT * FROM (SELECT * FROM read_parquet('s3://arc/_arc/rollup/posthog/events/c1/2026/10/19/cube.parquet') UNION ALL ` +
		`SELECT * FROM read_parquet(['s3://arc/posthog/events/2026/10/20/13/*.parquet', 's3://arc/posthog/events/2026/10/20/14/*.parquet'], union_by_name=true))`
	got := tailGlobsCovering(sql, "s3://arc/posthog/events/2026/10/20/14/x.parquet")
	if len(got) != 1 || got[0] != "s3://arc/posthog/events/2026/10/20/14/*.parquet" {
		t.Errorf("s3 URL: got %v", got)
	}
	got = tailGlobsCovering(sql, "http://minio:9000/arc/posthog/events/2026/10/20/13/y.parquet")
	if len(got) != 1 || got[0] != "s3://arc/posthog/events/2026/10/20/13/*.parquet" {
		t.Errorf("http URL: got %v", got)
	}
	if got = tailGlobsCovering(sql, "s3://arc/posthog/other/2026/10/20/14/x.parquet"); len(got) != 0 {
		t.Errorf("other table: got %v", got)
	}
}

// storage.s3_path_style=false (the default) makes httpfs report a virtual-hosted
// URL, with the bucket in the host: https://bucket.endpoint/key.
func TestTailGlobsCovering_VirtualHosted(t *testing.T) {
	sql := `SELECT * FROM read_parquet(['s3://arc/posthog/events/2026/10/20/13/*.parquet', 's3://arc/posthog/events/2026/10/20/14/*.parquet'])`
	got := tailGlobsCovering(sql, "https://arc.fsn1.example.com/posthog/events/2026/10/20/14/x.parquet")
	if len(got) != 1 || got[0] != "s3://arc/posthog/events/2026/10/20/14/*.parquet" {
		t.Errorf("virtual-hosted URL: got %v", got)
	}
	if got = tailGlobsCovering(sql, "https://other.fsn1.example.com/posthog/events/2026/10/20/14/x.parquet"); len(got) != 0 {
		t.Errorf("other bucket: got %v", got)
	}
}

func rawRetryCtx(t *testing.T) *fiber.Ctx {
	t.Helper()
	app := fiber.New()
	c := app.AcquireCtx(&fasthttp.RequestCtx{})
	t.Cleanup(func() { app.ReleaseCtx(c) })
	return c
}

// A raw tail file deleted by compaction re-runs the SAME merged query once and
// never builds the whole-range source fallback when the retry succeeds.
func TestRetryMissingRawTailFile_RerunsSameQueryOnce(t *testing.T) {
	h := &QueryHandler{logger: zerolog.Nop()}
	c := rawRetryCtx(t)
	transformCalls := 0
	r := &rollupExec{reqSQL: "q", transform: func(context.Context, string, string) (string, bool, error) {
		transformCalls++
		return "SOURCE", true, nil
	}}
	merged := "MERGED read_parquet(['s3://arc/db/t/2026/10/20/14/*.parquet'])"
	first := errors.New(`HTTP GET error on 's3://arc/db/t/2026/10/20/14/f.parquet' (HTTP 404)`)
	var ran []string
	run := func(q string) error { ran = append(ran, q); return nil }

	err := h.retryMissingRawTailFile(c, context.Background(), merged, r, first, run, "t")
	if err != nil {
		t.Fatalf("err = %v, want nil after retry", err)
	}
	if len(ran) != 1 || ran[0] != merged {
		t.Fatalf("ran %v, want the merged query once", ran)
	}
	if got := string(c.Response().Header.Peek("X-Arc-Rollup-Retry")); got != "raw-file-missing" {
		t.Errorf("X-Arc-Rollup-Retry = %q", got)
	}
	if shouldSourceFallback(err, merged, r.sourceFallback(context.Background(), err), nil) || transformCalls != 0 {
		t.Errorf("source fallback built after a successful retry (transform calls %d)", transformCalls)
	}
}

// A retry that fails again hands its error to the existing fallback path, and
// a second failure in the same request is not retried again.
func TestRetryMissingRawTailFile_FailedRetryFallsThrough(t *testing.T) {
	h := &QueryHandler{logger: zerolog.Nop()}
	c := rawRetryCtx(t)
	r := &rollupExec{reqSQL: "q", transform: func(context.Context, string, string) (string, bool, error) {
		return "SOURCE", true, nil
	}}
	merged := "MERGED"
	e404 := errors.New(`HTTP GET error on 's3://arc/db/t/2026/10/20/14/f.parquet' (HTTP 404)`)
	runs := 0
	run := func(string) error { runs++; return e404 }

	err := h.retryMissingRawTailFile(c, context.Background(), merged, r, e404, run, "t")
	if runs != 1 || err == nil {
		t.Fatalf("runs=%d err=%v, want one failed retry", runs, err)
	}
	if !shouldSourceFallback(err, merged, r.sourceFallback(context.Background(), err), nil) {
		t.Error("failed retry must keep the existing source fallback")
	}
	if err2 := h.retryMissingRawTailFile(c, context.Background(), merged, r, e404, run, "t"); runs != 1 || err2 != e404 {
		t.Errorf("second call retried again (runs=%d)", runs)
	}
}

func TestRetryMissingRawTailFile_NotApplicable(t *testing.T) {
	h := &QueryHandler{logger: zerolog.Nop()}
	c := rawRetryCtx(t)
	run := func(string) error { t.Fatal("must not run"); return nil }
	cube404 := errors.New(`HTTP GET error on 's3://arc/_arc/rollup/db/t/c/2026/10/19/x.parquet' (HTTP 404)`)
	raw404 := errors.New(`HTTP GET error on 's3://arc/db/t/2026/10/20/14/f.parquet' (HTTP 404)`)
	if err := h.retryMissingRawTailFile(c, context.Background(), "M", &rollupExec{}, cube404, run, "t"); err != cube404 {
		t.Error("cube 404 must not be retried")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.retryMissingRawTailFile(c, ctx, "M", &rollupExec{}, raw404, run, "t"); err != raw404 {
		t.Error("cancelled query must not be retried")
	}
}

// Review 2026-11-10 Q3: a query no cube serves (raw query, X-Arc-No-Rollup,
// router declined) that hits a raw file compaction deleted is re-run once
// with that partition re-listed, like a rollup-served query, instead of
// failing until the cached listing expires.
func TestRetryMissingRawTailFile_RawQuery(t *testing.T) {
	h := &QueryHandler{logger: zerolog.Nop()}
	c := rawRetryCtx(t)
	raw := "SELECT count(*) FROM read_parquet(['s3://arc/db/t/2026/10/20/14/*.parquet'])"
	raw404 := errors.New(`HTTP GET error on 's3://arc/db/t/2026/10/20/14/f.parquet' (HTTP 404)`)
	var ran []string
	run := func(q string) error { ran = append(ran, q); return nil }
	if err := h.retryMissingRawTailFile(c, context.Background(), raw, nil, raw404, run, "t"); err != nil {
		t.Fatalf("err = %v, want nil after the retry", err)
	}
	if len(ran) != 1 || ran[0] != raw {
		t.Fatalf("ran %v, want the same query once", ran)
	}
	if got := string(c.Response().Header.Peek("X-Arc-Retry")); got != "raw-file-missing" {
		t.Errorf("X-Arc-Retry = %q, want raw-file-missing", got)
	}
	if got := string(c.Response().Header.Peek("X-Arc-Rollup-Retry")); got != "" {
		t.Errorf("X-Arc-Rollup-Retry = %q on a raw query", got)
	}
	cube404 := errors.New(`HTTP GET error on 's3://arc/_arc/rollup/db/t/c/2026/10/19/x.parquet' (HTTP 404)`)
	if err := h.retryMissingRawTailFile(c, context.Background(), raw, nil, cube404, run, "t"); err != cube404 || len(ran) != 1 {
		t.Error("a cube object 404 is never retried as a raw file")
	}
}
