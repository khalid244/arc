package rollup

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// TestScan_MatchesOldGlobOnCorpus is the deep, real-data check for the discovery
// rewrite: against the live MinIO corpus, the new delimiter-walk scan() must discover
// EXACTLY the same {source -> days} as the old recursive "**" glob it replaced — no
// missing days, no extra days, no missing/extra sources. Proves the efficiency change
// is behavior-preserving on real S3 layout.
func TestScan_MatchesOldGlobOnCorpus(t *testing.T) {
	db := openTestDuck(t) // DuckDB wired to MinIO (for the old-glob ground truth)
	defer db.Close()
	requireCorpus(t, db)

	be, err := storage.NewS3Backend(&storage.S3Config{
		Bucket: testBucket, Region: "us-east-1", Endpoint: "http://" + testEndpoint,
		AccessKey: testKey, SecretKey: testSecret, UseSSL: false, PathStyle: true,
	}, zerolog.Nop())
	if err != nil {
		t.Fatalf("s3 backend: %v", err)
	}

	m := &Manager{
		db:  db,
		stg: be,
		log: zerolog.Nop(),
		s3:  S3Params{Bucket: testBucket},
		cfg: Config{Databases: []string{"default"}}, // scope to one db for a bounded test
	}

	// NEW path: the delimiter walk.
	gotDays := map[string]map[string]bool{}
	for src, days := range m.scan(context.Background()) {
		gotDays[src] = map[string]bool{}
		for _, d := range days {
			gotDays[src][d.Format("2006-01-02")] = true
		}
	}

	// OLD path: replicate the previous recursive-glob discovery exactly.
	oldDays := map[string]map[string]bool{}
	bucketPrefix := "s3://" + testBucket + "/"
	for _, file := range m.globFiles("s3://" + testBucket + "/default/*/**/*.parquet") {
		rel := strings.TrimPrefix(file, bucketPrefix)
		segs := strings.Split(rel, "/")
		if len(segs) < 6 {
			continue
		}
		dbn, meas := segs[0], segs[1]
		if m.skipMeasurement(dbn, meas) {
			continue
		}
		if _, err := time.Parse("2006/01/02", segs[2]+"/"+segs[3]+"/"+segs[4]); err != nil {
			continue
		}
		src := dbn + "." + meas
		if oldDays[src] == nil {
			oldDays[src] = map[string]bool{}
		}
		oldDays[src][segs[2]+"-"+segs[3]+"-"+segs[4]] = true
	}

	if len(oldDays) == 0 {
		t.Fatal("old glob discovered no default.* sources — corpus missing?")
	}

	// Equivalence: same sources, same day sets, both directions.
	for src, days := range oldDays {
		nd, ok := gotDays[src]
		if !ok {
			t.Errorf("new scan MISSING source %q (old had %d days)", src, len(days))
			continue
		}
		for d := range days {
			if !nd[d] {
				t.Errorf("source %q: new scan MISSING day %s", src, d)
			}
		}
		for d := range nd {
			if !days[d] {
				t.Errorf("source %q: new scan has EXTRA day %s", src, d)
			}
		}
	}
	for src := range gotDays {
		if _, ok := oldDays[src]; !ok {
			t.Errorf("new scan has EXTRA source %q not found by old glob", src)
		}
	}

	// Sanity: the known corpus measurement/day is present.
	if !gotDays["default.downloads"]["2025-12-28"] {
		t.Errorf("expected default.downloads to include 2025-12-28; got days=%d", len(gotDays["default.downloads"]))
	}
	t.Logf("EQUIVALENT: %d default.* sources; default.downloads=%d days (matches old glob)",
		len(gotDays), len(gotDays["default.downloads"]))
}
