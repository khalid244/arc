package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
)

// compactLocalTOML is a compactor-like arc.toml: events has sort keys and a
// column order, downloads has sort keys only.
const compactLocalTOML = `[ingest]
sort_keys = ["downloads:site,response,tag", "events:event,action,device_id,time"]

[compaction]
memory_limit = "3GB"
threads = 3
max_temp_directory_size = "7GiB"
column_order = ["events:lib,action,time,distinct_id"]
`

func withArcToml(t *testing.T, toml string) {
	t.Helper()
	dir := t.TempDir()
	t.Chdir(dir)
	if toml != "" {
		if err := os.WriteFile(filepath.Join(dir, "arc.toml"), []byte(toml), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestParseCompactLocalArgs(t *testing.T) {
	in := []string{"/data/a.parquet", "/data/b.parquet"}
	base := []string{"--output", "/out/x.parquet"}
	args := func(extra ...string) []string { return append(append(append([]string{}, base...), extra...), in...) }

	t.Run("from-config resolves the measurement's keys, order and limits", func(t *testing.T) {
		withArcToml(t, compactLocalTOML)
		rc, err := parseCompactLocalArgs(args("--measurement", "events", "--from-config"), config.Load)
		if err != nil {
			t.Fatal(err)
		}
		if want := []string{"event", "action", "device_id", "time"}; !reflect.DeepEqual(rc.SortKeys, want) {
			t.Fatalf("sort keys = %v, want %v", rc.SortKeys, want)
		}
		if want := []string{"lib", "action", "time", "distinct_id"}; !reflect.DeepEqual(rc.ColumnOrder, want) {
			t.Fatalf("column order = %v, want %v", rc.ColumnOrder, want)
		}
		if rc.MemoryLimit != "3GB" || rc.Threads != 3 || rc.MaxTempDirectorySize != "7GiB" {
			t.Fatalf("limits = %q %d %q", rc.MemoryLimit, rc.Threads, rc.MaxTempDirectorySize)
		}
		if !reflect.DeepEqual(rc.Inputs, in) || rc.Output != "/out/x.parquet" || rc.Measurement != "events" {
			t.Fatalf("inputs/output/measurement = %v %q %q", rc.Inputs, rc.Output, rc.Measurement)
		}
	})

	t.Run("flags override the config", func(t *testing.T) {
		withArcToml(t, compactLocalTOML)
		rc, err := parseCompactLocalArgs(args("--measurement", "events", "--from-config", "--sort-keys", "event,time", "--column-order", "action", "--threads", "8"), config.Load)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(rc.SortKeys, []string{"event", "time"}) || !reflect.DeepEqual(rc.ColumnOrder, []string{"action"}) || rc.Threads != 8 {
			t.Fatalf("overrides lost: %v %v %d", rc.SortKeys, rc.ColumnOrder, rc.Threads)
		}
	})

	t.Run("no-column-order is an explicit empty order", func(t *testing.T) {
		withArcToml(t, compactLocalTOML)
		rc, err := parseCompactLocalArgs(args("--measurement", "downloads", "--from-config", "--no-column-order"), config.Load)
		if err != nil {
			t.Fatal(err)
		}
		if rc.ColumnOrder != nil || !reflect.DeepEqual(rc.SortKeys, []string{"site", "response", "tag"}) {
			t.Fatalf("got order %v keys %v", rc.ColumnOrder, rc.SortKeys)
		}
	})

	t.Run("allow flags pass through", func(t *testing.T) {
		withArcToml(t, compactLocalTOML)
		rc, err := parseCompactLocalArgs(args("--measurement", "events", "--from-config", "--allow-absent-sort-keys", "--allow-absent-columns"), config.Load)
		if err != nil {
			t.Fatal(err)
		}
		if !rc.AllowAbsentSortKeys || !rc.AllowAbsentColumns {
			t.Fatalf("allow flags lost: %+v", rc)
		}
		rc, _ = parseCompactLocalArgs(args("--measurement", "events", "--from-config"), config.Load)
		if rc.AllowAbsentSortKeys || rc.AllowAbsentColumns {
			t.Fatal("absent keys/columns allowed by default")
		}
	})

	t.Run("explicit flags without config", func(t *testing.T) {
		t.Chdir(t.TempDir())
		rc, err := parseCompactLocalArgs(args("--measurement", "events", "--sort-keys", "event,action,device_id,time", "--column-order", "lib, action"), func() (*config.Config, error) {
			t.Fatal("config loaded without --from-config")
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(rc.ColumnOrder, []string{"lib", "action"}) || len(rc.SortKeys) != 4 {
			t.Fatalf("got order %v keys %v", rc.ColumnOrder, rc.SortKeys)
		}
	})

	// Every case below used to run, order by "time" alone, write the inputs'
	// column order and exit 0.
	failing := []struct {
		name, toml string
		args       []string
		want       string
	}{
		{"no flags at all", "", in, "--measurement is required"},
		{"no sort keys without config", "", args("--measurement", "events", "--column-order", "a"), "--sort-keys is required"},
		{"no column order choice without config", "", args("--measurement", "events", "--sort-keys", "event"), "--column-order or --no-column-order"},
		{"from-config without an arc.toml", "", args("--measurement", "events", "--from-config"), "ingest.sort_keys has no entry for measurement \"events\""},
		{"from-config, measurement typo", compactLocalTOML, args("--measurement", "event", "--from-config"), "ingest.sort_keys has no entry for measurement \"event\""},
		{"from-config, no column order entry", compactLocalTOML, args("--measurement", "downloads", "--from-config"), "compaction.column_order has no entry for measurement \"downloads\""},
		{"both column order flags", "", args("--measurement", "events", "--sort-keys", "event", "--column-order", "a", "--no-column-order"), "not both"},
		{"no output", "", []string{"--measurement", "events", "--sort-keys", "event", "--no-column-order", "a.parquet"}, "--output is required"},
		{"no inputs", "", []string{"--measurement", "events", "--sort-keys", "event", "--no-column-order", "--output", "x.parquet"}, "no input files"},
		{"sort keys that split to nothing", "", args("--measurement", "events", "--sort-keys", " , ", "--no-column-order"), "--sort-keys names no column"},
		{"column order that splits to nothing", "", args("--measurement", "events", "--sort-keys", "event", "--column-order", ","), "--column-order names no column"},
		{"from-config, overriding sort keys with nothing", compactLocalTOML, args("--measurement", "events", "--from-config", "--sort-keys", ","), "--sort-keys names no column"},
	}
	for _, tt := range failing {
		t.Run(tt.name, func(t *testing.T) {
			withArcToml(t, tt.toml)
			_, err := parseCompactLocalArgs(tt.args, config.Load)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want one containing %q", err, tt.want)
			}
		})
	}
}
