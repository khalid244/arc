package compaction

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestPresentColumnOrder(t *testing.T) {
	cols := map[string]struct{}{"time": {}, "Event": {}, "action": {}, "os_name": {}, `we"ird`: {}}
	tests := []struct {
		name        string
		order       []string
		wantPresent []string
		wantAbsent  []string
	}{
		{"nil order", nil, nil, nil},
		{"all present keep the listed order", []string{"os_name", "action", "time"}, []string{"os_name", "action", "time"}, nil},
		{"absent skipped", []string{"action", "error_message", "time"}, []string{"action", "time"}, []string{"error_message"}},
		// DuckDB binds identifiers case-insensitively; the output names the
		// column as the files spell it, so EXCLUDE and the select list agree.
		{"case-insensitive, schema spelling", []string{"event", "ACTION"}, []string{"Event", "action"}, nil},
		{"every listed column absent", []string{"nope", "gone"}, nil, []string{"nope", "gone"}},
		{"quote in a name", []string{`we"ird`}, []string{`we"ird`}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			present, absent := presentColumnOrder(tt.order, cols)
			if !reflect.DeepEqual(present, tt.wantPresent) || !reflect.DeepEqual(absent, tt.wantAbsent) {
				t.Fatalf("presentColumnOrder(%v) = %v, %v; want %v, %v", tt.order, present, absent, tt.wantPresent, tt.wantAbsent)
			}
		})
	}
}

func TestColumnOrderSelect(t *testing.T) {
	const timeExpr = `f("time") AS "time"`
	tests := []struct {
		name      string
		leading   []string
		coversAll bool
		timeExpr  string
		want      string
	}{
		// No order: exactly the projections compaction and reorg used before.
		{"none, time replaced", nil, false, timeExpr, `* REPLACE (f("time") AS "time")`},
		{"none, plain", nil, false, "", `*`},
		{"leading without time", []string{"action", "event"}, false, timeExpr,
			`"action", "event", * EXCLUDE ("action", "event") REPLACE (f("time") AS "time")`},
		// time listed: its normalizing expression takes time's slot, and the
		// star must not replace a column it excludes.
		{"leading with time", []string{"action", "time", "event"}, false, timeExpr,
			`"action", f("time") AS "time", "event", * EXCLUDE ("action", "time", "event")`},
		{"time listed in another case", []string{"TIME"}, false, timeExpr,
			`f("time") AS "time", * EXCLUDE ("TIME")`},
		{"leading, plain", []string{"action", "time"}, false, "",
			`"action", "time", * EXCLUDE ("action", "time")`},
		// Every column listed: no star, so nothing depends on how a DuckDB
		// version treats a star that excludes everything.
		{"covers all", []string{"action", "time"}, true, timeExpr, `"action", f("time") AS "time"`},
		{"quote escaped", []string{`we"ird`}, false, "", `"we""ird", * EXCLUDE ("we""ird")`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := columnOrderSelect(tt.leading, tt.coversAll, tt.timeExpr); got != tt.want {
				t.Fatalf("columnOrderSelect(%v, %v) =\n  %s\nwant\n  %s", tt.leading, tt.coversAll, got, tt.want)
			}
		})
	}
}

// With no column order the compaction statements are byte-identical to the
// ones built before the option existed.
func TestBuildCompactionQueryColumnOrderOffIsUnchanged(t *testing.T) {
	for _, tags := range [][]string{nil, {"host"}} {
		before := buildCompactionQueryKV(`['a.parquet', 'b.parquet']`, `ORDER BY "event", "time"`, "/tmp/out.parquet", tags, "ci")
		after := buildCompactionQueryLayout(`['a.parquet', 'b.parquet']`, `ORDER BY "event", "time"`, "/tmp/out.parquet", tags, "ci", nil, false)
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("tags %v: statements changed with no column order:\n%v\nvs\n%v", tags, before, after)
		}
	}
}

func TestManagerGetColumnOrder(t *testing.T) {
	m := NewManager(&ManagerConfig{
		StorageBackend:    newColumnOrderLocal(t),
		TempDirectory:     t.TempDir(),
		ColumnOrderConfig: map[string][]string{"events": {"action", "event"}},
		Logger:            zerolog.Nop(),
	})
	if got := m.GetColumnOrder("events"); !reflect.DeepEqual(got, []string{"action", "event"}) {
		t.Fatalf("GetColumnOrder(events) = %v", got)
	}
	// Unlisted measurement: no order (the old layout), never a default list.
	if got := m.GetColumnOrder("cpu"); got != nil {
		t.Fatalf("GetColumnOrder(cpu) = %v, want nil", got)
	}
}

// The Manager puts the measurement's column order into every subprocess job
// config, the JSON carries it, and the subprocess hands it to the job.
func TestSubprocessJobConfigCarriesColumnOrder(t *testing.T) {
	m := NewManager(&ManagerConfig{
		StorageBackend:    newColumnOrderLocal(t),
		TempDirectory:     t.TempDir(),
		ColumnOrderConfig: map[string][]string{"events": {"action", "event"}},
		Logger:            zerolog.Nop(),
	})
	cfg := m.subprocessJobConfig(Candidate{Database: "posthog", Measurement: "events", PartitionPath: "posthog/events/2026/11/10", Tier: "daily"}, "job")
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var back SubprocessJobConfig
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	jc := jobConfigFromSubprocess(&back, nil, nil, nil, zerolog.Nop())
	if !reflect.DeepEqual(jc.ColumnOrder, []string{"action", "event"}) {
		t.Fatalf("job config ColumnOrder = %v after the subprocess round trip", jc.ColumnOrder)
	}
	if got := NewJob(jc).ColumnOrder; !reflect.DeepEqual(got, []string{"action", "event"}) {
		t.Fatalf("job ColumnOrder = %v", got)
	}

	// A measurement without an order sends none: an older subprocess and a
	// newer one then build the same statements.
	cfg = m.subprocessJobConfig(Candidate{Database: "posthog", Measurement: "cpu", PartitionPath: "posthog/cpu/2026/11/10", Tier: "daily"}, "job")
	data, _ = json.Marshal(cfg)
	var raw map[string]any
	_ = json.Unmarshal(data, &raw)
	if _, ok := raw["column_order"]; ok {
		t.Fatalf("subprocess JSON carries column_order for a measurement without one: %s", data)
	}
}

func newColumnOrderLocal(t *testing.T) storage.Backend {
	t.Helper()
	b, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}
