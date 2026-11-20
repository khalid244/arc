package config

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

// compaction.column_order: default empty (compaction keeps each batch's own
// column order), the TOML array form, the whitespace-separated environment
// form, and the startup validation of the "measurement:col1,col2" entries.
func TestCompactionColumnOrderConfig(t *testing.T) {
	t.Run("default empty", func(t *testing.T) {
		t.Chdir(t.TempDir())
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if len(cfg.Compaction.ColumnOrder) != 0 {
			t.Fatalf("default column_order = %v, want empty", cfg.Compaction.ColumnOrder)
		}
	})

	t.Run("toml array", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		toml := "[compaction]\ncolumn_order = [\"events:event,action,time\", \"downloads:site\"]\n"
		if err := os.WriteFile(dir+"/arc.toml", []byte(toml), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"events:event,action,time", "downloads:site"}
		if !reflect.DeepEqual(cfg.Compaction.ColumnOrder, want) {
			t.Fatalf("column_order = %v, want %v", cfg.Compaction.ColumnOrder, want)
		}
	})

	t.Run("env whitespace form splits measurements", func(t *testing.T) {
		t.Chdir(t.TempDir())
		t.Setenv("ARC_COMPACTION_COLUMN_ORDER", "events:event,action downloads:site")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"events:event,action", "downloads:site"}
		if !reflect.DeepEqual(cfg.Compaction.ColumnOrder, want) {
			t.Fatalf("env column_order = %v, want %v", cfg.Compaction.ColumnOrder, want)
		}
	})

	t.Run("invalid entry fails startup", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		toml := "[compaction]\ncolumn_order = [\"events\"]\n"
		if err := os.WriteFile(dir+"/arc.toml", []byte(toml), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := Load()
		if err == nil || !strings.Contains(err.Error(), "compaction.column_order") {
			t.Fatalf("Load() error = %v, want an invalid compaction.column_order error", err)
		}
	})
}

func TestParseColumnOrder(t *testing.T) {
	tests := []struct {
		name    string
		in      []string
		want    map[string][]string
		wantErr string
	}{
		{name: "empty", in: nil, want: map[string][]string{}},
		{
			name: "trims spaces",
			in:   []string{" events : event , action ,time "},
			want: map[string][]string{"events": {"event", "action", "time"}},
		},
		{
			name: "two measurements",
			in:   []string{"events:a,b", "downloads:c"},
			want: map[string][]string{"events": {"a", "b"}, "downloads": {"c"}},
		},
		{name: "no colon", in: []string{"events"}, wantErr: "expected 'measurement:col1,col2'"},
		{name: "empty measurement", in: []string{":a"}, wantErr: "empty measurement"},
		{name: "empty column", in: []string{"events:a,,b"}, wantErr: "empty column"},
		{name: "no columns", in: []string{"events:"}, wantErr: "empty column"},
		// DuckDB binds identifiers case-insensitively, so "Action" and
		// "action" name the same column: listing it twice is a typo.
		{name: "duplicate column", in: []string{"events:action,event,Action"}, wantErr: "listed twice"},
		{name: "duplicate measurement", in: []string{"events:a", "events:b"}, wantErr: "more than once"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseColumnOrder(tt.in)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ParseColumnOrder(%q) error = %v, want one containing %q", tt.in, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseColumnOrder(%q): %v", tt.in, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ParseColumnOrder(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
