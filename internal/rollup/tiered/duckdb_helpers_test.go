package tiered

import (
	"testing"
)

func TestOpenWithDataSketches(t *testing.T) {
	db, err := OpenWithDataSketches("Asia/Riyadh")
	if err != nil {
		t.Fatalf("OpenWithDataSketches: %v", err)
	}
	defer db.Close()
	var tz string
	if err := db.QueryRow("SELECT current_setting('TimeZone')").Scan(&tz); err != nil {
		t.Fatal(err)
	}
	if tz != "Asia/Riyadh" {
		t.Errorf("TimeZone = %q, want Asia/Riyadh", tz)
	}
	// datasketches extension must be loaded
	var name string
	err = db.QueryRow("SELECT function_name FROM duckdb_functions() WHERE function_name = 'datasketch_hll_estimate' LIMIT 1").Scan(&name)
	if err != nil {
		t.Errorf("datasketches not loaded: %v", err)
	}
}
