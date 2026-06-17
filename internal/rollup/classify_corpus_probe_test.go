package rollup

import (
	"fmt"
	"testing"
)

// TestProbe_DownloadsCorpusResponse confirms, on the REAL downloads corpus in local
// MinIO, that (a) response is a DOUBLE (the prod root cause) and (b) the classifier
// fix now registers it as a dimension while keeping it a metric. Diagnostic — logs the
// profile so we can see Metrics/DimCard/ForcedMetrics on real data.
func TestProbe_DownloadsCorpusResponse(t *testing.T) {
	db := openTestDuck(t)
	defer db.Close()
	requireCorpus(t, db)

	readExpr := fmt.Sprintf("read_parquet('%s', union_by_name=true)", testDayGlob)

	var rt, st string
	if err := db.QueryRow("SELECT typeof(response), typeof(site) FROM " + readExpr + " WHERE response IS NOT NULL LIMIT 1").Scan(&rt, &st); err != nil {
		t.Fatalf("typeof probe: %v", err)
	}
	t.Logf("typeof(response)=%s  typeof(site)=%s", rt, st)

	cfg := ClassifyConfig{MaxDimCard: 8192, MaxPerDimCard: 50000} // prod values
	p, err := ProfileTable(db, "default.downloads", "time", "hour", readExpr, cfg)
	if err != nil {
		t.Fatalf("ProfileTable: %v", err)
	}
	t.Logf("Metrics       = %v", p.Metrics)
	t.Logf("DimCard       = %v", p.DimCard)
	t.Logf("ForcedMetrics = %v", p.ForcedMetrics)

	_, respIsDim := p.DimCard["response"]
	respIsMetric := false
	for _, m := range p.Metrics {
		if m == "response" {
			respIsMetric = true
		}
	}
	t.Logf("response: dim=%v metric=%v", respIsDim, respIsMetric)
	if !respIsDim {
		t.Errorf("expected response to be a dimension after the fix (DimCard=%v)", p.DimCard)
	}
	if !respIsMetric {
		t.Errorf("expected response to ALSO remain a metric (Metrics=%v)", p.Metrics)
	}
}
