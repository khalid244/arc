package rollup

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// The merge source branches hid compaction inputs silently, so an auto-mode
// query showed no sign of it in the logs while raw queries logged "Compaction
// active: excluding input files" (review 2026-11-10, item 3.2). It is logged
// with the same message prefix, and only when something is hidden.
func TestQR8_CompactionExcludesLogsTheExclusion(t *testing.T) {
	m, stg, _, dates := passFixture(t, 1)
	mm := qr8Manifests(t, stg.root)
	m.SetCompactionManifests(mm)
	var buf bytes.Buffer
	m.log = zerolog.New(&buf)

	if _, err := m.compactionExcludes(freshSrc); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Fatalf("nothing hidden, nothing logged; got %s", buf.String())
	}
	qr8Compact(t, m, mm, stg.root, dates[0], true)
	if got, err := m.compactionExcludes(freshSrc); err != nil || len(got) != 1 {
		t.Fatalf("excludes = %v, %v", got, err)
	}
	if !strings.Contains(buf.String(), "Compaction active: excluding input files") || !strings.Contains(buf.String(), `"excluded_files":1`) {
		t.Fatalf("the exclusion must be logged like the raw path's: %s", buf.String())
	}
}
