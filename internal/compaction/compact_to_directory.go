package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// compactToDirectory runs the compaction COPY against an output DIRECTORY
// using DuckDB's FILE_SIZE_BYTES + FILENAME_PATTERN so the merged data
// is rolled over into multiple bounded-size parquets. Returns the local
// paths of every produced output, sorted by filename for deterministic
// downstream upload order.
//
// Caller responsibilities:
//   - inputs are local file paths (caller already downloaded them)
//   - outDir exists and is empty (or at least contains no .parquet files
//     that would be mistaken for outputs)
//   - filenamePattern uses DuckDB's {uuid} / {i} placeholders so each
//     rolled output gets a unique name
//   - uploading the returned paths to durable storage and deleting source
//     inputs from durable storage is NOT done here
func compactToDirectory(
	ctx context.Context,
	db *sql.DB,
	inputs []string,
	outDir string,
	orderBy string,
	tagColumns []string,
	maxOutputBytes int64,
	filenamePattern string,
) ([]string, error) {
	if len(inputs) == 0 {
		return nil, fmt.Errorf("compactToDirectory: no inputs")
	}
	if maxOutputBytes <= 0 {
		return nil, fmt.Errorf("compactToDirectory: maxOutputBytes must be > 0")
	}
	if filenamePattern == "" {
		return nil, fmt.Errorf("compactToDirectory: filenamePattern must be set when maxOutputBytes > 0")
	}

	// Build DuckDB array literal of input paths.
	var b strings.Builder
	b.WriteByte('[')
	for i, p := range inputs {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteByte('\'')
		// minimal SQL escape (paths are caller-controlled local tempdir paths)
		b.WriteString(strings.ReplaceAll(strings.ReplaceAll(p, "\\", "\\\\"), "'", "''"))
		b.WriteByte('\'')
	}
	b.WriteByte(']')

	query := buildCompactionQuery(b.String(), orderBy, outDir, tagColumns, maxOutputBytes, filenamePattern)
	if _, err := db.ExecContext(ctx, query); err != nil {
		return nil, fmt.Errorf("compactToDirectory: COPY failed: %w", err)
	}

	// Walk outDir for parquet outputs.
	var outputs []string
	walkErr := filepath.Walk(outDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".parquet") {
			outputs = append(outputs, path)
		}
		return nil
	})
	if walkErr != nil {
		return nil, fmt.Errorf("compactToDirectory: walk output dir: %w", walkErr)
	}
	sort.Strings(outputs)
	return outputs, nil
}
