package api

import (
	"context"
	"database/sql"
	"time"

	"github.com/basekick-labs/arc/internal/rollup/tiered"
	"github.com/rs/zerolog"
)

// TieredRefresher polls SpecStore for a configured set of tables and updates
// the QueryHandler's tiered deps so the router always sees the latest spec.
// File existence in S3 is the source of truth for watermarks — no manifest polling.
//
// Lifecycle: NewTieredRefresher → Start(ctx) → ctx.Cancel() to stop.
type TieredRefresher struct {
	Handler     *QueryHandler
	DB          *sql.DB
	SpecStore   *tiered.SpecStore
	// FilesFor returns the FileIndex for a given table name. Production supplies
	// an *S3FileIndex; tests may inject a MemoryFileIndex.
	FilesFor    func(table string) tiered.FileIndex
	Tables      []string
	Interval    time.Duration
	DimRichCap  int
	GraceWindow time.Duration
	Metrics     tiered.MetricsSink // optional; passed into each RewriteDeps
	Logger      zerolog.Logger
	// StoragePrefix is prepended to every parquet path in router-emitted
	// read_parquet calls (e.g. "s3://hammel-arc/"). The query handler needs
	// this because S3FileIndex returns bucket-relative keys.
	StoragePrefix string
}

// Start runs the refresh loop. Returns immediately; loop runs in a
// goroutine. Refresh fires immediately on Start, then on Interval.
func (r *TieredRefresher) Start(ctx context.Context) {
	if r.Interval == 0 {
		r.Interval = 30 * time.Second
	}
	if r.DimRichCap == 0 {
		r.DimRichCap = 100
	}
	if r.GraceWindow == 0 {
		r.GraceWindow = 6 * time.Hour
	}

	go func() {
		r.refresh(ctx)
		t := time.NewTicker(r.Interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				r.refresh(ctx)
			}
		}
	}()
}

// refresh iterates the configured tables and for each loads the Spec,
// constructs fresh RewriteDeps with the table's FileIndex, and installs it
// via SetTieredDeps. On any per-table error, logs and skips that table —
// other tables continue.
func (r *TieredRefresher) refresh(ctx context.Context) {
	for _, table := range r.Tables {
		spec, err := r.SpecStore.Get(ctx, table)
		if err != nil {
			r.Logger.Debug().Str("table", table).Err(err).Msg("tiered spec unavailable; skipping")
			r.Handler.SetTieredDeps(table, nil)
			continue
		}
		s := spec
		deps := &tiered.RewriteDeps{
			DB:            r.DB,
			Files:         r.FilesFor(table),
			Spec:          &s,
			DimRichCap:    r.DimRichCap,
			GraceWindow:   r.GraceWindow,
			Metrics:       r.Metrics,
			StoragePrefix: r.StoragePrefix,
		}
		r.Handler.SetTieredDeps(table, deps)
	}
}
