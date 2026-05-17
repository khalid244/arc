package api

import (
	"context"
	"database/sql"
	"time"

	"github.com/basekick-labs/arc/internal/rollup/tiered"
	"github.com/rs/zerolog"
)

// TieredRefresher polls SpecStore + ManifestStore for a configured set
// of tables and updates the QueryHandler's tiered deps so the router
// always sees fresh watermarks. Runs in a background goroutine.
//
// Lifecycle: NewTieredRefresher → Start(ctx) → ctx.Cancel() to stop.
type TieredRefresher struct {
	Handler       *QueryHandler
	DB            *sql.DB
	SpecStore     *tiered.SpecStore
	ManifestStore *tiered.ManifestStore
	Tables        []string
	Interval      time.Duration
	DimRichCap    int
	GraceWindow   time.Duration
	Logger        zerolog.Logger
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

// refresh iterates the configured tables and for each loads Spec and
// Manifest from their respective stores, constructs a fresh RewriteDeps,
// and installs it via SetTieredDeps. On any per-table error, logs and
// skips that table — other tables continue.
func (r *TieredRefresher) refresh(ctx context.Context) {
	for _, table := range r.Tables {
		spec, err := r.SpecStore.Get(ctx, table)
		if err != nil {
			r.Logger.Debug().Str("table", table).Err(err).Msg("tiered spec unavailable; skipping")
			r.Handler.SetTieredDeps(table, nil)
			continue
		}
		m, err := r.ManifestStore.Get(ctx, table)
		if err != nil {
			r.Logger.Debug().Str("table", table).Err(err).Msg("tiered manifest unavailable; skipping")
			r.Handler.SetTieredDeps(table, nil)
			continue
		}
		s := spec
		deps := &tiered.RewriteDeps{
			DB:          r.DB,
			Manifest:    m,
			Spec:        &s,
			DimRichCap:  r.DimRichCap,
			GraceWindow: r.GraceWindow,
		}
		r.Handler.SetTieredDeps(table, deps)
	}
}
