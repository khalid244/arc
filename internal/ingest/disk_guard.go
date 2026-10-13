package ingest

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// ErrIngestBackpressure is returned for a live write refused because this node
// cannot take it safely right now: its local disk (WAL + stager dirs) is above
// the hard watermark, or (with wal.reject_on_append_failure) the WAL append
// failed. Nothing was acknowledged; handlers answer 503 with Retry-After so
// the client retries. Replay and recovery paths are never refused.
var ErrIngestBackpressure = errors.New("ingest backpressure: write refused, retry later")

// Backpressure rejection reasons (arc_ingest_backpressure_rejections_total).
const (
	BackpressureReasonLocalDisk = "local_disk"
	BackpressureReasonWALAppend = "wal_append"
)

// DiskState is the local-disk watermark state.
type DiskState int32

const (
	DiskOK DiskState = iota
	DiskSoft
	DiskHard
)

func (s DiskState) String() string {
	switch s {
	case DiskSoft:
		return "soft"
	case DiskHard:
		return "hard"
	default:
		return "ok"
	}
}

// DiskGuardConfig configures a DiskGuard.
type DiskGuardConfig struct {
	// Dirs are summed: the WAL dir and both stager dirs. A disk-backed
	// emptyDir's sizeLimit is enforced by EVICTING the pod, never by ENOSPC,
	// so the app must stop growing them itself, well below the limit.
	Dirs []string
	// MaxBytes is ingest.local_disk_max_bytes; 0 disables the guard.
	MaxBytes int64
	// SoftRatio (default 0.70) warns; HardRatio (default 0.85) refuses writes.
	SoftRatio float64
	HardRatio float64
	// Interval between background measurements (default 1s).
	Interval time.Duration
	Logger   zerolog.Logger
}

// DiskGuard measures the local ingest directories and refuses live writes
// above the hard watermark (IW-02). Writes are refused BEFORE the WAL append,
// so a refused write leaves no trace and is never acknowledged-then-dropped.
type DiskGuard struct {
	cfg   DiskGuardConfig
	used  atomic.Int64
	state atomic.Int32

	startOnce sync.Once
	stopOnce  sync.Once
	stopCh    chan struct{}
	doneCh    chan struct{}
	started   atomic.Bool
}

// NewDiskGuard returns a guard; call Start for background measurement.
func NewDiskGuard(cfg DiskGuardConfig) *DiskGuard {
	if cfg.SoftRatio <= 0 {
		cfg.SoftRatio = 0.70
	}
	if cfg.HardRatio <= 0 {
		cfg.HardRatio = 0.85
	}
	if cfg.Interval <= 0 {
		cfg.Interval = time.Second
	}
	cfg.Logger = cfg.Logger.With().Str("component", "ingest-disk-guard").Logger()
	if cfg.MaxBytes > 0 {
		metrics.Get().SetIngestLocalDisk(0, cfg.MaxBytes, int64(DiskOK))
	}
	return &DiskGuard{cfg: cfg, stopCh: make(chan struct{}), doneCh: make(chan struct{})}
}

// Enabled reports whether a budget is configured.
func (g *DiskGuard) Enabled() bool { return g != nil && g.cfg.MaxBytes > 0 }

// Used is the last measured total in bytes.
func (g *DiskGuard) Used() int64 { return g.used.Load() }

// State is the last measured watermark state.
func (g *DiskGuard) State() DiskState { return DiskState(g.state.Load()) }

// Gate is installed with ArrowBuffer.SetBackpressureGate. It returns an error
// wrapping ErrIngestBackpressure above the hard watermark, nil otherwise.
func (g *DiskGuard) Gate() error {
	if !g.Enabled() || g.State() != DiskHard {
		return nil
	}
	metrics.Get().IncIngestBackpressureRejection(BackpressureReasonLocalDisk)
	return fmt.Errorf("%w: local ingest disk at %d of %d bytes (hard watermark %.0f%% of ingest.local_disk_max_bytes)",
		ErrIngestBackpressure, g.Used(), g.cfg.MaxBytes, g.cfg.HardRatio*100)
}

// Refresh measures the directories now and updates the state and metrics.
func (g *DiskGuard) Refresh() int64 {
	if !g.Enabled() {
		return 0
	}
	var total int64
	for _, dir := range g.cfg.Dirs {
		total += dirBytes(dir)
	}
	g.used.Store(total)
	next := DiskOK
	switch {
	case float64(total) >= g.cfg.HardRatio*float64(g.cfg.MaxBytes):
		next = DiskHard
	case float64(total) >= g.cfg.SoftRatio*float64(g.cfg.MaxBytes):
		next = DiskSoft
	}
	prev := DiskState(g.state.Swap(int32(next)))
	metrics.Get().SetIngestLocalDisk(total, g.cfg.MaxBytes, int64(next))
	if next != prev {
		ev := g.cfg.Logger.Info()
		switch next {
		case DiskSoft:
			ev = g.cfg.Logger.Warn()
		case DiskHard:
			ev = g.cfg.Logger.Error()
		}
		ev.Int64("used_bytes", total).Int64("max_bytes", g.cfg.MaxBytes).
			Str("from", prev.String()).Str("to", next.String()).
			Msg("Local ingest disk watermark changed (WAL + stager dirs); at 'hard' live writes are refused with 503 + Retry-After")
	}
	return total
}

// Start measures once and then every Interval until Stop.
func (g *DiskGuard) Start() {
	if !g.Enabled() {
		return
	}
	g.startOnce.Do(func() {
		g.started.Store(true)
		g.Refresh()
		go func() {
			defer close(g.doneCh)
			t := time.NewTicker(g.cfg.Interval)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					g.Refresh()
				case <-g.stopCh:
					return
				}
			}
		}()
	})
}

// Stop ends background measurement.
func (g *DiskGuard) Stop() {
	g.stopOnce.Do(func() { close(g.stopCh) })
	if g.started.Load() {
		<-g.doneCh
	}
}

// dirBytes sums the sizes of the regular files under dir (missing = 0).
func dirBytes(dir string) int64 {
	var total int64
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			if d != nil && d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.Type().IsRegular() {
			if info, err := d.Info(); err == nil {
				total += info.Size()
			}
		}
		return nil
	})
	if _, err := os.Stat(dir); err != nil {
		return 0
	}
	return total
}
