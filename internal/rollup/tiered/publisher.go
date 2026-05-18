package tiered

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// Publisher wraps the builder + storage backend + manifest store to
// publish a precalc variant atomically:
//  1. Build the parquet into a local tmp path (DuckDB writes locally).
//  2. Upload the parquet bytes to the storage backend's final variant path.
//  3. Read current manifest, append entry, bump generation, write back.
//  4. On ErrManifestStale, retry from step 3 (a few times).
//
// LocalTmpDir is where DuckDB writes the intermediate parquet file
// before upload (must be local fs, since DuckDB COPY can't write to S3
// directly in many setups). Defaults to os.TempDir() if empty.
type Publisher struct {
	DB             *sql.DB
	Backend        storage.Backend
	Manifests      *ManifestStore
	BuilderVersion string
	HLLLgK         int
	KLLk           int
	LocalTmpDir    string
	MaxRetries     int         // default 5
	Metrics        MetricsSink // optional; nil = no metrics
}

// PublishSketchVariant builds the sketch variant for one bucket window
// and publishes it atomically. For tier > Tier1h it reads from the finer
// tier's precalc parquet files via the manifest rather than from raw source.
// Returns nil without building if no finer-tier files cover the window yet.
func (p *Publisher) PublishSketchVariant(ctx context.Context, table string, spec *Spec,
	args BuildArgs, tier Tier, variant string, bucketLo, bucketHi time.Time) error {

	if tier == Tier1h {
		return p.publishWith(ctx, table, spec, tier, variant, bucketLo, bucketHi,
			func(b *Builder, localOut string) error {
				return b.BuildSketchVariant(ctx, args, localOut)
			})
	}
	finer := finerTierFor(tier)
	m, err := p.Manifests.Get(ctx, table)
	if err != nil {
		m = &Manifest{Table: table}
	}
	backendPaths := m.FilesForTierVariantWindow(string(finer), variant, bucketLo, bucketHi)
	if len(backendPaths) == 0 {
		return nil
	}
	localPaths, cleanup, err := p.stageFinerTierFiles(ctx, backendPaths)
	if err != nil {
		return fmt.Errorf("stage finer-tier files: %w", err)
	}
	defer cleanup()
	rollupArgs := RollupArgs{
		TargetTier:  tier,
		SourcePaths: localPaths,
		MetricCols:  args.MetricCols,
		HLLCols:     args.HLLCols,
		KLLCols:     args.KLLCols,
	}
	return p.publishWith(ctx, table, spec, tier, variant, bucketLo, bucketHi,
		func(b *Builder, localOut string) error {
			return b.RollupSketchVariant(ctx, rollupArgs, localOut)
		})
}

// PublishPerDimVariant builds a per-dim variant and publishes atomically.
// For tier > Tier1h it reads from the finer tier's precalc parquet files.
// Returns nil without building if no finer-tier files cover the window yet.
func (p *Publisher) PublishPerDimVariant(ctx context.Context, table string, spec *Spec,
	args BuildArgs, dim string, tier Tier, bucketLo, bucketHi time.Time) error {

	variant := "by_" + dim
	if tier == Tier1h {
		return p.publishWith(ctx, table, spec, tier, variant, bucketLo, bucketHi,
			func(b *Builder, localOut string) error {
				return b.BuildPerDimVariant(ctx, args, spec, dim, localOut)
			})
	}
	finer := finerTierFor(tier)
	m, err := p.Manifests.Get(ctx, table)
	if err != nil {
		m = &Manifest{Table: table}
	}
	backendPaths := m.FilesForTierVariantWindow(string(finer), variant, bucketLo, bucketHi)
	if len(backendPaths) == 0 {
		return nil
	}
	localPaths, cleanup, err := p.stageFinerTierFiles(ctx, backendPaths)
	if err != nil {
		return fmt.Errorf("stage finer-tier files: %w", err)
	}
	defer cleanup()
	rollupArgs := RollupArgs{
		TargetTier:  tier,
		SourcePaths: localPaths,
		MetricCols:  args.MetricCols,
		HLLCols:     args.HLLCols,
	}
	return p.publishWith(ctx, table, spec, tier, variant, bucketLo, bucketHi,
		func(b *Builder, localOut string) error {
			return b.RollupPerDimVariant(ctx, rollupArgs, dim, localOut)
		})
}

// PublishDimRichVariant builds the dim-rich variant and publishes atomically.
// For tier > Tier1h it reads from the finer tier's precalc parquet files.
// Returns nil without building if no finer-tier files cover the window yet.
func (p *Publisher) PublishDimRichVariant(ctx context.Context, table string, spec *Spec,
	args BuildArgs, dimRichCap int, tier Tier, bucketLo, bucketHi time.Time) error {

	if tier == Tier1h {
		return p.publishWith(ctx, table, spec, tier, "all", bucketLo, bucketHi,
			func(b *Builder, localOut string) error {
				return b.BuildDimRichVariant(ctx, args, spec, dimRichCap, localOut)
			})
	}
	finer := finerTierFor(tier)
	m, err := p.Manifests.Get(ctx, table)
	if err != nil {
		m = &Manifest{Table: table}
	}
	backendPaths := m.FilesForTierVariantWindow(string(finer), "all", bucketLo, bucketHi)
	if len(backendPaths) == 0 {
		return nil
	}
	localPaths, cleanup, err := p.stageFinerTierFiles(ctx, backendPaths)
	if err != nil {
		return fmt.Errorf("stage finer-tier files: %w", err)
	}
	defer cleanup()
	rollupArgs := RollupArgs{
		TargetTier:  tier,
		SourcePaths: localPaths,
		MetricCols:  args.MetricCols,
	}
	return p.publishWith(ctx, table, spec, tier, "all", bucketLo, bucketHi,
		func(b *Builder, localOut string) error {
			return b.RollupDimRichVariant(ctx, rollupArgs, spec, dimRichCap, localOut)
		})
}

// stageFinerTierFiles downloads each backend path to a local temp file so
// that DuckDB can read them via read_parquet(). Returns the local paths and
// a cleanup func that removes the staged files.
func (p *Publisher) stageFinerTierFiles(ctx context.Context, backendPaths []string) ([]string, func(), error) {
	tmpDir := p.LocalTmpDir
	if tmpDir == "" {
		tmpDir = os.TempDir()
	}
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return nil, func() {}, fmt.Errorf("mkdir stage dir: %w", err)
	}
	locals := make([]string, 0, len(backendPaths))
	for _, bp := range backendPaths {
		id, err := randomFileID()
		if err != nil {
			for _, l := range locals {
				os.Remove(l)
			}
			return nil, func() {}, err
		}
		local := filepath.Join(tmpDir, "stage_"+id+".parquet")
		data, err := p.Backend.Read(ctx, bp)
		if err != nil {
			for _, l := range locals {
				os.Remove(l)
			}
			return nil, func() {}, fmt.Errorf("read backend path %s: %w", bp, err)
		}
		if err := os.WriteFile(local, data, 0o644); err != nil {
			for _, l := range locals {
				os.Remove(l)
			}
			return nil, func() {}, fmt.Errorf("write staged file: %w", err)
		}
		locals = append(locals, local)
	}
	cleanup := func() {
		for _, l := range locals {
			os.Remove(l)
		}
	}
	return locals, cleanup, nil
}

func (p *Publisher) publishWith(ctx context.Context, table string, spec *Spec,
	tier Tier, variant string, bucketLo, bucketHi time.Time,
	build func(*Builder, string) error) error {

	buildStart := time.Now()

	if p.MaxRetries == 0 {
		p.MaxRetries = 5
	}
	if p.LocalTmpDir == "" {
		p.LocalTmpDir = os.TempDir()
	}
	if err := os.MkdirAll(p.LocalTmpDir, 0o755); err != nil {
		if p.Metrics != nil {
			p.Metrics.AddBuildNanos(time.Since(buildStart).Nanoseconds())
			p.Metrics.IncBuildErrors()
		}
		return fmt.Errorf("mkdir tmp: %w", err)
	}

	fileID, err := randomFileID()
	if err != nil {
		if p.Metrics != nil {
			p.Metrics.AddBuildNanos(time.Since(buildStart).Nanoseconds())
			p.Metrics.IncBuildErrors()
		}
		return err
	}
	localOut := filepath.Join(p.LocalTmpDir, fileID+".parquet")
	defer os.Remove(localOut)

	hash, err := spec.SchemaHash()
	if err != nil {
		if p.Metrics != nil {
			p.Metrics.AddBuildNanos(time.Since(buildStart).Nanoseconds())
			p.Metrics.IncBuildErrors()
		}
		return fmt.Errorf("compute schema hash: %w", err)
	}

	b := &Builder{
		DB:             p.DB,
		HLLLgK:         p.HLLLgK,
		KLLk:           p.KLLk,
		SchemaHash:     hash,
		TierTZ:         spec.TZ,
		BuilderVersion: p.BuilderVersion,
		BucketLo:       bucketLo,
		BucketHi:       bucketHi,
	}
	if err := build(b, localOut); err != nil {
		if p.Metrics != nil {
			p.Metrics.AddBuildNanos(time.Since(buildStart).Nanoseconds())
			p.Metrics.IncBuildErrors()
		}
		return fmt.Errorf("build %s/%s: %w", tier, variant, err)
	}

	body, err := os.ReadFile(localOut)
	if err != nil {
		if p.Metrics != nil {
			p.Metrics.AddBuildNanos(time.Since(buildStart).Nanoseconds())
			p.Metrics.IncBuildErrors()
		}
		return fmt.Errorf("read local parquet: %w", err)
	}
	finalPath := VariantPath(table, tier, variant, bucketLo, fileID)
	if err := p.Backend.Write(ctx, finalPath, body); err != nil {
		if p.Metrics != nil {
			p.Metrics.AddBuildNanos(time.Since(buildStart).Nanoseconds())
			p.Metrics.IncBuildErrors()
		}
		return fmt.Errorf("write final parquet: %w", err)
	}

	for attempt := 0; attempt < p.MaxRetries; attempt++ {
		m, err := p.Manifests.Get(ctx, table)
		if err != nil {
			m = &Manifest{Table: table, Generation: 0}
		}
		m.Add(ManifestEntry{
			Path:       finalPath,
			SchemaHash: hash,
		})
		err = p.Manifests.Put(ctx, table, m)
		if err == nil {
			if p.Metrics != nil {
				p.Metrics.AddBuildNanos(time.Since(buildStart).Nanoseconds())
				p.Metrics.IncBuildSuccess()
			}
			return nil
		}
		if !errors.Is(err, ErrManifestStale) {
			if p.Metrics != nil {
				p.Metrics.AddBuildNanos(time.Since(buildStart).Nanoseconds())
				p.Metrics.IncBuildErrors()
			}
			return fmt.Errorf("manifest put: %w", err)
		}
	}
	if p.Metrics != nil {
		p.Metrics.AddBuildNanos(time.Since(buildStart).Nanoseconds())
		p.Metrics.IncBuildErrors()
	}
	return fmt.Errorf("manifest put: exhausted %d retries", p.MaxRetries)
}

func randomFileID() (string, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}
