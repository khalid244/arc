package tiered

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

// Publisher wraps the builder + storage backend to publish a precalc
// variant: build the parquet into a local tmp path, then upload to the
// final S3 path. File existence in S3 is the catalog — no manifest needed.
//
// LocalTmpDir is where DuckDB writes the intermediate parquet file
// before upload (must be local fs, since DuckDB COPY can't write to S3
// directly in many setups). Defaults to os.TempDir() if empty.
type Publisher struct {
	DB             *sql.DB
	Backend        storage.Backend
	// FilesFor returns the FileIndex for a given table. Called per-publish.
	FilesFor       func(table string) FileIndex
	BuilderVersion string
	HLLLgK         int
	KLLk           int
	LocalTmpDir    string
	Metrics        MetricsSink // optional; nil = no metrics
}

// PublishSketchVariant builds the sketch variant for one bucket window
// and publishes it atomically. Always reads from raw source — 1h is the
// only tier post single-tier migration.
func (p *Publisher) PublishSketchVariant(ctx context.Context, table string, spec *Spec,
	args BuildArgs, tier Tier, variant string, bucketLo, bucketHi time.Time) error {
	return p.publishWith(ctx, table, spec, tier, variant, bucketLo, bucketHi,
		func(b *Builder, localOut string) error {
			return b.BuildSketchVariant(ctx, args, localOut)
		})
}

// PublishPerDimVariant builds a per-dim variant and publishes atomically.
func (p *Publisher) PublishPerDimVariant(ctx context.Context, table string, spec *Spec,
	args BuildArgs, dim string, tier Tier, bucketLo, bucketHi time.Time) error {
	variant := "by_" + dim
	return p.publishWith(ctx, table, spec, tier, variant, bucketLo, bucketHi,
		func(b *Builder, localOut string) error {
			return b.BuildPerDimVariant(ctx, args, spec, dim, localOut)
		})
}

// PublishDimRichVariant builds the dim-rich variant and publishes atomically.
func (p *Publisher) PublishDimRichVariant(ctx context.Context, table string, spec *Spec,
	args BuildArgs, dimRichCap int, tier Tier, bucketLo, bucketHi time.Time) error {
	return p.publishWith(ctx, table, spec, tier, "all", bucketLo, bucketHi,
		func(b *Builder, localOut string) error {
			return b.BuildDimRichVariant(ctx, args, spec, dimRichCap, localOut)
		})
}

// PublishAllVariants builds all variants for one Tier1h bucket in a single
// GROUPING SETS pass and publishes each to its canonical storage path.
// outDir is a local temporary directory the caller has created; it is removed
// after all uploads complete (or fail).
func (p *Publisher) PublishAllVariants(ctx context.Context, table string, spec *Spec, args BuildArgs, dimRichCap int, tier Tier, lo, hi time.Time) error {
	buildStart := time.Now()

	root := p.localTmpRoot()
	if err := os.MkdirAll(root, 0o755); err != nil {
		if p.Metrics != nil {
			p.Metrics.AddBuildNanos(time.Since(buildStart).Nanoseconds())
			p.Metrics.IncBuildErrors()
		}
		return fmt.Errorf("mkdir tmp root: %w", err)
	}
	tmpDir, err := os.MkdirTemp(root, "arc_allvariants_*")
	if err != nil {
		if p.Metrics != nil {
			p.Metrics.AddBuildNanos(time.Since(buildStart).Nanoseconds())
			p.Metrics.IncBuildErrors()
		}
		return fmt.Errorf("mkdir tmp: %w", err)
	}
	defer os.RemoveAll(tmpDir)

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
		BucketLo:       lo,
		BucketHi:       hi,
	}

	variantFiles, err := b.BuildAllVariants(ctx, args, spec, dimRichCap, tmpDir)
	if err != nil {
		if p.Metrics != nil {
			p.Metrics.AddBuildNanos(time.Since(buildStart).Nanoseconds())
			p.Metrics.IncBuildErrors()
		}
		return fmt.Errorf("build all variants: %w", err)
	}

	if p.Metrics != nil {
		p.Metrics.AddBuildNanos(time.Since(buildStart).Nanoseconds())
	}

	var uploadErr error
	for variant, localPath := range variantFiles {
		body, rerr := os.ReadFile(localPath)
		if rerr != nil {
			if p.Metrics != nil {
				p.Metrics.IncBuildErrors()
			}
			uploadErr = fmt.Errorf("read local parquet %s: %w", variant, rerr)
			continue
		}
		fileID, rerr := randomFileID()
		if rerr != nil {
			if p.Metrics != nil {
				p.Metrics.IncBuildErrors()
			}
			uploadErr = rerr
			continue
		}
		finalPath := VariantPath(table, tier, variant, lo, fileID)
		if werr := p.Backend.Write(ctx, finalPath, body); werr != nil {
			if p.Metrics != nil {
				p.Metrics.IncBuildErrors()
			}
			uploadErr = fmt.Errorf("write %s: %w", variant, werr)
			continue
		}
		if p.Metrics != nil {
			p.Metrics.IncBuildSuccess()
		}
	}

	return uploadErr
}

// localTmpRoot returns the directory under which tmp dirs are created.
func (p *Publisher) localTmpRoot() string {
	if p.LocalTmpDir != "" {
		return p.LocalTmpDir
	}
	return os.TempDir()
}

func (p *Publisher) publishWith(ctx context.Context, table string, spec *Spec,
	tier Tier, variant string, bucketLo, bucketHi time.Time,
	build func(*Builder, string) error) error {

	buildStart := time.Now()

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

	if p.Metrics != nil {
		p.Metrics.AddBuildNanos(time.Since(buildStart).Nanoseconds())
		p.Metrics.IncBuildSuccess()
	}
	return nil
}

func randomFileID() (string, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
}
