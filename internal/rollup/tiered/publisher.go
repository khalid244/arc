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
	MaxRetries     int // default 5
}

// PublishSketchVariant builds the sketch variant for one bucket window
// and publishes it atomically.
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

func (p *Publisher) publishWith(ctx context.Context, table string, spec *Spec,
	tier Tier, variant string, bucketLo, bucketHi time.Time,
	build func(*Builder, string) error) error {

	if p.MaxRetries == 0 {
		p.MaxRetries = 5
	}
	if p.LocalTmpDir == "" {
		p.LocalTmpDir = os.TempDir()
	}
	if err := os.MkdirAll(p.LocalTmpDir, 0o755); err != nil {
		return fmt.Errorf("mkdir tmp: %w", err)
	}

	fileID, err := randomFileID()
	if err != nil {
		return err
	}
	localOut := filepath.Join(p.LocalTmpDir, fileID+".parquet")
	defer os.Remove(localOut)

	hash, err := spec.SchemaHash()
	if err != nil {
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
		return fmt.Errorf("build %s/%s: %w", tier, variant, err)
	}

	body, err := os.ReadFile(localOut)
	if err != nil {
		return fmt.Errorf("read local parquet: %w", err)
	}
	finalPath := VariantPath(table, tier, variant, bucketLo, fileID)
	if err := p.Backend.Write(ctx, finalPath, body); err != nil {
		return fmt.Errorf("write final parquet: %w", err)
	}

	for attempt := 0; attempt < p.MaxRetries; attempt++ {
		m, err := p.Manifests.Get(ctx, table)
		if err != nil {
			m = &Manifest{Table: table, Generation: 0}
		}
		m.Add(ManifestEntry{
			Tier:           string(tier),
			Variant:        variant,
			Path:           finalPath,
			BucketLo:       bucketLo,
			BucketHi:       bucketHi,
			SchemaHash:     hash,
			BuilderVersion: p.BuilderVersion,
		})
		err = p.Manifests.Put(ctx, table, m)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrManifestStale) {
			return fmt.Errorf("manifest put: %w", err)
		}
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
