package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/rollup"
	"github.com/basekick-labs/arc/internal/rollup/tiered"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func runPrecalcCLI(args []string) {
	if len(args) == 0 {
		printPrecalcHelp()
		return
	}
	switch args[0] {
	case "backfill":
		runPrecalcBackfill(context.Background(), args[1:])
	case "status":
		runPrecalcStatus(context.Background(), args[1:])
	case "help", "-h", "--help":
		printPrecalcHelp()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n\n", args[0])
		printPrecalcHelp()
		os.Exit(2)
	}
}

func printPrecalcHelp() {
	fmt.Println("Usage: arc precalc <subcommand> [args]")
	fmt.Println("")
	fmt.Println("Subcommands:")
	fmt.Println("  backfill <table>   Classify the table and build all tiers + variants from the full source range")
	fmt.Println("  status <table>     Print per-(tier, variant) watermark, file count, and total bytes")
	fmt.Println("")
}

func runPrecalcBackfill(ctx context.Context, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: arc precalc backfill <table> [--source <glob>] [--time-col <col>]")
		os.Exit(2)
	}
	table := args[0]
	sourceGlob := ""
	timeCol := "time"
	for i := 1; i < len(args); i++ {
		switch args[i] {
		case "--source":
			if i+1 < len(args) {
				sourceGlob = args[i+1]
				i++
			}
		case "--time-col":
			if i+1 < len(args) {
				timeCol = args[i+1]
				i++
			}
		}
	}
	if sourceGlob == "" {
		fmt.Fprintln(os.Stderr, "--source <glob> is required for backfill")
		fmt.Fprintln(os.Stderr, "  example: arc precalc backfill mydb.events --source 'mydb/events/**/*.parquet'")
		os.Exit(2)
	}

	cfg, viperInstance, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	rcfg, err := rollup.ParseConfig(viperInstance)
	if err != nil {
		fmt.Fprintf(os.Stderr, "rollup config: %v\n", err)
		os.Exit(1)
	}
	tieredCfg := rollup.ConvertConfig(rcfg)
	tieredCfg.Defaults()

	backend, err := precalcInitStorage(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "storage init: %v\n", err)
		os.Exit(1)
	}

	tz := tieredCfg.TZ
	if tz == "" {
		tz = "UTC"
	}
	db, err := tiered.OpenWithDataSketches(tz)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open duckdb: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if strings.HasPrefix(sourceGlob, "s3://") {
		if err := precalcConfigureS3(db, cfg); err != nil {
			fmt.Fprintf(os.Stderr, "configure s3: %v\n", err)
			os.Exit(1)
		}
	}

	srcExpr := fmt.Sprintf("read_parquet('%s', union_by_name=true)", escapeSQLLit(sourceGlob))

	specStore := tiered.NewSpecStore(backend)
	manifestStore := tiered.NewManifestStore(backend)

	spec, err := specStore.Get(ctx, table)
	if err != nil {
		fmt.Fprintf(os.Stderr, "spec not found for %s; run classify first or provide a spec.json\n", table)
		os.Exit(1)
	}
	if spec.TimeColumn != "" {
		timeCol = spec.TimeColumn
	}

	var srcMin, srcMax time.Time
	row := db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", timeCol, timeCol, srcExpr))
	if err := row.Scan(&srcMin, &srcMax); err != nil {
		fmt.Fprintf(os.Stderr, "query source range: %v\n", err)
		os.Exit(1)
	}
	if srcMin.IsZero() || srcMax.IsZero() {
		fmt.Fprintln(os.Stderr, "source is empty or has no time values")
		os.Exit(1)
	}

	publisher := &tiered.Publisher{
		DB:             db,
		Backend:        backend,
		Manifests:      manifestStore,
		BuilderVersion: Version,
		HLLLgK:         tieredCfg.HLLLgK,
		KLLk:           tieredCfg.KLLk,
		LocalTmpDir:    os.TempDir(),
	}

	loc, err := time.LoadLocation(tz)
	if err != nil {
		loc = time.UTC
	}

	buildArgs := tiered.BuildArgs{
		Source:     srcExpr,
		TimeColumn: timeCol,
	}

	plans := buildVariantPlans(&spec, tieredCfg.DimRichCap)

	fmt.Printf("backfill: table=%s tier=1h variants=%d source_range=[%s, %s]\n",
		table,
		len(plans),
		srcMin.In(loc).Format(time.RFC3339),
		srcMax.In(loc).Format(time.RFC3339),
	)

	bucket := truncToHour(srcMin, loc)
	total := 0
	errors := 0
	for !bucket.After(srcMax) {
		bucketEnd := bucket.Add(time.Hour)
		args := buildArgs
		args.Tier = tiered.Tier1h

		for _, plan := range plans {
			var pubErr error
			switch {
			case plan.Variant == "sketch":
				pubErr = publisher.PublishSketchVariant(ctx, table, &spec, args, tiered.Tier1h, plan.Variant, bucket, bucketEnd)
			case plan.Dim != "":
				pubErr = publisher.PublishPerDimVariant(ctx, table, &spec, args, plan.Dim, tiered.Tier1h, bucket, bucketEnd)
			case plan.Variant == "all":
				pubErr = publisher.PublishDimRichVariant(ctx, table, &spec, args, tieredCfg.DimRichCap, tiered.Tier1h, bucket, bucketEnd)
			}
			if pubErr != nil {
				fmt.Fprintf(os.Stderr, "  ERROR %s/%s %s: %v\n", tiered.Tier1h, plan.Variant, bucket.Format(time.RFC3339), pubErr)
				errors++
			} else {
				total++
			}
		}
		fmt.Printf("  1h %s done\n", bucket.In(loc).Format("2006-01-02 15:04"))
		bucket = bucketEnd
	}

	fmt.Printf("backfill complete: %d files written, %d errors\n", total, errors)
	if errors > 0 {
		os.Exit(1)
	}
}

func runPrecalcStatus(ctx context.Context, args []string) {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: arc precalc status <table>")
		os.Exit(2)
	}
	table := args[0]

	cfg, _, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	backend, err := precalcInitStorage(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "storage init: %v\n", err)
		os.Exit(1)
	}

	manifestStore := tiered.NewManifestStore(backend)
	m, err := manifestStore.Get(ctx, table)
	if err != nil {
		fmt.Fprintf(os.Stderr, "manifest not found for %s: %v\n", table, err)
		os.Exit(1)
	}

	type key struct{ tier, variant string }
	type stats struct {
		watermark time.Time
		files     int
	}
	seen := map[key]*stats{}
	for _, e := range m.Entries {
		if e.Obsolete {
			continue
		}
		_, tier, variant, _, _, ok := tiered.ParseVariantPath(e.Path)
		if !ok {
			continue
		}
		k := key{tier, variant}
		if _, ok := seen[k]; !ok {
			seen[k] = &stats{}
		}
		seen[k].files++
	}
	for k := range seen {
		seen[k].watermark = m.Watermark(k.tier, k.variant)
	}

	tierOrder := map[string]int{"1h": 0, "1d": 1, "1w": 2, "1mo": 3}
	type row struct {
		tier, variant string
		watermark     time.Time
		files         int
	}
	var rows []row
	for k, s := range seen {
		rows = append(rows, row{k.tier, k.variant, s.watermark, s.files})
	}
	sort.Slice(rows, func(i, j int) bool {
		oi, oj := tierOrder[rows[i].tier], tierOrder[rows[j].tier]
		if oi != oj {
			return oi < oj
		}
		return rows[i].variant < rows[j].variant
	})

	fmt.Printf("%-6s  %-20s  %-24s  %s\n", "Tier", "Variant", "Watermark", "Files")
	fmt.Println(strings.Repeat("-", 65))
	for _, r := range rows {
		wm := "(none)"
		if !r.watermark.IsZero() {
			wm = r.watermark.UTC().Format("2006-01-02 15:04 UTC")
		}
		fmt.Printf("%-6s  %-20s  %-24s  %d\n", r.tier, r.variant, wm, r.files)
	}
}

func precalcInitStorage(cfg *config.Config) (storage.Backend, error) {
	log := zerolog.New(os.Stderr).With().Timestamp().Logger()
	switch cfg.Storage.Backend {
	case "local", "":
		return storage.NewLocalBackend(cfg.Storage.LocalPath, log)
	case "s3", "minio":
		s3Cfg := &storage.S3Config{
			Bucket:    cfg.Storage.S3Bucket,
			Region:    cfg.Storage.S3Region,
			Endpoint:  cfg.Storage.S3Endpoint,
			AccessKey: cfg.Storage.S3AccessKey,
			SecretKey: cfg.Storage.S3SecretKey,
			UseSSL:    cfg.Storage.S3UseSSL,
			PathStyle: cfg.Storage.S3PathStyle,
			Prefix:    cfg.Storage.S3Prefix,
		}
		return storage.NewS3Backend(s3Cfg, log)
	default:
		return nil, fmt.Errorf("unsupported storage backend %q", cfg.Storage.Backend)
	}
}

func precalcConfigureS3(db *sql.DB, cfg *config.Config) error {
	for _, stmt := range []string{"INSTALL httpfs", "LOAD httpfs"} {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	esc := func(s string) string { return strings.ReplaceAll(s, "'", "''") }
	stmts := []string{
		fmt.Sprintf("SET GLOBAL s3_access_key_id='%s'", esc(cfg.Storage.S3AccessKey)),
		fmt.Sprintf("SET GLOBAL s3_secret_access_key='%s'", esc(cfg.Storage.S3SecretKey)),
	}
	if cfg.Storage.S3Region != "" {
		stmts = append(stmts, fmt.Sprintf("SET GLOBAL s3_region='%s'", esc(cfg.Storage.S3Region)))
	}
	if cfg.Storage.S3Endpoint != "" {
		ep := cfg.Storage.S3Endpoint
		ep = strings.TrimPrefix(strings.TrimPrefix(ep, "https://"), "http://")
		stmts = append(stmts, fmt.Sprintf("SET GLOBAL s3_endpoint='%s'", esc(ep)))
	}
	urlStyle := "vhost"
	if cfg.Storage.S3PathStyle {
		urlStyle = "path"
	}
	stmts = append(stmts, fmt.Sprintf("SET GLOBAL s3_url_style='%s'", urlStyle))
	useSSL := "true"
	if !cfg.Storage.S3UseSSL {
		useSSL = "false"
	}
	stmts = append(stmts, fmt.Sprintf("SET GLOBAL s3_use_ssl=%s", useSSL))
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return nil
}

func buildVariantPlans(spec *tiered.Spec, dimRichCap int) []variantPlan {
	out := []variantPlan{{Variant: "sketch"}}
	var dimNames []string
	for name, d := range spec.Dims {
		if (d.Role == "Dim" || d.Role == "PerDim") && len(d.KeptValues) > 0 {
			dimNames = append(dimNames, name)
		}
	}
	sort.Strings(dimNames)
	for _, name := range dimNames {
		out = append(out, variantPlan{Variant: "by_" + name, Dim: name})
	}
	for _, d := range spec.Dims {
		if d.Role == "Dim" && d.EffectiveCard <= dimRichCap {
			out = append(out, variantPlan{Variant: "all"})
			break
		}
	}
	return out
}

type variantPlan struct {
	Variant string
	Dim     string
}

func truncToHour(t time.Time, loc *time.Location) time.Time {
	t = t.In(loc)
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, loc)
}

func escapeSQLLit(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
