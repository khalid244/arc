// One-shot S3 delete using Arc's storage backend. Reads S3 config from
// environment variables (same as the compactor pod uses). Argv is the
// key(s) to delete, relative to the bucket prefix.
//
//	go run ./cmd/s3rm posthog/events/2026/05/06/events_..._daily.parquet
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: s3rm <key> [<key>...]")
		os.Exit(2)
	}
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	cfg := &storage.S3Config{
		Bucket:    os.Getenv("ARC_STORAGE_S3_BUCKET"),
		Region:    os.Getenv("ARC_STORAGE_S3_REGION"),
		Endpoint:  os.Getenv("ARC_STORAGE_S3_ENDPOINT"),
		AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		PathStyle: strings.EqualFold(os.Getenv("ARC_STORAGE_S3_PATH_STYLE"), "true"),
		UseSSL:    !strings.EqualFold(os.Getenv("ARC_STORAGE_S3_USE_SSL"), "false"),
	}
	if cfg.Bucket == "" || cfg.AccessKey == "" || cfg.SecretKey == "" {
		fmt.Fprintln(os.Stderr, "missing required env: ARC_STORAGE_S3_BUCKET / AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY")
		os.Exit(2)
	}

	backend, err := storage.NewS3Backend(cfg, logger)
	if err != nil {
		fmt.Fprintln(os.Stderr, "new backend:", err)
		os.Exit(1)
	}
	defer backend.Close()

	ctx := context.Background()
	exit := 0
	for _, k := range os.Args[1:] {
		size, _ := backend.StatFile(ctx, k)
		if size < 0 {
			fmt.Printf("SKIP (not found): %s\n", k)
			continue
		}
		fmt.Printf("DELETE: %s  (%d bytes)\n", k, size)
		if err := backend.Delete(ctx, k); err != nil {
			fmt.Fprintf(os.Stderr, "  err: %v\n", err)
			exit = 1
			continue
		}
		fmt.Printf("  OK\n")
	}
	os.Exit(exit)
}
