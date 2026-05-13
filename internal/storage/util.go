package storage

import "github.com/rs/zerolog"

// unwrapBackend returns the concrete backend, unwrapping ResilientBackend if present.
func unwrapBackend(backend Backend) Backend {
	if rb, ok := backend.(*ResilientBackend); ok {
		return rb.Unwrap()
	}
	return backend
}

// GetLocalBasePath returns the base filesystem path for local storage backends.
// For cloud backends (S3, Azure), it logs a warning and returns empty string.
// For unknown backends, it returns the provided fallback path.
//
// Parameters:
//   - backend: The storage backend to check
//   - logger: Logger for warnings about unsupported backends (can be nil)
//   - feature: Feature name for warning messages (e.g., "Continuous queries", "Retention")
//   - fallback: Default path to return for unknown backend types (use "" to disable)
func GetLocalBasePath(backend Backend, logger *zerolog.Logger, feature string, fallback string) string {
	switch b := unwrapBackend(backend).(type) {
	case *LocalBackend:
		return b.GetBasePath()
	case *S3Backend:
		if logger != nil {
			logger.Warn().Msgf("%s not fully supported for S3 backend yet", feature)
		}
		return ""
	case *AzureBlobBackend:
		if logger != nil {
			logger.Warn().Msgf("%s not fully supported for Azure backend yet", feature)
		}
		return ""
	default:
		return fallback
	}
}

// GetFullPath converts a relative storage key to a full path including protocol prefix.
// For S3: "db/m/file.parquet" -> "s3://bucket/db/m/file.parquet"
func GetFullPath(backend Backend, key string) string {
	switch b := unwrapBackend(backend).(type) {
	case *S3Backend:
		return "s3://" + b.GetBucket() + "/" + key
	case *AzureBlobBackend:
		return "azure://" + b.GetContainer() + "/" + key
	case *LocalBackend:
		return b.GetBasePath() + "/" + key
	default:
		return "./data/" + key
	}
}

// GetStoragePath returns the full storage path for a database/measurement with glob pattern.
// Supports all storage backends: local, S3, and Azure.
func GetStoragePath(backend Backend, database, measurement string) string {
	switch b := unwrapBackend(backend).(type) {
	case *S3Backend:
		return "s3://" + b.GetBucket() + "/" + b.GetPrefix() + database + "/" + measurement + "/**/*.parquet"
	case *AzureBlobBackend:
		return "azure://" + b.GetContainer() + "/" + database + "/" + measurement + "/**/*.parquet"
	case *LocalBackend:
		return b.GetBasePath() + "/" + database + "/" + measurement + "/**/*.parquet"
	default:
		return "./data/" + database + "/" + measurement + "/**/*.parquet"
	}
}

// GetPartitionGlob returns a read_parquet glob anchored to ONE partition
// directory (single-level glob, no `**`). partitionKey is a relative storage
// key like "default/downloads/2026/05/13/06" — caller is responsible for
// picking it. Used by schema inference to avoid bucket-wide `**/*.parquet`
// scans that hit thousands of files and race with ingest deletions.
func GetPartitionGlob(backend Backend, partitionKey string) string {
	switch b := unwrapBackend(backend).(type) {
	case *S3Backend:
		return "s3://" + b.GetBucket() + "/" + b.GetPrefix() + partitionKey + "/*.parquet"
	case *AzureBlobBackend:
		return "azure://" + b.GetContainer() + "/" + partitionKey + "/*.parquet"
	case *LocalBackend:
		return b.GetBasePath() + "/" + partitionKey + "/*.parquet"
	default:
		return "./data/" + partitionKey + "/*.parquet"
	}
}

// GetRollupStoragePath returns the read_parquet glob for rollup output at
// _arc/rollup/<storagePath>/dt=*/window_*.parquet, where storagePath comes
// from spec.StoragePath() (e.g. "default/events/all/1d").
func GetRollupStoragePath(backend Backend, storagePath string) string {
	switch b := unwrapBackend(backend).(type) {
	case *S3Backend:
		return "s3://" + b.GetBucket() + "/" + b.GetPrefix() + "_arc/rollup/" + storagePath + "/**/*.parquet"
	case *AzureBlobBackend:
		return "azure://" + b.GetContainer() + "/_arc/rollup/" + storagePath + "/**/*.parquet"
	case *LocalBackend:
		return b.GetBasePath() + "/_arc/rollup/" + storagePath + "/**/*.parquet"
	default:
		return "./data/_arc/rollup/" + storagePath + "/**/*.parquet"
	}
}
