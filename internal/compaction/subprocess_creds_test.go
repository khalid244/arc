package compaction

import (
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
)

// credFakeBackend is a storage.Backend (via embedding) that only implements the
// S3 credential accessors — enough to exercise credential extraction.
type credFakeBackend struct {
	storage.Backend
	ak, sk string
}

func (f credFakeBackend) GetAccessKey() string { return f.ak }
func (f credFakeBackend) GetSecretKey() string { return f.sk }

// unwrapFakeBackend mimics a resilience/caching wrapper: it hides the concrete
// backend behind Unwrap(), exactly like *storage.ResilientBackend does.
type unwrapFakeBackend struct {
	storage.Backend
	inner storage.Backend
}

func (f unwrapFakeBackend) Unwrap() storage.Backend { return f.inner }

func envHas(env []string, kv string) bool {
	for _, e := range env {
		if e == kv {
			return true
		}
	}
	return false
}

func TestBuildStorageCredentialEnv_BareS3(t *testing.T) {
	env := buildStorageCredentialEnv(credFakeBackend{ak: "AK", sk: "SK"})
	if !envHas(env, "AWS_ACCESS_KEY_ID=AK") || !envHas(env, "AWS_SECRET_ACCESS_KEY=SK") {
		t.Fatalf("bare S3 creds not extracted: %v", env)
	}
}

// TestBuildStorageCredentialEnv_WrappedS3 is the regression for the subprocess
// IMDS-auth failure: in production the manager is handed a *ResilientBackend,
// so a concrete *S3Backend type assertion misses it and the compaction
// subprocess gets NO credentials -> falls back to the EC2 IMDS chain -> fails.
// Credentials must be forwarded through the wrapper.
func TestBuildStorageCredentialEnv_WrappedS3(t *testing.T) {
	wrapped := unwrapFakeBackend{inner: credFakeBackend{ak: "AK", sk: "SK"}}
	env := buildStorageCredentialEnv(wrapped)
	if !envHas(env, "AWS_ACCESS_KEY_ID=AK") || !envHas(env, "AWS_SECRET_ACCESS_KEY=SK") {
		t.Fatalf("creds not forwarded through wrapper (subprocess would hit IMDS): %v", env)
	}
}

// Double-wrapped chains must still resolve.
func TestBuildStorageCredentialEnv_DoubleWrapped(t *testing.T) {
	chain := unwrapFakeBackend{inner: unwrapFakeBackend{inner: credFakeBackend{ak: "AK", sk: "SK"}}}
	env := buildStorageCredentialEnv(chain)
	if !envHas(env, "AWS_ACCESS_KEY_ID=AK") {
		t.Fatalf("creds not resolved through nested wrappers: %v", env)
	}
}

func TestBuildStorageCredentialEnv_EmptyCredsNoEnv(t *testing.T) {
	if env := buildStorageCredentialEnv(credFakeBackend{}); len(env) != 0 {
		t.Fatalf("expected no env for empty creds, got %v", env)
	}
}
