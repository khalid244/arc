package rollup

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// BuilderLockKey is the storage key under which the builder lock lives.
// Co-located with rollup metadata so operators see it alongside watermarks.
const BuilderLockKey = "_arc/rollup/_builder.lock"

// BuilderLockHeartbeat is how often the active builder rewrites the lock to
// keep it fresh. BuilderLockTTL is how stale a lock can be before another
// builder considers it abandoned.
const (
	BuilderLockHeartbeat = 30 * time.Second
	BuilderLockTTL       = 90 * time.Second
)

// builderLockFile is the on-disk representation of the lock.
type builderLockFile struct {
	InstanceID    string    `json:"instance_id"`
	AcquiredAt    time.Time `json:"acquired_at"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}

// BuilderLock provides cooperative single-writer enforcement for the rollup
// builder against shared object storage. Two nodes both with builder=true
// would otherwise race on watermark.json updates and produce permanent gaps.
//
// The protocol is heartbeat-based:
//  1. Acquire reads the existing lock (if any).
//     - If the lock's instance_id matches THIS process's instance ID, take
//     it over immediately (own-restart fast path).
//     - Else if it's fresh (heartbeat within BuilderLockTTL), refuse to start.
//     - Else write a new lock with this instance's ID.
//  2. Run a background goroutine that rewrites the lock every
//     BuilderLockHeartbeat to keep it fresh.
//  3. Release deletes the lock on graceful shutdown.
//
// Instance identity is derived from hostname + working-directory hash, so
// the SAME node restarting (graceful, OOM, kill -9, container restart) sees
// its previous lock as its own and reclaims it without waiting for TTL.
// A DIFFERENT host (different hostname) gets a different ID and blocks on
// the TTL as expected.
//
// Known limitations:
//   - TOCTOU: the Read+Write sequence is not atomic. Two DIFFERENT hosts
//     booting in the same window can both observe "no lock" and both write
//     their own. Mitigation: don't roll-deploy more than one builder=true
//     node at the same instant. For true atomicity, use S3 conditional writes
//     (PutObject with If-None-Match: *) — not implemented here.
//   - Wall-clock skew: nodes compare LastHeartbeat to their own time.Now().
//     NTP-synced infra is fine; severely-skewed clocks can cause false stale
//     detections.
//   - Network partition: an active builder that loses storage connectivity
//     for >TTL will have its lock stolen. When connectivity returns, both
//     nodes may briefly run schedulers. The builder does not yet check
//     "do I still hold the lock?" before each window.
type BuilderLock struct {
	backend    storage.Backend
	instanceID string
	logger     zerolog.Logger

	stopOnce sync.Once
	stopCh   chan struct{}
}

// NewBuilderLock returns an unacquired lock with a stable per-host instance ID.
// Same-host restarts will have the same instanceID and take over their own
// previous lock immediately. Different hosts get distinct IDs.
//
// instanceIDOverride, if non-empty, replaces the derived hostname-based ID.
// Useful for tests or for explicit per-deployment identity (e.g., a stable
// k8s deployment name).
func NewBuilderLock(backend storage.Backend, logger zerolog.Logger, instanceIDOverride string) *BuilderLock {
	id := instanceIDOverride
	if id == "" {
		id = stableInstanceID()
	}
	return &BuilderLock{
		backend:    backend,
		instanceID: id,
		logger:     logger.With().Str("component", "rollup-builder-lock").Logger(),
		stopCh:     make(chan struct{}),
	}
}

// stableInstanceID returns an ID that survives process restarts on the same
// host. Combines hostname (typically pod-name in k8s, machine name on VMs)
// with a hash of the current working directory to disambiguate multiple Arc
// instances running on the same host with different data directories.
func stableInstanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		// Fall back to random if hostname is unavailable. This is the rare
		// case where own-restart fast-path won't work.
		return "anon-" + randomInstanceID()
	}
	wd, _ := os.Getwd()
	h := sha256.Sum256([]byte(host + "|" + wd))
	return host + "/" + hex.EncodeToString(h[:4])
}

func randomInstanceID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Acquire attempts to take the builder lock. Returns an error if another
// instance holds a fresh lock. On success, callers should defer Release().
func (l *BuilderLock) Acquire(ctx context.Context) error {
	if data, err := l.backend.Read(ctx, BuilderLockKey); err == nil {
		var existing builderLockFile
		if err := json.Unmarshal(data, &existing); err == nil {
			age := time.Since(existing.LastHeartbeat)
			switch {
			case existing.InstanceID == l.instanceID:
				// Own-restart fast path: same host, same data dir → reclaim
				// without waiting for TTL. Common case after graceful restart,
				// container restart, kill -9.
				l.logger.Info().
					Str("instance_id", l.instanceID).
					Dur("prior_age", age.Truncate(time.Second)).
					Msg("Reclaiming own builder lock (same host)")
			case age < BuilderLockTTL:
				return fmt.Errorf(
					"another builder holds the lock: instance=%s, last_heartbeat=%s ago "+
						"(set [rollup].builder=false on this node, or wait %s for the lock to expire, "+
						"or delete %s on storage)",
					existing.InstanceID, age.Truncate(time.Second),
					(BuilderLockTTL - age).Truncate(time.Second), BuilderLockKey)
			default:
				l.logger.Warn().
					Str("stale_instance", existing.InstanceID).
					Dur("stale_age", age).
					Msg("Taking over stale builder lock")
			}
		}
		// If JSON decode failed, treat as no lock (we'll overwrite).
	}
	if err := l.write(ctx); err != nil {
		return fmt.Errorf("write builder lock: %w", err)
	}
	l.logger.Info().Str("instance_id", l.instanceID).Msg("Acquired builder lock")
	return nil
}

// StartHeartbeat refreshes the lock every BuilderLockHeartbeat until ctx
// is cancelled or Release is called. Run as a goroutine.
func (l *BuilderLock) StartHeartbeat(ctx context.Context) {
	t := time.NewTicker(BuilderLockHeartbeat)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-l.stopCh:
			return
		case <-t.C:
			if err := l.write(ctx); err != nil {
				l.logger.Error().Err(err).Msg("Failed to refresh builder lock")
			}
		}
	}
}

func (l *BuilderLock) write(ctx context.Context) error {
	now := time.Now().UTC()
	data, err := json.MarshalIndent(builderLockFile{
		InstanceID:    l.instanceID,
		AcquiredAt:    now, // overwritten if take-over; harmless
		LastHeartbeat: now,
	}, "", "  ")
	if err != nil {
		return err
	}
	return l.backend.Write(ctx, BuilderLockKey, data)
}

// Release deletes the lock file. Best-effort; safe to call multiple times.
func (l *BuilderLock) Release(ctx context.Context) {
	l.stopOnce.Do(func() { close(l.stopCh) })
	if err := l.backend.Delete(ctx, BuilderLockKey); err != nil {
		l.logger.Warn().Err(err).Msg("Failed to delete builder lock on shutdown")
		return
	}
	l.logger.Info().Str("instance_id", l.instanceID).Msg("Released builder lock")
}
