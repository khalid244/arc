package compaction

import (
	"context"
	"io"
	"sync"

	"github.com/basekick-labs/arc/internal/storage"
)

// hookBackend wraps a real *storage.LocalBackend and lets a test inject a
// failure (or a different answer) per call. It embeds the CONCRETE backend so
// the optional interfaces the reorg and manifest code type-assert
// (storage.ObjectLister, storage.DirectoryLister) still pass through. A nil
// hook delegates to the local backend unchanged. Hooks are guarded by a mutex
// because the reorg download and upload pools call the backend concurrently.
type hookBackend struct {
	*storage.LocalBackend

	mu          sync.Mutex
	listHook    func(prefix string) error
	readHook    func(path string) error
	readToHook  func(path string) error
	existsHook  func(path string) (exists bool, err error, handled bool)
	writeRHook  func(ctx context.Context, path string) error
	deleteBHook func(paths []string) error
}

func (b *hookBackend) set(fn func(b *hookBackend)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	fn(b)
}

func (b *hookBackend) List(ctx context.Context, prefix string) ([]string, error) {
	b.mu.Lock()
	h := b.listHook
	b.mu.Unlock()
	if h != nil {
		if err := h(prefix); err != nil {
			return nil, err
		}
	}
	return b.LocalBackend.List(ctx, prefix)
}

func (b *hookBackend) Read(ctx context.Context, path string) ([]byte, error) {
	b.mu.Lock()
	h := b.readHook
	b.mu.Unlock()
	if h != nil {
		if err := h(path); err != nil {
			return nil, err
		}
	}
	return b.LocalBackend.Read(ctx, path)
}

func (b *hookBackend) ReadTo(ctx context.Context, path string, w io.Writer) error {
	b.mu.Lock()
	h := b.readToHook
	b.mu.Unlock()
	if h != nil {
		if err := h(path); err != nil {
			return err
		}
	}
	return b.LocalBackend.ReadTo(ctx, path, w)
}

func (b *hookBackend) Exists(ctx context.Context, path string) (bool, error) {
	b.mu.Lock()
	h := b.existsHook
	b.mu.Unlock()
	if h != nil {
		if exists, err, handled := h(path); handled {
			return exists, err
		}
	}
	return b.LocalBackend.Exists(ctx, path)
}

func (b *hookBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	b.mu.Lock()
	h := b.writeRHook
	b.mu.Unlock()
	if h != nil {
		if err := h(ctx, path); err != nil {
			_, _ = io.Copy(io.Discard, r)
			return err
		}
	}
	return b.LocalBackend.WriteReader(ctx, path, r, size)
}

func (b *hookBackend) DeleteBatch(ctx context.Context, paths []string) error {
	b.mu.Lock()
	h := b.deleteBHook
	b.mu.Unlock()
	if h != nil {
		if err := h(paths); err != nil {
			return err
		}
	}
	return b.LocalBackend.DeleteBatch(ctx, paths)
}

var _ storage.Backend = (*hookBackend)(nil)
