//go:build windows

package compaction

import "os"

// rewriteLocksSupported: no flock on Windows, so RewriteLocal never sweeps
// another run's directories there.
const rewriteLocksSupported = false

func tryLockFile(f *os.File) (bool, error) { return false, nil }
