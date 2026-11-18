//go:build !windows

package compaction

import (
	"errors"
	"os"
	"syscall"
)

// rewriteLocksSupported: run directories are claimed with flock.
const rewriteLocksSupported = true

// tryLockFile takes an exclusive, non-blocking flock on f. false (and no
// error) means another open file description holds it: a live run, in this
// process, another one, or another PID namespace on the same filesystem.
// The lock goes away with the process, however it dies.
func tryLockFile(f *os.File) (bool, error) {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return false, nil
	}
	return false, err
}
