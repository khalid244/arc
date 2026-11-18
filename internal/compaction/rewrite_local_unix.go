//go:build !windows

package compaction

import (
	"errors"
	"syscall"
)

// processAlive reports whether a process with this PID exists on this host
// (signal 0 probes without delivering anything; EPERM means it exists but
// belongs to another user).
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
