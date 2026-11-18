//go:build windows

package compaction

// processAlive is conservative on Windows: every owner counts as alive, so
// RewriteLocal never sweeps another run's directories there.
func processAlive(pid int) bool { return true }
