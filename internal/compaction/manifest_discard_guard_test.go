package compaction

import (
	"fmt"
	"testing"

	"github.com/rs/zerolog"
)

// discardPartialOutput deletes an output whose size disagrees with its
// manifest so the job reruns from the inputs. That is only a rerun when the
// inputs are all still there; with any of them gone the output holds the only
// copy of their rows, and deleting it is data loss. Both shapes below lost the
// output (recovered=1, no error) before the guard.

type discardGuardCase struct {
	name     string
	uploaded bool
}

var discardGuardCases = []discardGuardCase{{"output uploaded", true}, {"output not marked uploaded", false}}

// rewriteManifest re-saves m at its own path (the path depends on tier,
// database and job ID only) after the caller changed it.
func (f *finishFixture) rewriteManifest(t *testing.T, m *Manifest) string {
	t.Helper()
	path, err := f.mm.WriteManifest(f.ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func (f *finishFixture) assertParked(t *testing.T, path string) {
	t.Helper()
	parked, err := quarantinePathFor(path)
	if err != nil {
		t.Fatal(err)
	}
	if f.exists(t, path) {
		t.Fatal("manifest still in the work set")
	}
	if !f.exists(t, parked) {
		t.Fatal("manifest not parked: the record of the output and its inputs is gone")
	}
}

// (b) The partial-DeleteBatch shape: one input live, the rest gone, an output
// whose size disagrees. Never an error, so it reached the discard directly.
func TestRecoverOrphanedManifests_SizeMismatchWithSomeInputsGoneKeepsOutput(t *testing.T) {
	for _, tc := range discardGuardCases {
		for _, conc := range []int{1, 4} {
			t.Run(fmt.Sprintf("%s, concurrency %d", tc.name, conc), func(t *testing.T) {
				f := newFinishFixture(t)
				_, m := f.wideManifest(t, "j", 6, 2) // only input 2 survives
				m.OutputUploaded = tc.uploaded
				path := f.rewriteManifest(t, m)
				f.put(t, m.OutputPath, "output rewritten to another size")
				f.mm.SetInputCheckConcurrency(conc)

				if _, err := f.mm.RecoverOrphanedManifests(f.ctx, nil, nil); err != nil {
					t.Fatalf("recovery: %v", err)
				}
				if !f.exists(t, m.OutputPath) {
					t.Fatal("output deleted while five of its six inputs are gone: their rows are lost")
				}
				if !f.exists(t, m.InputFiles[2]) {
					t.Fatal("the live input was deleted next to an output whose size disagrees")
				}
				f.assertParked(t, path)
			})
		}
	}
}

// (a) An input key no backend can address, every other input gone, a size
// mismatch: the #747 fall-through reached the discard.
func TestRecoverOrphanedManifests_UnaddressableInputSizeMismatchKeepsOutput(t *testing.T) {
	for _, tc := range discardGuardCases {
		for _, conc := range []int{1, 4} {
			t.Run(fmt.Sprintf("%s, concurrency %d", tc.name, conc), func(t *testing.T) {
				f := newFinishFixture(t)
				_, m := f.wideManifest(t, "j", 4)
				m.InputFiles[1] = `db\cpu/2026/11/10/04/in-bad.parquet`
				m.OutputUploaded = tc.uploaded
				path := f.rewriteManifest(t, m)
				f.put(t, m.OutputPath, "output rewritten to another size")
				f.mm.SetInputCheckConcurrency(conc)

				if _, err := f.mm.RecoverOrphanedManifests(f.ctx, nil, nil); err != nil {
					t.Fatalf("recovery: %v", err)
				}
				if !f.exists(t, m.OutputPath) {
					t.Fatal("output deleted while every addressable input is gone: their rows are lost")
				}
				if f.exists(t, path) {
					t.Fatal("manifest kept: an unaddressable key fails the same way every pass")
				}
				if !tc.uploaded {
					f.assertParked(t, path)
				}
			})
		}
	}
}

// Unchanged: every input present and an output whose size disagrees is a
// partial upload, discarded so the job reruns from the inputs. An
// unaddressable sibling does not block that.
func TestRecoverOrphanedManifests_SizeMismatchWithAllInputsPresentDiscards(t *testing.T) {
	for _, withBadKey := range []bool{false, true} {
		t.Run(fmt.Sprintf("unaddressable sibling %v", withBadKey), func(t *testing.T) {
			f := newFinishFixture(t)
			_, m := f.wideManifest(t, "j", 4, 0, 1, 2, 3)
			if withBadKey {
				m.InputFiles = append(m.InputFiles, `db\cpu/2026/11/10/04/in-bad.parquet`)
			}
			path := f.rewriteManifest(t, m)
			f.put(t, m.OutputPath, "out")

			if _, err := f.mm.RecoverOrphanedManifests(f.ctx, nil, nil); err != nil {
				t.Fatalf("recovery: %v", err)
			}
			if f.exists(t, m.OutputPath) || f.exists(t, path) {
				t.Fatal("partial output with every input live: want output and manifest discarded for a rerun")
			}
			for _, in := range m.InputFiles[:4] {
				if !f.exists(t, in) {
					t.Fatalf("input %s deleted next to a partial output", in)
				}
			}
		})
	}
}

// A manifest whose only inputs are unaddressable proves nothing about where
// its rows are, so its output is never discarded either.
func TestRecoverOrphanedManifests_OnlyUnaddressableInputsKeepsOutput(t *testing.T) {
	f := newFinishFixture(t)
	_, m := f.wideManifest(t, "j", 0)
	m.InputFiles = []string{`db\cpu/2026/11/10/04/in-bad.parquet`}
	m.OutputUploaded = false
	path := f.rewriteManifest(t, m)
	f.put(t, m.OutputPath, "output rewritten to another size")

	if _, err := f.mm.RecoverOrphanedManifests(f.ctx, nil, nil); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if !f.exists(t, m.OutputPath) {
		t.Fatal("output deleted with no addressable input to rerun from")
	}
	f.assertParked(t, path)
}

// A failed input HEAD while deciding whether to discard keeps everything for
// the next pass.
func TestRecoverOrphanedManifests_DiscardCheckErrorKeepsEverything(t *testing.T) {
	f := newFinishFixture(t)
	_, m := f.wideManifest(t, "j", 4, 0, 1, 2, 3)
	m.OutputUploaded = false
	path := f.rewriteManifest(t, m)
	f.put(t, m.OutputPath, "out")
	hb := newHeadBackend(f.backend, 0)
	hb.failKeys[m.InputFiles[2]] = true
	mm := NewManifestManager(hb, zerolog.Nop())

	n, err := mm.RecoverOrphanedManifests(f.ctx, nil, nil)
	if err == nil || n != 0 {
		t.Fatalf("recovered = %d, err = %v; want 0 and the check error", n, err)
	}
	if !f.exists(t, m.OutputPath) || !f.exists(t, path) {
		t.Fatal("output or manifest removed although an input check failed")
	}
}
