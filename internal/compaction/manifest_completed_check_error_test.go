package compaction

import (
	"context"
	"fmt"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// listingHeadBackend is headBackend plus ListObjects, so recovery takes its
// ObjectLister size check (production's ResilientBackend over S3 has it).
type listingHeadBackend struct {
	*headBackend
	lister storage.ObjectLister
}

func (b *listingHeadBackend) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	return b.lister.ListObjects(ctx, prefix)
}

// Base bug: recoverLoadedManifest discarded jobCompleted's error. A manifest
// whose output is uploaded and whose inputs are all gone, but one of whose
// input HEADs failed (a Hetzner 503), fell through to the partial-upload size
// check; an output whose size disagrees with the manifest was then deleted as
// "partial", and with the inputs already gone that deleted the only copy of
// their rows. An input check that cannot answer must leave the manifest and
// the output alone for this pass.
func TestRecoverOrphanedManifests_InputCheckErrorNeverDiscardsOutput(t *testing.T) {
	for _, tc := range []struct {
		name   string
		conc   int
		lister bool
	}{
		{"serial, stat size check", 1, false},
		{"serial, list size check", 1, true},
		{"concurrent, stat size check", 4, false},
		{"concurrent, list size check", 4, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFinishFixture(t)
			path, m := f.wideManifest(t, "j", 12)
			f.put(t, m.OutputPath, "output rewritten to another size") // size disagrees with the manifest
			hb := newHeadBackend(f.backend, 0)
			hb.failKeys[m.InputFiles[5]] = true
			var backend storage.Backend = hb
			if tc.lister {
				backend = &listingHeadBackend{headBackend: hb, lister: f.backend}
			}
			mm := NewManifestManager(backend, zerolog.Nop())
			mm.SetInputCheckConcurrency(tc.conc)

			n, err := mm.RecoverOrphanedManifests(f.ctx, nil, nil)
			if err == nil {
				t.Fatal("recovery reported success although an input check failed")
			}
			if n != 0 {
				t.Fatalf("recovered = %d, want 0", n)
			}
			if !f.exists(t, m.OutputPath) {
				t.Fatal("output deleted while its inputs are gone: those rows are lost")
			}
			if !f.exists(t, path) {
				t.Fatal("manifest deleted although an input check failed")
			}

			// Once the store answers again the manifest is finished and the
			// output kept, as for any manifest whose inputs are gone.
			delete(hb.failKeys, m.InputFiles[5])
			n, err = mm.RecoverOrphanedManifests(f.ctx, nil, nil)
			if err != nil || n != 1 {
				t.Fatalf("second pass: recovered = %d, err = %v; want 1, nil", n, err)
			}
			if !f.exists(t, m.OutputPath) || f.exists(t, path) {
				t.Fatal("second pass: want the output kept and the manifest deleted")
			}
		})
	}
}

// The same failed check with a matching output size: recovery used to fall
// through and re-delete the (already gone) inputs. Now it waits for a pass
// whose check answers; nothing is deleted meanwhile.
func TestRecoverOrphanedManifests_InputCheckErrorKeepsEverything(t *testing.T) {
	f := newFinishFixture(t)
	path, m := f.wideManifest(t, "j", 12, 3) // input 3 still exists
	hb := newHeadBackend(f.backend, 0)
	hb.failKeys[m.InputFiles[1]] = true
	mm := NewManifestManager(hb, zerolog.Nop())

	n, err := mm.RecoverOrphanedManifests(f.ctx, nil, nil)
	if err == nil || n != 0 {
		t.Fatalf("recovered = %d, err = %v; want 0 and the check error", n, err)
	}
	if !f.exists(t, path) || !f.exists(t, m.OutputPath) || !f.exists(t, m.InputFiles[3]) {
		t.Fatal("a failed input check must leave manifest, output and inputs alone")
	}
}

// An input key no backend can address fails Exists the same way on every
// pass (#747). Treating that as "cannot tell, retry" would hold the manifest
// forever, so recovery still falls through to the kept-output branch, which
// drops such a key and deletes the rest (TestRecoverManifestSkipsUnusableInput
// covers the not-uploaded shape; this is the uploaded one).
func TestRecoverOrphanedManifests_InvalidInputKeyStillRecovers(t *testing.T) {
	for _, conc := range []int{1, 4} {
		t.Run(fmt.Sprintf("concurrency %d", conc), func(t *testing.T) {
			f := newFinishFixture(t)
			_, m := f.wideManifest(t, "j", 3) // every addressable input already gone
			m.InputFiles[1] = `db\cpu/2026/11/10/04/in-bad.parquet`
			path, err := f.mm.WriteManifest(f.ctx, m) // same path: overwrite with the bad key
			if err != nil {
				t.Fatal(err)
			}
			mm := NewManifestManager(f.backend, zerolog.Nop())
			mm.SetInputCheckConcurrency(conc)

			n, err := mm.RecoverOrphanedManifests(f.ctx, nil, nil)
			if err != nil || n != 1 {
				t.Fatalf("recovered = %d, err = %v; want 1, nil", n, err)
			}
			if f.exists(t, path) || !f.exists(t, m.OutputPath) {
				t.Fatal("want manifest deleted and output kept")
			}
		})
	}
}
