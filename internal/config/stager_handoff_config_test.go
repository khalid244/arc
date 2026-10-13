package config

import (
	"os"
	"testing"
)

// Pod-deletion protection without a PVC, off by default:
// ingest.stager_handoff_url (a peer Service URL), ingest.stager_handoff_token
// and ingest.stager_import_enabled.
func TestStagerHandoffConfig(t *testing.T) {
	t.Run("defaults off", func(t *testing.T) {
		t.Chdir(t.TempDir())
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Ingest.StagerHandoffURL != "" || cfg.Ingest.StagerHandoffToken != "" || cfg.Ingest.StagerImportEnabled {
			t.Fatalf("defaults: url %q token set=%v import %v; want all off",
				cfg.Ingest.StagerHandoffURL, cfg.Ingest.StagerHandoffToken != "", cfg.Ingest.StagerImportEnabled)
		}
	})
	t.Run("toml", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		toml := "[ingest]\nstager_handoff_url = \"http://hammel-arc-ingest-svc:8000\"\nstager_import_enabled = true\n"
		if err := os.WriteFile(dir+"/arc.toml", []byte(toml), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Ingest.StagerHandoffURL != "http://hammel-arc-ingest-svc:8000" || !cfg.Ingest.StagerImportEnabled {
			t.Fatalf("got url %q import %v", cfg.Ingest.StagerHandoffURL, cfg.Ingest.StagerImportEnabled)
		}
	})
	t.Run("re-offer window", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Ingest.StagerHandoffReofferSeconds != 60 {
			t.Fatalf("default stager_handoff_reoffer_seconds = %d, want 60", cfg.Ingest.StagerHandoffReofferSeconds)
		}
		if err := os.WriteFile(dir+"/arc.toml", []byte("[ingest]\nstager_handoff_reoffer_seconds = 15\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if cfg, err = Load(); err != nil || cfg.Ingest.StagerHandoffReofferSeconds != 15 {
			t.Fatalf("stager_handoff_reoffer_seconds = 15: got %v, %v", cfg.Ingest.StagerHandoffReofferSeconds, err)
		}
		if err := os.WriteFile(dir+"/arc.toml", []byte("[ingest]\nstager_handoff_reoffer_seconds = 0\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(); err == nil {
			t.Fatal("Load() accepted stager_handoff_reoffer_seconds = 0")
		}
	})
	t.Run("rejects a non-http url", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		if err := os.WriteFile(dir+"/arc.toml", []byte("[ingest]\nstager_handoff_url = \"peer:8000\"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(); err == nil {
			t.Fatal("Load() accepted stager_handoff_url without http(s)://")
		}
	})
}
