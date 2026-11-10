package config

import "testing"

func TestDatabaseHTTPKeepAliveConfig(t *testing.T) {
	cases := []struct {
		name  string
		value string
		want  bool
	}{
		{"default off", "", false},
		{"on", "true", true},
		{"off", "false", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			if tc.value != "" {
				t.Setenv("ARC_DATABASE_HTTP_KEEP_ALIVE", tc.value)
			}
			cfg, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Database.HTTPKeepAlive != tc.want {
				t.Fatalf("HTTPKeepAlive = %v, want %v", cfg.Database.HTTPKeepAlive, tc.want)
			}
		})
	}
}
