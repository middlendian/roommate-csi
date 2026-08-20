package objectfs

import (
	"testing"
	"time"
)

func TestParseConfigDefaults(t *testing.T) {
	cfg, err := ParseConfig(map[string]string{
		"objectKind": "Secret",
		"objectName": "oauth-credentials",
	}, "my-app")
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.LeaseName != "roommate-oauth-credentials" {
		t.Errorf("LeaseName = %q, want roommate-oauth-credentials", cfg.LeaseName)
	}
	if cfg.FileMode != 0o600 {
		t.Errorf("FileMode = %o, want 600", cfg.FileMode)
	}
	if cfg.DirMode != 0o700 {
		t.Errorf("DirMode = %o, want 700", cfg.DirMode)
	}
	if cfg.StalenessBound != DefaultStalenessBound {
		t.Errorf("StalenessBound = %v, want %v", cfg.StalenessBound, DefaultStalenessBound)
	}
	if cfg.LeaseDuration != DefaultLeaseDuration {
		t.Errorf("LeaseDuration = %v, want %v", cfg.LeaseDuration, DefaultLeaseDuration)
	}
	// Namespace always comes from pod info, never from attributes.
	if cfg.Namespace != "my-app" {
		t.Errorf("Namespace = %q, want my-app", cfg.Namespace)
	}
}

func TestParseConfigOverrides(t *testing.T) {
	cfg, err := ParseConfig(map[string]string{
		"objectKind":            "ConfigMap",
		"objectName":            "app-config",
		"leaseName":             "custom-lease",
		"fileMode":              "0644",
		"dirMode":               "0755",
		"uid":                   "1000",
		"gid":                   "2000",
		"stalenessBoundSeconds": "5",
		"leaseDurationSeconds":  "30",
	}, "my-app")
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.LeaseName != "custom-lease" {
		t.Errorf("LeaseName = %q", cfg.LeaseName)
	}
	if cfg.FileMode != 0o644 || cfg.DirMode != 0o755 {
		t.Errorf("modes = %o %o", cfg.FileMode, cfg.DirMode)
	}
	if cfg.UID != 1000 || cfg.GID != 2000 {
		t.Errorf("uid/gid = %d %d", cfg.UID, cfg.GID)
	}
	if cfg.StalenessBound != 5*time.Second {
		t.Errorf("StalenessBound = %v", cfg.StalenessBound)
	}
	if cfg.LeaseDuration != 30*time.Second {
		t.Errorf("LeaseDuration = %v", cfg.LeaseDuration)
	}
}

func TestParseConfigRejectsBadInput(t *testing.T) {
	tests := []struct {
		name string
		attr map[string]string
		ns   string
	}{
		{"missing kind", map[string]string{"objectName": "x"}, "my-app"},
		{"missing name", map[string]string{"objectKind": "Secret"}, "my-app"},
		{"bad kind", map[string]string{"objectKind": "Pod", "objectName": "x"}, "my-app"},
		{"bad object name", map[string]string{"objectKind": "Secret", "objectName": "Not_Valid"}, "my-app"},
		{"bad file mode", map[string]string{"objectKind": "Secret", "objectName": "x", "fileMode": "zzz"}, "my-app"},
		{"missing namespace", map[string]string{"objectKind": "Secret", "objectName": "x"}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseConfig(tt.attr, tt.ns); err == nil {
				t.Fatal("ParseConfig succeeded, want error")
			}
		})
	}
}

// A namespace attribute must not be honoured: cross-namespace access is a
// non-goal, and silently accepting one would be a security hole.
func TestParseConfigIgnoresNamespaceAttribute(t *testing.T) {
	cfg, err := ParseConfig(map[string]string{
		"objectKind": "Secret",
		"objectName": "oauth-credentials",
		"namespace":  "kube-system",
	}, "my-app")
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.Namespace != "my-app" {
		t.Fatalf("Namespace = %q, want my-app — attribute must be ignored", cfg.Namespace)
	}
}
