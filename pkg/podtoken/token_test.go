package podtoken

import (
	"strings"
	"testing"
)

func ctxWithToken(tokens string) map[string]string {
	return map[string]string{
		KeyTokens:         tokens,
		KeyPodNamespace:   "my-app",
		KeyPodServiceAcct: "session-runner",
		KeyPodUID:         "abc-123",
	}
}

func TestExtractDefaultAudience(t *testing.T) {
	got, err := Extract(ctxWithToken(`{"":{"token":"tok-abc","expirationTimestamp":"2026-08-20T12:00:00Z"}}`))
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if got != "tok-abc" {
		t.Fatalf("token = %q, want tok-abc", got)
	}
}

// CSIDriver requests audience "", but tolerate a driver configured with a
// named audience rather than failing the mount.
func TestExtractFallsBackToSoleAudience(t *testing.T) {
	got, err := Extract(ctxWithToken(`{"api":{"token":"tok-xyz","expirationTimestamp":"2026-08-20T12:00:00Z"}}`))
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if got != "tok-xyz" {
		t.Fatalf("token = %q, want tok-xyz", got)
	}
}

func TestExtractErrors(t *testing.T) {
	tests := []struct {
		name string
		ctx  map[string]string
	}{
		{"missing key", map[string]string{}},
		{"empty value", ctxWithToken("")},
		{"not json", ctxWithToken("{{{")},
		{"no audiences", ctxWithToken("{}")},
		{"empty token", ctxWithToken(`{"":{"token":""}}`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Extract(tt.ctx); err == nil {
				t.Fatal("Extract succeeded, want error")
			}
		})
	}
}

// The error must never contain the token itself — it is surfaced verbatim in
// a kubectl describe pod event.
func TestExtractErrorDoesNotLeakToken(t *testing.T) {
	_, err := Extract(ctxWithToken(`{"":{"token":"super-secret-value"`))
	if err == nil {
		t.Fatal("want error")
	}
	if strings.Contains(err.Error(), "super-secret-value") {
		t.Fatalf("error leaked the token: %v", err)
	}
}

func TestPodIdentity(t *testing.T) {
	ns, sa, uid, err := PodIdentity(ctxWithToken(`{"":{"token":"t"}}`))
	if err != nil {
		t.Fatalf("PodIdentity: %v", err)
	}
	if ns != "my-app" || sa != "session-runner" || uid != "abc-123" {
		t.Fatalf("got %q %q %q", ns, sa, uid)
	}
}

func TestPodIdentityRequiresPodInfoOnMount(t *testing.T) {
	if _, _, _, err := PodIdentity(map[string]string{}); err == nil {
		t.Fatal("want error when podInfoOnMount fields are absent")
	}
}
