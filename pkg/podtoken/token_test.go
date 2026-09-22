package podtoken

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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

// TestWrapTokenAuthUsesLatestToken proves the whole point of the atomic
// holder: a token swapped in after the client is built (exactly what a
// republish does to mounts.Live.Token) reaches the API server on the very
// next request, rather than being frozen at construction time. Before this
// fix, ClientFor baked rest.Config.BearerToken in once and cleared
// BearerTokenFile — client-go's only dynamic-token mechanism — so nothing
// would ever have caught a regression back to that shape.
func TestWrapTokenAuthUsesLatestToken(t *testing.T) {
	var gotAuth atomic.Pointer[string]
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		gotAuth.Store(&h)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"kind":"Secret","apiVersion":"v1","metadata":{"name":"x"},"data":{}}`))
	}))
	defer srv.Close()

	holder := new(atomic.Pointer[string])
	first := "tok-1"
	holder.Store(&first)

	cfg := &rest.Config{Host: srv.URL}
	wrapTokenAuth(cfg, holder)
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("NewForConfig: %v", err)
	}

	ctx := t.Context()
	if _, err := client.CoreV1().Secrets("ns").Get(ctx, "x", metav1.GetOptions{}); err != nil {
		t.Fatalf("first Get: %v", err)
	}
	if got := gotAuth.Load(); got == nil || *got != "Bearer tok-1" {
		t.Fatalf("Authorization = %v, want Bearer tok-1", got)
	}

	// Swap the token — this is what swapToken does to live.Token on a
	// republish, roughly ten times a second.
	second := "tok-2"
	holder.Store(&second)

	if _, err := client.CoreV1().Secrets("ns").Get(ctx, "x", metav1.GetOptions{}); err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if got := gotAuth.Load(); got == nil || *got != "Bearer tok-2" {
		t.Fatalf("Authorization after swap = %v, want Bearer tok-2 — the next request must carry the new token", got)
	}
}
