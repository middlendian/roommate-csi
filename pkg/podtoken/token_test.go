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
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
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

// TestClearCredentialsZeroesEveryField guards C1's security half: nothing
// previously asserted that ClientFor's credential-zeroing block actually
// clears every field client-go can authenticate with. BearerTokenFile in
// particular matters most — a reintroduced value is client-go's own
// dynamically-reloaded bearer-token mechanism, so it would keep working
// silently, layering the driver's own ServiceAccount identity back on top
// of wrapTokenAuth's round tripper and collapsing the zero-RBAC model the
// whole design rests on, with no visible symptom.
func TestClearCredentialsZeroesEveryField(t *testing.T) {
	cfg := &rest.Config{
		Host:            "https://example.invalid",
		BearerToken:     "driver-own-token",
		BearerTokenFile: "/var/run/secrets/kubernetes.io/serviceaccount/token",
		Username:        "admin",
		Password:        "hunter2",
		TLSClientConfig: rest.TLSClientConfig{
			CertFile: "/tls/client.crt",
			KeyFile:  "/tls/client.key",
			CertData: []byte("cert-bytes"),
			KeyData:  []byte("key-bytes"),
		},
		AuthProvider: &clientcmdapi.AuthProviderConfig{Name: "gcp"},
		ExecProvider: &clientcmdapi.ExecConfig{Command: "some-credential-plugin"},
	}

	clearCredentials(cfg)

	if cfg.BearerToken != "" {
		t.Errorf("BearerToken = %q, want empty", cfg.BearerToken)
	}
	if cfg.BearerTokenFile != "" {
		t.Errorf("BearerTokenFile = %q, want empty", cfg.BearerTokenFile)
	}
	if cfg.Username != "" || cfg.Password != "" {
		t.Errorf("Username/Password = %q/%q, want empty/empty", cfg.Username, cfg.Password)
	}
	if cfg.CertFile != "" || cfg.KeyFile != "" {
		t.Errorf("CertFile/KeyFile = %q/%q, want empty/empty", cfg.CertFile, cfg.KeyFile)
	}
	if cfg.CertData != nil || cfg.KeyData != nil {
		t.Errorf("CertData/KeyData = %v/%v, want nil/nil", cfg.CertData, cfg.KeyData)
	}
	if cfg.AuthProvider != nil {
		t.Errorf("AuthProvider = %v, want nil", cfg.AuthProvider)
	}
	if cfg.ExecProvider != nil {
		t.Errorf("ExecProvider = %v, want nil", cfg.ExecProvider)
	}
}
