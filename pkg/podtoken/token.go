// Package podtoken turns the volume context kubelet supplies into a
// Kubernetes client authenticated as the consuming pod.
//
// This is where roommate's central security property lives: the driver's own
// ServiceAccount holds no API permissions, so every call must carry a token
// kubelet minted for the pod being served. A pod can therefore only reach
// objects its own RBAC already allows.
package podtoken

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Volume context keys populated by kubelet. The token key requires
// CSIDriver.spec.tokenRequests; the pod-info keys require podInfoOnMount.
const (
	// KeyTokens is the volume context key for service account tokens.
	KeyTokens = "csi.storage.k8s.io/serviceAccount.tokens"
	// KeyPodNamespace is the volume context key for the pod's namespace.
	KeyPodNamespace = "csi.storage.k8s.io/pod.namespace"
	// KeyPodName is the volume context key for the pod's name.
	KeyPodName = "csi.storage.k8s.io/pod.name"
	// KeyPodUID is the volume context key for the pod's UID.
	KeyPodUID = "csi.storage.k8s.io/pod.uid"
	// KeyPodServiceAcct is the volume context key for the pod's service account.
	KeyPodServiceAcct = "csi.storage.k8s.io/serviceAccount.name"
)

// ErrNoToken means kubelet supplied no usable token, which almost always
// means CSIDriver.spec.tokenRequests is missing or misconfigured.
var ErrNoToken = errors.New("roommate: no service account token in volume context")

type tokenEntry struct {
	Token               string `json:"token"`
	ExpirationTimestamp string `json:"expirationTimestamp"`
}

// Extract returns the pod's bearer token. It prefers the default audience
// ("" — what our CSIDriver requests) and falls back to the sole entry if the
// driver was configured with a named audience instead.
//
// Errors never include the raw payload: this message is surfaced verbatim in
// a FailedMount event.
func Extract(volumeContext map[string]string) (string, error) {
	raw, ok := volumeContext[KeyTokens]
	if !ok || raw == "" {
		return "", ErrNoToken
	}
	var byAudience map[string]tokenEntry
	if err := json.Unmarshal([]byte(raw), &byAudience); err != nil {
		return "", fmt.Errorf("%w: token payload is not valid JSON", ErrNoToken)
	}
	if entry, ok := byAudience[""]; ok && entry.Token != "" {
		return entry.Token, nil
	}
	if len(byAudience) == 1 {
		for _, entry := range byAudience {
			if entry.Token != "" {
				return entry.Token, nil
			}
		}
	}
	return "", ErrNoToken
}

// PodIdentity returns the consuming pod's namespace, ServiceAccount name, and
// UID. All three require podInfoOnMount: true on the CSIDriver object.
func PodIdentity(volumeContext map[string]string) (namespace, serviceAccount, uid string, err error) {
	namespace = volumeContext[KeyPodNamespace]
	serviceAccount = volumeContext[KeyPodServiceAcct]
	uid = volumeContext[KeyPodUID]
	if namespace == "" || serviceAccount == "" || uid == "" {
		return "", "", "", errors.New(
			"roommate: pod identity missing from volume context; " +
				"set podInfoOnMount: true on the CSIDriver object")
	}
	return namespace, serviceAccount, uid, nil
}

// ClientFor builds a client authenticated as the pod. holder supplies the
// bearer token for every outgoing request, read fresh each time via
// wrapTokenAuth — not baked in once at construction — so a token a later
// republish swaps into holder (see mounts.Live.Token) takes effect on the
// very next API call instead of being frozen for the life of the mount.
//
// It starts from the in-cluster config for the API server address and CA
// only, then replaces every credential field. Leaving the driver's own
// ServiceAccount token in place — or its token *file*, which is client-go's
// only other source of a dynamically-reloaded bearer token — would silently
// restore the confused-deputy problem this package exists to prevent.
// BearerToken is left empty so client-go installs no competing bearer
// round-tripper of its own; wrapTokenAuth's transport is the sole source of
// the Authorization header from here on.
func ClientFor(holder *atomic.Pointer[string]) (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}
	clearCredentials(cfg)
	wrapTokenAuth(cfg, holder)
	return kubernetes.NewForConfig(cfg)
}

// clearCredentials strips every credential field client-go would otherwise
// use to authenticate cfg, so wrapTokenAuth's round tripper is the sole
// source of the Authorization header. In particular BearerTokenFile must
// stay cleared: it is client-go's own dynamically-reloaded bearer-token
// mechanism, and a reintroduced value would work silently — the driver's
// own ServiceAccount identity would quietly replace the pod's, collapsing
// the zero-RBAC model this package exists to enforce, with no visible
// symptom until an audit or an incident.
//
// Split out from ClientFor so it can be exercised against a bare
// *rest.Config, without needing a real in-cluster environment — the same
// seam wrapTokenAuth already uses.
func clearCredentials(cfg *rest.Config) {
	cfg.BearerToken = ""
	cfg.BearerTokenFile = ""
	cfg.Username, cfg.Password = "", ""
	cfg.CertFile, cfg.KeyFile = "", ""
	cfg.CertData, cfg.KeyData = nil, nil
	cfg.AuthProvider = nil
	cfg.ExecProvider = nil
}

// wrapTokenAuth installs a round tripper on cfg that sets the Authorization
// header from holder.Load() on every outgoing request. Split out from
// ClientFor so it can be exercised against a bare *rest.Config pointed at a
// test server, without needing a real in-cluster environment.
func wrapTokenAuth(cfg *rest.Config, holder *atomic.Pointer[string]) {
	cfg.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return &tokenRoundTripper{holder: holder, next: rt}
	})
}

// tokenRoundTripper reads holder on every RoundTrip call, so a token swap
// (atomic.Pointer[string].Store) is visible to the very next request this
// client sends, not just to clients built after the swap.
type tokenRoundTripper struct {
	holder *atomic.Pointer[string]
	next   http.RoundTripper
}

func (t *tokenRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	if tok := t.holder.Load(); tok != nil && *tok != "" {
		req.Header.Set("Authorization", "Bearer "+*tok)
	}
	return t.next.RoundTrip(req)
}
