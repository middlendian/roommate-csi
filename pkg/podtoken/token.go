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
	KeyPodServiceAcct = "csi.storage.k8s.io/pod.service-account.name"
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

// ClientFor builds a client authenticated as the pod.
//
// It starts from the in-cluster config for the API server address and CA
// only, then replaces every credential field. Leaving the driver's own
// ServiceAccount token in place would silently restore the confused-deputy
// problem this package exists to prevent.
func ClientFor(token string) (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}
	cfg.BearerToken = token
	cfg.BearerTokenFile = ""
	cfg.Username, cfg.Password = "", ""
	cfg.CertFile, cfg.KeyFile = "", ""
	cfg.CertData, cfg.KeyData = nil, nil
	cfg.AuthProvider = nil
	cfg.ExecProvider = nil
	return kubernetes.NewForConfig(cfg)
}
