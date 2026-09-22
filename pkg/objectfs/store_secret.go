package objectfs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

// SecretStore backs a mount with a corev1.Secret.
type SecretStore struct {
	client kubernetes.Interface
	ns     string
	name   string
}

// NewSecretStore returns a Store backed by the named Secret. The client must
// be built from the consuming pod's token.
func NewSecretStore(c kubernetes.Interface, ns, name string) *SecretStore {
	return &SecretStore{client: c, ns: ns, name: name}
}

// Describe returns a human-readable descriptor of the Secret, e.g. "secrets/oauth-credentials".
func (s *SecretStore) Describe() string { return "secrets/" + s.name }

// Get performs a quorum read from etcd by using empty GetOptions, ensuring the read-after-write guarantee.
func (s *SecretStore) Get(ctx context.Context) (*Snapshot, error) {
	// Empty GetOptions == quorum read. Do not set ResourceVersion.
	obj, err := s.client.CoreV1().Secrets(s.ns).Get(ctx, s.name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return s.Decode(obj)
}

// Watch starts a watch on the Secret scoped by metadata.name field selector, satisfying RBAC resourceNames constraints.
func (s *SecretStore) Watch(ctx context.Context, sinceRV string) (watch.Interface, error) {
	return s.client.CoreV1().Secrets(s.ns).Watch(ctx, metav1.ListOptions{
		FieldSelector:   fields.OneTermEqualSelector("metadata.name", s.name).String(),
		ResourceVersion: sinceRV,
	})
}

// Decode extracts the data map from a Secret object into a Snapshot with deep-copied byte slices.
func (s *SecretStore) Decode(obj runtime.Object) (*Snapshot, error) {
	sec, ok := obj.(*corev1.Secret)
	if !ok {
		return nil, fmt.Errorf("expected *v1.Secret, got %T", obj)
	}
	data := make(map[string][]byte, len(sec.Data))
	for k, v := range sec.Data {
		cp := make([]byte, len(v))
		copy(cp, v)
		data[k] = cp
	}
	return &Snapshot{
		Data:            data,
		ResourceVersion: sec.ResourceVersion,
		FetchedAt:       time.Now(),
	}, nil
}

// Patch applies a merge patch to the Secret's data map, setting and deleting keys as specified.
func (s *SecretStore) Patch(ctx context.Context, set map[string][]byte, del []string) error {
	// json.Marshal base64-encodes []byte, which is exactly the wire form the
	// API server expects for Secret.data. A nil entry deletes the key.
	data := make(map[string]interface{}, len(set)+len(del))
	for k, v := range set {
		data[k] = v
	}
	for _, k := range del {
		data[k] = nil
	}
	body, err := json.Marshal(map[string]interface{}{"data": data})
	if err != nil {
		return err
	}
	_, err = s.client.CoreV1().Secrets(s.ns).Patch(
		ctx, s.name, types.MergePatchType, body, metav1.PatchOptions{})
	return err
}
