package objectfs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

// ConfigMapStore backs a mount with a corev1.ConfigMap. A ConfigMap splits
// its map across data (UTF-8 strings) and binaryData (bytes); the mount
// presents both as one flat directory.
type ConfigMapStore struct {
	client kubernetes.Interface
	ns     string
	name   string
}

// NewConfigMapStore returns a Store backed by the named ConfigMap. The client
// must be built from the consuming pod's token.
func NewConfigMapStore(c kubernetes.Interface, ns, name string) *ConfigMapStore {
	return &ConfigMapStore{client: c, ns: ns, name: name}
}

func (s *ConfigMapStore) Describe() string { return "configmaps/" + s.name }

func (s *ConfigMapStore) Get(ctx context.Context) (*Snapshot, error) {
	// Empty GetOptions == quorum read. Do not set ResourceVersion.
	obj, err := s.client.CoreV1().ConfigMaps(s.ns).Get(ctx, s.name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return s.Decode(obj)
}

func (s *ConfigMapStore) Watch(ctx context.Context, sinceRV string) (watch.Interface, error) {
	return s.client.CoreV1().ConfigMaps(s.ns).Watch(ctx, metav1.ListOptions{
		FieldSelector:   fields.OneTermEqualSelector("metadata.name", s.name).String(),
		ResourceVersion: sinceRV,
	})
}

func (s *ConfigMapStore) Decode(obj runtime.Object) (*Snapshot, error) {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		return nil, fmt.Errorf("expected *v1.ConfigMap, got %T", obj)
	}
	data := make(map[string][]byte, len(cm.Data)+len(cm.BinaryData))
	for k, v := range cm.Data {
		data[k] = []byte(v)
	}
	for k, v := range cm.BinaryData {
		cp := make([]byte, len(v))
		copy(cp, v)
		data[k] = cp
	}
	return &Snapshot{
		Data:            data,
		ResourceVersion: cm.ResourceVersion,
		FetchedAt:       time.Now(),
	}, nil
}

func (s *ConfigMapStore) Patch(ctx context.Context, set map[string][]byte, del []string) error {
	// The API server rejects a key present in both maps, so every write nulls
	// the half it is not using. A deletion nulls both, since the caller does
	// not know which half held the key.
	data := map[string]interface{}{}
	binary := map[string]interface{}{}

	for k, v := range set {
		if utf8.Valid(v) {
			data[k] = string(v)
			binary[k] = nil
		} else {
			binary[k] = v // json.Marshal base64-encodes []byte
			data[k] = nil
		}
	}
	for _, k := range del {
		data[k] = nil
		binary[k] = nil
	}

	body, err := json.Marshal(map[string]interface{}{
		"data":       data,
		"binaryData": binary,
	})
	if err != nil {
		return err
	}
	_, err = s.client.CoreV1().ConfigMaps(s.ns).Patch(
		ctx, s.name, types.MergePatchType, body, metav1.PatchOptions{})
	return err
}
