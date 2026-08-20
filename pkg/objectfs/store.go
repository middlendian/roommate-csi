package objectfs

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
)

// Store is the only abstraction that talks to the API server about object
// content. Secret and ConfigMap differ solely in how their data map is
// encoded; everything above this interface is shared.
//
// Every implementation authenticates as the consuming pod, never as the
// driver.
type Store interface {
	// Get performs a quorum read: GetOptions.ResourceVersion MUST be empty
	// so the request is served from etcd rather than the API server's watch
	// cache. This is the read-after-write guarantee.
	Get(ctx context.Context) (*Snapshot, error)

	// Watch starts a watch scoped by a metadata.name field selector. RBAC
	// resourceNames rejects an unscoped watch with 403, so the selector is
	// mandatory.
	Watch(ctx context.Context, sinceRV string) (watch.Interface, error)

	// Decode converts an object from a watch event into a Snapshot.
	Decode(obj runtime.Object) (*Snapshot, error)

	// Patch applies a per-key merge patch. Keys absent from both arguments
	// are untouched because the request never mentions them — there is no
	// read-modify-write and no optimistic concurrency check.
	Patch(ctx context.Context, set map[string][]byte, del []string) error

	// Describe identifies the object for error messages, e.g.
	// `secrets/oauth-credentials`.
	Describe() string
}
