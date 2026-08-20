package objectfs

import (
	"sort"
	"time"
)

// MaxObjectBytes is etcd's practical ceiling on a single object. Writes that
// would exceed it fail with ENOSPC rather than an opaque API rejection.
const MaxObjectBytes = 1 << 20

// Snapshot is an immutable view of one Secret or ConfigMap at a single
// resourceVersion. Handles pin one for their lifetime, which reproduces the
// atomicity of kubelet's ..data symlink flip: a handle never observes a
// partial transition between two versions.
//
// Treat the Data map as read-only. Use With to derive a modified copy.
type Snapshot struct {
	Data            map[string][]byte
	ResourceVersion string
	FetchedAt       time.Time
}

// Get returns a copy of the value for key, so a caller writing into the
// returned slice cannot corrupt the shared snapshot.
func (s *Snapshot) Get(key string) ([]byte, bool) {
	v, ok := s.Data[key]
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

// Keys returns the snapshot's keys in sorted order, so readdir output is
// stable across calls.
func (s *Snapshot) Keys() []string {
	keys := make([]string, 0, len(s.Data))
	for k := range s.Data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Size is the total number of value bytes, used for the ENOSPC check.
func (s *Snapshot) Size() int {
	n := 0
	for _, v := range s.Data {
		n += len(v)
	}
	return n
}

// With returns a new Snapshot with set applied and del removed. The receiver
// is unchanged. ResourceVersion is cleared because the result does not
// correspond to any version the API server has seen.
func (s *Snapshot) With(set map[string][]byte, del []string) *Snapshot {
	data := make(map[string][]byte, len(s.Data)+len(set))
	for k, v := range s.Data {
		data[k] = v
	}
	for k, v := range set {
		data[k] = v
	}
	for _, k := range del {
		delete(data, k)
	}
	return &Snapshot{Data: data, FetchedAt: s.FetchedAt}
}
