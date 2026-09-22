package objectfs

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
)

// recordingStore captures what Patch was asked to do.
//
// mu guards set/del/patches: a test driving Patch through a real FUSE mount
// (pkg/objectfs's fs_write_test.go) calls Patch from the go-fuse server's
// own goroutine while the test goroutine reads these fields, so plain field
// access races under -race even though the syscalls that trigger each Patch
// call are themselves ordered.
type recordingStore struct {
	*stubStore

	mu       sync.Mutex
	set      map[string][]byte
	del      []string
	patchErr error
	patches  int
}

func newRecordingStore(data map[string][]byte) *recordingStore {
	return &recordingStore{stubStore: newStubStore(data)}
}

func (r *recordingStore) Patch(_ context.Context, set map[string][]byte, del []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.patches++
	r.set, r.del = set, del
	return r.patchErr
}

// patchCount returns the number of Patch calls so far, synchronized so it is
// safe to read from a goroutine other than the one driving Patch.
func (r *recordingStore) patchCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.patches
}

// lastSet returns the set map from the most recent Patch call.
func (r *recordingStore) lastSet() map[string][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.set
}

// lastDel returns the del slice from the most recent Patch call.
func (r *recordingStore) lastDel() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.del
}

func TestCommitPatchesOnlyDirtyKeys(t *testing.T) {
	s := newRecordingStore(map[string][]byte{"a": []byte("1"), "b": []byte("2")})
	c := NewCommitter(s, NewCache(s, time.Hour))

	err := c.Commit(context.Background(), nil,
		map[string][]byte{"a": []byte("9")}, []string{"b"})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if len(s.set) != 1 || string(s.set["a"]) != "9" {
		t.Errorf("set = %v, want only a=9", s.set)
	}
	if len(s.del) != 1 || s.del[0] != "b" {
		t.Errorf("del = %v, want only [b]", s.del)
	}
}

// A merge patch has no optimistic concurrency check, so a single Patch call
// is the whole write. Any retry loop here would be a design regression.
func TestCommitDoesNotRetry(t *testing.T) {
	s := newRecordingStore(map[string][]byte{"a": []byte("1")})
	s.patchErr = errors.New("boom")
	c := NewCommitter(s, NewCache(s, time.Hour))

	if err := c.Commit(context.Background(), nil, map[string][]byte{"a": []byte("9")}, nil); err == nil {
		t.Fatal("Commit succeeded despite a Patch error")
	}
	if s.patches != 1 {
		t.Fatalf("patches = %d, want exactly 1 — no retry loop", s.patches)
	}
}

func TestCommitRefusesOversizedWrite(t *testing.T) {
	s := newRecordingStore(map[string][]byte{"a": []byte("1")})
	c := NewCommitter(s, NewCache(s, time.Hour))

	big := []byte(strings.Repeat("x", MaxObjectBytes+1))
	err := c.Commit(context.Background(), nil, map[string][]byte{"a": big}, nil)
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("Commit err = %v, want ErrTooLarge", err)
	}
	if s.patches != 0 {
		t.Fatal("oversized write reached the API server")
	}
}

// Fencing: if renewal has stopped succeeding we must not write under a lock
// that may already belong to someone else.
func TestCommitFencesOnLostLease(t *testing.T) {
	s := newRecordingStore(map[string][]byte{"a": []byte("1")})
	c := NewCommitter(s, NewCache(s, time.Hour))

	kc := fake.NewSimpleClientset()
	lease := NewLeaseManager(kc, "my-app", "roommate-oauth-credentials", "podA:1", testLeaseDur)
	// Never acquired, so Healthy() is false.

	err := c.Commit(context.Background(), lease, map[string][]byte{"a": []byte("9")}, nil)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Commit err = %v, want ErrLeaseLost", err)
	}
	if s.patches != 0 {
		t.Fatal("wrote despite an unhealthy lease")
	}
}

// After a successful write the writing node reads its own writes without a
// round trip.
func TestCommitUpdatesCacheOptimistically(t *testing.T) {
	s := newRecordingStore(map[string][]byte{"a": []byte("1")})
	cache := NewCache(s, time.Hour)
	if _, err := cache.Fresh(context.Background()); err != nil {
		t.Fatalf("warm cache: %v", err)
	}
	c := NewCommitter(s, cache)

	if err := c.Commit(context.Background(), nil, map[string][]byte{"a": []byte("9")}, nil); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if v, _ := cache.Current().Get("a"); string(v) != "9" {
		t.Fatalf("cache a = %q, want 9 immediately after commit", v)
	}
}
