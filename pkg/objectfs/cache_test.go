package objectfs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
)

// stubStore counts calls so tests can assert on API traffic, which is the
// whole point of the staleness bound.
type stubStore struct {
	mu       sync.Mutex
	gets     int
	getErr   error
	snap     *Snapshot
	watchCh  chan watch.Event
	watchErr error
}

func newStubStore(data map[string][]byte) *stubStore {
	return &stubStore{
		snap:    &Snapshot{Data: data, ResourceVersion: "1", FetchedAt: time.Now()},
		watchCh: make(chan watch.Event, 8),
	}
}

func (s *stubStore) Get(context.Context) (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	if s.getErr != nil {
		return nil, s.getErr
	}
	cp := *s.snap
	cp.FetchedAt = time.Now()
	return &cp, nil
}

func (s *stubStore) getCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets
}

func (s *stubStore) Watch(context.Context, string) (watch.Interface, error) {
	if s.watchErr != nil {
		return nil, s.watchErr
	}
	return watch.NewProxyWatcher(s.watchCh), nil
}

func (s *stubStore) Decode(obj runtime.Object) (*Snapshot, error) {
	return obj.(*snapObject).snap, nil
}

func (s *stubStore) Patch(context.Context, map[string][]byte, []string) error { return nil }
func (s *stubStore) Describe() string                                         { return "stub/obj" }

// snapObject smuggles a Snapshot through the runtime.Object interface so the
// stub can drive Decode without a real API type.
type snapObject struct {
	runtime.Object
	snap *Snapshot
}

func (o *snapObject) DeepCopyObject() runtime.Object { return o }

func TestCacheFreshAlwaysHitsTheAPI(t *testing.T) {
	s := newStubStore(map[string][]byte{"a": []byte("1")})
	c := NewCache(s, time.Hour) // bound is irrelevant to Fresh

	for i := 0; i < 3; i++ {
		if _, err := c.Fresh(context.Background()); err != nil {
			t.Fatalf("Fresh: %v", err)
		}
	}
	if got := s.getCount(); got != 3 {
		t.Fatalf("gets = %d, want 3 — Fresh must never serve from cache", got)
	}
}

func TestCacheMaybeFreshRespectsTheBound(t *testing.T) {
	s := newStubStore(map[string][]byte{"a": []byte("1")})
	c := NewCache(s, time.Hour)

	first := c.MaybeFresh(context.Background()) // cold: must fetch
	if first == nil {
		t.Fatal("MaybeFresh returned nil on cold cache")
	}
	if got := s.getCount(); got != 1 {
		t.Fatalf("gets = %d, want 1", got)
	}

	c.MaybeFresh(context.Background()) // warm and inside bound: no fetch
	if got := s.getCount(); got != 1 {
		t.Fatalf("gets = %d, want still 1 — inside the bound", got)
	}
}

func TestCacheMaybeFreshRefetchesPastTheBound(t *testing.T) {
	s := newStubStore(map[string][]byte{"a": []byte("1")})
	c := NewCache(s, time.Nanosecond)

	c.MaybeFresh(context.Background())
	time.Sleep(time.Millisecond)
	c.MaybeFresh(context.Background())

	if got := s.getCount(); got != 2 {
		t.Fatalf("gets = %d, want 2 — past the bound must refetch", got)
	}
}

// A failed refresh degrades to stale rather than failing the read: a stale
// credential costs the consumer a 401 and a retry, a failed read costs an outage.
func TestCacheMaybeFreshServesStaleOnError(t *testing.T) {
	s := newStubStore(map[string][]byte{"a": []byte("1")})
	c := NewCache(s, time.Nanosecond)

	warm := c.MaybeFresh(context.Background())
	if warm == nil {
		t.Fatal("cold MaybeFresh returned nil")
	}

	s.mu.Lock()
	s.getErr = errors.New("apiserver unreachable")
	s.mu.Unlock()

	time.Sleep(time.Millisecond)
	got := c.MaybeFresh(context.Background())
	if got == nil {
		t.Fatal("MaybeFresh returned nil instead of degrading to stale")
	}
	if v, _ := got.Get("a"); string(v) != "1" {
		t.Fatalf("stale value = %q, want 1", v)
	}
}

func TestCacheSetIsVisibleImmediately(t *testing.T) {
	s := newStubStore(map[string][]byte{"a": []byte("1")})
	c := NewCache(s, time.Hour)

	c.Set(&Snapshot{Data: map[string][]byte{"a": []byte("2")}, ResourceVersion: "9", FetchedAt: time.Now()})

	if v, _ := c.Current().Get("a"); string(v) != "2" {
		t.Fatalf("Current() = %q, want 2", v)
	}
}

func TestCacheRunAppliesWatchEvents(t *testing.T) {
	s := newStubStore(map[string][]byte{"a": []byte("1")})
	c := NewCache(s, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	next := &Snapshot{Data: map[string][]byte{"a": []byte("2")}, ResourceVersion: "2", FetchedAt: time.Now()}
	s.watchCh <- watch.Event{Type: watch.Modified, Object: &snapObject{snap: next}}

	deadline := time.After(2 * time.Second)
	for {
		if cur := c.Current(); cur != nil {
			if v, _ := cur.Get("a"); string(v) == "2" {
				return
			}
		}
		select {
		case <-deadline:
			t.Fatal("watch event never applied to cache")
		case <-time.After(5 * time.Millisecond):
		}
	}
}
