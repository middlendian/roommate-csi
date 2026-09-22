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

	// watchCalls counts Watch invocations so tests can assert reconnect
	// attempts are throttled rather than unbounded.
	watchCalls int

	// decodes counts Decode invocations, which happen once per watch event
	// actually pulled off the result channel and processed by watchOnce.
	// Tests use this to wait until an injected event has definitely been
	// handled — including, deliberately, one the ordering guard then drops
	// — rather than sleeping and hoping the goroutine got scheduled in time.
	decodes int
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

// setSnap replaces the stub's underlying snapshot, so a later Get reflects
// it. Unlike driving a change through Cache.Set, this simulates what a real
// remote change (e.g. another writer's delete) looks like by the time a
// mandatory quorum read (Cache.Fresh) reaches the store — Cache.Set alone
// would just be overwritten by that same quorum read.
func (s *stubStore) setSnap(snap *Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snap = snap
}

func (s *stubStore) Watch(context.Context, string) (watch.Interface, error) {
	s.mu.Lock()
	s.watchCalls++
	err := s.watchErr
	s.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return watch.NewProxyWatcher(s.watchCh), nil
}

func (s *stubStore) watchCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.watchCalls
}

func (s *stubStore) Decode(obj runtime.Object) (*Snapshot, error) {
	s.mu.Lock()
	s.decodes++
	s.mu.Unlock()
	return obj.(*snapObject).snap, nil
}

func (s *stubStore) decodeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.decodes
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

// A watch that can never establish — e.g. RBAC granting get but not watch —
// must not spin Run's Fresh/Watch loop against the API server unthrottled.
func TestCacheRunBacksOffOnWatchEstablishFailure(t *testing.T) {
	s := newStubStore(map[string][]byte{"a": []byte("1")})
	s.watchErr = errors.New("watch forbidden") // set before Run starts, so no lock is needed here

	c := NewCache(s, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	go c.Run(ctx)

	time.Sleep(300 * time.Millisecond)
	cancel()

	if got := s.watchCallCount(); got > 10 {
		t.Fatalf("Watch called %d times in 300ms with a failing watch — want a bounded, backed-off retry count", got)
	}
}

// TestCacheSetFromWatchDropsStaleResourceVersion is C2's core regression
// guard: Set(RV52) then Set(RV51) through the watch path must leave RV52
// installed. Before the fix, watchOnce called the unconditional Set for
// every watch event, so a quorum read (Fresh) at RV52 followed by a
// late-arriving RV51 watch event — one event of lag behind a client-go
// StreamWatcher is routine, not exotic — would silently clobber the fresher
// value with older content. That is the exact mechanism a lock holder's
// mandatory quorum read exists to prevent.
func TestCacheSetFromWatchDropsStaleResourceVersion(t *testing.T) {
	s := newStubStore(map[string][]byte{"session.key": []byte("REFRESHED")})
	c := NewCache(s, time.Hour)

	c.setFromWatch(&Snapshot{
		Data: map[string][]byte{"session.key": []byte("REFRESHED")}, ResourceVersion: "52",
	})
	c.setFromWatch(&Snapshot{
		Data: map[string][]byte{"session.key": []byte("EXPIRED")}, ResourceVersion: "51",
	})

	cur := c.Current()
	if cur.ResourceVersion != "52" {
		t.Fatalf("ResourceVersion = %q, want 52 — an older watch event must not replace a newer snapshot", cur.ResourceVersion)
	}
	if v, _ := cur.Get("session.key"); string(v) != "REFRESHED" {
		t.Fatalf("session.key = %q, want REFRESHED", v)
	}
}

// Equal ResourceVersions (a redelivered or duplicate event) must also not
// displace the current entry — "<=", not "<".
func TestCacheSetFromWatchDropsEqualResourceVersion(t *testing.T) {
	s := newStubStore(map[string][]byte{"a": []byte("1")})
	c := NewCache(s, time.Hour)

	first := &Snapshot{Data: map[string][]byte{"a": []byte("first")}, ResourceVersion: "10"}
	c.setFromWatch(first)
	c.setFromWatch(&Snapshot{Data: map[string][]byte{"a": []byte("second")}, ResourceVersion: "10"})

	if v, _ := c.Current().Get("a"); string(v) != "first" {
		t.Fatalf("a = %q, want first — a same-RV event must not displace the current entry", v)
	}
}

// A newer ResourceVersion must still install normally.
func TestCacheSetFromWatchAppliesNewerResourceVersion(t *testing.T) {
	s := newStubStore(map[string][]byte{"a": []byte("1")})
	c := NewCache(s, time.Hour)

	c.setFromWatch(&Snapshot{Data: map[string][]byte{"a": []byte("old")}, ResourceVersion: "10"})
	c.setFromWatch(&Snapshot{Data: map[string][]byte{"a": []byte("new")}, ResourceVersion: "11"})

	if v, _ := c.Current().Get("a"); string(v) != "new" {
		t.Fatalf("a = %q, want new", v)
	}
}

// An unparsable ResourceVersion on either side must install rather than
// silently drop: refusing to make a disprovable ordering call is safer than
// discarding a legitimate update.
func TestCacheSetFromWatchInstallsOnUnparsableResourceVersion(t *testing.T) {
	s := newStubStore(map[string][]byte{"a": []byte("1")})
	c := NewCache(s, time.Hour)

	c.setFromWatch(&Snapshot{Data: map[string][]byte{"a": []byte("first")}, ResourceVersion: "not-a-number"})
	c.setFromWatch(&Snapshot{Data: map[string][]byte{"a": []byte("second")}, ResourceVersion: "not-a-number-either"})

	if v, _ := c.Current().Get("a"); string(v) != "second" {
		t.Fatalf("a = %q, want second — unparsable RVs must not block installation", v)
	}
}

// TestCacheRunBacksOffOnRepeatedInstantWatchClose is M10's regression
// guard. Before the fix, watchOnce's watchClosed outcome reset the backoff
// unconditionally, with no minimum-duration or minimum-events check — a
// watch that closes the instant it's established (every stubStore.Watch
// call here returns a ProxyWatcher over an already-closed channel) hits
// that reset on literally every iteration and, since the watchClosed branch
// also never called sleepCtx, spun Fresh+Watch against the store with no
// throttling at all.
func TestCacheRunBacksOffOnRepeatedInstantWatchClose(t *testing.T) {
	s := newStubStore(map[string][]byte{"a": []byte("1")})
	close(s.watchCh) // every subsequent Watch() returns an already-closed channel

	c := NewCache(s, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	go c.Run(ctx)

	time.Sleep(300 * time.Millisecond)
	cancel()

	if got := s.watchCallCount(); got > 20 {
		t.Fatalf("Watch called %d times in 300ms against a watch that closes instantly every "+
			"time — want a bounded, backed-off retry count", got)
	}
}

// A Deleted event on the watch must not trigger an instant reconnect: Run
// has to back off first, the same as any other disconnect that isn't a
// clean, long-lived channel close.
func TestCacheRunBacksOffAfterDeletedEvent(t *testing.T) {
	s := newStubStore(map[string][]byte{"a": []byte("1")})
	c := NewCache(s, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	deadline := time.After(2 * time.Second)
	for s.watchCallCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("initial watch never established")
		case <-time.After(5 * time.Millisecond):
		}
	}

	s.watchCh <- watch.Event{Type: watch.Deleted}

	// Right after the Deleted event, a reconnect must not have happened yet
	// — it has to go through backoff first.
	time.Sleep(50 * time.Millisecond)
	if got := s.watchCallCount(); got != 1 {
		t.Fatalf("Watch reconnected instantly after Deleted (calls=%d) — want backoff before retry", got)
	}

	// It does eventually reconnect.
	deadline = time.After(2 * time.Second)
	for s.watchCallCount() < 2 {
		select {
		case <-deadline:
			t.Fatal("watch never reconnected after Deleted event")
		case <-time.After(5 * time.Millisecond):
		}
	}
}
