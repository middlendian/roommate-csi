package objectfs

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/watch"
)

// DefaultStalenessBound is how long the cache may go without confirmation
// from the API server before the next open() forces a quorum read. It exists
// to catch a watch that has died silently — the same job kubelet's periodic
// resync does for native projection.
const DefaultStalenessBound = 30 * time.Second

// Cache holds the current Snapshot for one mounted object.
//
// Reads are eventual, fed by a watch. The read-after-write guarantee is not
// provided here — it comes from callers invoking Fresh at the moment they
// acquire the Lease.
type Cache struct {
	store Store
	bound time.Duration

	snap atomic.Pointer[Snapshot]

	mu       sync.Mutex
	lastSync time.Time
}

// NewCache returns a Cache over store. A non-positive bound falls back to
// DefaultStalenessBound.
func NewCache(store Store, bound time.Duration) *Cache {
	if bound <= 0 {
		bound = DefaultStalenessBound
	}
	return &Cache{store: store, bound: bound}
}

// Current returns the cached Snapshot, or nil if nothing has been fetched.
func (c *Cache) Current() *Snapshot { return c.snap.Load() }

// Set installs snap as current. Called after a successful write so the
// writing node reads its own writes without a round trip.
func (c *Cache) Set(snap *Snapshot) {
	if snap == nil {
		return
	}
	c.snap.Store(snap)
	c.mu.Lock()
	c.lastSync = time.Now()
	c.mu.Unlock()
}

// Fresh performs a quorum read and installs the result. This is the
// read-after-write guarantee; it must be called on every Lease acquisition
// and must never be served from cache.
func (c *Cache) Fresh(ctx context.Context) (*Snapshot, error) {
	snap, err := c.store.Get(ctx)
	if err != nil {
		return nil, err
	}
	c.Set(snap)
	return snap, nil
}

// MaybeFresh returns the current Snapshot, refreshing first if the cache has
// gone unconfirmed for longer than the bound.
//
// It never returns an error: if the refresh fails, the stale snapshot is
// served anyway. A stale value costs the consumer a 401 and a retry; a
// failed read costs it an outage.
func (c *Cache) MaybeFresh(ctx context.Context) *Snapshot {
	cur := c.snap.Load()

	c.mu.Lock()
	age := time.Since(c.lastSync)
	c.mu.Unlock()

	if cur != nil && age < c.bound {
		return cur
	}
	if snap, err := c.Fresh(ctx); err == nil {
		return snap
	}
	return cur
}

// Run drives the watch loop until ctx is cancelled. On any disconnect it
// re-syncs with a quorum read before re-establishing the watch, so the cache
// is never trusted across a gap it cannot account for.
func (c *Cache) Run(ctx context.Context) {
	backoff := 100 * time.Millisecond
	const maxBackoff = 5 * time.Second

	for ctx.Err() == nil {
		snap, err := c.Fresh(ctx)
		if err != nil {
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}
		backoff = 100 * time.Millisecond

		if !c.watchOnce(ctx, snap.ResourceVersion) {
			return
		}
	}
}

// watchOnce runs a single watch until it closes or errors. It reports false
// only when ctx is done, so the caller can distinguish shutdown from a
// reconnect.
func (c *Cache) watchOnce(ctx context.Context, sinceRV string) bool {
	w, err := c.store.Watch(ctx, sinceRV)
	if err != nil {
		return ctx.Err() == nil
	}
	defer w.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case ev, ok := <-w.ResultChan():
			if !ok {
				return true // channel closed: reconnect via a fresh Get
			}
			switch ev.Type {
			case watch.Added, watch.Modified:
				if snap, err := c.store.Decode(ev.Object); err == nil {
					c.Set(snap)
				}
			case watch.Deleted:
				// Keep serving the last snapshot; writes will fail loudly.
				return true
			case watch.Error:
				return true
			}
		}
	}
}

// sleepCtx reports false if ctx finished first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
