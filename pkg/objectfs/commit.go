package objectfs

import (
	"context"
	"errors"
)

// ErrTooLarge means the write would push the object past etcd's practical
// ceiling. Surfaced as ENOSPC so the consumer sees a filesystem-shaped error
// rather than an opaque API rejection.
var ErrTooLarge = errors.New("roommate: object would exceed size limit")

// Committer turns a handle's buffered writes into a single per-key merge
// patch.
//
// It is deliberately thin. Because a merge patch mentions only the keys we
// touched, keys we never wrote are preserved by the request itself — there is
// no read-modify-write, no optimistic concurrency check, and no retry loop.
type Committer struct {
	store Store
	cache *Cache
}

// NewCommitter returns a Committer writing through store and refreshing cache.
func NewCommitter(store Store, cache *Cache) *Committer {
	return &Committer{store: store, cache: cache}
}

// Commit applies set and del as one merge patch.
//
// If lease is non-nil it is fenced first: a lease whose renewal has stopped
// succeeding refuses the write rather than committing under a lock that may
// already belong to someone else.
func (c *Committer) Commit(ctx context.Context, lease *LeaseManager, set map[string][]byte, del []string) error {
	if len(set) == 0 && len(del) == 0 {
		return nil
	}
	if lease != nil && !lease.Healthy() {
		return ErrLeaseLost
	}
	if err := c.checkSize(set, del); err != nil {
		return err
	}
	if err := c.store.Patch(ctx, set, del); err != nil {
		return err
	}
	// Read-your-own-writes locally, without a round trip.
	if cur := c.cache.Current(); cur != nil {
		c.cache.Set(cur.With(set, del))
	}
	return nil
}

// checkSize projects the write onto the current snapshot and rejects it if
// the result would exceed the ceiling. Without a cached snapshot the write is
// allowed through and the API server has the final say.
func (c *Committer) checkSize(set map[string][]byte, del []string) error {
	cur := c.cache.Current()
	if cur == nil {
		for _, v := range set {
			if len(v) > MaxObjectBytes {
				return ErrTooLarge
			}
		}
		return nil
	}
	if cur.With(set, del).Size() > MaxObjectBytes {
		return ErrTooLarge
	}
	return nil
}
