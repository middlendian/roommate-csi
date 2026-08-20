package objectfs

import (
	"context"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// handle is one open file description.
//
// It owns three things that must stay consistent: the snapshot pinned at
// open, the write buffer that has not yet been committed, and the Lease if
// this handle took one.
type handle struct {
	vol *Volume
	key string

	mu   sync.Mutex
	snap *Snapshot
	// buf is the handle's pinned content. Read hands out sub-slices of it
	// directly (fuse.ReadResultData aliases rather than copies) and go-fuse
	// writes that slice to the kernel after Read returns and h.mu is
	// released. So: never mutate buf's existing backing array in place —
	// not here, not in the write path. A future Write must build its result
	// in a freshly allocated slice and swap it in, never
	// copy(buf[off:], data), or it can tear a read that is still in flight
	// against the old array.
	buf []byte
	// dirty is set by the write path (Task 12); unused until then.
	dirty bool //nolint:unused

	// lease is acquired by the write path (Task 13); unused until then.
	lease *LeaseManager //nolint:unused
}

var _ fs.FileReader = (*handle)(nil)

func newHandle(v *Volume, key string, snap *Snapshot, value []byte) *handle {
	return &handle{vol: v, key: key, snap: snap, buf: value}
}

// Read serves dest from the buffer pinned at Open, never from a later
// snapshot, so the read reflects one coherent version.
func (h *handle) Read(_ context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if off < 0 || off >= int64(len(h.buf)) {
		return fuse.ReadResultData(nil), 0
	}
	end := off + int64(len(dest))
	if end > int64(len(h.buf)) {
		end = int64(len(h.buf))
	}
	return fuse.ReadResultData(h.buf[off:end]), 0
}
