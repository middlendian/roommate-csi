package objectfs

import (
	"context"
	"errors"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	// not here, not in the write path. Write and truncate both build their
	// result in a freshly allocated slice and swap it in, never
	// copy(buf[off:], data) or a reslice of the existing array, or they
	// could tear a read that is still in flight against the old array.
	buf []byte
	// dirty reports whether buf holds writes not yet committed to the store.
	dirty bool

	// lease is the Lease this handle holds, if any. Set by the lock path
	// (Task 13); Release must commit before releasing it.
	lease *LeaseManager
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

var (
	_ fs.FileWriter   = (*handle)(nil)
	_ fs.FileFlusher  = (*handle)(nil)
	_ fs.FileFsyncer  = (*handle)(nil)
	_ fs.FileReleaser = (*handle)(nil)
)

// Write buffers into the handle. Nothing reaches the API server until flush,
// fsync, or release — there is no time-based writeback, exactly as a local
// filesystem defers to its page cache.
//
// The result is always built in a freshly allocated slice, even when the
// write does not grow the buffer. h.buf's existing backing array may be
// aliased by an in-flight Read that already handed a sub-slice of it to the
// kernel (see the buf field comment above): mutating that array in place —
// copy(h.buf[off:], data) — could tear a read still being copied out after
// Read returned and h.mu was released. Writes are rare and reads are
// frequent, so paying one allocation per write is the right trade.
func (h *handle) Write(_ context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if off < 0 {
		return 0, syscall.EINVAL
	}
	size := len(h.buf)
	if end := int(off) + len(data); end > size {
		size = end
	}
	next := make([]byte, size)
	copy(next, h.buf)
	copy(next[off:], data)
	h.buf = next
	h.dirty = true
	return uint32(len(data)), 0
}

// truncate resizes the buffer. Called via Setattr when a file is opened with
// O_TRUNC, which is what os.WriteFile does.
//
// Like Write, the result is always a freshly allocated slice — never a
// reslice or in-place mutation of h.buf's existing backing array — for the
// same reason: that array may still be aliased by an in-flight Read.
func (h *handle) truncate(size uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if int(size) == len(h.buf) {
		return
	}
	next := make([]byte, size)
	copy(next, h.buf)
	h.buf = next
	h.dirty = true
}

// Flush commits any outstanding write. Called on every close(2), and
// possibly more than once per handle (e.g. dup'd descriptors each closing).
func (h *handle) Flush(ctx context.Context) syscall.Errno {
	return h.commit(ctx)
}

// Fsync commits any outstanding write, matching local filesystem fsync(2)
// semantics.
func (h *handle) Fsync(ctx context.Context, _ uint32) syscall.Errno {
	return h.commit(ctx)
}

// Release commits any outstanding write and then releases the Lease. The
// order is load-bearing: the next holder's mandatory quorum read must be able
// to observe our write, so releasing first would reintroduce the very race
// this driver exists to close.
func (h *handle) Release(ctx context.Context) syscall.Errno {
	errno := h.commit(ctx)
	if h.lease != nil {
		_ = h.lease.Release(ctx)
		h.lease = nil
	}
	return errno
}

// commit patches the buffer's current contents to the store if dirty, then
// clears dirty. A clean handle is a no-op, so a handle that is never written
// never sends a patch on close.
func (h *handle) commit(ctx context.Context) syscall.Errno {
	h.mu.Lock()
	if !h.dirty {
		h.mu.Unlock()
		return 0
	}
	value := make([]byte, len(h.buf))
	copy(value, h.buf)
	lease := h.lease
	h.mu.Unlock()

	if err := h.vol.Committer.Commit(ctx, lease, map[string][]byte{h.key: value}, nil); err != nil {
		return errnoFor(err)
	}

	h.mu.Lock()
	h.dirty = false
	h.mu.Unlock()
	return 0
}

// errnoFor maps a commit failure to the closest filesystem error, so a
// consumer sees a filesystem-shaped problem rather than an opaque API one.
func errnoFor(err error) syscall.Errno {
	switch {
	case errors.Is(err, ErrTooLarge):
		return syscall.ENOSPC
	case errors.Is(err, ErrLeaseLost):
		return syscall.EIO
	case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
		return syscall.EACCES
	case apierrors.IsNotFound(err):
		return syscall.ENOENT
	case apierrors.IsRequestEntityTooLargeError(err):
		return syscall.ENOSPC
	default:
		return syscall.EIO
	}
}
