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

	// lease is the Lease this handle holds, if any. Set by setlk, cleared by
	// unlock and Release. Always accessed under mu — setlk assigns it from
	// the FUSE server's own goroutine, which can run concurrently with
	// Read/Write/commit on the same handle (dup'd fds, or a second thread
	// racing the lock call against an in-flight I/O).
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

	h.mu.Lock()
	lease := h.lease
	h.lease = nil
	h.mu.Unlock()

	if lease != nil {
		_ = lease.Release(ctx)
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

var (
	_ fs.FileSetlker  = (*handle)(nil)
	_ fs.FileSetlkwer = (*handle)(nil)
)

// Setlk is the non-blocking lock path: flock(LOCK_NB).
func (h *handle) Setlk(ctx context.Context, _ uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	return h.setlk(ctx, lk, flags, false)
}

// Setlkw is the blocking lock path.
//
// It blocks indefinitely rather than timing out, matching local filesystem
// semantics — an open file keeps its lock for as long as it wants. The
// kernel's FUSE interrupt path cancels ctx when the waiting process is
// signalled, so a signal still breaks the wait.
func (h *handle) Setlkw(ctx context.Context, _ uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	return h.setlk(ctx, lk, flags, true)
}

// setlk maps a flock(2) request onto this handle's Lease.
//
// Byte-range POSIX locks (fcntl F_SETLK/F_SETLKW without FUSE_LK_FLOCK) are
// not supported — flags carries FUSE_LK_FLOCK only for whole-file flock(2),
// which is the only lock mode this driver exists to serve.
//
// F_RDLCK (LOCK_SH) is mapped to the same exclusive Lease as F_WRLCK
// (LOCK_EX): over-strict, deliberately. A caller taking a shared lock is
// asking for "no writer is mid-write", and only the Lease — a single
// exclusive holder — can promise that.
func (h *handle) setlk(ctx context.Context, lk *fuse.FileLock, flags uint32, blocking bool) syscall.Errno {
	if flags&fuse.FUSE_LK_FLOCK == 0 {
		return syscall.ENOTSUP
	}

	if lk.Typ == syscall.F_UNLCK {
		return h.unlock(ctx)
	}

	h.mu.Lock()
	if h.lease != nil {
		h.mu.Unlock()
		return 0 // this handle already holds it
	}
	h.mu.Unlock()

	lease := h.vol.NewLease()
	if blocking {
		if err := lease.Acquire(ctx); err != nil {
			return syscall.EINTR
		}
	} else {
		ok, err := lease.TryAcquire(ctx)
		if err != nil {
			return errnoFor(err)
		}
		if !ok {
			return syscall.EWOULDBLOCK
		}
	}

	// The mandatory quorum read: this is the entire read-after-write
	// guarantee. It is what lets a consumer holding the lock ask "did
	// someone already refresh?" and get a truthful answer, rather than a
	// possibly-stale cached view that predates the write the lock was meant
	// to serialise against.
	if _, err := h.vol.Cache.Fresh(ctx); err != nil {
		_ = lease.Release(ctx)
		return errnoFor(err)
	}

	h.mu.Lock()
	h.lease = lease
	// Re-pin to the freshly read snapshot, unless this handle already has
	// uncommitted writes of its own — those must win over whatever the
	// quorum read just observed. snap.Get returns a fresh copy, so this
	// swaps in a new backing array rather than mutating buf's existing one,
	// preserving the copy-on-write discipline an in-flight Read depends on.
	if h.dirty {
		h.mu.Unlock()
		return 0
	}
	if snap := h.vol.Cache.Current(); snap != nil {
		h.snap = snap
		if value, ok := snap.Get(h.key); ok {
			h.buf = value
		}
	}
	h.mu.Unlock()
	return 0
}

// unlock commits before releasing the Lease, so the next holder's mandatory
// fresh read can observe this holder's write. Releasing first would
// reintroduce the exact race this driver exists to close.
func (h *handle) unlock(ctx context.Context) syscall.Errno {
	errno := h.commit(ctx)

	h.mu.Lock()
	lease := h.lease
	h.lease = nil
	h.mu.Unlock()

	if lease != nil {
		_ = lease.Release(ctx)
	}
	return errno
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
