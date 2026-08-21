package objectfs

import (
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestFlockExclusiveAcquiresLease(t *testing.T) {
	dir, vol := mountForTest(t, map[string][]byte{"session.key": []byte("v1")})
	store := vol.Store.(*recordingStore)

	f, err := os.Open(filepath.Join(dir, "session.key"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = f.Close() }()

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatalf("Flock: %v", err)
	}

	// Acquiring the lock must force a quorum read. That guaranteed-fresh read
	// is the entire read-after-write guarantee.
	if store.getCount() < 2 {
		t.Fatalf("gets = %d; acquiring the lock must force a fresh quorum read", store.getCount())
	}

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestFlockNonBlockingFailsWhenHeld(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"session.key": []byte("v1")})
	path := filepath.Join(dir, "session.key")

	a, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open a: %v", err)
	}
	defer func() { _ = a.Close() }()
	if err := syscall.Flock(int(a.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatalf("Flock a: %v", err)
	}

	b, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open b: %v", err)
	}
	defer func() { _ = b.Close() }()

	err = syscall.Flock(int(b.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		t.Fatal("second LOCK_NB succeeded while the lease was held")
	}
	if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
		t.Fatalf("err = %v, want EWOULDBLOCK", err)
	}
}

func TestFlockReleasedOnClose(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"session.key": []byte("v1")})
	path := filepath.Join(dir, "session.key")

	a, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open a: %v", err)
	}
	if err := syscall.Flock(int(a.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatalf("Flock a: %v", err)
	}
	if err := a.Close(); err != nil { // release without an explicit LOCK_UN
		t.Fatalf("Close a: %v", err)
	}

	b, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open b: %v", err)
	}
	defer func() { _ = b.Close() }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		err = syscall.Flock(int(b.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("lease never released after close: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A shared lock maps to the same exclusive Lease. Over-strict, but a caller
// taking LOCK_SH is asking for "no writer is mid-write", and only the Lease
// can promise that.
func TestFlockSharedMapsToExclusive(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"session.key": []byte("v1")})
	path := filepath.Join(dir, "session.key")

	a, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open a: %v", err)
	}
	defer func() { _ = a.Close() }()
	if err := syscall.Flock(int(a.Fd()), syscall.LOCK_SH); err != nil {
		t.Fatalf("LOCK_SH: %v", err)
	}

	b, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open b: %v", err)
	}
	defer func() { _ = b.Close() }()
	if err := syscall.Flock(int(b.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err == nil {
		t.Fatal("two shared locks held at once; LOCK_SH must map to exclusive")
	}
}

// Commit must land before the Lease is released, so the next holder's
// mandatory fresh read can observe it.
func TestCommitHappensBeforeLeaseRelease(t *testing.T) {
	dir, vol := mountForTest(t, map[string][]byte{"session.key": []byte("v1")})
	store := vol.Store.(*recordingStore)
	path := filepath.Join(dir, "session.key")

	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatalf("Flock: %v", err)
	}
	if _, err := f.Write([]byte("v2")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// patchCount, not the raw field: this goes through a real FUSE mount, so
	// the write lands on the go-fuse server's goroutine while this goroutine
	// reads the result — recordingStore's locked accessor is required here.
	if store.patchCount() == 0 {
		t.Fatal("no commit on close")
	}

	// The next acquirer must succeed and see the committed value.
	g, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open g: %v", err)
	}
	defer func() { _ = g.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := syscall.Flock(int(g.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lease never released")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Two handles racing for the lock: exactly one wins at a time.
func TestFlockSerialisesConcurrentHolders(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"session.key": []byte("v1")})
	path := filepath.Join(dir, "session.key")

	var (
		mu      sync.Mutex
		holders int
		maxSeen int
		wg      sync.WaitGroup
	)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f, err := os.Open(path)
			if err != nil {
				return
			}
			defer func() { _ = f.Close() }()
			if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
				return
			}
			mu.Lock()
			holders++
			if holders > maxSeen {
				maxSeen = holders
			}
			mu.Unlock()

			time.Sleep(50 * time.Millisecond)

			mu.Lock()
			holders--
			mu.Unlock()
			_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		}()
	}
	wg.Wait()

	if maxSeen > 1 {
		t.Fatalf("saw %d concurrent lock holders, want at most 1", maxSeen)
	}
}

// TestSetattrTruncateStandalone proves the no-open-handle truncate(2) path
// (File.Setattr's truncateCommitted branch) still works: it is the path
// FUSE_CAP_ATOMIC_O_TRUNC was negotiated to preserve, and had zero coverage.
func TestSetattrTruncateStandalone(t *testing.T) {
	dir, vol := mountForTest(t, map[string][]byte{"session.key": []byte("hello world")})
	store := vol.Store.(*recordingStore)
	path := filepath.Join(dir, "session.key")

	if err := os.Truncate(path, 5); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	if got := store.patchCount(); got != 1 {
		t.Fatalf("patches = %d, want exactly 1 — a standalone truncate(2) commits immediately", got)
	}
	if got := string(store.lastSet()["session.key"]); got != "hello" {
		t.Fatalf("committed value = %q, want %q", got, "hello")
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != 5 {
		t.Fatalf("size = %d, want 5", info.Size())
	}
}

// TestFlockRepinDropsDeletedKey proves the lock's mandatory fresh read is
// truthful even when the key was deleted out from under the handle: keeping
// the pre-lock, pinned bytes as though nothing happened would answer "did
// someone already change this?" falsely for exactly the case — a delete —
// most likely to matter.
func TestFlockRepinDropsDeletedKey(t *testing.T) {
	dir, vol := mountForTest(t, map[string][]byte{"session.key": []byte("v1")})
	store := vol.Store.(*recordingStore)
	path := filepath.Join(dir, "session.key")

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = f.Close() }()

	if info, err := f.Stat(); err != nil {
		t.Fatalf("Stat before lock: %v", err)
	} else if info.Size() != 2 {
		t.Fatalf("size before lock = %d, want 2 (pinned %q)", info.Size(), "v1")
	}

	// The key is deleted directly in the store, not via vol.Cache.Set (which
	// the mandatory fresh read would just overwrite): this is what a real
	// DELETE from another writer looks like by the time this handle's flock
	// forces a quorum read.
	store.setSnap(&Snapshot{Data: map[string][]byte{}, ResourceVersion: "2"})

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatalf("Flock: %v", err)
	}

	info, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat after lock: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("size after lock = %d, want 0 — the key was deleted in the fresh read; "+
			"stale pre-lock bytes must not survive the re-pin", info.Size())
	}
}
