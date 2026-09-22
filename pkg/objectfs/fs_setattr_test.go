package objectfs

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// TestSetattrDirectTouchFillsNlinkAndSize is TestSetattrTouchPreservesSizeAndNlink
// without a real FUSE mount: it calls File.Setattr directly with a
// SetAttrIn matching no branch (no size, mode, uid, or gid — exactly what a
// touch(1)-shaped ATIME/MTIME-only SETATTR looks like), and inspects the
// AttrOut go-fuse would hand to the kernel. Runs everywhere /dev/fuse
// doesn't, unlike every other test in this file.
func TestSetattrDirectTouchFillsNlinkAndSize(t *testing.T) {
	store := newRecordingStore(map[string][]byte{"session.key": []byte("hello")})
	cache := NewCache(store, time.Hour)
	if _, err := cache.Fresh(context.Background()); err != nil {
		t.Fatalf("warm cache: %v", err)
	}
	vol := &Volume{Cfg: Config{FileMode: 0o600}, Store: store, Cache: cache, Committer: NewCommitter(store, cache)}
	f := &File{vol: vol, key: "session.key"}

	var in fuse.SetAttrIn
	var out fuse.AttrOut
	if errno := f.Setattr(context.Background(), nil, &in, &out); errno != 0 {
		t.Fatalf("Setattr errno = %v", errno)
	}
	if out.Size != 5 {
		t.Fatalf("Size = %d, want 5 — an ATIME/MTIME-only SETATTR must not report the file truncated", out.Size)
	}
	if out.Nlink != 1 {
		t.Fatalf("Nlink = %d, want 1 — a zeroed Nlink marks a live inode as unlinked", out.Nlink)
	}
}

// TestSetattrTouchPreservesSizeAndNlink is I1's regression guard.
// go-fuse's bridge only patches AttrOut.Mode's type bits after Setattr
// returns (fs/bridge.go SetAttr), so anything this handler leaves zero is
// exactly what the kernel applies. Before the fix, Setattr wrote Size only
// inside its GetSize() branch and never wrote Nlink at all, so a SETATTR
// carrying only ATIME/MTIME — exactly what touch(1), cp -p, and
// os.Chtimes send — matched neither branch and returned a bare zeroed
// AttrOut: the kernel then truncates the page cache to 0 (fuse_change_
// attributes) and marks a live inode unlinked (set_nlink(inode, 0)).
func TestSetattrTouchPreservesSizeAndNlink(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"session.key": []byte("hello")})
	path := filepath.Join(dir, "session.key")

	now := time.Now()
	if err := os.Chtimes(path, now, now); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != 5 {
		t.Fatalf("size after touch = %d, want 5 — a touch(1)-shaped SETATTR "+
			"(ATIME/MTIME only) must not truncate the file", info.Size())
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("Sys() is not *syscall.Stat_t")
	}
	if st.Nlink != 1 {
		t.Fatalf("Nlink after touch = %d, want 1 — a zeroed Nlink marks a live inode as unlinked", st.Nlink)
	}
}

// A Setattr against an open handle with pending, uncommitted writes must
// report that handle's buffered size, not the last-committed snapshot's —
// the same rule File.Getattr follows for the same reason: an open fd's
// fstat must never disagree with what Read on that fd will return.
func TestSetattrTouchOnDirtyHandleReportsBufferedSize(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"session.key": []byte("v1")})
	path := filepath.Join(dir, "session.key")

	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Write([]byte("longer-value")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	now := time.Now()
	if err := os.Chtimes(path, now, now); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}

	info, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != int64(len("longer-value")) {
		t.Fatalf("size after touch on dirty handle = %d, want %d", info.Size(), len("longer-value"))
	}
}

// TestRootGetattrReportsNlink2 proves the mount root reports a conventional
// directory link count instead of the zero a caller checking st_nlink would
// treat as already-unlinked.
func TestRootGetattrReportsNlink2(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"a": []byte("1")})

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("Sys() is not *syscall.Stat_t")
	}
	if st.Nlink != 2 {
		t.Fatalf("root Nlink = %d, want 2", st.Nlink)
	}
}
