package objectfs

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestReaddirListsKeys(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{
		"config.json":       []byte("{}"),
		".credentials.json": []byte("creds"),
		"session.key":       []byte("v1"),
	})

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var got []string
	for _, e := range entries {
		got = append(got, e.Name())
	}
	sort.Strings(got)
	want := []string{".credentials.json", "config.json", "session.key"}
	if len(got) != len(want) {
		t.Fatalf("entries = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("entries = %v, want %v", got, want)
		}
	}
}

func TestReadFileContents(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"session.key": []byte("hello world")})

	got, err := os.ReadFile(filepath.Join(dir, "session.key"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "hello world" {
		t.Fatalf("contents = %q, want hello world", got)
	}
}

func TestStatReportsSizeAndMode(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"session.key": []byte("12345")})

	fi, err := os.Stat(filepath.Join(dir, "session.key"))
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size() != 5 {
		t.Errorf("Size() = %d, want 5", fi.Size())
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Errorf("Mode() = %o, want 600", perm)
	}
	if fi.IsDir() {
		t.Error("regular file reported as a directory")
	}
}

func TestStatMissingFile(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"a": []byte("1")})

	if _, err := os.Stat(filepath.Join(dir, "nope")); !os.IsNotExist(err) {
		t.Fatalf("Stat(nope) err = %v, want IsNotExist", err)
	}
}

// A handle pins its snapshot at open. A concurrent remote change must not be
// visible through it — this is what reproduces kubelet's atomic ..data flip
// and prevents a read splicing two versions together.
//
// The change is landed via vol.Cache.Set, the same layer Getattr, Lookup,
// and Open all read through (f.vol.Cache.MaybeFresh). Mutating the backing
// store directly would not prove anything: StalenessBound is an hour, so
// Cache never re-queries the store during this test, and a regression that
// made handle.Read re-consult Cache on every call (instead of serving its
// pinned buffer) would go undetected.
func TestOpenHandlePinsSnapshot(t *testing.T) {
	dir, vol := mountForTest(t, map[string][]byte{"session.key": []byte("v1")})

	f, err := os.Open(filepath.Join(dir, "session.key"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	// Remote change lands in the cache after open.
	vol.Cache.Set(&Snapshot{Data: map[string][]byte{"session.key": []byte("v2")}, ResourceVersion: "2"})

	buf := make([]byte, 16)
	n, err := f.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(buf[:n]) != "v1" {
		t.Fatalf("read = %q, want v1 — the handle must keep its pinned snapshot", buf[:n])
	}
}

func TestSubdirectoriesAreRejected(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"a": []byte("1")})

	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o700); err == nil {
		t.Fatal("Mkdir succeeded; object keys are flat so it must fail")
	}
}

// TestStatReportsStableInode is I5's regression guard: fs.Options.EntryTimeout
// is nil, so the kernel re-looks-up a path on essentially every access.
// Before hashKey, Lookup left StableAttr.Ino at its zero value, and
// go-fuse's newInodeUnlocked fills a zero Ino from an incrementing counter
// — so every open()/stat() by path minted a fresh inode number for what is,
// as far as any consumer can tell, the very same file. Programs that detect
// rotation by comparing inode identity (os.SameFile, a raw st_ino compare)
// see the file as replaced on every check.
func TestStatReportsStableInode(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"session.key": []byte("v1")})
	path := filepath.Join(dir, "session.key")

	first, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat 1: %v", err)
	}
	second, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat 2: %v", err)
	}
	if !os.SameFile(first, second) {
		t.Fatal("two stats of the same path report different inodes")
	}
}

// Two distinct keys must not collide onto the same inode number — that
// would make go-fuse's addNewChild treat them as the same node.
func TestStatReportsDistinctInodesForDistinctKeys(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"a": []byte("1"), "b": []byte("2")})

	sa, err := os.Stat(filepath.Join(dir, "a"))
	if err != nil {
		t.Fatalf("Stat a: %v", err)
	}
	sb, err := os.Stat(filepath.Join(dir, "b"))
	if err != nil {
		t.Fatalf("Stat b: %v", err)
	}
	if os.SameFile(sa, sb) {
		t.Fatal("distinct keys a and b report the same inode")
	}
}
