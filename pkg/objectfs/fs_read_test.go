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
func TestOpenHandlePinsSnapshot(t *testing.T) {
	dir, store := mountForTest(t, map[string][]byte{"session.key": []byte("v1")})

	f, err := os.Open(filepath.Join(dir, "session.key"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	// Remote change lands after open.
	store.mu.Lock()
	store.snap = &Snapshot{Data: map[string][]byte{"session.key": []byte("v2")}, ResourceVersion: "2"}
	store.mu.Unlock()

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
