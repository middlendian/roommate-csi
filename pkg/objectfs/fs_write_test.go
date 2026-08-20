package objectfs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteCommitsOnClose(t *testing.T) {
	dir, vol := mountForTest(t, map[string][]byte{"session.key": []byte("v1")})
	store := vol.Store.(*recordingStore)

	if err := os.WriteFile(filepath.Join(dir, "session.key"), []byte("v2"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if store.patchCount() == 0 {
		t.Fatal("close did not commit")
	}
	if got := string(store.lastSet()["session.key"]); got != "v2" {
		t.Fatalf("patched value = %q, want v2", got)
	}
}

// A merge patch must mention only what changed — that is what makes
// concurrent writes to different keys structurally safe.
func TestWritePatchesOnlyTheWrittenKey(t *testing.T) {
	dir, vol := mountForTest(t, map[string][]byte{
		"session.key": []byte("v1"),
		"config.json": []byte("{}"),
	})
	store := vol.Store.(*recordingStore)

	if err := os.WriteFile(filepath.Join(dir, "session.key"), []byte("v2"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	set := store.lastSet()
	if _, ok := set["config.json"]; ok {
		t.Fatal("patch mentioned config.json, which was never written")
	}
	if len(set) != 1 {
		t.Fatalf("patch set = %v, want only session.key", set)
	}
}

func TestCreateNewFile(t *testing.T) {
	dir, vol := mountForTest(t, map[string][]byte{"a": []byte("1")})
	store := vol.Store.(*recordingStore)

	if err := os.WriteFile(filepath.Join(dir, "new.json"), []byte("{}"), 0o600); err != nil {
		t.Fatalf("create: %v", err)
	}
	if got := string(store.lastSet()["new.json"]); got != "{}" {
		t.Fatalf("new.json = %q, want {}", got)
	}
}

func TestCreateRejectsInvalidKey(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"a": []byte("1")})

	err := os.WriteFile(filepath.Join(dir, "not valid"), []byte("x"), 0o600)
	if err == nil {
		t.Fatal("created a file whose name is not a valid object key")
	}
}

func TestUnlinkDeletesKey(t *testing.T) {
	dir, vol := mountForTest(t, map[string][]byte{"a": []byte("1"), "b": []byte("2")})
	store := vol.Store.(*recordingStore)

	if err := os.Remove(filepath.Join(dir, "b")); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	del := store.lastDel()
	if len(del) != 1 || del[0] != "b" {
		t.Fatalf("del = %v, want [b]", del)
	}
	if set := store.lastSet(); len(set) != 0 {
		t.Fatalf("unlink also set keys: %v", set)
	}
}

func TestRenameIsCopyPlusDelete(t *testing.T) {
	dir, vol := mountForTest(t, map[string][]byte{"old.json": []byte("data")})
	store := vol.Store.(*recordingStore)

	if err := os.Rename(filepath.Join(dir, "old.json"), filepath.Join(dir, "new.json")); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if got := string(store.lastSet()["new.json"]); got != "data" {
		t.Fatalf("new.json = %q, want data", got)
	}
	del := store.lastDel()
	if len(del) != 1 || del[0] != "old.json" {
		t.Fatalf("del = %v, want [old.json]", del)
	}
}

// Modes come from mount attributes and are not stored in the object, so the
// object stays interoperable with kubelet projection and kubectl edit.
func TestChmodIsRefused(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"a": []byte("1")})

	if err := os.Chmod(filepath.Join(dir, "a"), 0o644); err == nil {
		t.Fatal("chmod succeeded; modes come from mount attributes")
	}
}

func TestWriteBeyondCeilingReturnsENOSPC(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"a": []byte("1")})

	big := []byte(strings.Repeat("x", MaxObjectBytes+1))
	err := os.WriteFile(filepath.Join(dir, "a"), big, 0o600)
	if err == nil {
		t.Fatal("oversized write succeeded")
	}
	if !strings.Contains(err.Error(), "no space") {
		t.Fatalf("err = %v, want ENOSPC", err)
	}
}

// Nothing reaches the API server until flush/close: there is no time-based
// writeback.
func TestNoWritebackBeforeClose(t *testing.T) {
	dir, vol := mountForTest(t, map[string][]byte{"a": []byte("1")})
	store := vol.Store.(*recordingStore)

	f, err := os.OpenFile(filepath.Join(dir, "a"), os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write([]byte("2")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if store.patchCount() != 0 {
		t.Fatal("write reached the API server before flush")
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if store.patchCount() == 0 {
		t.Fatal("close did not commit")
	}
}

// File.Getattr's fh-aware branch (Task 11) must size an open fd's fstat from
// the handle's pinned buffer, not the live snapshot. Otherwise fstat(fd)
// could report a size newer than what Read on that same fd will ever
// return — the same spliced-read failure pinning exists to prevent. This
// guards that branch: a revert to always reading the live snapshot would
// slip through unnoticed without it.
func TestFstatOnOpenFdUsesHandleSize(t *testing.T) {
	dir, vol := mountForTest(t, map[string][]byte{"session.key": []byte("12345")})

	f, err := os.Open(filepath.Join(dir, "session.key"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	// The object changes size in the cache after Open, without going through
	// this handle.
	vol.Cache.Set(&Snapshot{
		Data:            map[string][]byte{"session.key": []byte("a much longer value than before")},
		ResourceVersion: "2",
	})

	fi, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size() != 5 {
		t.Fatalf("fstat size = %d, want 5 (the pinned handle buffer), not the live snapshot's size", fi.Size())
	}
}
