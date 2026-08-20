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

// os.WriteFile's open(O_TRUNC) on a key that already exists must fold the
// truncate into the same handle as the write, producing exactly one Patch.
// A second, earlier Patch would mean the truncate committed live before the
// real content did — the bug this test exists to catch: if the real content
// then never lands (rejected, or the process dying before flush), the key
// is left permanently empty with no way to recover the original value. See
// mount.go's CAP_ATOMIC_O_TRUNC comment and File.Open's O_TRUNC handling.
func TestOpenTruncOnExistingKeyIssuesExactlyOnePatch(t *testing.T) {
	dir, vol := mountForTest(t, map[string][]byte{"session.key": []byte("v1")})
	store := vol.Store.(*recordingStore)

	if err := os.WriteFile(filepath.Join(dir, "session.key"), []byte("v2"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if got := store.patchCount(); got != 1 {
		t.Fatalf("patches = %d, want exactly 1 — a second patch means the truncate committed live, ahead of the real content", got)
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

// A rejected oversized write must leave the key's original value intact.
// Before CAP_ATOMIC_O_TRUNC negotiation, the pre-open truncate for O_TRUNC
// committed an empty value immediately and unconditionally; when the real
// content was then rejected here for being too large, nothing ever
// recommitted the original "1" — the key was left permanently empty. That
// is the property that actually matters, not just that the write errors.
func TestWriteBeyondCeilingReturnsENOSPC(t *testing.T) {
	dir, vol := mountForTest(t, map[string][]byte{"a": []byte("1")})
	store := vol.Store.(*recordingStore)

	big := []byte(strings.Repeat("x", MaxObjectBytes+1))
	err := os.WriteFile(filepath.Join(dir, "a"), big, 0o600)
	if err == nil {
		t.Fatal("oversized write succeeded")
	}
	if !strings.Contains(err.Error(), "no space") {
		t.Fatalf("err = %v, want ENOSPC", err)
	}

	if got := store.patchCount(); got != 0 {
		t.Fatalf("patches = %d, want 0 — the rejected write must never have reached the store", got)
	}

	got, err := os.ReadFile(filepath.Join(dir, "a"))
	if err != nil {
		t.Fatalf("ReadFile after rejected write: %v", err)
	}
	if string(got) != "1" {
		t.Fatalf("value after rejected write = %q, want the original \"1\" to survive", got)
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
