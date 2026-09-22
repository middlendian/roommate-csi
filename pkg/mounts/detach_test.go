package mounts

import (
	"path/filepath"
	"testing"
)

// DetachStale must be a safe, privilege-free no-op for the overwhelmingly
// common case: a target that was never mounted at all (a genuine first
// publish, before MkdirAll has even run).
func TestDetachStaleNoOpWhenTargetDoesNotExist(t *testing.T) {
	target := filepath.Join(t.TempDir(), "never-created")
	if err := DetachStale(target); err != nil {
		t.Fatalf("DetachStale on a nonexistent path: %v", err)
	}
}

// An ordinary directory — same device as its parent — must not trigger an
// unmount attempt at all, so this is safe to call unprivileged.
func TestDetachStaleNoOpWhenNotAMountpoint(t *testing.T) {
	dir := t.TempDir()
	if err := DetachStale(dir); err != nil {
		t.Fatalf("DetachStale on a plain directory: %v", err)
	}
}

func TestIsMountpointFalseForOrdinaryDir(t *testing.T) {
	dir := t.TempDir()
	mounted, err := isMountpoint(dir)
	if err != nil {
		t.Fatalf("isMountpoint: %v", err)
	}
	if mounted {
		t.Fatal("an ordinary temp dir reported as mounted")
	}
}

func TestIsMountpointFalseForMissingPath(t *testing.T) {
	mounted, err := isMountpoint(filepath.Join(t.TempDir(), "missing"))
	if err != nil {
		t.Fatalf("isMountpoint: %v", err)
	}
	if mounted {
		t.Fatal("a nonexistent path reported as mounted")
	}
}
