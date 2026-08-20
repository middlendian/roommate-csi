package mounts

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func newLive() *Live {
	return &Live{Cancel: func() {}, Unmount: func() error { return nil }}
}

func testMount(target string) Mount {
	return Mount{TargetPath: target, ObjectKind: "Secret", ObjectName: "oauth-credentials", Namespace: "my-app"}
}

func TestRegistryPutGetDelete(t *testing.T) {
	r, err := NewRegistry(filepath.Join(t.TempDir(), "published.json"))
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	if _, ok := r.Get("/t/1"); ok {
		t.Fatal("empty registry returned a mount")
	}
	if err := r.Put("/t/1", newLive(), testMount("/t/1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, ok := r.Get("/t/1"); !ok {
		t.Fatal("Get after Put missed")
	}
	if err := r.Delete(context.Background(), "/t/1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok := r.Get("/t/1"); ok {
		t.Fatal("Get after Delete hit")
	}
}

// The state file is the only thing that tells a post-restart republish from a
// genuine first publish. Getting this wrong means either erroring on
// republish (which deletes the mount point, k8s#121271) or silently
// swallowing a real authorization failure.
func TestRegistrySurvivesRestart(t *testing.T) {
	state := filepath.Join(t.TempDir(), "published.json")

	first, err := NewRegistry(state)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	if err := first.Put("/t/1", newLive(), testMount("/t/1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Simulate a plugin restart: in-memory state is gone, the file is not.
	second, err := NewRegistry(state)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, ok := second.Get("/t/1"); ok {
		t.Fatal("live mount survived a restart; only the record should")
	}
	if !second.WasPublished("/t/1") {
		t.Fatal("WasPublished false after restart; republish would wrongly error")
	}
	if second.WasPublished("/t/never") {
		t.Fatal("WasPublished true for a target never published")
	}
}

func TestRegistryDeleteRemovesFromState(t *testing.T) {
	state := filepath.Join(t.TempDir(), "published.json")
	r, _ := NewRegistry(state)
	_ = r.Put("/t/1", newLive(), testMount("/t/1"))
	if err := r.Delete(context.Background(), "/t/1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	again, _ := NewRegistry(state)
	if again.WasPublished("/t/1") {
		t.Fatal("record survived Delete")
	}
}

func TestRegistryDeleteRunsTeardown(t *testing.T) {
	r, _ := NewRegistry(filepath.Join(t.TempDir(), "published.json"))

	cancelled, unmounted := false, false
	live := &Live{
		Cancel:  func() { cancelled = true },
		Unmount: func() error { unmounted = true; return nil },
	}
	_ = r.Put("/t/1", live, testMount("/t/1"))

	if err := r.Delete(context.Background(), "/t/1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !cancelled {
		t.Error("watch context not cancelled")
	}
	if !unmounted {
		t.Error("filesystem not unmounted")
	}
}

func TestRegistryTolerAtesCorruptStateFile(t *testing.T) {
	state := filepath.Join(t.TempDir(), "published.json")
	if err := os.WriteFile(state, []byte("{{{ not json"), 0o600); err != nil {
		t.Fatal(err)
	}

	// A corrupt file must not wedge the plugin on every restart; start empty.
	r, err := NewRegistry(state)
	if err != nil {
		t.Fatalf("NewRegistry on corrupt state: %v", err)
	}
	if r.WasPublished("/t/1") {
		t.Fatal("corrupt state produced phantom records")
	}
}

func TestRegistryTokenSwapIsRaceFree(t *testing.T) {
	r, _ := NewRegistry(filepath.Join(t.TempDir(), "published.json"))
	live := newLive()
	_ = r.Put("/t/1", live, testMount("/t/1"))

	got, _ := r.Get("/t/1")
	tok := "tok-1"
	got.Token.Store(&tok)
	if v := got.Token.Load(); v == nil || *v != "tok-1" {
		t.Fatalf("Token = %v, want tok-1", v)
	}
}
