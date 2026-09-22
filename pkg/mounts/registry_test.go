package mounts

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func newLive() *Live {
	return &Live{Cancel: func() {}, Unmount: func() error { return nil }}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
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

// TestRegistryDeleteDetachesStaleMountAfterRestart is C3's unpublish-side
// regression guard: a record with no live entry — exactly what a plugin
// restart leaves behind — must still attempt a detach, not silently drop
// the record and skip unmounting entirely (the pre-fix behaviour that left
// a dead mount neither a future republish nor kubelet's own rmdir could
// clear).
func TestRegistryDeleteDetachesStaleMountAfterRestart(t *testing.T) {
	state := filepath.Join(t.TempDir(), "published.json")
	first, err := NewRegistry(state)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	if err := first.Put("/t/1", newLive(), testMount("/t/1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Simulate a plugin restart: reopening from the same state file loses
	// the in-memory Live but keeps the record.
	second, err := NewRegistry(state)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}

	var gotTarget string
	orig := detachStaleFn
	detachStaleFn = func(target string) error { gotTarget = target; return nil }
	t.Cleanup(func() { detachStaleFn = orig })

	if err := second.Delete(context.Background(), "/t/1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if gotTarget != "/t/1" {
		t.Fatalf("detachStaleFn called with %q, want /t/1", gotTarget)
	}
	if second.WasPublished("/t/1") {
		t.Fatal("record survived Delete")
	}
}

// A target with no record at all (never published, or already deleted) must
// not trigger a detach attempt — there is nothing stale to clean up.
func TestRegistryDeleteSkipsDetachForUnknownTarget(t *testing.T) {
	r, err := NewRegistry(filepath.Join(t.TempDir(), "published.json"))
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	called := false
	orig := detachStaleFn
	detachStaleFn = func(string) error { called = true; return nil }
	t.Cleanup(func() { detachStaleFn = orig })

	if err := r.Delete(context.Background(), "/never/published"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if called {
		t.Fatal("detachStaleFn called for a target with no record at all")
	}
}

// A detach failure after a restart must surface as an error, exactly like an
// Unmount failure on a live mount does — and it must still persist the
// record removal, matching the live-mount branch's behaviour just above it.
func TestRegistryDeleteSurfacesDetachFailure(t *testing.T) {
	state := filepath.Join(t.TempDir(), "published.json")
	first, _ := NewRegistry(state)
	_ = first.Put("/t/1", newLive(), testMount("/t/1"))
	second, _ := NewRegistry(state)

	orig := detachStaleFn
	detachStaleFn = func(string) error { return fmt.Errorf("boom") }
	t.Cleanup(func() { detachStaleFn = orig })

	if err := second.Delete(context.Background(), "/t/1"); err == nil {
		t.Fatal("Delete succeeded despite a detach failure")
	}
	if second.WasPublished("/t/1") {
		t.Fatal("record survived a failed Delete — persist must still run")
	}
}

// Shutdown must unmount every live target and cancel its watch context,
// without touching records — a graceful restart is not an unpublish, and
// the next process incarnation's republish must still see WasPublished true.
func TestRegistryShutdownUnmountsLiveTargetsWithoutForgettingThem(t *testing.T) {
	r, err := NewRegistry(filepath.Join(t.TempDir(), "published.json"))
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	var cancelled, unmounted bool
	live := &Live{
		Cancel:  func() { cancelled = true },
		Unmount: func() error { unmounted = true; return nil },
	}
	if err := r.Put("/t/1", live, testMount("/t/1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	r.Shutdown(discardLogger())

	if !cancelled {
		t.Error("watch context not cancelled")
	}
	if !unmounted {
		t.Error("filesystem not unmounted")
	}
	if !r.WasPublished("/t/1") {
		t.Fatal("record forgotten by Shutdown — a graceful restart must still remount, not first-publish")
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

// TestRegistryConcurrentPutDeleteSurvivesRestart verifies that concurrent
// Put and Delete operations on different targets don't lose updates. This
// catches a lost-update race where the persist() method released the mutex
// before file I/O, allowing two concurrent calls to race their snapshots
// and the later one to overwrite the earlier one's changes to disk.
func TestRegistryConcurrentPutDeleteSurvivesRestart(t *testing.T) {
	state := filepath.Join(t.TempDir(), "published.json")
	r, err := NewRegistry(state)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	// Do many concurrent Put/Delete operations on different targets.
	// With the lost-update race, some records would silently vanish from disk.
	done := make(chan error, 20)
	for i := 0; i < 10; i++ {
		target := fmt.Sprintf("/t/%d", i)
		go func(tgt string) {
			done <- r.Put(tgt, newLive(), testMount(tgt))
		}(target)
	}
	for i := 10; i < 20; i++ {
		target := fmt.Sprintf("/t/%d", i)
		go func(tgt string) {
			done <- r.Put(tgt, newLive(), testMount(tgt))
		}(target)
	}

	for i := 0; i < 10; i++ {
		if err := <-done; err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	for i := 0; i < 10; i++ {
		if err := <-done; err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// Interleave some deletes with the remaining puts.
	for i := 0; i < 5; i++ {
		target := fmt.Sprintf("/t/%d", i)
		if err := r.Delete(context.Background(), target); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}

	// After restart, verify the expected records survived.
	reopen, err := NewRegistry(state)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}

	for i := 0; i < 5; i++ {
		target := fmt.Sprintf("/t/%d", i)
		if reopen.WasPublished(target) {
			t.Errorf("deleted target %s still in state", target)
		}
	}
	for i := 5; i < 20; i++ {
		target := fmt.Sprintf("/t/%d", i)
		if !reopen.WasPublished(target) {
			t.Errorf("surviving target %s lost from state after restart", target)
		}
	}
}
