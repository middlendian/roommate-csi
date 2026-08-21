package driver

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"testing/synctest"

	"github.com/container-storage-interface/spec/lib/go/csi"

	"github.com/middlendian/roommate-csi/pkg/mounts"
)

// TestConcurrentFirstPublishDoesNotErrorAfterEarlierSuccess reproduces the
// race a CSI CO retry can trigger: NodePublishVolume for a target that is
// mid-publish (objectfs.Mount has succeeded but Registry.Put has not landed
// yet) must not independently decide the target has never published and
// fail. See targetLock's doc comment for why that matters.
//
// It runs inside a testing/synctest bubble so the interleaving is exact,
// not timing-dependent: synctest.Wait returns only once the background
// goroutine below is durably blocked acquiring target's lock. That is a
// channel receive, not a sync.Mutex.Lock — synctest's Wait explicitly does
// not treat mutex contention as durable blocking, which is exactly why
// targetLock is implemented over a channel rather than sync.Mutex. There is
// no sleep in this test and nothing to race.
//
// This test genuinely discriminates, but not on the error return alone: the
// "earlier publish" here is faked by calling registry.Put directly (this
// sandbox has no real cluster or FUSE for a real publish() to succeed
// against), so the only call that could ever reach publish() in this test
// is the blocked, racing one. If the post-lock re-check is dropped,
// `restarting := n.registry.WasPublished` still observes the entry seeded
// below (WasPublished checks the live map too), so the error from that
// redundant publish() call gets swallowed by the restart-error-swallow
// branch regardless of whether the fix is present — an assertion on
// err == nil alone would pass either way, exactly the trap flagged in
// review. What the re-check actually prevents is calling publish() at all
// once the target is already live: without it, the racing call falls
// through to a *second* objectfs.Mount over an already-published target,
// which — if it somehow succeeded — would overwrite the registry entry and
// strand the first mount's watch goroutine and FUSE server (see
// NodeServer.publishAttempts's doc comment). So this test asserts
// publishAttempts == 0, not just err == nil. Confirmed locally by
// temporarily removing the post-lock re-check: err stays nil (swallowed as
// predicted) but publishAttempts == 1, and the assertion below catches it.
func TestConcurrentFirstPublishDoesNotErrorAfterEarlierSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reg, err := mounts.NewRegistry(filepath.Join(t.TempDir(), "published.json"))
		if err != nil {
			t.Fatalf("NewRegistry: %v", err)
		}
		n := NewNodeServer("node-1", reg, slog.Default())
		target := filepath.Join(t.TempDir(), "target")

		// Simulate a publish already in flight for this target: take its
		// lock exactly as NodePublishVolume does on a Get miss.
		tl := n.lockTarget(target)

		// Garbage volume context: no token, no pod info. If the retry below
		// ever reaches publish(), it fails.
		vc := map[string]string{"csi.storage.k8s.io/ephemeral": "true"}

		done := make(chan error, 1)
		go func() {
			_, err := n.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
				VolumeId: "v", TargetPath: target, VolumeContext: vc,
			})
			done <- err
		}()

		// Deterministic barrier: returns only once the goroutine above is
		// durably blocked on tl, and nowhere else.
		synctest.Wait()

		// Simulate the in-flight publish succeeding, exactly as
		// publish()+Registry.Put would have, before releasing the lock.
		if err := reg.Put(target, &mounts.Live{
			Cancel: func() {}, Unmount: func() error { return nil },
		}, mounts.Mount{TargetPath: target, ObjectKind: "Secret", ObjectName: "oauth-credentials", Namespace: "my-app"}); err != nil {
			t.Fatalf("seed registry: %v", err)
		}
		n.unlockTarget(target, tl)

		if err := <-done; err != nil {
			t.Fatalf("concurrent NodePublishVolume returned an error for a target that published while it waited: %v", err)
		}
		if got := n.publishAttempts.Load(); got != 0 {
			t.Fatalf("publishAttempts = %d, want 0 (the racing call should have taken the registry-hit "+
				"path, not re-run publish() over an already-published target)", got)
		}
	})
}

// TestRemountAfterRestartFailureReturnsOK covers the restart branch that
// TestRepublishNeverErrorsAfterFirstSuccess does not: a target the state
// file says was published, but with no live entry in this process's
// registry — a plugin restart. This is the second-most load-bearing branch
// in the file (node.go's `if restarting { ...; return OK }`) and had zero
// test coverage.
//
// It genuinely discriminates: if the restarting check were removed (i.e.
// publish() failures were always propagated), this test fails with the
// underlying podtoken error instead of observing a nil error.
func TestRemountAfterRestartFailureReturnsOK(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "published.json")
	target := filepath.Join(t.TempDir(), "target")

	// First incarnation: a successful publish, recorded to the state file.
	first, err := mounts.NewRegistry(statePath)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	if err := first.Put(target, &mounts.Live{
		Cancel: func() {}, Unmount: func() error { return nil },
	}, mounts.Mount{TargetPath: target, ObjectKind: "Secret", ObjectName: "oauth-credentials", Namespace: "my-app"}); err != nil {
		t.Fatalf("seed registry: %v", err)
	}

	// Second incarnation, same state file: the post-restart state.
	// WasPublished(target) is true; Get(target) is not — there is no live
	// entry until a remount rebuilds one.
	second, err := mounts.NewRegistry(statePath)
	if err != nil {
		t.Fatalf("NewRegistry (post-restart): %v", err)
	}
	n := NewNodeServer("node-1", second, slog.Default())

	// A remount attempt guaranteed to fail deterministically: no token in
	// the volume context, so publish() fails at podtoken.Extract, well
	// before touching a real cluster or FUSE.
	resp, err := n.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:   "v",
		TargetPath: target,
		VolumeContext: map[string]string{
			"csi.storage.k8s.io/ephemeral":                "true",
			"csi.storage.k8s.io/pod.namespace":            "my-app",
			"csi.storage.k8s.io/pod.name":                 "p",
			"csi.storage.k8s.io/pod.uid":                  "uid",
			"csi.storage.k8s.io/pod.service-account.name": "session-runner",
			"objectKind": "Secret",
			"objectName": "oauth-credentials",
		},
	})
	if err != nil {
		t.Fatalf("remount after restart returned an error, which deletes the mount point: %v", err)
	}
	if resp == nil {
		t.Fatal("nil response")
	}
}
