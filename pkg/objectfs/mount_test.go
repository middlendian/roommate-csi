package objectfs

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// TestMountGrantsAtomicOTrunc proves the kernel actually grants
// FUSE_CAP_ATOMIC_O_TRUNC on this test environment, not merely that Mount
// requested it. Requesting ExtraCapabilities is not the same as receiving
// them — see checkAtomicOTrunc's doc comment for what silently breaks if a
// kernel ever declines.
func TestMountGrantsAtomicOTrunc(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("/dev/fuse unavailable; skipping FUSE test")
	}

	store := newRecordingStore(map[string][]byte{"session.key": []byte("v1")})
	cfg := Config{
		Namespace: "my-app", ObjectKind: KindSecret, ObjectName: "oauth-credentials",
		LeaseName: "roommate-oauth-credentials",
		FileMode:  0o600, DirMode: 0o700,
		StalenessBound: time.Hour, LeaseDuration: testLeaseDur,
	}
	cache := NewCache(store, cfg.StalenessBound)
	if _, err := cache.Fresh(context.Background()); err != nil {
		t.Fatalf("warm cache: %v", err)
	}
	vol := &Volume{
		Cfg: cfg, Store: store, Cache: cache,
		Committer: NewCommitter(store, cache),
		podUID:    "test-pod-uid",
	}

	dir := t.TempDir()
	srv, err := Mount(dir, vol)
	if err != nil {
		t.Fatalf("Mount: %v", err)
	}
	t.Cleanup(func() {
		if err := srv.Unmount(); err != nil {
			t.Logf("unmount: %v", err)
		}
	})
	waitMounted(t, srv)

	if srv.KernelSettings().Flags64()&fuse.CAP_ATOMIC_O_TRUNC == 0 {
		t.Fatal("kernel did not grant FUSE_CAP_ATOMIC_O_TRUNC on this test environment")
	}
}
