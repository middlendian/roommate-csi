package objectfs

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// mountForTest mounts a volume backed by an in-memory store on a temp dir and
// returns the mountpoint. It skips when /dev/fuse is unavailable.
func mountForTest(t *testing.T, data map[string][]byte) (string, *recordingStore) {
	t.Helper()
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("/dev/fuse unavailable; skipping FUSE test")
	}

	store := newRecordingStore(data)
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
	return dir, store
}

func waitMounted(t *testing.T, srv *fuse.Server) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- srv.WaitMount() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitMount: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("mount did not become ready")
	}
}
