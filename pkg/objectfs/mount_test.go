package objectfs

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"k8s.io/client-go/kubernetes/fake"
)

// TestMountGrantsAtomicOTrunc proves the kernel actually grants
// FUSE_CAP_ATOMIC_O_TRUNC on this test environment, not merely that Mount
// requested it. Requesting ExtraCapabilities is not the same as receiving
// them — see checkAtomicOTrunc's doc comment for what silently breaks if a
// kernel ever declines.
//
// This mounts directly rather than via mountForTest because it needs the
// returned *fuse.Server to inspect KernelSettings, and mountForTest doesn't
// hand that back.
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
		client:    fake.NewSimpleClientset(),
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

// TestMountOptionsDirectMount pins the options that let the driver image
// ship without fusermount3. DirectMount makes go-fuse call mount(2) and
// umount(2) itself (always succeeds as root in the privileged DaemonSet);
// it must NOT be DirectMountStrict, because the unprivileged FUSE tests in
// this package rely on go-fuse falling back to fusermount3 when mount(2)
// returns EPERM. DirectMountFlags stays zero so go-fuse applies
// MS_NOSUID|MS_NODEV, the same flags fusermount3 uses.
func TestMountOptionsDirectMount(t *testing.T) {
	opts := mountOptions(&Volume{Cfg: Config{ObjectName: "oauth-credentials"}})

	if !opts.DirectMount {
		t.Error("DirectMount = false; the distroless image has no fusermount3")
	}
	if opts.DirectMountStrict {
		t.Error("DirectMountStrict = true; unprivileged FUSE tests need the fusermount3 fallback")
	}
	if opts.DirectMountFlags != 0 {
		t.Errorf("DirectMountFlags = %#x, want 0 (go-fuse default MS_NOSUID|MS_NODEV)", opts.DirectMountFlags)
	}
	// The pre-existing options must survive the refactor.
	if !opts.AllowOther || !opts.EnableLocks {
		t.Errorf("AllowOther=%v EnableLocks=%v, want both true", opts.AllowOther, opts.EnableLocks)
	}
	if opts.ExtraCapabilities&fuse.CAP_ATOMIC_O_TRUNC == 0 {
		t.Error("ExtraCapabilities lost CAP_ATOMIC_O_TRUNC")
	}
	if opts.FsName != "oauth-credentials" || opts.Name != "roommate" {
		t.Errorf("FsName=%q Name=%q, want %q/%q", opts.FsName, opts.Name, "oauth-credentials", "roommate")
	}
}
