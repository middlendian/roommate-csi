package objectfs

import (
	"log"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Mount serves v at dir and returns the running server. The caller unmounts
// via Server.Unmount.
//
// AllowOther is required because the FUSE server runs as root in the
// DaemonSet while consuming processes usually do not. Mounting as root does
// not need user_allow_other in /etc/fuse.conf.
//
// EnableLocks makes the kernel negotiate FUSE_CAP_FLOCK_LOCKS, without which
// flock never reaches the filesystem at all.
//
// ExtraCapabilities negotiates FUSE_CAP_ATOMIC_O_TRUNC, which folds O_TRUNC
// into the OPEN request for open(O_TRUNC) on an existing file, instead of
// the kernel sending a separate SETATTR(size=0) *before* OPEN — a request
// with no file handle attached yet, since the file isn't open. Without this,
// File.Open never sees O_TRUNC and the truncate has nowhere to buffer, so it
// would have to commit immediately, outside the handle's buffer/commit-on-
// close model — for the primary rewrite-an-existing-key idiom
// (os.WriteFile), that risks leaving a key permanently empty if the
// following write never lands (rejected, or the process dies first). With
// this negotiated, File.Open handles O_TRUNC directly; SETATTR's fh-less
// path is then only reachable from a genuine standalone truncate(2), where
// committing immediately is correct because there is no handle at all.
func Mount(dir string, v *Volume) (*fuse.Server, error) {
	root := &Root{vol: v}
	srv, err := fs.Mount(dir, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther:        true,
			FsName:            v.Cfg.ObjectName,
			Name:              "roommate",
			EnableLocks:       true,
			ExtraCapabilities: fuse.CAP_ATOMIC_O_TRUNC,
		},
	})
	if err != nil {
		return nil, err
	}
	checkAtomicOTrunc(srv)
	return srv, nil
}

// checkAtomicOTrunc logs loudly if the kernel did not grant
// FUSE_CAP_ATOMIC_O_TRUNC despite it being requested.
//
// Requesting the capability is not the same as receiving it: an older kernel
// (or one where the feature was disabled) silently declines it, and the VFS
// falls back to sending a pre-open SETATTR(size=0) with no file handle
// attached. That is exactly the fh-less truncate path File.Setattr commits
// immediately — so a decline silently reopens the data-loss window
// ExtraCapabilities was set to close, with no error surfaced anywhere else.
func checkAtomicOTrunc(srv *fuse.Server) {
	settings := srv.KernelSettings()
	if settings.Flags64()&fuse.CAP_ATOMIC_O_TRUNC == 0 {
		log.Printf("roommate: WARNING: kernel did not grant FUSE_CAP_ATOMIC_O_TRUNC; " +
			"O_TRUNC opens will fall back to a pre-open SETATTR(size=0), " +
			"reopening the truncate-then-write data-loss window")
	}
}
