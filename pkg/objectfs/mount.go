package objectfs

import (
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
func Mount(dir string, v *Volume) (*fuse.Server, error) {
	root := &Root{vol: v}
	return fs.Mount(dir, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther:  true,
			FsName:      v.Cfg.ObjectName,
			Name:        "roommate",
			EnableLocks: true,
		},
	})
}
