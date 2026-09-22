package mounts

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

// DetachStale force-unmounts target if the kernel still has a mount entry
// there that this process no longer owns — the shape left behind by a
// node-plugin restart: the old process is gone, but closing (or losing)
// /dev/fuse aborts the FUSE connection without removing the mount itself.
// Only umount(2) (or its fusermount3 wrapper) does that.
//
// Until something detaches it, target stays a mountpoint whose every
// operation returns ENOTCONN: a remount's MkdirAll sees that on its Stat
// call and fails, and an unpublish's rmdir fails too, leaking the mount into
// the node's table and leaving the pod's directory impossible for kubelet to
// clean up.
//
// target not being a mountpoint at all — the overwhelmingly common case, a
// genuine first publish — is the fast, no-op path: isMountpoint decides that
// with a couple of stat(2) calls, deliberately before ever attempting
// umount(2), which needs CAP_SYS_ADMIN the caller may not always have (e.g.
// this function's own unit tests, run unprivileged).
func DetachStale(target string) error {
	mounted, err := isMountpoint(target)
	if err != nil {
		return fmt.Errorf("stat %s: %w", target, err)
	}
	if !mounted {
		return nil
	}

	if err := unix.Unmount(target, unix.MNT_DETACH); err == nil {
		return nil
	} else if out, ferr := exec.Command("fusermount3", "-u", "-z", target).CombinedOutput(); ferr != nil {
		// The raw syscall failed. Fall back to the same tool go-fuse itself
		// shells out to for unmounting, in case the permission or namespace
		// context of a plain umount2 call differs from what fusermount3's
		// setuid-root helper can do.
		return fmt.Errorf("umount2(MNT_DETACH) %s: %w; fusermount3 -u -z: %v (%s)",
			target, err, ferr, out)
	}
	return nil
}

// isMountpoint reports whether target is currently mounted.
//
// The primary check is a device-number comparison against target's parent
// directory — the same trick the mountpoint(1) tool uses — and needs no
// privilege at all, unlike actually attempting to unmount: this is what
// keeps DetachStale a safe no-op to call on every publish, including the
// overwhelmingly common case where target isn't a mountpoint yet.
//
// A dead FUSE mount is special-cased: once its server is gone, even Lstat on
// the mountpoint itself returns ENOTCONN (confirmed against
// MkdirAll/Mkdir/Lstat in the field — this is precisely what makes a restart
// otherwise unrecoverable), so that specific error is treated as "yes,
// mounted" rather than propagated as a stat failure. target simply not
// existing yet (no prior mount attempt at all) is "not mounted".
func isMountpoint(target string) (bool, error) {
	info, err := os.Lstat(target)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		if errors.Is(err, syscall.ENOTCONN) {
			return true, nil
		}
		return false, err
	}
	parent, err := os.Lstat(filepath.Dir(target))
	if err != nil {
		return false, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	pst, pok := parent.Sys().(*syscall.Stat_t)
	if !ok || !pok {
		return false, nil
	}
	return st.Dev != pst.Dev, nil
}
