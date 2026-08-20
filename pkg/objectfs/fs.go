package objectfs

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Root is the mount's only directory. Object keys are flat, so there are no
// subdirectories and mkdir is refused.
type Root struct {
	fs.Inode
	vol *Volume
}

var (
	_ fs.NodeReaddirer = (*Root)(nil)
	_ fs.NodeLookuper  = (*Root)(nil)
	_ fs.NodeGetattrer = (*Root)(nil)
	_ fs.NodeMkdirer   = (*Root)(nil)
)

// Getattr reports the mount root as a directory using Cfg.DirMode, UID, and
// GID.
func (r *Root) Getattr(_ context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | r.vol.Cfg.DirMode
	out.Uid = r.vol.Cfg.UID
	out.Gid = r.vol.Cfg.GID
	return 0
}

// Readdir lists the current snapshot's keys as regular files.
func (r *Root) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	snap := r.vol.Cache.MaybeFresh(ctx)
	if snap == nil {
		return nil, syscall.EIO
	}
	keys := snap.Keys()
	entries := make([]fuse.DirEntry, 0, len(keys))
	for _, k := range keys {
		entries = append(entries, fuse.DirEntry{Name: k, Mode: fuse.S_IFREG})
	}
	return fs.NewListDirStream(entries), 0
}

// Lookup finds a direct child of the root by object key, returning ENOENT if
// the key is absent from the current snapshot.
func (r *Root) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	snap := r.vol.Cache.MaybeFresh(ctx)
	if snap == nil {
		return nil, syscall.EIO
	}
	value, ok := snap.Get(name)
	if !ok {
		return nil, syscall.ENOENT
	}
	child := &File{vol: r.vol, key: name}
	fillFileAttr(&out.Attr, r.vol.Cfg, len(value))
	return r.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFREG}), 0
}

// Mkdir always fails: object keys are flat, so a directory cannot exist.
func (r *Root) Mkdir(context.Context, string, uint32, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.ENOTSUP
}

var (
	_ fs.NodeCreater   = (*Root)(nil)
	_ fs.NodeUnlinker  = (*Root)(nil)
	_ fs.NodeRenamer   = (*Root)(nil)
	_ fs.NodeSetattrer = (*File)(nil)
)

// Create adds a new key. The value is empty until the handle's write is
// committed by flush, fsync, or release.
func (r *Root) Create(ctx context.Context, name string, _, _ uint32, out *fuse.EntryOut) (
	*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	if !ValidKey(name) {
		return nil, nil, 0, syscall.EINVAL
	}
	child := &File{vol: r.vol, key: name}
	h := newHandle(r.vol, name, r.vol.Cache.Current(), nil)
	h.dirty = true // an empty create must still produce a key
	fillFileAttr(&out.Attr, r.vol.Cfg, 0)
	inode := r.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFREG})
	return inode, h, 0, 0
}

// Unlink removes the key from the object in a single merge patch.
func (r *Root) Unlink(ctx context.Context, name string) syscall.Errno {
	if err := r.vol.Committer.Commit(ctx, nil, nil, []string{name}); err != nil {
		return errnoFor(err)
	}
	return 0
}

// Rename is a copy-key plus delete-key in one patch, since a key cannot move.
func (r *Root) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, _ uint32) syscall.Errno {
	if newParent != fs.InodeEmbedder(r) {
		return syscall.EXDEV // flat namespace: there is nowhere else to go
	}
	if !ValidKey(newName) {
		return syscall.EINVAL
	}
	snap := r.vol.Cache.MaybeFresh(ctx)
	if snap == nil {
		return syscall.EIO
	}
	value, ok := snap.Get(name)
	if !ok {
		return syscall.ENOENT
	}
	err := r.vol.Committer.Commit(ctx, nil, map[string][]byte{newName: value}, []string{name})
	if err != nil {
		return errnoFor(err)
	}
	return 0
}

// Setattr handles the truncate half of O_TRUNC and refuses everything else.
// Modes and ownership come from mount attributes and are not stored in the
// object, so chmod cannot round-trip.
//
// go-fuse never negotiates FUSE_CAP_ATOMIC_O_TRUNC (see doInit in its fuse
// package), so the kernel does not fold O_TRUNC into the OPEN request; for
// an existing file it sends a separate SETATTR(size=0) *before* OPEN, with
// no file handle attached yet. fh is only a *handle once Open has run, so
// that pre-open truncate falls to truncateCommitted, which commits directly
// since there is no handle to buffer it into.
func (f *File) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if _, ok := in.GetMode(); ok {
		return syscall.EPERM
	}
	if _, ok := in.GetUID(); ok {
		return syscall.EPERM
	}
	if _, ok := in.GetGID(); ok {
		return syscall.EPERM
	}
	if size, ok := in.GetSize(); ok {
		if h, ok := fh.(*handle); ok {
			h.truncate(size)
		} else if errno := f.truncateCommitted(ctx, size); errno != 0 {
			return errno
		}
		out.Size = size
	}
	out.Mode = fuse.S_IFREG | f.vol.Cfg.FileMode
	out.Uid = f.vol.Cfg.UID
	out.Gid = f.vol.Cfg.GID
	return 0
}

// truncateCommitted resizes the key's current value and commits immediately.
// Used only when Setattr has no open handle to buffer the change into (see
// Setattr's doc comment).
func (f *File) truncateCommitted(ctx context.Context, size uint64) syscall.Errno {
	snap := f.vol.Cache.MaybeFresh(ctx)
	if snap == nil {
		return syscall.EIO
	}
	value, _ := snap.Get(f.key)
	switch {
	case uint64(len(value)) > size:
		value = value[:size]
	case uint64(len(value)) < size:
		grown := make([]byte, size)
		copy(grown, value)
		value = grown
	}
	if err := f.vol.Committer.Commit(ctx, nil, map[string][]byte{f.key: value}, nil); err != nil {
		return errnoFor(err)
	}
	return 0
}

// fillFileAttr populates attr for a regular file of the given size using
// cfg's FileMode, UID, and GID.
func fillFileAttr(attr *fuse.Attr, cfg Config, size int) {
	attr.Mode = fuse.S_IFREG | cfg.FileMode
	attr.Size = uint64(size)
	attr.Uid = cfg.UID
	attr.Gid = cfg.GID
	attr.Nlink = 1
}

// File is one object key.
type File struct {
	fs.Inode
	vol *Volume
	key string
}

var (
	_ fs.NodeGetattrer = (*File)(nil)
	_ fs.NodeOpener    = (*File)(nil)
)

// Getattr reports the file's size and Cfg.FileMode/UID/GID.
//
// If fh is an open handle, the size comes from that handle's pinned buffer
// rather than the live snapshot — otherwise fstat(fd) could report a size
// newer than what Read on that same fd will ever return (the object grew or
// shrank after Open), producing a short or over-long read that looks exactly
// like the spliced-read failure pinning exists to prevent. With no open
// handle (a bare path-based stat), the live snapshot is the only source of
// truth, and ENOENT is returned if the key has since been removed from it.
func (f *File) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if h, ok := fh.(*handle); ok {
		h.mu.Lock()
		size := len(h.buf)
		h.mu.Unlock()
		fillFileAttr(&out.Attr, f.vol.Cfg, size)
		return 0
	}

	snap := f.vol.Cache.MaybeFresh(ctx)
	if snap == nil {
		return syscall.EIO
	}
	value, ok := snap.Get(f.key)
	if !ok {
		return syscall.ENOENT
	}
	fillFileAttr(&out.Attr, f.vol.Cfg, len(value))
	return 0
}

// Open pins the current snapshot into the handle. Every read from this handle
// is served from that pinned value for its whole lifetime, reproducing the
// atomicity of kubelet's ..data flip: a handle never observes a partial
// transition, and a read can never splice two versions together.
func (f *File) Open(ctx context.Context, _ uint32) (fs.FileHandle, uint32, syscall.Errno) {
	snap := f.vol.Cache.MaybeFresh(ctx)
	if snap == nil {
		return nil, 0, syscall.EIO
	}
	value, ok := snap.Get(f.key)
	if !ok {
		return nil, 0, syscall.ENOENT
	}
	return newHandle(f.vol, f.key, snap, value), 0, 0
}
