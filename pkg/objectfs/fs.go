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
	r.fillFileAttr(&out.Attr, len(value))
	return r.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFREG}), 0
}

// Mkdir always fails: object keys are flat, so a directory cannot exist.
func (r *Root) Mkdir(context.Context, string, uint32, *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.ENOTSUP
}

// fillFileAttr populates attr for a regular file of the given size using
// Cfg.FileMode, UID, and GID.
func (r *Root) fillFileAttr(attr *fuse.Attr, size int) {
	attr.Mode = fuse.S_IFREG | r.vol.Cfg.FileMode
	attr.Size = uint64(size)
	attr.Uid = r.vol.Cfg.UID
	attr.Gid = r.vol.Cfg.GID
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

// Getattr reports the file's current size and Cfg.FileMode/UID/GID, or
// ENOENT if the key has since been removed from the snapshot.
func (f *File) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	snap := f.vol.Cache.MaybeFresh(ctx)
	if snap == nil {
		return syscall.EIO
	}
	value, ok := snap.Get(f.key)
	if !ok {
		return syscall.ENOENT
	}
	out.Mode = fuse.S_IFREG | f.vol.Cfg.FileMode
	out.Size = uint64(len(value))
	out.Uid = f.vol.Cfg.UID
	out.Gid = f.vol.Cfg.GID
	out.Nlink = 1
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
