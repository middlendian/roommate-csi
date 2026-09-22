package objectfs

import (
	"hash/fnv"
	"regexp"
)

// maxKeyLen matches the API server's limit on a data map key.
const maxKeyLen = 253

var keyPattern = regexp.MustCompile(`^[-._a-zA-Z0-9]+$`)

// ValidKey reports whether name is usable as a Secret or ConfigMap data key,
// and therefore as a filename in a roommate mount. The API server enforces
// the same rule, so rejecting here turns an opaque 422 at write time into an
// immediate EINVAL at create time.
func ValidKey(name string) bool {
	if name == "" || name == "." || name == ".." || len(name) > maxKeyLen {
		return false
	}
	return keyPattern.MatchString(name)
}

// hashKey derives a stable inode number from an object key. Every fs.NodeXxxer
// call site that mints a *File for the same key must pass the same value
// here, so go-fuse's addNewChild (which deduplicates by StableAttr, Ino
// included) recognizes repeat lookups of the same key as the same inode
// instead of minting a fresh one from its own incrementing counter each
// time. FNV-64a is not cryptographic — it doesn't need to be, only stable
// and cheap.
func hashKey(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}
