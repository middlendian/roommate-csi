package objectfs

import "regexp"

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
