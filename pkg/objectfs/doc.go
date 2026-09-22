// Package objectfs presents a Kubernetes Secret or ConfigMap as a writable
// POSIX directory over FUSE, coordinating writers with a coordination.k8s.io
// Lease.
//
// Every API call in this package is made with the consuming pod's own
// ServiceAccount token. The driver's ServiceAccount holds no permissions.
package objectfs
