# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- CSI Node driver (`roommate.csi`) mounting a Kubernetes `Secret` or
  `ConfigMap` as a writable, cross-node shared directory via FUSE
  (`github.com/hanwen/go-fuse/v2`).
- `flock` support backed by one `coordination.k8s.io` `Lease` per mounted
  object, with background renewal and a mandatory quorum read on
  acquisition — the read-after-write guarantee the driver exists to
  provide.
- Read, write, create, delete, and rename of individual object keys as
  files in a flat directory, with copy-on-write handle buffers so
  concurrent readers and writers never see torn data.
- Per-key JSON merge-patch write path: no read-modify-write, no
  `resourceVersion` compare-and-swap, no retry loop.
- Snapshot pinning at `open()`, matching kubelet's native Secret-projection
  consistency model: an update is all-or-nothing across the whole object,
  and a handle opened before an update keeps reading the version it opened
  against.
- Staleness-bound resync (default 30s) as a safety net for a watch that has
  died silently, mirroring kubelet's periodic resync.
- Authorization as the consuming pod's own identity: the driver's
  ServiceAccount holds zero Kubernetes API permissions. Access to a Secret,
  ConfigMap, or Lease is entirely the requesting pod's own RBAC, using a
  token kubelet mints per `CSIDriver.spec.tokenRequests`.
- `PermissionDenied` errors on first publish carry copy-pasteable RBAC YAML
  (`pkg/driver/rbachint.go`), surfaced via kubelet's `FailedMount` event.
- Republish-safe `NodePublishVolume`: never returns an error once a target
  has published, so a transient token or API failure surfaces as `EACCES`
  from the FUSE data path instead of causing kubelet to delete the mount
  point.
- Node-plugin restart recovery: a `published.json` state file lets a
  republish call distinguish "already mounted, just swap the token" from
  "plugin restarted, remount" without erroring either way.
- Inline ephemeral volume support only, configured entirely via
  `volumeAttributes` (`objectKind`, `objectName`, `leaseName`, `fileMode`,
  `dirMode`, `uid`, `gid`, `stalenessBoundSeconds`, `leaseDurationSeconds`).
- Deployment manifests (`deploy/kustomize/base`): namespace, zero-RBAC
  driver `ServiceAccount`, `CSIDriver` object, a `roommate-user`
  convenience `ClusterRole`, and the node `DaemonSet`.
- Example per-object `Role`/`RoleBinding` and pod manifests
  (`examples/rbac.yaml`, `examples/pod.yaml`).
- Unit tests across `pkg/objectfs`, `pkg/driver`, `pkg/mounts`, and
  `pkg/podtoken`, including real-FUSE filesystem tests requiring
  `/dev/fuse`.
- `envtest`-based API-semantics suite (`test/apisemantics`) against a real
  apiserver and etcd, covering merge-patch key-level semantics, Lease CAS
  rejection of a stale writer, quorum-GET read-after-write, and the
  `resourceNames`-watch field-selector requirement that a fake clientset
  cannot exercise.
- Kind-based end-to-end suite (`test/e2e`, build tag `e2e`) covering a
  basic read/write round trip, RBAC denial surfacing, and
  `TestRefreshRaceProducesExactlyOneRefresh` — the deterministic,
  two-node proof that concurrent lock/refresh/unlock cycles produce
  exactly one refresh, not two.
- Container image for `linux/amd64` and `linux/arm64`, built with ko on a
  distroless static base (`.ko.yaml`, `make ko`). The driver mounts FUSE
  with go-fuse `DirectMount`, so the image needs no `fusermount3`.
- `livenessprobe` sidecar in the DaemonSet, calling the driver's CSI
  `Probe` RPC; replaces a shell-based exec probe.
- Release pipeline: **Cut release** opens a `release/vX.Y.Z` PR; merging
  it tags, pushes `ghcr.io/middlendian/roommate-csi:vX.Y.Z`, and creates
  a GitHub release from this file.

[Unreleased]: https://github.com/middlendian/roommate-csi/commits/main
