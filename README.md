# roommate-csi

A Kubernetes CSI driver that mounts a `Secret` or `ConfigMap` as a
**writable, cross-node shared directory**, with `flock` backed by a
`coordination.k8s.io` `Lease`. Point an unmodified program that keeps
mutable state in a locked file at the mount, and it works unchanged —
across every node in the cluster, not just one.

Roommates share a lease. So do the pods that mount one of these volumes.

## Why

Some programs keep mutable state in a file on local disk, using `flock` for
mutual exclusion and relying on local filesystem semantics for
read-after-write consistency. OAuth credentials that get refreshed are the
canonical case: read constantly, written rarely, and *dangerous* to write
twice.

When those programs run as pods spread across a cluster, that pattern has
nowhere to live — `ReadWriteOnce` volumes can't cross nodes, `ReadWriteMany`
over NFSv3 has no trustworthy locking, and a `Secret` (reachable from every
node, already consistent) can currently only be mounted read-only.

The sharp edge is the refresh race, and it isn't hypothetical:

```
pod A:  flock -> read (T1, expired) -> refresh with R1 -> write T2/R2 -> unlock
pod B:  flock -> read (STALE: T1)   -> "still expired" -> refresh with R1 again
```

With refresh-token rotation — now the norm — `R1` was already consumed by A.
B's replay either fails outright or trips the provider's reuse detection and
**revokes the entire token family, logging out every pod at once**. A stale
read on the hot path costs one `401` and a retry. A stale read *inside the
lock* costs an outage.

Kubernetes already has both primitives needed to close that race: a `Secret`
or `ConfigMap` is a consistent store reachable from every node, and a
`Lease` is a real distributed mutual-exclusion primitive. This driver puts a
POSIX file interface in front of both.

## How it differs

Everything else in this space is one-directional — external store to pod,
read-only — and none of it locks:

| Project | Shape | Writable? | Locking? |
|---|---|---|---|
| kubelet native Secret projection | `..data` symlink flip | no | no |
| `secrets-store-csi-driver` | external store → pod | no | no |
| OpenShift `csi-driver-shared-resource` | cross-namespace share | no, `readOnly: true` required | no |
| Vault Agent Injector | sidecar writes files | no (one-directional) | no |
| **roommate-csi** | pod ⇄ Secret/ConfigMap | **yes** | **yes, via Lease** |

The writable-plus-distributed-lock half is what none of the above provide,
and it's the entire reason this project exists.

## Install

Apply the base manifests — namespace, driver ServiceAccount (zero RBAC, see
[Security](#security) below), `CSIDriver` object, and the node DaemonSet —
from a checkout of this repo:

```sh
kubectl apply -k deploy/kustomize/base
```

This is the only cluster-wide step. There is no controller, no
`StorageClass`, and nothing else to install.

### Grant a pod access

The driver holds no permissions of its own, so a pod that wants to mount a
volume needs a `Role` in its own namespace naming exactly the `Secret` (or
`ConfigMap`) and `Lease` it needs. Copy this from `examples/rbac.yaml`:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  namespace: my-app
  name: roommate-oauth-credentials
rules:
  - apiGroups: [""]
    resources: ["secrets"]
    resourceNames: ["oauth-credentials"]
    verbs: ["get", "watch", "patch"]
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    resourceNames: ["roommate-oauth-credentials"]
    verbs: ["get", "update"]
  # RBAC cannot scope "create" by resourceNames — the object's name isn't
  # known at authorization time, so a resourceNames-restricted create rule
  # silently denies every create. This has to be its own, unscoped rule.
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["create"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  namespace: my-app
  name: roommate-oauth-credentials
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: roommate-oauth-credentials
subjects:
  - kind: ServiceAccount
    name: session-runner
    namespace: my-app
```

Do not "simplify" the Lease rules back into one entry. RBAC's `resourceNames`
restricts a verb to specific object names, but a `create` request doesn't
carry the name of an object that doesn't exist yet, so a `create` rule
*cannot* be scoped by `resourceNames` — if you try, the request is silently
denied every time, for every name. The `get`/`update` pair stays scoped; only
`create` has to be split out unscoped. This exact mistake shipped once in
this repo's own copy-pasteable RBAC hint; the two-rule shape above is the
fix.

If a pod isn't authorized, `NodePublishVolume` fails with the exact YAML to
apply, surfaced via kubelet's `FailedMount` event — run `kubectl describe
pod` to see it. The driver never grants access on your behalf; it only tells
you what to grant.

For operator convenience at the cost of broader scope, `roommate-user` (a
`ClusterRole` shipped in the base manifests) grants `get`/`watch`/`patch` on
every Secret and ConfigMap, plus Lease `get`/`create`/`update`, in whatever
namespace a `RoleBinding` (never a `ClusterRoleBinding`) binds it to.

### Mount it in a pod

Copy from `examples/pod.yaml`:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: roommate-demo
  namespace: my-app
spec:
  serviceAccountName: session-runner
  containers:
    - name: app
      image: debian:bookworm-slim
      command: ["sleep", "infinity"]
      volumeMounts:
        - {name: creds, mountPath: /creds}
  volumes:
    - name: creds
      csi:
        driver: roommate.csi
        volumeAttributes:
          objectKind: Secret
          objectName: oauth-credentials
```

`/creds` now contains one file per key in the `oauth-credentials` Secret,
writable, and `flock`-able across every pod in the cluster that mounts the
same object.

## The consumer contract

**The driver's guarantee is scoped to the moment right after you take the
lock — what you do with it is on you.** Concretely:

```
lock -> re-read (guaranteed fresh) -> "did someone already refresh?"
     -> yes: use theirs, skip the refresh entirely
     -> no:  refresh, write, unlock
```

Holding the `flock` guarantees that the very next read of the file reflects
a quorum read of the backing object — not a cached or eventually-consistent
view, but one taken *after* the lock was acquired. That is the entire
read-after-write guarantee this driver provides, and it is real.

What it cannot do is make you look. **A consumer that refreshes
unconditionally after taking the lock — "I have the lock, so I'll refresh
now" — will still double-refresh**, because two pods can each acquire the
lock in turn, each see the lock as license to act, and each refresh without
ever asking whether the other already did. The driver guarantees the
post-lock read is fresh; it cannot make the consumer look at it before
deciding to write.

Implement the double-checked pattern above, or the refresh race this driver
exists to close comes right back — just moved from "who wins the flock" to
"who bothers to check."

One more consequence worth stating plainly: **a writer that never took the
lock gets no guarantee at all.** It writes, and it silently wins over
whatever another writer was doing. Locking is opt-in on every write path,
exactly as on a local filesystem — nothing enforces it for you.

## Configuration

Set these under `volumeAttributes` on the inline CSI volume:

| Key | Required | Default | Notes |
|---|---|---|---|
| `objectKind` | yes | — | `Secret` or `ConfigMap` |
| `objectName` | yes | — | in the pod's own namespace |
| `leaseName` | no | `roommate-<objectName>` | must be a DNS subdomain |
| `fileMode` | no | `0600` | |
| `dirMode` | no | `0700` | |
| `uid` / `gid` | no | `0` | |
| `stalenessBoundSeconds` | no | `30` | resync threshold on `open()` |
| `leaseDurationSeconds` | no | `15` | renewal runs at one third of this |

The namespace is always the consuming pod's own, taken from
`podInfoOnMount` — it is never configurable, because cross-namespace access
is a non-goal.

**Non-root containers must set `uid`/`gid` explicitly.** The CSIDriver
object sets `fsGroupPolicy: None`, so a pod's `securityContext.fsGroup` has
no effect on this mount — kubelet never runs its usual chown/chmod pass over
it. (`fsGroupPolicy: File` isn't an option here: `chmod`/`chown` on these
files always returns `EPERM`/`ENOTSUP`, since modes and ownership are mount
attributes, not part of the Secret or ConfigMap, and kubelet treats that
refusal as a fatal `SetUp` failure — the pod would never start.) With the
defaults (`uid: 0, gid: 0, fileMode: 0600`), a container running as a
non-root UID cannot read its own mounted credentials. Set `uid`/`gid` in
`volumeAttributes` to match the container's `runAsUser`/`runAsGroup`
instead:

```yaml
volumeAttributes:
  objectKind: Secret
  objectName: oauth-credentials
  uid: "1000"
  gid: "1000"
```

## Security

**The driver's ServiceAccount holds zero Kubernetes API permissions.** Every
read, watch, patch, and lock is authenticated as the *consuming pod's own*
ServiceAccount, using a token kubelet mints per `CSIDriver.spec.tokenRequests`
and passes in the volume context — the driver never needs impersonation
rights either.

This is what makes inline ephemeral volumes safe: a pod author can name any
Secret or ConfigMap they like in `volumeAttributes`, but the API server will
only ever serve the objects that pod's *own* RBAC already permits. The
node DaemonSet is privileged (it needs `/dev/fuse` and bidirectional mount
propagation), but compromising it does not grant any additional Kubernetes
API reach beyond what a privileged DaemonSet already implies — there is no
credential to steal, because the driver never held one.

Object contents live only in the FUSE server's memory for the lifetime of
the mount; the driver never writes them to disk itself.

## Limitations

Stated here rather than discovered later:

- **`inotify` does not fire on remote changes, in v1.** FUSE-based
  filesystems don't traverse the VFS on a remote write, so nothing emits an
  inotify event. Programs that detect rotation by watching for a symlink
  flip (as kubelet's native projection does) will not see one. Programs
  that re-read on failure (e.g. a `401`) will work fine. This is a deferred
  limitation, not a permanent one — see the design doc's *inotify* section
  for the two known ways to add it later.
- **Read-after-write consistency only under the lock.** Unlocked readers get
  eventual consistency bounded by watch propagation, same as any other
  watch-fed cache.
- **No backstop for unlocked writers.** They silently win; there is no
  `resourceVersion` compare-and-swap on the write path.
- **Whole-file locking only.** `flock` maps to one Lease per object; POSIX
  byte-range locks (`fcntl F_SETLK`) are not supported.
- **~1 MiB ceiling**, inherited from etcd's practical object-size limit.
- **Flat namespace.** Object keys have no subdirectories; `mkdir`, `symlink`,
  and `link` all return `ENOTSUP`.
- **`chmod` returns `EPERM`.** File and directory modes come from mount
  attributes, not from the object, so they stay interoperable with
  kubelet's native projection and `kubectl edit`.
- **A node-plugin restart requires existing consumer pods to be recreated.**
  kubelet bind-mounts the CSI `target_path` into a consuming container's
  mount namespace once, at container start, with private propagation. A
  node-plugin restart tears down and rebuilds the host-side FUSE mount at
  that same path within about 100ms — but the already-running pod's
  bind-mount reference does not see the replacement; the driver cannot
  repair it in place. Every path in that pod's view of the volume returns
  `ENOTCONN` from that point on, reopen or not, and the pod must be deleted
  and recreated to get a working mount again. Only *new* mounts (a pod
  created after the restart) recover automatically, within ~100ms.
- **Inline ephemeral volumes only.** No `PersistentVolume`, no
  `PersistentVolumeClaim`, no `StorageClass`, no dynamic provisioning, and no
  controller service — the driver never creates or deletes the backing
  object.
- **One `Secret` or `ConfigMap` per volume**, in the consuming pod's own
  namespace only. No cross-namespace or cross-cluster access.

See `docs/superpowers/specs/2026-08-20-roommate-csi-design.md` for the full
design, including the write path, lease semantics, and the failure-mode
table.

## License

GPLv3. See `LICENSE`.
