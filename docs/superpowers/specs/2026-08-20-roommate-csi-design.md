# roommate-csi — v1 design

**Status:** proposed
**Date:** 2026-08-20

A CSI driver that mounts a Kubernetes `Secret` or `ConfigMap` as a
**writable, cross-node shared directory**, with `flock` backed by a
`coordination.k8s.io` `Lease`.

Roommates share a lease. So do the pods that mount one of these volumes.

## Problem

Some programs keep mutable state in a file on local disk, using `flock`
for mutual exclusion and relying on local filesystem semantics for
read-after-write consistency. OAuth credentials that get refreshed are the
canonical case: read constantly, written rarely, and *dangerous* to write
twice.

When those programs run as pods spread across a cluster, that pattern has
nowhere to live:

- `ReadWriteOnce` volumes cannot cross nodes.
- `ReadWriteMany` over NFSv3 has no trustworthy locking.
- Cluster filesystems (GFS2, OCFS2) need a distributed lock manager and are
  far too heavy for small nodes.
- A `Secret` is reachable from every node and already consistent — but every
  existing way to mount one is **read-only**.

The refresh race is the sharp edge, and it is not hypothetical:

```
pod A:  flock -> read (T1, expired) -> refresh with R1 -> write T2/R2 -> unlock
pod B:  flock -> read (STALE: T1)   -> "still expired" -> refresh with R1 again
```

With refresh-token rotation — now the norm — `R1` was consumed by A. B's
replay either fails outright or trips the provider's reuse detection and
**revokes the entire token family**, logging out every pod at once. A stale
read on the hot path costs one `401` and a retry. A stale read *inside the
lock* costs an outage.

The insight this driver is built on: Kubernetes already provides both
primitives needed to fix that. A `Secret` or `ConfigMap` is a consistent
store reachable from every node, and a `Lease` is a real distributed mutual
exclusion primitive. Put a POSIX file interface in front of them and
unmodified programs work unchanged.

## Why nothing existing does this

The ecosystem is uniformly one-directional — external store to pod,
read-only:

| Project | Shape | Writable? |
|---|---|---|
| kubelet native Secret projection | `..data` symlink flip | no |
| `secrets-store-csi-driver` | external store → pod | no |
| OpenShift `csi-driver-shared-resource` | cross-namespace share | no, `readOnly: true` required |
| Vault Agent Injector | sidecar writes files | no (one-directional) |

None of them implement locking of any kind. The mount-and-authorize half of
this design has good prior art to copy; the **writable + distributed-lock
half is unbuilt**, and that is the part this project exists to provide.

## Scope

### In scope for v1

- One `Secret` **or** `ConfigMap` per volume, mounted as a flat directory
  whose files are the object's keys.
- Reads, writes, creates, deletes, renames of those files.
- `flock` → `Lease`, whole-object, with background renewal.
- Read-after-write consistency for any consumer that takes the lock.
- Inline ephemeral volumes only.

### Non-goals for v1

- Subdirectories or nested paths. Object keys are flat.
- POSIX byte-range locks (`fcntl` `F_SETLK` on a range). Whole-file only.
- Cross-namespace or cross-cluster access.
- Dynamic provisioning. The driver never creates or deletes the backing
  object.
- Objects larger than etcd's practical ~1 MiB ceiling.
- Encryption beyond what Kubernetes `Secret` storage already provides.
- Persistent (PV/PVC) volumes.

## Architecture

One binary. One DaemonSet. No controller.

```
                    ┌──────────────────────────────────────┐
   consuming pod    │  /creds/                             │
   (any node)       │    ├── .credentials.json             │
                    │    ├── config.json                   │
                    │    └── session.key                   │
                    └───────────────┬──────────────────────┘
                                    │ FUSE
                    ┌───────────────┴──────────────────────┐
   roommate node    │  per (pod, volume) FUSE server       │
   DaemonSet        │    snapshot cache  ← watch           │
   (privileged,     │    lease manager   ← renewal loop    │
    zero RBAC)      │    client built from THE POD'S token │
                    └───────────────┬──────────────────────┘
                                    │ authenticated as the consuming pod
                    ┌───────────────┴──────────────────────┐
   kube-apiserver   │  Secret/ConfigMap    Lease           │
                    └──────────────────────────────────────┘
```

There is deliberately no controller, no `StorageClass`, no `CreateVolume`,
and no `PersistentVolume`. The entire driver is a node plugin whose only
substantive RPC is `NodePublishVolume`.

### Package layout

```
cmd/node/                  the only binary
pkg/driver/                CSI Identity + Node servers (publish-only)
pkg/objectfs/
  fs.go                    go-fuse nodes: Root (dir) + File
  snapshot.go              immutable {keys→bytes, resourceVersion}
  store.go                 Store interface
  store_secret.go          Secret implementation
  store_configmap.go       ConfigMap implementation
  cache.go                 watch loop, staleness bound
  commit.go                per-key merge-patch writes
  lease.go                 flock → Lease acquire/renew/release
  keys.go                  object key ⇄ filename validation
pkg/mounts/                target_path → live mount registry + state file
deploy/kustomize/
hack/                      smoke.sh, e2e.sh, kind config
test/e2e/                  build tag: e2e
```

**FUSE binding:** `github.com/hanwen/go-fuse/v2`. Actively maintained,
materially faster than `bazil.org/fuse`, and its `fs.Inode` API models a
small dynamic tree cleanly. Requires `MountOptions.EnableLocks` so the
kernel negotiates `FUSE_CAP_FLOCK_LOCKS`.

## Authorization: the pod's identity, not the driver's

**The driver's ServiceAccount holds zero Kubernetes API permissions.** Every
call to read, watch, patch, or lock is authenticated as the *consuming
pod's* ServiceAccount.

This is the single most important property in the design, and it is what
makes inline ephemeral volumes safe. A pod author can name any object they
like in `volumeAttributes`; the API server will only serve the ones that
pod's own RBAC already permits.

`CSIDriver.spec.tokenRequests` causes **kubelet** to mint the token and pass
it in the volume context, so the driver needs no impersonation rights
either:

```yaml
apiVersion: storage.k8s.io/v1
kind: CSIDriver
metadata:
  name: roommate.csi
spec:
  attachRequired: false
  podInfoOnMount: true
  requiresRepublish: true
  fsGroupPolicy: File
  volumeLifecycleModes: ["Ephemeral"]
  tokenRequests:
    - audience: ""
```

### The entire grant

One `Role` in the pod's own namespace. Nothing to grant in the driver's
namespace, nothing to keep in sync across namespaces, no drift:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  namespace: my-app
  name: roommate-claude-credentials
rules:
  - apiGroups: [""]
    resources: ["secrets"]
    resourceNames: ["claude-credentials"]
    verbs: ["get", "watch", "patch"]
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    resourceNames: ["roommate-claude-credentials"]
    verbs: ["get", "create", "update"]
```

`patch` rather than `update` is deliberate — it is the narrower grant, and
the write path only ever patches.

### Why not the alternatives

- **Driver identity + namespaced RBAC.** Makes the already-privileged
  DaemonSet a confused deputy: anyone who can get a volume pointed at an
  object can read it, regardless of their own RBAC.
- **Driver identity + `SubjectAccessReview`.** Restores per-pod
  authorization, but requires granting the driver access to every object it
  might serve *and* granting the pod access for the check to pass — two
  grants, in two namespaces, for one capability. Brittle to operate.
- **Sharing CRD** (the OpenShift model). Works, but adds an API type and
  still needs two grants.

## Deployment shape

The DaemonSet is `privileged: true` — required for `Bidirectional` mount
propagation on the kubelet directory and for `/dev/fuse`. It needs no
`/dev/loop*`, no `mount.nfs`, and, again, **no Kubernetes RBAC**.

Because the FUSE server runs as root and consuming processes may not, mounts
use `allow_other`. Mounting as root does not require `user_allow_other` in
`/etc/fuse.conf`.

## Data model

The object's `data` map is the directory. Each key is one file.

```
Secret claude-credentials              /creds/
  data:                                  ├── .credentials.json
    .credentials.json: <base64>          ├── config.json
    config.json:       <base64>          └── session.key
    session.key:       <base64>
```

`ConfigMap` merges `data` (strings) and `binaryData` (bytes) on read; on
write, valid-UTF-8 content goes to `data` and the rest to `binaryData`.
`Secret` uses `data` only. Everything above the `Store` interface — cache,
commit, lease, FUSE — is shared between the two.

```go
type Snapshot struct {
    Data            map[string][]byte
    ResourceVersion string
    FetchedAt       time.Time
}

type Store interface {
    Get(ctx context.Context) (*Snapshot, error)              // quorum read
    Watch(ctx context.Context, sinceRV string) (<-chan *Snapshot, error)
    Patch(ctx context.Context, set map[string][]byte, del []string) error
}
```

### Filenames

Object keys are constrained by the Kubernetes API to `[-._a-zA-Z0-9]+`, and
may not be `.` or `..`. Consequences:

- No subdirectories. `mkdir`, `symlink`, `link` → `ENOTSUP`.
- `create` with a name outside that charset → `EINVAL`.
- `rename` is a copy-key plus delete-key in a single patch.

### Modes and ownership

Taken from mount attributes, **not stored in the object**, so the object
stays interoperable with kubelet's native projection and with `kubectl
edit`. `chmod` inside the pod returns `EPERM`.

Defaults: `fileMode: 0600`, `dirMode: 0700`, `uid: 0`, `gid: 0`. Combined
with `fsGroupPolicy: File`, kubelet applies the pod's `fsGroup`.

## Read path and consistency

> **"Quorum GET"** throughout this document means a `GET` with
> `resourceVersion` left **unset**, which the API server serves from etcd
> with a quorum read. This is distinct from `resourceVersion: "0"`, which is
> served from the API server's watch cache and may be stale. The difference
> is the entire read-after-write guarantee, so it is never left to chance in
> the implementation.

**The model matches kubelet's native Secret projection**, deliberately.

Kubelet stages a new timestamped directory containing *every* key and
atomically flips one `..data` symlink. So an update is all-or-nothing across
the whole object; a reader that opens two files after the flip sees a
consistent set; and a reader holding a descriptor across the flip keeps
reading the old, now-unlinked inode until it reopens.

roommate reproduces exactly that:

- The cache holds an `atomic.Pointer[Snapshot]` fed by a watch.
- `open()` **pins** the current snapshot into the file handle for that
  handle's lifetime.
- New `open()` calls see the newer snapshot; existing handles do not.

Pinning is required for correctness, not polish. A patch replaces the
object's version wholesale; an unpinned handle could read offset 0 from one
version and offset 4096 from the next, handing the consumer spliced,
corrupt JSON.

### Staleness bound

The watch records `lastSyncAt`. If `open()` finds it older than
`stalenessBoundSeconds` (default 30), it performs a quorum `GET` first. This
is the same safety net kubelet provides with its periodic resync — it
catches a watch that has died silently.

If that `GET` fails, the stale snapshot is served anyway. A stale credential
costs a `401` and a retry; a failed read costs an outage.

### The read-after-write guarantee

Eventual consistency is fine on the hot path and *not* fine in the refresh
path. The guarantee is therefore scoped to the lock:

```
flock(LOCK_EX)
    acquire Lease
    MANDATORY quorum GET, replace snapshot   ← the guarantee lives here
```

That is what makes the standard double-checked pattern sound:

```
lock -> re-read (guaranteed fresh) -> "did someone already refresh?"
     -> yes: use theirs, skip the refresh entirely
     -> no:  refresh, write, unlock
```

Without a guaranteed-fresh read after acquiring the lock, that check is
meaningless and the refresh race returns.

**A consumer that writes without taking the lock gets no guarantee.** This
is a deliberate, load-bearing constraint, stated plainly in the README
rather than hidden.

## Write path

Per-key merge-patch. There is no read-modify-write, no `resourceVersion`
CAS, no retry loop, and no merge or reconciliation logic anywhere:

```
PATCH  application/merge-patch+json
  {"data": {"session.key": "<base64>"}}       # write
  {"data": {"config.json": null}}             # delete
```

The API server merges at the key level. Keys this handle never touched are
untouched *because the patch never mentions them* — that is a property of
the request, not something the driver computes. Concurrent writes to
different keys cannot interfere, structurally.

Writes buffer in the handle and commit on `flush`, `fsync`, or `release`.
There is no time-based writeback: an application that holds a file open
forever and never syncs never commits, exactly as on a local filesystem.

One asymmetry worth stating precisely: **locks behave like a local
filesystem; writes behave like NFS.** Within a pod, one FUSE server serves
all handles, so sibling processes see buffered writes immediately. Across
pods, visibility is open-to-close.

### What this gives up

Prior design exploration proposed keeping the `resourceVersion` CAS as a
correctness backstop that would surface `EIO` when a caller wrote without
holding the lock. Patch semantics have no such precondition, so **that
backstop is gone**: an unlocked writer silently wins rather than erroring.

This is consistent with the stated model — the Lease is the *sole* mutual
exclusion mechanism — and it is the price of a write path with no
conflict-handling code at all. It is recorded here as a real property that
was considered and dropped, not an oversight.

A merge patch *can* carry `metadata.resourceVersion` as a precondition, so
reinstating optimistic concurrency later costs one field and no
restructuring.

## Locking

**One Lease per object.** Never per key.

```yaml
apiVersion: coordination.k8s.io/v1
kind: Lease
metadata:
  name: roommate-claude-credentials   # default: roommate-<objectName>
  namespace: my-app                   # always the consuming pod's namespace
spec:
  holderIdentity: <podUID>:<handleID>
  leaseDurationSeconds: 15
```

- Renewal every 5s in a background goroutine, for as long as the handle
  holds the lock. There is **no maximum hold time** — an open file keeps its
  lock as long as it wants, as on a local filesystem.
- A blocking `flock(LOCK_EX)` against a live holder **blocks indefinitely**,
  interruptible via FUSE's interrupt path so a signal still breaks it.
  `LOCK_NB` returns `EWOULDBLOCK` immediately.
- `LOCK_SH` maps to the same exclusive Lease. Over-strict — concurrent
  readers serialize — but a caller taking a shared lock is asking for "no
  writer is mid-write", and only the Lease can actually promise that.
  Readers that do not lock are unaffected and stay fast.
- Acquisition: `GET` the Lease; create it if absent; take ownership if
  present but expired (`renewTime + duration < now`), using CAS on the
  Lease's own `resourceVersion`; otherwise back off and retry.

### Ordering is load-bearing

```
1. COMMIT pending writes
2. THEN release the Lease
```

The next holder's mandatory quorum `GET` must be able to observe our write.
Releasing first would reintroduce precisely the race this driver exists to
close.

### Fencing

Before committing, the driver verifies that background renewal is still
healthy. If renewal has been failing, the write is refused with `EIO`
rather than committed under a lock we may no longer hold.

### Why not per-key Leases

Considered and rejected. Per-key locking combined with indefinite blocking
acquisition produces classic AB/BA deadlock — two pods taking two files in
opposite orders, with no timeout to break it. One Lease per object makes
that structurally impossible.

Secondary reasons: `resourceNames` cannot enumerate keys that do not exist
yet, so RBAC would have to be namespace-wide on Leases or edited per new
file; and object keys permit `_` and uppercase while Lease names are DNS
subdomains, forcing an opaque hashing scheme.

## Mount lifecycle

`NodePublishVolume` is the entire driver. Everything else is bookkeeping.

```
FIRST publish for a target_path
  volumeContext:
    objectKind = Secret
    objectName = claude-credentials
    csi.storage.k8s.io/pod.namespace            = my-app
    csi.storage.k8s.io/pod.service-account.name = session-runner
    csi.storage.k8s.io/serviceAccount.tokens    = {"": {token, expiry}}

  1. build rest.Config with BearerToken = the pod's token
  2. quorum GET      403 -> PermissionDenied, naming the SA and missing verb
                     404 -> NotFound
  3. start watch
  4. mount FUSE at target_path (allow_other, EnableLocks)
  5. record in registry + published-state file

REPUBLISH  (every 0.1s, forever)
  registry hit  -> atomic token swap, return OK.  NEVER errors.
  registry miss -> plugin restarted: remount silently, retry on failure,
                   still never error

UNPUBLISH
  release Lease if held -> unmount FUSE -> stop watch -> drop state
```

### Why republish never errors

`requiresRepublish: true` makes kubelet call `NodePublishVolume` **every 0.1
seconds** — it rides kubelet's hardcoded `reconcilerLoopSleepPeriod` and is
not tunable. Two consequences shape the implementation:

1. The handler must be trivially cheap: a registry lookup and an atomic
   token swap. It must not stat, fork, or re-verify the mount.
2. **kubernetes/kubernetes#121271** — when a republish call returns a final
   error, kubelet deletes the mount point from the host filesystem, and
   later successful calls cannot restore the pod's view. So: once a target
   has been published successfully, its handler never returns an error
   again. A rejected or expired token is stashed and surfaced as `EACCES`
   from the *FUSE data path* instead, which is the correct layer to fail at
   — revocation still bites, the mount survives, and buffered writes are
   not destroyed by a transient API blip.

On a genuine *first* publish, erroring is correct — it is how an
unauthorized pod learns it is unauthorized. After a plugin restart the
in-memory registry is gone and the two cases are indistinguishable, so a
small `published.json` state file records live targets and tells them apart.

### Plugin restart

Republish notices a missing mount within ~100ms and rebuilds it. File
descriptors the pod already held break permanently (`ENOTCONN`) — FUSE
cannot reattach them. For a file read occasionally that is a non-issue; for
a pod holding a descriptor open continuously it is not. Documented, not
solved in v1.

## Failure modes

| Condition | Behaviour |
|---|---|
| API server unreachable | reads serve last-known-good; writes `EIO` (fail closed) |
| Lease holder crashes | 15s expiry, next acquirer takes over |
| Watch disconnects | quorum `GET` before trusting cache again |
| Token expired, republish stalled | reads from cache; writes `EIO` |
| RBAC revoked mid-mount | next API call `403` → `EACCES`; mount survives |
| Object exceeds ~1 MiB | `ENOSPC` at write time, not an opaque API rejection |
| Object deleted underneath | reads serve last snapshot; writes fail; logged loudly |
| Lease renewal lost mid-hold | commit refused with `EIO` |
| Node plugin restart | remount within ~100ms; open descriptors break |

## Configuration

`volumeAttributes` on the inline volume:

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

```yaml
volumes:
  - name: creds
    csi:
      driver: roommate.csi
      volumeAttributes:
        objectKind: Secret
        objectName: claude-credentials
```

The namespace is always the consuming pod's, taken from `podInfoOnMount`
and never configurable — cross-namespace access is a non-goal.

## Security considerations

- The DaemonSet is privileged but holds **no** Kubernetes API permissions.
  Compromising it yields node-level access, which a privileged DaemonSet
  already implies, but grants no additional API reach.
- Object contents live in the FUSE server's memory for the lifetime of the
  mount. They are not written to disk by the driver.
- The pod's token is held in memory and rotated by kubelet. It is never
  logged; error messages name the ServiceAccount and the missing verb, never
  the token.
- `allow_other` makes the mount visible to any UID on the node that can
  reach the target path. The target path lives under the kubelet pod
  directory, whose permissions already scope it.

## Testing

**Unit** — the bulk of correctness lives here and needs neither root nor a
cluster. `k8s.io/client-go/kubernetes/fake` with custom reactors for the
cache, patch, and Lease logic. Note that the fake clientset does not
faithfully model `resourceVersion` semantics; Lease CAS behaviour is
verified against a real API server instead (see below).

**Filesystem** — mount on a temp directory against an in-memory `Store` and
drive real syscalls: `open`/`read`/`write`/`rename`/`unlink`/`flock`.
Requires `/dev/fuse`, which GitHub's `ubuntu-latest` runners provide.

**API semantics** — `envtest` against a real apiserver + etcd, for the
claims we cannot fake: that merge-patch merges at the key level, that Lease
CAS rejects a stale writer, and that a quorum `GET` reflects a just-
completed patch.

**End-to-end** — kind, two nodes. The test that matters is the refresh race:
two pods on different nodes both observe an expired credential, both take
the lock, and exactly one refresh occurs while the other observes the new
value. Plus revocation (drop the `RoleBinding` → `EACCES`), an external
writer touching an unrelated key, and node-plugin restart recovery.

Driving the refresh race deterministically — rather than passing by luck —
is called out below as an open question.

## Limitations

Stated in the README, not discovered later:

- **`inotify` does not fire on remote changes — in v1.** Programs that
  detect rotation by watching for kubelet's `..data` symlink flip will not
  work. Programs that re-read on `401` will. This is a deferred limitation
  rather than a permanent one; see *inotify* below.
- **Read-after-write only under the lock.** Unlocked readers get eventual
  consistency bounded by watch propagation.
- **No backstop for unlocked writers.** They silently win.
- **Whole-file locking only.** No byte ranges.
- **~1 MiB ceiling**, inherited from etcd.
- **Flat namespace.** No subdirectories.
- **`chmod` returns `EPERM`.** Modes come from mount attributes.
- **Open descriptors break across a node-plugin restart.**

## inotify

Deferred from v1, but researched enough to record that it is achievable.

`fsnotify` is a **VFS-layer** mechanism: events are emitted from local
syscall paths as the kernel executes an operation. A change that only the
FUSE server knows about never traverses the VFS, so nothing emits. libfuse's
own wiki states it directly — *"Fsnotify does not work right now with FUSE
based filesystems and network filesystems."* This is the same reason inotify
has never worked for remote changes on NFS or SMB.

There is an irony worth stating: the property that lets this driver
intercept writes and `flock` — being a userspace filesystem — is exactly
what costs it inotify. A driver that merely projected files onto tmpfs and
flipped a symlink would get inotify for free and could do none of what this
project exists to do.

**Kernel support is not coming soon.** A 2021 RFC series added general
fsnotify support to FUSE (`FUSE_NOTIFY_FSNOTIFY` plus a `fuse_fsnotify_event`
inode operation), motivated by virtiofs. There is no evidence it landed, and
virtiofs still documents inotify as unsupported. Do not design around it.

Two routes work today, without kernel changes:

**Route A — `NotifyDelete`.** Exactly one FUSE notification reaches inotify
watchers. Per libfuse, `fuse_lowlevel_notify_delete` will, *"if there are any
inotify watches registered for the dentry,"* inform the watchers *"that the
dentry has been deleted."* go-fuse exposes this as `NotifyDelete` —
explicitly *"equivalent to NotifyEntry, but also sends an event to inotify
watchers."* Its sibling `EntryNotify` does **not**.

Firing `NotifyDelete` at changed entries delivers a *delete*, not a
*modify*. That satisfies the common Go `fsnotify` reloader idiom — watch a
file, on `REMOVE`/`RENAME` re-add the watch and re-read — and does nothing
for a consumer waiting on `IN_MODIFY` or `IN_CLOSE_WRITE`. Small, roughly
twenty lines.

**Route B — drive the VFS deliberately.** Emulate kubelet's layout: carry a
real `..data` entry, and on a remote change have the server perform an
actual `rename()` **through its own mount path**, from a goroutine that is
not servicing a request. The kernel then emits `IN_MOVED_FROM` /
`IN_MOVED_TO` naturally, because a real VFS operation really happened —
indistinguishable from kubelet's flip. Anything that watches a projected
Secret today would work unmodified.

The hazard is classic FUSE self-deadlock: a server performing I/O on its own
filesystem. Both libfuse and go-fuse warn about it explicitly (*"You should
not hold any FUSE filesystem locks, as that can lead to deadlock"*). Real,
but well understood, and confined to one goroutine under a documented rule.

Route B is the one that delivers genuine parity and is the recommended path
if this is ever needed.

## Open questions

1. **Deterministic e2e for the refresh race.** Needs a way to hold both pods
   at the pre-lock barrier and release them together, so the test proves
   serialization rather than observing it by chance.
2. **Watch fan-out.** Each (pod, volume) gets its own watch. A node running
   many pods against one object holds that many watches on it. Correct, but
   a shared per-node cache may be worth it later — it would need care, since
   each pod's client has a different identity.
3. **Lease duration tuning.** 15s/5s is inherited from earlier design work
   and has not been validated against real lock-hold durations.
4. **`NodeGetVolumeStats`.** Deferred from v1. Would report the 1 MiB
   ceiling and current serialized size.
5. **Consumer cooperation.** The double-checked pattern (lock → re-read →
   *already refreshed?* → maybe write) is what makes this safe. A consumer
   that unconditionally refreshes after taking the lock still double-
   refreshes. The guaranteed-fresh read gives it the information; it has to
   look.
