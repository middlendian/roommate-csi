# CLAUDE.md — roommate-csi contributor / agent guide

## Project goal

roommate-csi is a Kubernetes CSI driver that mounts a `Secret` or
`ConfigMap` as a **writable, cross-node shared directory**, with `flock`
backed by a `coordination.k8s.io` `Lease`. It exists to close the refresh
race: two pods on different nodes both see a stale credential, both refresh,
and a rotating-refresh-token provider revokes the whole token family. See
`docs/superpowers/specs/2026-08-20-roommate-csi-design.md` for the full
design — it is the authority on what this driver is and what it deliberately
gives up. `README.md` is the user-facing summary; keep both in sync when
surface changes.

## Repo map

```
cmd/node/                 the only binary; no controller service exists
pkg/driver/               CSI Identity + Node gRPC servers (publish-only)
  rbachint.go              generates the copy-pasteable RBAC YAML for a
                           PermissionDenied error
  node.go                  NodePublishVolume: first-publish vs. republish
  targetlock.go            per-target_path lock serializing first publish
pkg/objectfs/
  fs.go                    go-fuse nodes: Root (dir) + File
  handle.go                open file handle: buffer, dirty flag, Lease
  snapshot.go              immutable {keys->bytes, resourceVersion}
  store.go / store_secret.go / store_configmap.go   Store interface + impls
  cache.go                 watch-fed atomic.Pointer[Snapshot], staleness bound
  commit.go                per-key merge-patch writes
  lease.go                 flock -> Lease acquire/renew/release
  keys.go                  object key <-> filename validation
  volume.go                wires Store + Cache + Committer + Lease together
pkg/mounts/                target_path -> live mount registry + state file
pkg/podtoken/              extracts the pod's identity/token from volume context
deploy/kustomize/base/     namespace, driver ServiceAccount (zero RBAC),
                           CSIDriver, roommate-user ClusterRole, DaemonSet
examples/                  rbac.yaml (per-object Role+RoleBinding), pod.yaml
hack/                      e2e.sh, kind.yaml, changelog.sh (+ _test.sh:
                           CHANGELOG promote/notes for release workflows)
.ko.yaml                   image build: distroless, amd64+arm64
.github/workflows/         cut-release -> tag-and-release -> release
test/apisemantics/         envtest suite against a real apiserver + etcd
test/e2e/                  kind, three nodes (1 control-plane + 2 workers); build tag: e2e
docs/superpowers/specs/    the v1 design doc
docs/superpowers/plans/    the v1 implementation plan
```

## Build / test commands

```sh
make build       # go build -o bin/roommate-node ./cmd/node
make test        # go test ./...
make test-race   # go test -race ./...
make cover       # go test -race -coverprofile=cover.out ./...
make vet         # go vet ./...
make fmt-check   # fail if any file isn't gofmt -s clean (CI gate)
make fmt         # gofmt -s -w in place
make lint        # golangci-lint run
make tidy        # go mod tidy
make tidy-check  # fail if go.mod/go.sum need tidying (CI gate)
make check       # fmt-check + vet + lint + tidy-check + cover + build
make envtest     # real apiserver + etcd via setup-envtest; see below
make e2e         # kind cluster + go test -tags=e2e ./test/e2e/...; see below
make ko          # ko build linux/amd64 + linux/arm64, no push
make ko-local    # ko build for the host arch into the local Docker daemon
make changelog-test  # tests for hack/changelog.sh (CI gate)
```

What each target needs beyond a Go toolchain:

- `make test`, `make test-race`, `make cover`, `make vet`, `make build` —
  nothing extra. The FUSE-backed tests in `pkg/objectfs` mount on a temp
  directory and need `/dev/fuse` to be present (true on GitHub's
  `ubuntu-latest` runners and most Linux dev boxes).
- `make lint` — `golangci-lint` on `PATH`. Its config
  (`.golangci.yml`) sets `build-tags: [e2e]` specifically so `test/e2e`
  (which never runs under `go test ./...`) still gets linted; do not remove
  that without another way to lint that package.
- `make envtest` — `setup-envtest` on `PATH`, plus a downloaded etcd/
  kube-apiserver binary set (`setup-envtest use <version> -p path`, cached
  under `~/.cache/kubebuilder-envtest` or platform equivalent). Pin
  `setup-envtest` to a release whose own `go.mod` requires Go 1.25, not
  1.26+ — as of this writing the versions published as the standalone
  `sigs.k8s.io/controller-runtime/tools/setup-envtest` module (v0.24.0+)
  all require Go 1.26. Installing an in-repo commit at the `v0.23.0` tag
  (which still requires only Go 1.25) via its pseudo-version works instead:
  `go install sigs.k8s.io/controller-runtime/tools/setup-envtest@<commit-of-v0.23.0>`.
  This suite is what proves the claims a fake clientset can't: that
  merge-patch merges at the key level, that Lease CAS rejects a stale
  writer, that a quorum `GET` observes a just-completed patch, and that a
  watch scoped by `resourceNames` genuinely requires the
  `metadata.name` field selector — the last one is a fake-clientset blind
  spot that would otherwise only surface in a live cluster.
- `make ko` / `make ko-local` — `ko` on `PATH` (v0.19.1); `VERSION`
  defaults from `git describe`.
- `make e2e` — Docker and `kind` on `PATH`, plus `/dev/fuse` on the host
  (the script `modprobe fuse`s if it's missing and fails loudly if that
  doesn't work). Builds the image, loads it into a three-node (1
  control-plane + 2 workers) kind cluster,
  applies `deploy/kustomize/base`, and runs the `e2e`-tagged suite in
  `test/e2e/`. `TestRefreshRaceProducesExactlyOneRefresh` is the test that
  matters: it is the end-to-end proof of this driver's whole premise.
  `make e2e` builds the image with `ko`, so `ko` must be on `PATH` too;
  Docker is still needed for `kind`.

## Invariants an agent must not break

These are load-bearing, several were real bugs once, and none of them are
enforced by the type system — breaking one compiles clean and can still pass
most of the unit suite.

- **The driver's ServiceAccount gets zero RBAC.** No `Role`, `ClusterRole`,
  or binding for it, anywhere. Every API call the driver makes is
  authenticated as the *consuming pod's* ServiceAccount via a token kubelet
  mints per `CSIDriver.spec.tokenRequests`. Adding a permission for the
  driver's own identity reintroduces the confused-deputy problem this design
  exists to avoid.
- **Quorum GET means `GetOptions` with `ResourceVersion` left empty — never
  `"0"`.** `resourceVersion: "0"` is served from the API server's watch
  cache and can be stale; leaving it unset forces a quorum read from etcd.
  The entire read-after-write guarantee (the thing that makes the
  double-checked refresh pattern sound) depends on this one field staying
  empty at exactly the two call sites in `store_secret.go` /
  `store_configmap.go` that are commented `// Empty GetOptions == quorum
  read. Do not set ResourceVersion.` Do not "helpfully" add caching there.
- **Watches must carry a `metadata.name` field selector.** RBAC states that
  restricting `list`/`watch` by `resourceNames` requires the client to pass
  a `metadata.name` field selector matching that name, or the request is
  denied — not narrowed, denied. A shared informer would compile, pass
  every unit test against the fake clientset, and then 403 in a real
  cluster, because the fake clientset doesn't enforce this and the default
  `SharedInformerFactory` doesn't set a field selector. This is exactly why
  there is no informer anywhere in this codebase and why
  `test/apisemantics` (real apiserver, real RBAC) exists at all —
  `TestResourceNamesRequiresNameFieldSelectorOnWatch` is the regression test
  for this specific gap.
- **`NodePublishVolume` must never return an error once a target has
  published.** Per kubernetes/kubernetes#121271, a republish call that
  returns a final error causes kubelet to delete the mount point from the
  host filesystem, and no later successful call can restore it. Since
  `requiresRepublish: true` makes kubelet call `NodePublishVolume` roughly
  every 100ms forever, a rejected or expired token on republish must be
  stashed and surfaced as `EACCES` from the FUSE data path instead — never
  as an RPC error. Erroring is only correct on a genuine *first* publish
  (that's how an unauthorized pod learns it's unauthorized); after a plugin
  restart the in-memory registry is gone, which is why `pkg/mounts` persists
  a state file to tell the two cases apart.
- **Writes are per-key merge patches. No read-modify-write, no CAS, no retry
  loop.** The write path sends `PATCH application/merge-patch+json` with
  only the keys this handle touched; the API server does the merge. Adding
  a `resourceVersion` precondition, a read-before-write, or retry logic here
  reintroduces exactly the complexity the design traded away — see the
  spec's *What this gives up* section. If optimism ever needs reinstating,
  it costs one field on the patch body, not a restructuring.
- **`Release`/unlock must commit before releasing the Lease.** Both
  `handle.Release` and `handle.unlock` call `h.commit(ctx)` first and only
  then release the Lease. The next holder's mandatory quorum GET must be
  able to observe this holder's write; releasing first reopens the refresh
  race this driver exists to close. Do not reorder these for any reason,
  including "it looks more symmetric."
- **Copy-on-write on the handle buffer — never mutate `h.buf`'s backing
  array in place.** `Read` hands out sub-slices of `h.buf` directly to
  go-fuse, which writes them to the kernel *after* `Read` returns and
  `h.mu` is released. `Write`, `truncate`, and the re-pin in `setlk` all
  allocate a fresh slice and swap it in (`next := make(...); copy(next,
  ...); h.buf = next`) rather than `copy(h.buf[off:], data)` or a reslice.
  An in-place mutation can tear a read that's still being copied out from
  under a lock that was released before the copy finished.
- **Tests must not be tautological with the constants they validate.** A
  test built from the same `Key*` constant it's checking (as a `want`, or as
  a fixture map key) passes for any value of that constant, because both
  sides drift together. That is exactly how `KeyPodServiceAcct` shipped
  with the wrong wire key once — every mount failed, and every unit test
  stayed green, because every test constructed its fixtures from the
  constant itself instead of the literal CSI pod-info string. See
  `pkg/podtoken/keys_test.go`'s `TestVolumeContextKeyLiterals`, which
  deliberately hardcodes the expected literal on the right-hand side and
  must stay that way — do not "simplify" it back to referencing the
  constants.
- **The image has no shell and no `fusermount3`.** It is distroless
  (`.ko.yaml`). go-fuse mounts with `DirectMount` (`pkg/objectfs/mount.go`),
  which works because the driver runs as root. Keep it `DirectMount`, never
  `DirectMountStrict`: the unprivileged FUSE unit tests depend on the
  `fusermount3` fallback. Don't add exec probes or anything else that
  shells out inside the driver container; liveness is the `livenessprobe`
  sidecar.

## Other things worth knowing before changing code

- **Inline ephemeral volumes only.** No PV, no PVC, no StorageClass, and no
  controller service — `NodePublishVolume` is the entire driver. Don't add
  `CREATE_DELETE_VOLUME` or similar controller capabilities without
  revisiting the whole authorization model in the spec.
- **One Lease per object, never per key.** Per-key locking plus indefinite
  blocking acquisition produces AB/BA deadlock with no timeout to break it;
  see the spec's *Why not per-key Leases*.
- **`fs.Inode`/handle changes touch three tasks' worth of history.** The
  `handle` struct (`mu`, `snap`, `buf`, `dirty`, `lease`) is shared across
  the read, write, and lock paths in `handle.go` — don't redefine or
  partially shadow its fields elsewhere.
- Every shell-out (there are few — this driver is FUSE + client-go, not
  loop devices or mount(8)) should still go through a fakeable seam in
  tests rather than hitting the real syscall/API directly where avoidable.
- gRPC handlers map errors to canonical codes via
  `google.golang.org/grpc/status` on the driver side, and to `syscall.Errno`
  via `errnoFor` on the FUSE side (`pkg/objectfs/handle.go`). Never `panic`
  in either.
- Linux-only syscalls live in files that already require Linux semantically
  (FUSE mounts don't exist elsewhere); no build tags are needed because
  nothing in this repo builds on non-Linux in CI.

## Pre-merge checklist

- `make build` and `make vet` clean.
- `make test` (or `make test-race` for anything touching concurrency —
  `pkg/objectfs`, `pkg/mounts`, `pkg/driver`) passes.
- `make fmt-check` and `make lint` clean.
- `make envtest` passes if the change touches quorum-read semantics,
  merge-patch behavior, Lease CAS, or the field-selector watch — anything
  in `pkg/objectfs/store_secret.go`, `store_configmap.go`, `commit.go`, or
  `lease.go`.
- `make e2e` passes locally if the change could plausibly affect the
  kubelet-driven mount lifecycle, the RBAC-denial surface, or the refresh
  race itself — anything in `pkg/driver`, `pkg/mounts`, or the DaemonSet
  manifest.
- New behavior covered by a unit test in the relevant package, and that
  test is not tautological with any constant it exists to validate (see
  above).
- README.md and this file updated when the CSI surface, RBAC shape, or
  configuration attributes change.

## Releasing

Run the **Cut release** workflow with `vX.Y.Z` (needs a non-empty
`## [Unreleased]` section in `CHANGELOG.md`), then review and merge the
`release/vX.Y.Z` PR it opens. Merging triggers **Tag and release**, which
tags the merge commit, pushes the multi-arch image, and creates the GitHub
release. To re-run a release (e.g. after a flake), dispatch **Tag and
release** by hand with the version.

A few things about the pipeline worth knowing before you touch it:

- The repo setting "Allow GitHub Actions to create and approve pull
  requests" (Settings → Actions → General) must be enabled, or **Cut
  release**'s `gh pr create` fails.
- The release PR is opened with `GITHUB_TOKEN`, so `pull_request` CI does
  not run on it — the **Tag and release** run is where problems actually
  show up.
- The ghcr.io package is created on the first release push and is private
  by default; make it public once so clusters can pull the image without
  credentials.
- If **Cut release** fails after pushing `release/vX.Y.Z` but before
  opening the PR, delete that branch before re-running the workflow.
- A tag ruleset on `refs/tags/v*` is recommended, though not required by
  the pipeline itself.
