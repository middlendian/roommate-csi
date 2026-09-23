# Multi-arch images with ko, and a tag/release pipeline

**Status:** draft
**Date:** 2026-09-23

Build the driver image with [ko](https://ko.build) for `linux/amd64` and
`linux/arm64` in a single job, and add a cut → tag → release pipeline
modeled on `middlendian/fileblock-csi`'s.

## Problem

- The only image today is built by `docker build` in CI's `image` job, for
  the runner's architecture (amd64) only. Nothing publishes it anywhere.
- arm64 nodes cannot run the driver at all.
- There is no release process: no tags, no GitHub releases, and
  `deploy/kustomize/base` points at `:dev`.

fileblock-csi solves the release half with three workflows and GoReleaser,
but builds its image as a per-arch matrix on native runners plus a separate
manifest-merge job. ko cross-compiles Go and assembles the multi-arch index
itself, so the same result is one job with no QEMU and no manifest step.

## Why the image can be distroless

ko layers a static Go binary onto a base image; it cannot `apt-get`
anything. The current image is `debian:bookworm-slim` plus `fuse3` for two
reasons, both of which go away:

1. **`fusermount3`.** go-fuse mounts *and* unmounts through the setuid
   `fusermount3` helper unless `MountOptions.DirectMount` is set. With
   `DirectMount: true`, go-fuse calls `mount(2)`/`umount(2)` itself first
   and falls back to `fusermount3` only if that fails. The driver runs as
   root in a privileged DaemonSet, so the direct path always succeeds in
   production. The unprivileged FUSE unit tests on CI runners get
   `EPERM` from `mount(2)` and fall back to `fusermount3` exactly as they
   do today — which is why this is `DirectMount`, not `DirectMountStrict`.
   `DirectMountFlags` stays zero, which go-fuse maps to
   `MS_NOSUID|MS_NODEV`, the same flags `fusermount3` applies.
2. **`/bin/sh` for the liveness probe.** The `roommate` container's probe is
   `exec: /bin/sh -c "test -S /csi/csi.sock"`. It is replaced by the
   standard CSI `livenessprobe` sidecar (below), which needs no shell in the
   driver image.

`pkg/mounts.DetachStale` keeps its `fusermount3 -u -z` fallback after
`umount2(MNT_DETACH)`. In the image the binary is absent, so the fallback
fails with a clear `exec` error — but only after the raw syscall, which
succeeds as root, has already failed. Its comment is updated to say so.

Base image: `gcr.io/distroless/static-debian12` (multi-arch, CA certificates
included, runs as root by default — required for `/dev/fuse` and
`mount(2)`). Not the `:nonroot` variant.

## Liveness: `livenessprobe` sidecar

Add `registry.k8s.io/sig-storage/livenessprobe:v2.20.0` to the DaemonSet,
sharing the `socket-dir` volume, listening on a fixed health port
(`--health-port=9809`). It calls the driver's CSI `Probe` RPC
(`pkg/driver/identity.go`) over `/csi/csi.sock`. The `roommate` container's
probe becomes `httpGet: {path: /healthz, port: 9809}`.

This checks more than the old probe did: a wedged gRPC server with its
socket still on disk now fails liveness. The DaemonSet does not use
`hostNetwork`, so the port cannot collide with other drivers.

## Build configuration

`.ko.yaml`:

```yaml
defaultBaseImage: gcr.io/distroless/static-debian12
defaultPlatforms: [linux/amd64, linux/arm64]
builds:
  - id: roommate-node
    main: ./cmd/node
    env: [CGO_ENABLED=0]
    flags: [-trimpath]
    ldflags: ["-s -w -X main.version={{.Env.VERSION}}"]
```

`VERSION` must be set for every ko invocation; the Makefile defaults it
from `git describe --tags --always --dirty`, falling back to `dev`, the way
fileblock's does.

The feasibility of this config was checked in this session: `ko build`
compiled `./cmd/node` for both platforms from an arm64 host; only the final
load into a (missing) local Docker daemon failed.

Makefile:

- `docker` target removed, along with `Dockerfile` and `.dockerignore`.
- `ko` — `ko build --bare --push=false ./cmd/node` for both platforms
  (builds and discards; a local "does it build" check needing no daemon).
- `ko-local` — `ko build --local --bare --platform=linux/$(go env GOARCH)`,
  loading into the local Docker daemon as `ko.local:<tag>`.
- `release-snapshot` — `goreleaser release --snapshot --clean
  --skip=publish`, as in fileblock.

`mise.toml` is not added; ko is installed in CI by `ko-build/setup-ko`.

## CI (`ci.yml`)

The `check` job is unchanged. The `image` job becomes:

1. `actions/setup-go` + `ko-build/setup-ko@v0.10` (ko v0.19.1).
2. `VERSION=ci ko build --bare --push=false ./cmd/node` — both platforms
   must build.
3. `ko build --local --bare --platform=linux/amd64 ./cmd/node`, then
   `docker run --rm <ref> -h` as the smoke test.

The `fusermount3 -V` smoke test is dropped; there is no `fusermount3` to
find, by design.

## e2e (`hack/e2e.sh`)

Replace `docker build` + `kind load docker-image` with:

```sh
IMAGE_REF="$(KO_DOCKER_REPO=kind.local KIND_CLUSTER_NAME="$CLUSTER" \
  VERSION=e2e ko build --bare --platform="linux/$(go env GOARCH)" ./cmd/node)"
```

`kind.local` makes ko load the image straight into the kind nodes. The
script then applies `deploy/kustomize/base` through a temporary
kustomization (in a `mktemp -d` directory) whose `images:` entry rewrites
`ghcr.io/middlendian/roommate-csi` to `IMAGE_REF`. The base is not edited.

`e2e.yml` adds `ko-build/setup-ko`. The e2e suite runs unchanged against
the distroless image; this is the test that proves `DirectMount` and the
`livenessprobe` sidecar work under a real kubelet, including
`TestRefreshRaceProducesExactlyOneRefresh` and the node-plugin restart
recovery test (which exercises `DetachStale`).

## Release pipeline

Three workflows, copied from fileblock-csi with the changes noted.

### `cut-release.yml` (workflow_dispatch, input `version`)

As fileblock's: validates `vX.Y.Z[-pre]`, refuses an existing tag, branch
or CHANGELOG section, requires a non-empty `[Unreleased]`, promotes
`## [Unreleased]` to `## [X.Y.Z] - <date>`, rewrites the compare links,
bumps `newTag` in `deploy/kustomize/base/kustomization.yaml`, pushes
`release/vX.Y.Z`, and opens a PR.

Change: **first release.** fileblock extracts the previous tag from the
`[Unreleased]: …/compare/<prev>...HEAD` link and errors without one. Here
that link is currently `…/commits/main`. When no compare link is present,
the workflow treats this as the first release: the new version's link is
`…/releases/tag/vX.Y.Z` and `[Unreleased]` becomes
`…/compare/vX.Y.Z...HEAD`. The PR body says "from `dev`" for the tag bump.

### `tag-and-release.yml` (PR closed on main, or workflow_dispatch)

Identical to fileblock's: runs only for a merged PR whose head ref starts
with `release/v`, reads the version from the branch name, verifies
`newTag` matches, creates and pushes the annotated tag (idempotently), then
calls `release.yml`. `workflow_dispatch` is the re-run escape hatch.

### `release.yml` (workflow_call, input `version`)

Two jobs instead of fileblock's three:

- **`image`** — checkout at the tag, `setup-go`, `setup-ko`, `ko login
  ghcr.io` with `GITHUB_TOKEN`, then

  ```sh
  KO_DOCKER_REPO=ghcr.io/middlendian/roommate-csi VERSION=$TAG \
    ko build --bare --tags="$TAG[,latest]" \
    --image-label=org.opencontainers.image.…=… ./cmd/node
  ```

  `latest` is added only when the version contains no `-`. The OCI labels
  match fileblock's set (title, description, url, source, version,
  revision, licenses=GPL-3.0-or-later). ko pushes both platform images and
  the index in one step, which replaces fileblock's per-arch matrix and its
  `imagetools create` manifest job.
- **`release`** — as fileblock's: extract the `## [X.Y.Z]` CHANGELOG
  section to `$RUNNER_TEMP`, run GoReleaser with `--release-notes`.

`.goreleaser.yaml` builds `roommate-node` for linux/amd64+arm64 with the
same ldflags as `.ko.yaml`, archives it with LICENSE/README/CHANGELOG, and
creates the GitHub release (`prerelease: auto`, changelog disabled). Its
`before` hook runs `go mod tidy`, as fileblock's does. GoReleaser does not
build images; a comment says ko does.

Permissions are scoped as in fileblock: `contents: write` + `packages:
write` on the release workflows, `contents: write` + `pull-requests:
write` on cut-release, `contents: read` on CI.

## Manual repository setup (not done by this change)

- Add a tag ruleset on `refs/tags/v*` (block creation, update, deletion,
  non-fast-forward) matching fileblock-csi's "version tags" ruleset, with
  GitHub Actions allowed to create tags. Without it any writer can push a
  `v*` tag by hand; nothing in the pipeline depends on the ruleset.
- The ghcr package `roommate-csi` is created on first push, and private by
  default for a user-owned package; make it public after the first release
  so clusters can pull without credentials.

## Docs

- README: next to the existing `kubectl apply -k deploy/kustomize/base`,
  note that released images are published for amd64 + arm64 at
  `ghcr.io/middlendian/roommate-csi:vX.Y.Z`, and that `base` on `main`
  tracks the most recent release's tag (bumped by `cut-release`).
- CLAUDE.md: replace `make docker` with the ko targets; note that the image
  has no shell and no `fusermount3`, that `DirectMount` is what makes that
  safe, and that it must stay `DirectMount` (not `Strict`) for the
  unprivileged FUSE unit tests.
- CHANGELOG `[Unreleased]`: replace the "Container image build
  (`Dockerfile`)" entry with the ko multi-arch image, release pipeline,
  and livenessprobe sidecar.

## Testing

- Unit: a test in `pkg/objectfs` asserting `Mount` sets `DirectMount` and
  not `DirectMountStrict`, via a small exported-for-test options builder so
  the assertion doesn't need a mount.
- CI `image` job: both platforms build; the amd64 image runs `-h`.
- `make e2e` in CI: the whole suite against the distroless image, which is
  the real proof for `DirectMount`, `DetachStale` and the new probe.
- Release workflows cannot be exercised before merge. After merge, cut a
  prerelease (e.g. `v0.1.0-rc.1`) to exercise the pipeline end to end
  without moving `:latest`; verify with `docker buildx imagetools inspect`
  that the index lists both platforms.

## Out of scope

- Image signing / SBOM attestations (ko can do both later; fileblock does
  neither).
- Changing the e2e cluster to exercise arm64.
