# ko multi-arch images and release pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the roommate-csi driver image with ko for linux/amd64 and linux/arm64 in one job, and add a cut-release → tag-and-release → release pipeline that publishes it to ghcr and creates a GitHub release with CHANGELOG notes.

**Architecture:** go-fuse switches to `DirectMount` so the distroless image needs no `fusermount3`; the shell-based liveness probe becomes the CSI `livenessprobe` sidecar. `.ko.yaml` replaces the Dockerfile everywhere (Makefile, CI, e2e, release). Release logic that touches CHANGELOG.md lives in one tested shell script (`hack/changelog.sh`) that the workflows call, instead of being inlined in YAML.

**Tech Stack:** Go 1.25, go-fuse v2.11.0, ko v0.19.1 (`ko-build/setup-ko@v0.10`), GitHub Actions, `gh` CLI, kustomize (via `kubectl kustomize`), bash + GNU sed/awk.

**Spec:** `docs/superpowers/specs/2026-09-23-ko-multiarch-release-design.md`

## Global Constraints

- Base image: `gcr.io/distroless/static-debian12` (root; NOT `:nonroot`).
- Platforms: `linux/amd64`, `linux/arm64`.
- ldflags: `-s -w -X main.version=<VERSION>`; `-trimpath`; `CGO_ENABLED=0`.
- Image repo: `ghcr.io/middlendian/roommate-csi`.
- go-fuse: `DirectMount: true`, `DirectMountStrict` false, `DirectMountFlags` zero.
- livenessprobe sidecar: `registry.k8s.io/sig-storage/livenessprobe:v2.20.0`, `--health-port=9809`, roommate container probe `httpGet /healthz :9809`.
- ko in CI: `ko-build/setup-ko@v0.10` with `version: v0.19.1`.
- Version regex: `^v[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.-]+)?$`; prerelease ⇔ version contains `-`; `:latest` only for non-prereleases.
- No GoReleaser anywhere. GitHub release via `gh release create --verify-tag`.
- Release job `needs: image`; after creating the release it reads the body back and fails if blank.
- Never add RBAC for the driver ServiceAccount (repo invariant, CLAUDE.md).
- Commits: small and focused, identity is the repo's configured `middlendian`; never amend/rebase/force-push.
- Every commit message ends with:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01DQSHtHYvbSbnfaPCtcYTJ7
  ```

## Environment notes for implementers

- This dev pod is **arm64 with no Docker daemon and no `/dev/fuse`**. FUSE tests in `pkg/objectfs` skip locally; they run in CI. `make e2e` cannot run locally; it runs in the `e2e` workflow on the PR.
- A two-platform `ko build` takes ~12 minutes on this host. Run it with `run_in_background` and wait, don't let it hit the 10-minute foreground timeout.
- ko is installed at `$HOME/go/bin/ko` (v0.19.1); add `$HOME/go/bin` to `PATH`.
- `kubectl kustomize` is available for rendering manifests.

## Review Focus

1. **Re-running a release after a partial failure** (`tag-and-release` workflow_dispatch after the GitHub release already exists) — expected: the run succeeds and updates the notes, not fails on "release already exists". Pinned in Task 4 (`create-or-edit` step) and exercised by the step's own branch logic.
2. **First-ever release** — CHANGELOG has `[Unreleased]: …/commits/main`, not a compare link — expected: promote succeeds, new section links to `…/releases/tag/vX.Y.Z`. Pinned by `hack/changelog_test.sh` case `first release`.
3. **Empty `[Unreleased]` section, or version already present** — expected: cut-release fails before pushing a branch. Pinned by `hack/changelog_test.sh` cases `empty unreleased` and `duplicate version`.
4. **Notes extraction for a version with no CHANGELOG section** — expected: non-zero exit, no blank release. Pinned by `hack/changelog_test.sh` case `notes missing version`.
5. **Unprivileged FUSE mount with `DirectMount`** (CI runners, dev boxes) — expected: silently falls back to `fusermount3`; existing FUSE tests keep passing. Pinned by the existing `pkg/objectfs` FUSE suite in CI (`make cover`), plus Task 1's options test proving `Strict` is off.

---

### Task 1: go-fuse DirectMount

**Files:**
- Modify: `pkg/objectfs/mount.go`
- Modify: `pkg/objectfs/mount_test.go`
- Modify: `pkg/mounts/detach.go` (comment only)

**Interfaces:**
- Produces: `func mountOptions(v *Volume) fuse.MountOptions` (unexported, package `objectfs`). `Mount` uses it.

- [ ] **Step 1: Write the failing test** — append to `pkg/objectfs/mount_test.go`:

```go
// TestMountOptionsDirectMount pins the options that let the driver image
// ship without fusermount3. DirectMount makes go-fuse call mount(2) and
// umount(2) itself (always succeeds as root in the privileged DaemonSet);
// it must NOT be DirectMountStrict, because the unprivileged FUSE tests in
// this package rely on go-fuse falling back to fusermount3 when mount(2)
// returns EPERM. DirectMountFlags stays zero so go-fuse applies
// MS_NOSUID|MS_NODEV, the same flags fusermount3 uses.
func TestMountOptionsDirectMount(t *testing.T) {
	opts := mountOptions(&Volume{Cfg: Config{ObjectName: "oauth-credentials"}})

	if !opts.DirectMount {
		t.Error("DirectMount = false; the distroless image has no fusermount3")
	}
	if opts.DirectMountStrict {
		t.Error("DirectMountStrict = true; unprivileged FUSE tests need the fusermount3 fallback")
	}
	if opts.DirectMountFlags != 0 {
		t.Errorf("DirectMountFlags = %#x, want 0 (go-fuse default MS_NOSUID|MS_NODEV)", opts.DirectMountFlags)
	}
	// The pre-existing options must survive the refactor.
	if !opts.AllowOther || !opts.EnableLocks {
		t.Errorf("AllowOther=%v EnableLocks=%v, want both true", opts.AllowOther, opts.EnableLocks)
	}
	if opts.ExtraCapabilities&fuse.CAP_ATOMIC_O_TRUNC == 0 {
		t.Error("ExtraCapabilities lost CAP_ATOMIC_O_TRUNC")
	}
	if opts.FsName != "oauth-credentials" || opts.Name != "roommate" {
		t.Errorf("FsName=%q Name=%q, want %q/%q", opts.FsName, opts.Name, "oauth-credentials", "roommate")
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./pkg/objectfs/ -run TestMountOptionsDirectMount -v`
Expected: build failure `undefined: mountOptions`.

- [ ] **Step 3: Implement** — in `pkg/objectfs/mount.go`, replace the inline `fuse.MountOptions{...}` literal in `Mount` with a call to a new function, and add a paragraph to `Mount`'s doc comment. Resulting code:

```go
// ...existing Mount doc comment paragraphs stay as they are, then add:
//
// DirectMount makes go-fuse call mount(2)/umount(2) itself instead of
// shelling out to the setuid fusermount3 helper. As root in the privileged
// DaemonSet that always succeeds, which is why the driver image (distroless,
// see .ko.yaml) carries no fusermount3 at all. It is deliberately not
// DirectMountStrict: unprivileged callers — this package's FUSE tests on CI
// runners — get EPERM from mount(2) and must fall back to fusermount3.
func Mount(dir string, v *Volume) (*fuse.Server, error) {
	root := &Root{vol: v}
	srv, err := fs.Mount(dir, root, &fs.Options{MountOptions: mountOptions(v)})
	if err != nil {
		return nil, err
	}
	checkAtomicOTrunc(srv)
	return srv, nil
}

// mountOptions returns the FUSE mount options for v. Split out of Mount so
// the options can be asserted without a mount.
func mountOptions(v *Volume) fuse.MountOptions {
	return fuse.MountOptions{
		AllowOther:        true,
		FsName:            v.Cfg.ObjectName,
		Name:              "roommate",
		EnableLocks:       true,
		ExtraCapabilities: fuse.CAP_ATOMIC_O_TRUNC,
		DirectMount:       true,
	}
}
```

- [ ] **Step 4: Update the `DetachStale` comment** in `pkg/mounts/detach.go`. Replace the comment block inside the `else if` branch (the one beginning `// The raw syscall failed. Fall back to the same tool…`) with:

```go
		// The raw syscall failed. Fall back to fusermount3 in case the
		// permission or namespace context of a plain umount2 call differs
		// from what its setuid-root helper can do. The driver image
		// (distroless, see .ko.yaml) does not ship fusermount3, so in
		// production this fallback fails with an exec error — but only
		// after umount2 has already failed, which it doesn't as root.
```

- [ ] **Step 5: Run the package tests, vet, and lint**

Run: `go test ./pkg/objectfs/ ./pkg/mounts/ && go vet ./... && make fmt-check && golangci-lint run ./pkg/...`
Expected: PASS (FUSE tests SKIP locally without `/dev/fuse`; they run in CI). If `golangci-lint` is not on PATH, note it in the report; CI lints.

- [ ] **Step 6: Commit**

```bash
git add pkg/objectfs/mount.go pkg/objectfs/mount_test.go pkg/mounts/detach.go
git commit -m "objectfs: mount with go-fuse DirectMount

Call mount(2)/umount(2) directly so the driver image needs no
fusermount3. Not Strict: unprivileged FUSE tests fall back to it.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DQSHtHYvbSbnfaPCtcYTJ7"
```

---

### Task 2: ko build replaces the Dockerfile (Makefile + CI image job)

**Files:**
- Create: `.ko.yaml`
- Delete: `Dockerfile`, `.dockerignore`
- Modify: `Makefile` (remove `IMAGE`/`TAG`/`docker`; add `VERSION`, `ko`, `ko-local`)
- Modify: `.github/workflows/ci.yml` (the `image` job only)

**Interfaces:**
- Produces: `make ko` (both platforms, no push), `make ko-local` (host arch into local Docker daemon, prints ref). `VERSION` env var consumed by `.ko.yaml`. Tasks 3 and 4 call `ko` directly with the same `.ko.yaml`.

- [ ] **Step 1: Create `.ko.yaml`**

```yaml
# ko builds the driver image: a static Go binary on distroless, for both
# architectures, with no Dockerfile. The base has no shell and no
# fusermount3 — the driver mounts via go-fuse DirectMount
# (pkg/objectfs/mount.go) and the DaemonSet's liveness check is the
# livenessprobe sidecar, so neither is needed. It runs as root, which
# /dev/fuse and mount(2) require; do not switch to the :nonroot variant.
#
# VERSION must be set in the environment for every ko invocation.
defaultBaseImage: gcr.io/distroless/static-debian12
defaultPlatforms:
  - linux/amd64
  - linux/arm64
builds:
  - id: roommate-node
    main: ./cmd/node
    env:
      - CGO_ENABLED=0
    flags:
      - -trimpath
    ldflags:
      - -s -w -X main.version={{.Env.VERSION}}
```

- [ ] **Step 2: Edit the Makefile.** Delete these lines:

```make
IMAGE   ?= ghcr.io/middlendian/roommate-csi
TAG     ?= dev

docker:
	docker build --build-arg VERSION=$(TAG) -t $(IMAGE):$(TAG) .
```

Replace `docker` with `ko ko-local` in the `.PHONY` line. Add near the top, after `GOFILES`:

```make
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
export VERSION
```

Add where `docker` was:

```make
IMAGE_REPO ?= ghcr.io/middlendian/roommate-csi

# Build the image for every platform in .ko.yaml and discard it: a "does it
# build" check that needs no registry and no Docker daemon.
ko:
	KO_DOCKER_REPO=$(IMAGE_REPO) ko build --bare --push=false ./cmd/node

# Build for the host architecture and load it into the local Docker daemon.
ko-local:
	KO_DOCKER_REPO=ko.local ko build --bare --local --platform=linux/$$(go env GOARCH) ./cmd/node
```

- [ ] **Step 3: Delete the Dockerfile and .dockerignore**

Run: `git rm Dockerfile .dockerignore`

- [ ] **Step 4: Verify `make ko` builds both platforms** (run in background; ~12 min on this arm64 host)

Run: `PATH=$HOME/go/bin:$PATH make ko 2>&1 | tail -5`
Expected: log lines `Building github.com/middlendian/roommate-csi/cmd/node for linux/amd64` and `... for linux/arm64/v8` (or `linux/arm64`), exit 0, no Docker/registry error. If ko refuses because `--push=false` without `--tarball`/`--local` still wants a publisher, change the target to add `--tarball=/dev/null` and re-run; record which form worked in the commit message.

Also verify `VERSION` is actually stamped. Run: `VERSION=v9.9.9 go build -ldflags "-X main.version=$VERSION" -o /tmp/claude-rn ./cmd/node && grep -c v9.9.9 /tmp/claude-rn` — expected ≥1 (sanity-check that `main.version` is the right symbol; it is declared in `cmd/node/main.go`).

- [ ] **Step 5: Replace the CI `image` job** in `.github/workflows/ci.yml` (leave the `check` job untouched):

```yaml
  image:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-go@v6
        with:
          go-version: '1.25'
      - uses: ko-build/setup-ko@v0.10
        with:
          version: v0.19.1
      - name: ko build linux/amd64 + linux/arm64
        env:
          VERSION: ci
        run: make ko
      - name: smoke test -h
        # The image is distroless: no shell and, by design, no fusermount3
        # (the driver uses go-fuse DirectMount). -h is the whole smoke test.
        env:
          VERSION: ci
        run: |
          ref=$(KO_DOCKER_REPO=ko.local ko build --bare --local --platform=linux/amd64 ./cmd/node)
          docker run --rm "$ref" -h
```

- [ ] **Step 6: Lint the workflow**

Run: `go install github.com/rhysd/actionlint/cmd/actionlint@latest && $HOME/go/bin/actionlint .github/workflows/ci.yml`
Expected: no output. (If the install fails on Go version, try `@v1.7.7`.)

- [ ] **Step 7: Commit**

```bash
git add .ko.yaml Makefile .github/workflows/ci.yml
git commit -m "build: ko multi-arch image replaces the Dockerfile

.ko.yaml builds linux/amd64 + linux/arm64 on distroless static. make ko
builds both without pushing; CI's image job runs it and smoke-tests -h.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DQSHtHYvbSbnfaPCtcYTJ7"
```

---

### Task 3: livenessprobe sidecar and ko-based e2e

**Files:**
- Modify: `deploy/kustomize/base/daemonset.yaml`
- Modify: `hack/e2e.sh`
- Modify: `.github/workflows/e2e.yml`

**Interfaces:**
- Consumes: `.ko.yaml` from Task 2; `VERSION` env var.
- Produces: rendered DaemonSet with three containers: `node-driver-registrar`, `roommate`, `liveness-probe`.

Deviation from the spec, deliberately: the spec says e2e applies base "through a temporary kustomization". kustomize refuses an absolute path to a base outside the kustomization root (`new root … cannot be absolute`, checked in this session), so e2e instead renders base with `kubectl kustomize` and substitutes the image line, with a guard that fails if the substitution didn't happen.

- [ ] **Step 1: Write a failing render check** — run it now, before editing, to see it fail:

```bash
out=$(kubectl kustomize deploy/kustomize/base)
echo "$out" | grep -q 'image: registry.k8s.io/sig-storage/livenessprobe:v2.20.0' && echo "sidecar OK" || echo "sidecar MISSING"
echo "$out" | grep -q '/bin/sh' && echo "shell probe PRESENT" || echo "shell probe gone"
echo "$out" | grep -A3 'livenessProbe:' | grep -q 'path: /healthz' && echo "httpGet OK" || echo "httpGet MISSING"
```

Expected now: `sidecar MISSING`, `shell probe PRESENT`, `httpGet MISSING`.

- [ ] **Step 2: Edit `deploy/kustomize/base/daemonset.yaml`.** In the `roommate` container, replace the whole `livenessProbe:` block (the `exec: /bin/sh -c "test -S /csi/csi.sock"` one) with:

```yaml
          ports:
            - {name: healthz, containerPort: 9809, protocol: TCP}
          # The image is distroless — no shell — so liveness is the
          # livenessprobe sidecar below, which calls the driver's CSI Probe
          # RPC over the socket. That also catches a wedged gRPC server
          # whose socket file still exists, which the old exec probe didn't.
          livenessProbe:
            httpGet: {path: /healthz, port: healthz}
            initialDelaySeconds: 10
            timeoutSeconds: 3
            periodSeconds: 30
            failureThreshold: 5
```

Then add a third container after `roommate` (same indentation as the other `- name:` entries under `containers:`):

```yaml
        - name: liveness-probe
          image: registry.k8s.io/sig-storage/livenessprobe:v2.20.0
          args:
            - --csi-address=/csi/csi.sock
            # Shares the pod's network namespace with the roommate
            # container, which declares the port. hostNetwork is false, so
            # this can't collide with another driver's probe on the node.
            - --health-port=9809
          resources:
            requests: {cpu: 10m, memory: 32Mi}
          volumeMounts:
            - {name: socket-dir, mountPath: /csi}
```

- [ ] **Step 3: Re-run the render check from Step 1**

Expected: `sidecar OK`, `shell probe gone`, `httpGet OK`. Also run `kubectl kustomize deploy/kustomize/base >/dev/null` — must exit 0.

- [ ] **Step 4: Edit `hack/e2e.sh`.** Remove the `IMAGE=` line near the top. Replace these two lines:

```bash
docker build --build-arg VERSION=e2e -t "$IMAGE" "$ROOT"
kind load docker-image "$IMAGE" --name "$CLUSTER"

kubectl --context "kind-$CLUSTER" apply -k "$ROOT/deploy/kustomize/base"
```

with:

```bash
# ko builds for the host architecture and, via KO_DOCKER_REPO=kind.local,
# loads the image straight into every node of the kind cluster.
IMAGE_REF="$(cd "$ROOT" && KO_DOCKER_REPO=kind.local KIND_CLUSTER_NAME="$CLUSTER" \
  VERSION=e2e ko build --bare --platform="linux/$(go env GOARCH)" ./cmd/node)"
echo "built $IMAGE_REF"

# Point base at the image just built. kustomize won't take an absolute
# path to a base from a temporary overlay, so render and substitute —
# and fail loudly if the image line wasn't found, rather than deploying
# whatever tag base happens to reference.
MANIFESTS="$(kubectl kustomize "$ROOT/deploy/kustomize/base" \
  | sed "s|image: ghcr.io/middlendian/roommate-csi:.*|image: ${IMAGE_REF}|")"
if ! grep -qF "image: ${IMAGE_REF}" <<<"$MANIFESTS"; then
  echo "error: failed to substitute the roommate image into the rendered manifests" >&2
  exit 1
fi
kubectl --context "kind-$CLUSTER" apply -f - <<<"$MANIFESTS"
```

- [ ] **Step 5: Test the substitution logic locally** (no cluster needed):

```bash
IMAGE_REF=kind.local/node:abc123
MANIFESTS="$(kubectl kustomize deploy/kustomize/base | sed "s|image: ghcr.io/middlendian/roommate-csi:.*|image: ${IMAGE_REF}|")"
grep -n 'image:' <<<"$MANIFESTS"
```

Expected: three image lines — registrar, `kind.local/node:abc123`, livenessprobe. Also `bash -n hack/e2e.sh` exits 0.

- [ ] **Step 6: Edit `.github/workflows/e2e.yml`** — add after the `helm/kind-action` step:

```yaml
      - uses: ko-build/setup-ko@v0.10
        with:
          version: v0.19.1
```

Run `$HOME/go/bin/actionlint .github/workflows/e2e.yml` — expected no output.

- [ ] **Step 7: Commit**

```bash
git add deploy/kustomize/base/daemonset.yaml hack/e2e.sh .github/workflows/e2e.yml
git commit -m "deploy: livenessprobe sidecar; e2e builds the image with ko

The distroless image has no shell for the exec liveness probe. The CSI
livenessprobe sidecar calls Probe over the socket instead. e2e loads the
ko-built image straight into kind.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DQSHtHYvbSbnfaPCtcYTJ7"
```

- [ ] **Step 8: Note for the controller** — e2e can't run in this pod. It runs on the PR (e2e.yml triggers on `pull_request`). The controller must push and check the `e2e` and `ci` runs after this task (see Task 5, Step 6).

---

### Task 4: CHANGELOG tooling and release workflows

**Files:**
- Create: `hack/changelog.sh`
- Create: `hack/changelog_test.sh`
- Create: `.github/workflows/cut-release.yml`
- Create: `.github/workflows/tag-and-release.yml`
- Create: `.github/workflows/release.yml`
- Modify: `Makefile` (add `changelog-test`)
- Modify: `.github/workflows/ci.yml` (run `make changelog-test` in `check`)

**Interfaces:**
- Produces:
  - `hack/changelog.sh promote <vX.Y.Z> <YYYY-MM-DD> <repo-url> [file]` — edits file in place, prints previous tag (empty on first release) to stdout, exits non-zero on: bad version, missing `## [Unreleased]`, empty Unreleased, section already present, no `[Unreleased]:` link line.
  - `hack/changelog.sh notes <vX.Y.Z> [file]` — prints that version's section body to stdout; exits non-zero if empty/missing.
- Consumes: `.ko.yaml` (Task 2).

- [ ] **Step 1: Write the failing test** — `hack/changelog_test.sh` (mark executable):

```bash
#!/usr/bin/env bash
# Tests for hack/changelog.sh. Run: make changelog-test
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CL="$ROOT/hack/changelog.sh"
URL="https://github.com/middlendian/roommate-csi"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
fail=0

check() { # name, expected-file, actual-file
  if diff -u "$2" "$3"; then echo "ok   $1"; else echo "FAIL $1"; fail=1; fi
}
expect_error() { # name, command...
  local name="$1"; shift
  if "$@" >/dev/null 2>&1; then echo "FAIL $name (expected non-zero exit)"; fail=1; else echo "ok   $name"; fi
}

# --- first release: [Unreleased] links to commits/main, no previous tag ---
cat >"$TMP/first.md" <<'EOF'
# Changelog

## [Unreleased]

### Added

- Thing one.

[Unreleased]: https://github.com/middlendian/roommate-csi/commits/main
EOF
prev=$("$CL" promote v0.1.0 2026-09-23 "$URL" "$TMP/first.md")
cat >"$TMP/first.want" <<'EOF'
# Changelog

## [Unreleased]

## [0.1.0] - 2026-09-23

### Added

- Thing one.

[Unreleased]: https://github.com/middlendian/roommate-csi/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/middlendian/roommate-csi/releases/tag/v0.1.0
EOF
check "first release" "$TMP/first.want" "$TMP/first.md"
[[ -z "$prev" ]] && echo "ok   first release prints no previous tag" || { echo "FAIL first release printed prev=$prev"; fail=1; }

# --- notes for the version just promoted ---
"$CL" notes v0.1.0 "$TMP/first.md" >"$TMP/notes.got"
printf '\n### Added\n\n- Thing one.\n\n' >"$TMP/notes.want"
check "notes" "$TMP/notes.want" "$TMP/notes.got"

# --- second release: compare link present, older sections kept ---
cp "$TMP/first.md" "$TMP/second.md"
sed -i 's|^## \[Unreleased\]$|## [Unreleased]\n\n### Fixed\n\n- Thing two.|' "$TMP/second.md"
prev=$("$CL" promote v0.2.0-rc.1 2026-10-01 "$URL" "$TMP/second.md")
[[ "$prev" == "v0.1.0" ]] && echo "ok   second release prints previous tag" || { echo "FAIL second release prev=$prev"; fail=1; }
grep -qx '## \[0.2.0-rc.1\] - 2026-10-01' "$TMP/second.md" && echo "ok   prerelease heading" || { echo "FAIL prerelease heading"; fail=1; }
grep -qx "\[0.2.0-rc.1\]: $URL/compare/v0.1.0...v0.2.0-rc.1" "$TMP/second.md" && echo "ok   compare link" || { echo "FAIL compare link"; fail=1; }
grep -qx "\[Unreleased\]: $URL/compare/v0.2.0-rc.1...HEAD" "$TMP/second.md" && echo "ok   unreleased link" || { echo "FAIL unreleased link"; fail=1; }
"$CL" notes v0.2.0-rc.1 "$TMP/second.md" | grep -q 'Thing two' && echo "ok   notes stop at next section" || { echo "FAIL notes 0.2.0-rc.1"; fail=1; }
"$CL" notes v0.2.0-rc.1 "$TMP/second.md" | grep -q 'Thing one' && { echo "FAIL notes leaked older section"; fail=1; } || echo "ok   notes exclude older section"

# --- error cases ---
cp "$TMP/first.md" "$TMP/dup.md"
expect_error "duplicate version" "$CL" promote v0.1.0 2026-09-24 "$URL" "$TMP/dup.md"
expect_error "empty unreleased" "$CL" promote v0.1.1 2026-09-24 "$URL" "$TMP/dup.md"
expect_error "bad version" "$CL" promote 0.1.1 2026-09-24 "$URL" "$TMP/first.md"
expect_error "notes missing version" "$CL" notes v9.9.9 "$TMP/first.md"
printf '# Changelog\n\n## [Unreleased]\n\n- x\n' >"$TMP/nolink.md"
expect_error "no unreleased link" "$CL" promote v0.1.0 2026-09-24 "$URL" "$TMP/nolink.md"

# --- the real CHANGELOG must be promotable as a first release ---
cp "$ROOT/CHANGELOG.md" "$TMP/real.md"
"$CL" promote v0.1.0 2026-09-23 "$URL" "$TMP/real.md" >/dev/null && "$CL" notes v0.1.0 "$TMP/real.md" | grep -q '### Added' \
  && echo "ok   real CHANGELOG" || { echo "FAIL real CHANGELOG"; fail=1; }

exit "$fail"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `chmod +x hack/changelog_test.sh && hack/changelog_test.sh`
Expected: fails immediately — `hack/changelog.sh: No such file or directory`.

- [ ] **Step 3: Implement `hack/changelog.sh`** (mark executable):

```bash
#!/usr/bin/env bash
# CHANGELOG.md operations for the release workflows (.github/workflows/
# cut-release.yml and release.yml), kept here so they're testable:
# hack/changelog_test.sh. Requires GNU sed and awk.
#
#   changelog.sh promote <vX.Y.Z> <YYYY-MM-DD> <repo-url> [file]
#       Move [Unreleased] entries under a new "## [X.Y.Z] - date" heading
#       and rewrite the link references. Prints the previous release tag,
#       or nothing on the first release.
#   changelog.sh notes <vX.Y.Z> [file]
#       Print the body of that version's section.
set -euo pipefail

VERSION_RE='^v[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.-]+)?$'

die() { echo "changelog.sh: $*" >&2; exit 1; }

check_version() {
  [[ "$1" =~ $VERSION_RE ]] || die "version '$1' does not look like vX.Y.Z[-prerelease]"
}

promote() {
  local version="$1" today="$2" repo_url="$3" file="${4:-CHANGELOG.md}"
  check_version "$version"
  local semver="${version#v}"

  grep -qx '## \[Unreleased\]' "$file" || die "no '## [Unreleased]' heading in $file"
  grep -qE "^## \[${semver//./\\.}\] - " "$file" && die "$file already has a ## [$semver] section"
  grep -q '^\[Unreleased\]: ' "$file" || die "no '[Unreleased]: ' link reference in $file"

  local content
  content=$(awk '/^## \[Unreleased\]$/{found=1; next} found && /^## \[/{exit} found && /^\[.+\]:/{exit} found && /[^[:space:]]/{print; exit}' "$file")
  [[ -n "$content" ]] || die "## [Unreleased] in $file is empty; add entries before cutting a release"

  # Previous tag comes from the [Unreleased] compare link. Before the first
  # release that link is .../commits/main, and there is no previous tag.
  local prev
  prev=$(sed -n 's|^\[Unreleased\]: .*/compare/\(.*\)\.\.\.HEAD$|\1|p' "$file")

  sed -i "0,/^## \[Unreleased\]$/{s|^## \[Unreleased\]$|## [Unreleased]\n\n## [$semver] - $today|}" "$file"
  sed -i "s|^\[Unreleased\]: .*$|[Unreleased]: $repo_url/compare/$version...HEAD|" "$file"
  local link="$repo_url/releases/tag/$version"
  [[ -n "$prev" ]] && link="$repo_url/compare/$prev...$version"
  sed -i "/^\[Unreleased\]: /a[$semver]: $link" "$file"

  echo "$prev"
}

notes() {
  local version="$1" file="${2:-CHANGELOG.md}"
  check_version "$version"
  local out
  out=$(awk -v v="${version#v}" '
    /^## \[/ {
      if (printing) exit
      if (index($0, "## [" v "]") == 1) { printing = 1; next }
    }
    printing && /^\[.+\]:/ { exit }
    printing { print }
  ' "$file"; echo x)
  out="${out%x}"
  [[ -n "${out//[[:space:]]/}" ]] || die "no '## [${version#v}]' section with content in $file"
  printf '%s' "$out"
}

cmd="${1:-}"; shift || true
case "$cmd" in
  promote) [[ $# -ge 3 ]] || die "usage: promote <vX.Y.Z> <YYYY-MM-DD> <repo-url> [file]"; promote "$@" ;;
  notes)   [[ $# -ge 1 ]] || die "usage: notes <vX.Y.Z> [file]"; notes "$@" ;;
  *) die "usage: changelog.sh {promote|notes} ..." ;;
esac
```

(`index(...) == 1` instead of fileblock's regex match so dots in the version are literal; the `echo x` trick preserves trailing newlines through command substitution.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `chmod +x hack/changelog.sh && hack/changelog_test.sh`
Expected: every line `ok`, exit 0. If `notes` differs from `notes.want` only in trailing blank lines, fix `notes.want` to match what the awk genuinely emits (the section body up to the next `## [` or link line) — do not strip content.

- [ ] **Step 5: Wire the test into make and CI.** Makefile: add `changelog-test` to `.PHONY` and:

```make
changelog-test:
	hack/changelog_test.sh
```

In `.github/workflows/ci.yml` `check` job, change the step `make fmt-check vet tidy-check cover` — both its `name:` and `run:` — to `make fmt-check vet tidy-check cover changelog-test`.

- [ ] **Step 6: Create `.github/workflows/cut-release.yml`**

```yaml
name: Cut release

# Creates a release/vX.Y.Z branch from main with the CHANGELOG promoted and
# the kustomization newTag bumped, then opens a PR. Merging the PR triggers
# tag-and-release.yml, which creates the tag and runs the publish pipeline.
on:
  workflow_dispatch:
    inputs:
      version:
        description: "Release version (e.g. v0.1.0)"
        type: string
        required: true

permissions:
  contents: write       # push branch
  pull-requests: write  # open PR

jobs:
  cut:
    name: Cut ${{ inputs.version }}
    runs-on: ubuntu-latest
    env:
      VERSION: ${{ inputs.version }}
      BRANCH: release/${{ inputs.version }}
      KUSTOMIZATION: deploy/kustomize/base/kustomization.yaml
    steps:
      - uses: actions/checkout@v6
        with:
          ref: main
          fetch-depth: 0

      - name: Check tag and branch don't exist
        run: |
          set -euo pipefail
          if git ls-remote --exit-code --tags origin "$VERSION" >/dev/null 2>&1; then
            echo "::error::tag $VERSION already exists on origin"; exit 1
          fi
          if git ls-remote --exit-code --heads origin "$BRANCH" >/dev/null 2>&1; then
            echo "::error::branch $BRANCH already exists on origin"; exit 1
          fi

      - name: Promote CHANGELOG.md
        # Validates the version and the [Unreleased] section too; see
        # hack/changelog.sh. Prints the previous tag, empty on the first release.
        run: |
          set -euo pipefail
          prev=$(hack/changelog.sh promote "$VERSION" "$(date +%Y-%m-%d)" "https://github.com/$GITHUB_REPOSITORY")
          echo "PREV_TAG=$prev" >> "$GITHUB_ENV"

      - name: Bump kustomization newTag
        run: |
          set -euo pipefail
          current=$(awk '/^[[:space:]]+newTag:/ {print $2; exit}' "$KUSTOMIZATION")
          if [ "$current" = "$VERSION" ]; then
            echo "::error::$KUSTOMIZATION already has newTag: $VERSION"; exit 1
          fi
          echo "OLD_TAG=$current" >> "$GITHUB_ENV"
          sed -i -E "s|^([[:space:]]+)newTag: .*$|\1newTag: $VERSION|" "$KUSTOMIZATION"

      - name: Commit and push release branch
        run: |
          set -euo pipefail
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
          git checkout -b "$BRANCH"
          git add CHANGELOG.md "$KUSTOMIZATION"
          git commit -m "release: $VERSION"
          git push -u origin "$BRANCH"

      - name: Open pull request
        env:
          GH_TOKEN: ${{ github.token }}
        run: |
          set -euo pipefail
          gh pr create --base main --head "$BRANCH" --title "release: $VERSION" --body "$(cat <<EOF
          ## Summary

          Cuts release **$VERSION**.

          - Promotes \`## [Unreleased]\` → \`## [${VERSION#v}] - $(date +%Y-%m-%d)\` in \`CHANGELOG.md\`.
          - Bumps \`$KUSTOMIZATION\` \`newTag\` from \`$OLD_TAG\` to \`$VERSION\`.
          - Previous release: ${PREV_TAG:-none (first release)}.

          ## Merge instructions

          Squash-merge or merge-commit both work. \`tag-and-release.yml\` reads
          the version from the PR's head branch name (\`$BRANCH\`), not the
          commit message.

          After merge:
          - Multi-arch image (linux/amd64, linux/arm64) at
            \`ghcr.io/middlendian/roommate-csi:$VERSION\` (and \`:latest\` for
            non-prereleases) is published.
          - A GitHub release is created with the CHANGELOG section as its body.
          EOF
          )"
```

- [ ] **Step 7: Create `.github/workflows/tag-and-release.yml`**

```yaml
name: Tag and release

# Triggered when a release PR (head branch release/vX.Y.Z) is merged into
# main. The version comes from the head branch name, so squash and
# merge-commit modes both work. Creates the vX.Y.Z tag at the merge commit,
# then calls release.yml. workflow_dispatch re-runs a release by hand (e.g.
# after a flake); every step is safe to repeat.
on:
  pull_request:
    types: [closed]
    branches: [main]
  workflow_dispatch:
    inputs:
      version:
        description: "Release version (e.g. v0.1.1) — bypasses PR detection"
        type: string
        required: true

permissions:
  contents: write   # push tag, create GitHub release
  packages: write   # push images to ghcr.io

jobs:
  detect:
    name: Detect release PR
    runs-on: ubuntu-latest
    if: >-
      github.event_name == 'workflow_dispatch' ||
      (github.event.pull_request.merged == true &&
       startsWith(github.event.pull_request.head.ref, 'release/v'))
    outputs:
      version: ${{ steps.detect.outputs.version }}
      sha: ${{ steps.detect.outputs.sha }}
    steps:
      - id: detect
        env:
          DISPATCH_VERSION: ${{ inputs.version }}
          PR_HEAD_REF: ${{ github.event.pull_request.head.ref }}
          PR_MERGE_SHA: ${{ github.event.pull_request.merge_commit_sha }}
          RUN_SHA: ${{ github.sha }}
        run: |
          set -euo pipefail
          re='^v[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.-]+)?$'
          if [ -n "$DISPATCH_VERSION" ]; then
            version="$DISPATCH_VERSION"; sha="$RUN_SHA"
            echo "::notice::workflow_dispatch override: version=$version"
          else
            version="${PR_HEAD_REF#release/}"; sha="$PR_MERGE_SHA"
            echo "::notice::Detected release PR: head=$PR_HEAD_REF version=$version"
          fi
          if [[ ! "$version" =~ $re ]]; then
            echo "::error::version $version does not look like vX.Y.Z[-prerelease]"; exit 1
          fi
          echo "version=$version" >> "$GITHUB_OUTPUT"
          echo "sha=$sha" >> "$GITHUB_OUTPUT"

  tag:
    name: Create ${{ needs.detect.outputs.version }} tag
    needs: detect
    runs-on: ubuntu-latest
    env:
      VERSION: ${{ needs.detect.outputs.version }}
    steps:
      - uses: actions/checkout@v6
        with:
          ref: ${{ needs.detect.outputs.sha }}
          fetch-depth: 0
          fetch-tags: true
      - name: Verify kustomization newTag matches release version
        run: |
          set -euo pipefail
          k=deploy/kustomize/base/kustomization.yaml
          tag=$(awk '/^[[:space:]]+newTag:/ {print $2; exit}' "$k")
          if [ "$tag" != "$VERSION" ]; then
            echo "::error::$k newTag ($tag) does not match release version ($VERSION)"; exit 1
          fi
      - name: Create and push tag
        run: |
          set -euo pipefail
          if git rev-parse "$VERSION" >/dev/null 2>&1 || git ls-remote --exit-code --tags origin "$VERSION" >/dev/null 2>&1; then
            echo "::notice::tag $VERSION already exists; skipping create"; exit 0
          fi
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
          git tag -a "$VERSION" -m "$VERSION"
          git push origin "$VERSION"

  release:
    name: Build and publish ${{ needs.detect.outputs.version }}
    needs: [detect, tag]
    uses: ./.github/workflows/release.yml
    secrets: inherit
    with:
      version: ${{ needs.detect.outputs.version }}
```

- [ ] **Step 8: Create `.github/workflows/release.yml`**

```yaml
name: Release

# Reusable workflow, called by tag-and-release.yml after it creates the
# vX.Y.Z tag. Not triggered by tag pushes directly.
on:
  workflow_call:
    inputs:
      version:
        description: "Release version (e.g. v0.1.0)"
        type: string
        required: true

permissions:
  contents: write   # create GitHub release
  packages: write   # push images to ghcr.io

env:
  GO_VERSION: "1.25"
  IMAGE: ghcr.io/middlendian/roommate-csi

jobs:
  image:
    name: Image (linux/amd64 + linux/arm64)
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      TAG: ${{ inputs.version }}
    steps:
      - uses: actions/checkout@v6
        with:
          ref: ${{ inputs.version }}
      - uses: actions/setup-go@v6
        with:
          go-version: ${{ env.GO_VERSION }}
      - uses: ko-build/setup-ko@v0.10
        with:
          version: v0.19.1
      - name: Log in to GHCR
        env:
          TOKEN: ${{ github.token }}
          ACTOR: ${{ github.actor }}
        run: echo "$TOKEN" | ko login ghcr.io --username "$ACTOR" --password-stdin
      - name: Build and push
        # ko cross-compiles both platforms and pushes them plus the index in
        # one step — no per-arch runners, no QEMU, no manifest-merge job.
        env:
          KO_DOCKER_REPO: ${{ env.IMAGE }}
          VERSION: ${{ inputs.version }}
          REVISION: ${{ github.sha }}
        run: |
          set -euo pipefail
          tags="$TAG"
          # :latest only for non-prereleases (SemVer prerelease has a '-').
          [[ "$TAG" == *-* ]] || tags="$tags,latest"
          ko build --bare --tags="$tags" \
            --image-label=org.opencontainers.image.title=roommate-csi \
            --image-label="org.opencontainers.image.description=Kubernetes CSI driver mounting a Secret or ConfigMap as a writable, cross-node shared directory" \
            --image-label=org.opencontainers.image.url=https://github.com/middlendian/roommate-csi \
            --image-label=org.opencontainers.image.source=https://github.com/middlendian/roommate-csi \
            --image-label="org.opencontainers.image.version=$TAG" \
            --image-label="org.opencontainers.image.revision=$REVISION" \
            --image-label=org.opencontainers.image.licenses=GPL-3.0-only \
            ./cmd/node

  release:
    name: GitHub release
    needs: image   # never publish a release whose image failed to push
    runs-on: ubuntu-latest
    timeout-minutes: 10
    env:
      VERSION: ${{ inputs.version }}
      GH_TOKEN: ${{ github.token }}
      NOTES: ${{ runner.temp }}/release-notes.md
    steps:
      - uses: actions/checkout@v6
        with:
          ref: ${{ inputs.version }}
      - name: Extract release notes from CHANGELOG.md
        run: |
          set -euo pipefail
          hack/changelog.sh notes "$VERSION" > "$NOTES"
          echo "::group::Release notes for $VERSION"; cat "$NOTES"; echo "::endgroup::"
      - name: Create or update GitHub release
        # Editing when it already exists makes a workflow_dispatch re-run of
        # a half-finished release succeed instead of failing here.
        run: |
          set -euo pipefail
          if gh release view "$VERSION" >/dev/null 2>&1; then
            gh release edit "$VERSION" --notes-file "$NOTES"
          else
            prerelease=()
            [[ "$VERSION" == *-* ]] && prerelease=(--prerelease)
            gh release create "$VERSION" --title "$VERSION" --verify-tag \
              --notes-file "$NOTES" "${prerelease[@]}"
          fi
      - name: Verify the published release body is not blank
        # fileblock-csi shipped 13 releases with blank bodies because nothing
        # checked what was actually published. Check it.
        run: |
          set -euo pipefail
          body=$(gh release view "$VERSION" --json body --jq .body)
          if [ -z "${body//[[:space:]]/}" ]; then
            echo "::error::GitHub release $VERSION has a blank body"; exit 1
          fi
```

Note on the license label: the repo's LICENSE is GPLv3 and README says "GPLv3" with no "or later" grant, so `GPL-3.0-only` — not fileblock's `GPL-3.0-or-later`.

- [ ] **Step 9: Lint all workflows and scripts**

Run: `$HOME/go/bin/actionlint && bash -n hack/changelog.sh hack/changelog_test.sh && make changelog-test`
Expected: actionlint prints nothing; tests all `ok`. (If `shellcheck` is installed, actionlint also checks `run:` blocks; fix anything it reports.)

- [ ] **Step 10: Commit**

```bash
git add hack/changelog.sh hack/changelog_test.sh Makefile .github/workflows/ci.yml \
  .github/workflows/cut-release.yml .github/workflows/tag-and-release.yml .github/workflows/release.yml
git commit -m "release: cut-release, tag-and-release, and release workflows

Modeled on fileblock-csi's pipeline. ko pushes the multi-arch image in one
job; gh creates the GitHub release from the CHANGELOG section and the job
fails if the published body is blank. CHANGELOG handling lives in
hack/changelog.sh with tests, including the first-release case.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DQSHtHYvbSbnfaPCtcYTJ7"
```

---

### Task 5: Docs, CHANGELOG, and CI verification

**Files:**
- Modify: `README.md` (Install section, ~line 60)
- Modify: `CLAUDE.md` (repo map, build/test commands, invariants, pre-merge checklist)
- Modify: `CHANGELOG.md` (`[Unreleased]`)

- [ ] **Step 1: README.** After the `kubectl apply -k deploy/kustomize/base` code block in `## Install`, add:

```markdown
Images are published for `linux/amd64` and `linux/arm64` at
`ghcr.io/middlendian/roommate-csi:vX.Y.Z` (and `:latest` for the newest
non-prerelease). On `main`, `deploy/kustomize/base` points at the most
recent release's tag; check out a release tag to install exactly that
version.
```

- [ ] **Step 2: CLAUDE.md.**
  - Repo map: add `.ko.yaml` (image build: distroless, amd64+arm64), `hack/changelog.sh` (+ `_test.sh`) (CHANGELOG promote/notes for release workflows), and under `.github/workflows/` the three release workflows (`cut-release` → `tag-and-release` → `release`).
  - Build/test commands: replace `make docker      # docker build (IMAGE/TAG overridable)` with
    ```
    make ko          # ko build linux/amd64 + linux/arm64, no push
    make ko-local    # ko build for the host arch into the local Docker daemon
    make changelog-test  # tests for hack/changelog.sh (CI gate)
    ```
    and in "What each target needs", add: `make ko` / `make ko-local` — `ko` on PATH (v0.19.1), `VERSION` defaults from `git describe`; `make e2e` now needs `ko` instead of `docker build` (Docker is still needed for kind).
  - Invariants: add a bullet —
    **The image has no shell and no `fusermount3`.** It is distroless (`.ko.yaml`). go-fuse mounts with `DirectMount` (`pkg/objectfs/mount.go`), which works because the driver runs as root. Keep it `DirectMount`, never `DirectMountStrict`: the unprivileged FUSE unit tests depend on the `fusermount3` fallback. Don't add exec probes or anything else that shells out inside the driver container; liveness is the `livenessprobe` sidecar.
  - Add a short "Releasing" section: run the **Cut release** workflow with `vX.Y.Z` (needs a non-empty `## [Unreleased]`), review and merge the `release/vX.Y.Z` PR; `tag-and-release` tags, pushes the image, and creates the GitHub release. Re-run by dispatching **Tag and release** with the version.

- [ ] **Step 3: CHANGELOG.md.** In `## [Unreleased]` → `### Added`, replace the line `- Container image build (\`Dockerfile\`) and \`make docker\` target.` with:

```markdown
- Container image for `linux/amd64` and `linux/arm64`, built with ko on a
  distroless static base (`.ko.yaml`, `make ko`). The driver mounts FUSE
  with go-fuse `DirectMount`, so the image needs no `fusermount3`.
- `livenessprobe` sidecar in the DaemonSet, calling the driver's CSI
  `Probe` RPC; replaces a shell-based exec probe.
- Release pipeline: **Cut release** opens a `release/vX.Y.Z` PR; merging
  it tags, pushes `ghcr.io/middlendian/roommate-csi:vX.Y.Z`, and creates
  a GitHub release from this file.
```

- [ ] **Step 4: Verify** — `make changelog-test` (the real-CHANGELOG case must still pass), `go build ./... && make vet && make fmt-check`.

- [ ] **Step 5: Commit**

```bash
git add README.md CLAUDE.md CHANGELOG.md
git commit -m "docs: ko image, livenessprobe sidecar, and release process

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DQSHtHYvbSbnfaPCtcYTJ7"
```

- [ ] **Step 6: Push and verify CI** (controller, not a subagent): `git push`, then watch `gh pr checks 2 --watch`. Required green: `ci / check`, `ci / image`, `e2e / e2e`. e2e is the real proof for DirectMount, `DetachStale`, and the livenessprobe sidecar under a real kubelet — `TestRefreshRaceProducesExactlyOneRefresh` in particular. If e2e fails, pull logs with `gh run view --log-failed` and debug with superpowers:systematic-debugging before changing anything.
