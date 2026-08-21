# roommate-csi v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a CSI driver that mounts a Kubernetes `Secret` or `ConfigMap`
as a writable, cross-node shared directory, with `flock` backed by a
`coordination.k8s.io` `Lease`.

**Architecture:** One binary, one privileged DaemonSet, no controller. Inline
ephemeral volumes only, so `NodePublishVolume` is the sole substantive RPC.
Every Kubernetes API call is authenticated as the *consuming pod's*
ServiceAccount using a token kubelet mints via `CSIDriver.tokenRequests` — the
driver's own ServiceAccount holds zero API permissions. A go-fuse server per
(pod, volume) serves a whole-object snapshot pinned per open file handle.

**Tech Stack:** Go 1.25, `github.com/hanwen/go-fuse/v2`, `k8s.io/client-go`,
`github.com/container-storage-interface/spec`, gRPC. Tests use
`k8s.io/client-go/kubernetes/fake`, `sigs.k8s.io/controller-runtime/pkg/envtest`,
and kind.

**Spec:** `docs/superpowers/specs/2026-08-20-roommate-csi-design.md`

## Global Constraints

Every task's requirements implicitly include this section.

- **Driver name:** `roommate.csi`. **Module:** `github.com/middlendian/roommate-csi`.
- **Go 1.25.** Linux-only; no build tags needed since nothing builds elsewhere in CI.
- **License GPLv3.** Do not modify, move, or replace `LICENSE`.
- **The driver's ServiceAccount gets ZERO Kubernetes RBAC.** No `secrets`,
  no `configmaps`, no `leases`, no `subjectaccessreviews`. Any task that
  adds an RBAC rule for the driver's own ServiceAccount is wrong.
- **Every API call uses the pod's token**, taken from volume context key
  `csi.storage.k8s.io/serviceAccount.tokens`.
- **Watches MUST set `fieldSelector=metadata.name=<objectName>`.** RBAC
  `resourceNames` rejects an unscoped watch with 403. Never use a shared
  informer — a reflector LISTs first, which needs the `list` verb we do not
  grant.
- **Quorum GET means `metav1.GetOptions{}` with `ResourceVersion` left
  empty.** Never pass `ResourceVersion: "0"`; that reads the API server's
  watch cache and silently breaks the read-after-write guarantee.
- **Writes are per-key merge-patches only.** No read-modify-write, no CAS on
  the object, no retry loop, no merge logic.
- **`NodePublishVolume` must never return an error for a target that has
  already published successfully** (kubernetes/kubernetes#121271 deletes the
  mount point on a republish error). Surface failures as `EACCES` from the
  FUSE layer instead.
- **Object size ceiling: 1 MiB** (`1 << 20`), surfaced as `ENOSPC`.
- **Never name a specific downstream consumer** in code, comments, docs, or
  test fixtures. Use `oauth-credentials` as the example object name.
- **Commit trailers** on every commit:
  ```
  Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
  ```

## File Structure

| Path | Responsibility |
|---|---|
| `cmd/node/main.go` | Flag parsing, gRPC server bootstrap. No logic. |
| `pkg/driver/identity.go` | CSI Identity service. |
| `pkg/driver/node.go` | CSI Node service: publish/unpublish/republish. |
| `pkg/driver/server.go` | gRPC listener on a unix socket. |
| `pkg/objectfs/keys.go` | Object key ⇄ filename validation. Pure functions. |
| `pkg/objectfs/snapshot.go` | Immutable whole-object snapshot type. |
| `pkg/objectfs/store.go` | `Store` interface; shared decode helpers. |
| `pkg/objectfs/store_secret.go` | `Secret` implementation of `Store`. |
| `pkg/objectfs/store_configmap.go` | `ConfigMap` implementation of `Store`. |
| `pkg/objectfs/cache.go` | Watch loop, staleness bound, snapshot pointer. |
| `pkg/objectfs/commit.go` | Size check, lease fencing, patch dispatch. |
| `pkg/objectfs/lease.go` | Lease acquire / renew / release. |
| `pkg/objectfs/fs.go` | go-fuse `Root` and `File` nodes. |
| `pkg/objectfs/handle.go` | Per-open file handle: pinned snapshot, write buffer, lock state. |
| `pkg/objectfs/volume.go` | Wires cache + committer + lease into one mountable unit. |
| `pkg/mounts/registry.go` | `target_path` → live mount; `published.json` state. |
| `pkg/podtoken/token.go` | Parse the pod token from volume context; build a `rest.Config`. |
| `deploy/kustomize/base/` | CSIDriver, DaemonSet, `roommate-user` ClusterRole. |
| `test/e2e/` | kind-driven suite, build tag `e2e`. |

---

### Task 1: Project scaffolding and CI gates

Establishes the toolchain so every later task has a working `make check`.

**Files:**
- Create: `go.mod`, `Makefile`, `.golangci.yml`, `.gitignore`,
  `.github/workflows/ci.yml`, `pkg/objectfs/doc.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `make check` — the gate every later task runs before committing.

- [ ] **Step 1: Initialise the module and pin dependencies**

```bash
cd /workspace/middlendian/roommate-csi
go mod init github.com/middlendian/roommate-csi
go get github.com/container-storage-interface/spec@v1.12.0
go get github.com/hanwen/go-fuse/v2@latest
go get k8s.io/client-go@latest k8s.io/api@latest k8s.io/apimachinery@latest
go get google.golang.org/grpc@latest
go mod tidy
```

Then edit `go.mod` so the first lines read exactly:

```
module github.com/middlendian/roommate-csi

go 1.25.0
```

- [ ] **Step 2: Write a package doc file so the tree compiles**

Create `pkg/objectfs/doc.go`:

```go
// Package objectfs presents a Kubernetes Secret or ConfigMap as a writable
// POSIX directory over FUSE, coordinating writers with a coordination.k8s.io
// Lease.
//
// Every API call in this package is made with the consuming pod's own
// ServiceAccount token. The driver's ServiceAccount holds no permissions.
package objectfs
```

- [ ] **Step 3: Write the Makefile**

Create `Makefile`:

```make
BIN     := bin
PKGS    := ./...
GOFILES := $(shell find . -name '*.go' -not -path './vendor/*')

.PHONY: build test test-race cover vet fmt fmt-check lint tidy tidy-check check clean

build:
	go build -o $(BIN)/roommate-node ./cmd/node

test:
	go test $(PKGS)

test-race:
	go test -race $(PKGS)

cover:
	go test -race -coverprofile=cover.out $(PKGS)

vet:
	go vet $(PKGS)

fmt:
	gofmt -s -w $(GOFILES)

fmt-check:
	@out=$$(gofmt -s -l $(GOFILES)); \
	if [ -n "$$out" ]; then echo "not gofmt-clean:"; echo "$$out"; exit 1; fi

lint:
	golangci-lint run

tidy:
	go mod tidy

tidy-check:
	@cp go.mod go.mod.bak; cp go.sum go.sum.bak; \
	go mod tidy; \
	if ! diff -q go.mod go.mod.bak >/dev/null || ! diff -q go.sum go.sum.bak >/dev/null; then \
	  mv go.mod.bak go.mod; mv go.sum.bak go.sum; echo "go mod tidy needed"; exit 1; \
	fi; \
	rm -f go.mod.bak go.sum.bak

check: fmt-check vet lint tidy-check cover build

clean:
	rm -rf $(BIN) cover.out
```

- [ ] **Step 4: Write the lint config**

Create `.golangci.yml`:

```yaml
version: "2"
linters:
  enable:
    - errcheck
    - govet
    - ineffassign
    - staticcheck
    - unused
    - bodyclose
    - misspell
    - revive
issues:
  max-issues-per-linter: 0
  max-same-issues: 0
```

- [ ] **Step 5: Write .gitignore additions**

Append to the existing `.gitignore`:

```
bin/
cover.out
*.bak
```

- [ ] **Step 6: Write the CI workflow**

Create `.github/workflows/ci.yml`:

```yaml
name: ci
on:
  push:
  pull_request:

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.25'
      - uses: golangci/golangci-lint-action@v6
        with:
          version: latest
          args: --timeout=5m
      - name: make check
        run: make fmt-check vet tidy-check cover build
```

- [ ] **Step 7: Verify the gate passes**

Run: `make fmt-check vet tidy-check build`
Expected: all succeed, `bin/roommate-node` is NOT built yet (no `cmd/node`
exists). If `build` fails with "no Go files", that is expected at this
point — comment out the `build` target invocation for this run only and
confirm the rest pass.

- [ ] **Step 8: Commit**

```bash
git add go.mod go.sum Makefile .golangci.yml .gitignore .github pkg/objectfs/doc.go
git commit -m "$(cat <<'EOF'
build: project scaffolding and CI gates

Go module, Makefile, golangci-lint config, and a CI workflow running the
same targets so local and CI agree.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
EOF
)"
```

---

### Task 2: Object key ⇄ filename validation

Kubernetes constrains object keys to `[-._a-zA-Z0-9]+`. That constraint is
the filesystem's namespace rule, so it gets its own tested unit.

**Files:**
- Create: `pkg/objectfs/keys.go`
- Test: `pkg/objectfs/keys_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `func ValidKey(name string) bool` — used by `fs.go` to reject
  `create` and `rename` targets with `EINVAL`.

- [ ] **Step 1: Write the failing test**

Create `pkg/objectfs/keys_test.go`:

```go
package objectfs

import "testing"

func TestValidKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want bool
	}{
		{"simple", "config.json", true},
		{"leading dot", ".credentials.json", true},
		{"underscore", "MY_TOKEN", true},
		{"hyphen", "session-key", true},
		{"digits", "key123", true},
		{"empty", "", false},
		{"dot", ".", false},
		{"dotdot", "..", false},
		{"slash", "a/b", false},
		{"space", "my key", false},
		{"colon", "a:b", false},
		{"newline", "a\nb", false},
		{"too long", string(make([]byte, 254)), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ValidKey(tt.key); got != tt.want {
				t.Errorf("ValidKey(%q) = %v, want %v", tt.key, got, tt.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./pkg/objectfs/ -run TestValidKey -v`
Expected: FAIL — `undefined: ValidKey`

- [ ] **Step 3: Write the implementation**

Create `pkg/objectfs/keys.go`:

```go
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
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./pkg/objectfs/ -run TestValidKey -v`
Expected: PASS, all subtests.

- [ ] **Step 5: Commit**

```bash
git add pkg/objectfs/keys.go pkg/objectfs/keys_test.go
git commit -m "$(cat <<'EOF'
objectfs: object key to filename validation

The API server's data-key charset is the mount's namespace rule. Rejecting
at create time turns an opaque 422 on write into an immediate EINVAL.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
EOF
)"
```

---

### Task 3: Snapshot type

The immutable whole-object value that every read is served from. Pinning one
of these per open handle is what reproduces kubelet's atomic `..data` flip.

**Files:**
- Create: `pkg/objectfs/snapshot.go`
- Test: `pkg/objectfs/snapshot_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `type Snapshot struct { Data map[string][]byte; ResourceVersion string; FetchedAt time.Time }`
  - `func (s *Snapshot) Get(key string) ([]byte, bool)`
  - `func (s *Snapshot) Keys() []string` — sorted
  - `func (s *Snapshot) Size() int` — total value bytes
  - `func (s *Snapshot) With(set map[string][]byte, del []string) *Snapshot`
  - `const MaxObjectBytes = 1 << 20`

- [ ] **Step 1: Write the failing test**

Create `pkg/objectfs/snapshot_test.go`:

```go
package objectfs

import (
	"testing"
	"time"
)

func newSnap(data map[string][]byte) *Snapshot {
	return &Snapshot{Data: data, ResourceVersion: "42", FetchedAt: time.Unix(0, 0)}
}

func TestSnapshotGet(t *testing.T) {
	s := newSnap(map[string][]byte{"a": []byte("one")})

	got, ok := s.Get("a")
	if !ok || string(got) != "one" {
		t.Fatalf("Get(a) = %q, %v; want \"one\", true", got, ok)
	}
	if _, ok := s.Get("missing"); ok {
		t.Error("Get(missing) reported ok")
	}
}

func TestSnapshotGetIsACopy(t *testing.T) {
	s := newSnap(map[string][]byte{"a": []byte("one")})

	got, _ := s.Get("a")
	got[0] = 'X'

	again, _ := s.Get("a")
	if string(again) != "one" {
		t.Fatalf("snapshot mutated through returned slice: %q", again)
	}
}

func TestSnapshotKeysSorted(t *testing.T) {
	s := newSnap(map[string][]byte{"c": {}, "a": {}, "b": {}})

	want := []string{"a", "b", "c"}
	got := s.Keys()
	if len(got) != len(want) {
		t.Fatalf("Keys() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Keys() = %v, want %v", got, want)
		}
	}
}

func TestSnapshotSize(t *testing.T) {
	s := newSnap(map[string][]byte{"a": []byte("12345"), "b": []byte("123")})
	if got := s.Size(); got != 8 {
		t.Fatalf("Size() = %d, want 8", got)
	}
}

func TestSnapshotWith(t *testing.T) {
	s := newSnap(map[string][]byte{"a": []byte("one"), "b": []byte("two")})

	next := s.With(map[string][]byte{"a": []byte("ONE")}, []string{"b"})

	if got, _ := next.Get("a"); string(got) != "ONE" {
		t.Errorf("a = %q, want ONE", got)
	}
	if _, ok := next.Get("b"); ok {
		t.Error("b should have been deleted")
	}
	if got, _ := s.Get("a"); string(got) != "one" {
		t.Errorf("original mutated: a = %q", got)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./pkg/objectfs/ -run TestSnapshot -v`
Expected: FAIL — `undefined: Snapshot`

- [ ] **Step 3: Write the implementation**

Create `pkg/objectfs/snapshot.go`:

```go
package objectfs

import (
	"sort"
	"time"
)

// MaxObjectBytes is etcd's practical ceiling on a single object. Writes that
// would exceed it fail with ENOSPC rather than an opaque API rejection.
const MaxObjectBytes = 1 << 20

// Snapshot is an immutable view of one Secret or ConfigMap at a single
// resourceVersion. Handles pin one for their lifetime, which reproduces the
// atomicity of kubelet's ..data symlink flip: a handle never observes a
// partial transition between two versions.
//
// Treat the Data map as read-only. Use With to derive a modified copy.
type Snapshot struct {
	Data            map[string][]byte
	ResourceVersion string
	FetchedAt       time.Time
}

// Get returns a copy of the value for key, so a caller writing into the
// returned slice cannot corrupt the shared snapshot.
func (s *Snapshot) Get(key string) ([]byte, bool) {
	v, ok := s.Data[key]
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

// Keys returns the snapshot's keys in sorted order, so readdir output is
// stable across calls.
func (s *Snapshot) Keys() []string {
	keys := make([]string, 0, len(s.Data))
	for k := range s.Data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Size is the total number of value bytes, used for the ENOSPC check.
func (s *Snapshot) Size() int {
	n := 0
	for _, v := range s.Data {
		n += len(v)
	}
	return n
}

// With returns a new Snapshot with set applied and del removed. The receiver
// is unchanged. ResourceVersion is cleared because the result does not
// correspond to any version the API server has seen.
func (s *Snapshot) With(set map[string][]byte, del []string) *Snapshot {
	data := make(map[string][]byte, len(s.Data)+len(set))
	for k, v := range s.Data {
		data[k] = v
	}
	for k, v := range set {
		data[k] = v
	}
	for _, k := range del {
		delete(data, k)
	}
	return &Snapshot{Data: data, FetchedAt: s.FetchedAt}
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./pkg/objectfs/ -run TestSnapshot -v`
Expected: PASS, all five tests.

- [ ] **Step 5: Commit**

```bash
git add pkg/objectfs/snapshot.go pkg/objectfs/snapshot_test.go
git commit -m "$(cat <<'EOF'
objectfs: immutable whole-object snapshot

Handles pin a Snapshot for their lifetime, reproducing the atomicity of
kubelet's ..data flip: a handle never observes a partial transition between
versions. Get returns a copy so a caller cannot corrupt the shared value.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
EOF
)"
```

---

### Task 4: Store interface and the Secret implementation

The only code that talks to the API server about object content. Both the
quorum-GET rule and the field-selector rule live here, so both get tests.

**Files:**
- Create: `pkg/objectfs/store.go`, `pkg/objectfs/store_secret.go`
- Test: `pkg/objectfs/store_secret_test.go`

**Interfaces:**
- Consumes: `Snapshot` (Task 3).
- Produces:
  - `type Store interface { Get(ctx) (*Snapshot, error); Watch(ctx, sinceRV string) (watch.Interface, error); Decode(runtime.Object) (*Snapshot, error); Patch(ctx, set map[string][]byte, del []string) error; Describe() string }`
  - `func NewSecretStore(c kubernetes.Interface, ns, name string) *SecretStore`

- [ ] **Step 1: Write the failing test**

Create `pkg/objectfs/store_secret_test.go`:

```go
package objectfs

import (
	"context"
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func secretFixture() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: "oauth-credentials", Namespace: "my-app", ResourceVersion: "42",
		},
		Data: map[string][]byte{"session.key": []byte("v1"), "config.json": []byte("{}")},
	}
}

func TestSecretStoreGet(t *testing.T) {
	c := fake.NewSimpleClientset(secretFixture())
	s := NewSecretStore(c, "my-app", "oauth-credentials")

	snap, err := s.Get(context.Background())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got, _ := snap.Get("session.key"); string(got) != "v1" {
		t.Errorf("session.key = %q, want v1", got)
	}
	if snap.ResourceVersion != "42" {
		t.Errorf("ResourceVersion = %q, want 42", snap.ResourceVersion)
	}
	if snap.FetchedAt.IsZero() {
		t.Error("FetchedAt not stamped")
	}
}

// A quorum read requires ResourceVersion to be empty. Passing "0" would be
// served from the API server's watch cache and silently break the
// read-after-write guarantee, so assert on the actual GetOptions.
func TestSecretStoreGetIsQuorumRead(t *testing.T) {
	c := fake.NewSimpleClientset(secretFixture())
	var seen metav1.GetOptions
	c.PrependReactor("get", "secrets", func(a ktesting.Action) (bool, runtime.Object, error) {
		seen = a.(ktesting.GetActionImpl).GetOptions
		return false, nil, nil
	})

	if _, err := NewSecretStore(c, "my-app", "oauth-credentials").Get(context.Background()); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if seen.ResourceVersion != "" {
		t.Fatalf("GetOptions.ResourceVersion = %q, want empty (quorum read)", seen.ResourceVersion)
	}
}

// RBAC resourceNames rejects an unscoped watch with 403, so the field
// selector is a correctness requirement, not an optimisation.
func TestSecretStoreWatchSetsNameFieldSelector(t *testing.T) {
	c := fake.NewSimpleClientset(secretFixture())
	var seen string
	c.PrependWatchReactor("secrets", func(a ktesting.Action) (bool, watch.Interface, error) {
		seen = a.(ktesting.WatchActionImpl).WatchRestrictions.Fields.String()
		return false, nil, nil
	})

	w, err := NewSecretStore(c, "my-app", "oauth-credentials").Watch(context.Background(), "42")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()

	if seen != "metadata.name=oauth-credentials" {
		t.Fatalf("field selector = %q, want metadata.name=oauth-credentials", seen)
	}
}

func TestSecretStorePatchBody(t *testing.T) {
	c := fake.NewSimpleClientset(secretFixture())
	var body []byte
	c.PrependReactor("patch", "secrets", func(a ktesting.Action) (bool, runtime.Object, error) {
		body = a.(ktesting.PatchActionImpl).Patch
		return true, secretFixture(), nil
	})

	err := NewSecretStore(c, "my-app", "oauth-credentials").Patch(
		context.Background(),
		map[string][]byte{"session.key": []byte("v2")},
		[]string{"config.json"},
	)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}

	var got struct {
		Data map[string]*string `json:"data"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("patch body not JSON: %v (%s)", err, body)
	}
	// Secret data values are base64 in JSON.
	if got.Data["session.key"] == nil || *got.Data["session.key"] != "djI=" {
		t.Errorf("session.key = %v, want base64 of v2", got.Data["session.key"])
	}
	if v, ok := got.Data["config.json"]; !ok || v != nil {
		t.Errorf("config.json = %v, want explicit null for deletion", v)
	}
	// A merge patch must not mention keys we did not touch.
	if _, ok := got.Data["untouched"]; ok {
		t.Error("patch mentioned a key it should not have")
	}
}
```

Add `"k8s.io/apimachinery/pkg/watch"` to the import block.

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./pkg/objectfs/ -run TestSecretStore -v`
Expected: FAIL — `undefined: NewSecretStore`

- [ ] **Step 3: Write the Store interface**

Create `pkg/objectfs/store.go`:

```go
package objectfs

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
)

// Store is the only abstraction that talks to the API server about object
// content. Secret and ConfigMap differ solely in how their data map is
// encoded; everything above this interface is shared.
//
// Every implementation authenticates as the consuming pod, never as the
// driver.
type Store interface {
	// Get performs a quorum read: GetOptions.ResourceVersion MUST be empty
	// so the request is served from etcd rather than the API server's watch
	// cache. This is the read-after-write guarantee.
	Get(ctx context.Context) (*Snapshot, error)

	// Watch starts a watch scoped by a metadata.name field selector. RBAC
	// resourceNames rejects an unscoped watch with 403, so the selector is
	// mandatory.
	Watch(ctx context.Context, sinceRV string) (watch.Interface, error)

	// Decode converts an object from a watch event into a Snapshot.
	Decode(obj runtime.Object) (*Snapshot, error)

	// Patch applies a per-key merge patch. Keys absent from both arguments
	// are untouched because the request never mentions them — there is no
	// read-modify-write and no optimistic concurrency check.
	Patch(ctx context.Context, set map[string][]byte, del []string) error

	// Describe identifies the object for error messages, e.g.
	// `secrets/oauth-credentials`.
	Describe() string
}
```

- [ ] **Step 4: Write the Secret implementation**

Create `pkg/objectfs/store_secret.go`:

```go
package objectfs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

// SecretStore backs a mount with a corev1.Secret.
type SecretStore struct {
	client kubernetes.Interface
	ns     string
	name   string
}

// NewSecretStore returns a Store backed by the named Secret. The client must
// be built from the consuming pod's token.
func NewSecretStore(c kubernetes.Interface, ns, name string) *SecretStore {
	return &SecretStore{client: c, ns: ns, name: name}
}

func (s *SecretStore) Describe() string { return "secrets/" + s.name }

func (s *SecretStore) Get(ctx context.Context) (*Snapshot, error) {
	// Empty GetOptions == quorum read. Do not set ResourceVersion.
	obj, err := s.client.CoreV1().Secrets(s.ns).Get(ctx, s.name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return s.Decode(obj)
}

func (s *SecretStore) Watch(ctx context.Context, sinceRV string) (watch.Interface, error) {
	return s.client.CoreV1().Secrets(s.ns).Watch(ctx, metav1.ListOptions{
		FieldSelector:   fields.OneTermEqualSelector("metadata.name", s.name).String(),
		ResourceVersion: sinceRV,
	})
}

func (s *SecretStore) Decode(obj runtime.Object) (*Snapshot, error) {
	sec, ok := obj.(*corev1.Secret)
	if !ok {
		return nil, fmt.Errorf("expected *v1.Secret, got %T", obj)
	}
	data := make(map[string][]byte, len(sec.Data))
	for k, v := range sec.Data {
		cp := make([]byte, len(v))
		copy(cp, v)
		data[k] = cp
	}
	return &Snapshot{
		Data:            data,
		ResourceVersion: sec.ResourceVersion,
		FetchedAt:       time.Now(),
	}, nil
}

func (s *SecretStore) Patch(ctx context.Context, set map[string][]byte, del []string) error {
	// json.Marshal base64-encodes []byte, which is exactly the wire form the
	// API server expects for Secret.data. A nil entry deletes the key.
	data := make(map[string]interface{}, len(set)+len(del))
	for k, v := range set {
		data[k] = v
	}
	for _, k := range del {
		data[k] = nil
	}
	body, err := json.Marshal(map[string]interface{}{"data": data})
	if err != nil {
		return err
	}
	_, err = s.client.CoreV1().Secrets(s.ns).Patch(
		ctx, s.name, types.MergePatchType, body, metav1.PatchOptions{})
	return err
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./pkg/objectfs/ -run TestSecretStore -v`
Expected: PASS, all four tests. If `TestSecretStoreWatchSetsNameFieldSelector`
fails to compile, confirm the `watch` import is present in the test file.

- [ ] **Step 6: Commit**

```bash
git add pkg/objectfs/store.go pkg/objectfs/store_secret.go pkg/objectfs/store_secret_test.go
git commit -m "$(cat <<'EOF'
objectfs: Store interface and Secret implementation

Two constraints are enforced by test rather than convention, because both
fail silently rather than loudly:

- Get must leave GetOptions.ResourceVersion empty. Passing "0" reads the
  API server's watch cache and breaks read-after-write.
- Watch must set a metadata.name field selector. RBAC resourceNames rejects
  an unscoped watch with 403.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
EOF
)"
```

---

### Task 5: The ConfigMap implementation

`ConfigMap` splits its map across `data` (UTF-8 strings) and `binaryData`
(bytes), and the API server rejects a key present in both. That split is the
only difference from `SecretStore`.

**Files:**
- Create: `pkg/objectfs/store_configmap.go`
- Test: `pkg/objectfs/store_configmap_test.go`

**Interfaces:**
- Consumes: `Store` (Task 4), `Snapshot` (Task 3).
- Produces: `func NewConfigMapStore(c kubernetes.Interface, ns, name string) *ConfigMapStore`

- [ ] **Step 1: Write the failing test**

Create `pkg/objectfs/store_configmap_test.go`:

```go
package objectfs

import (
	"context"
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func configMapFixture() *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "app-config", Namespace: "my-app", ResourceVersion: "7",
		},
		Data:       map[string]string{"config.json": "{}"},
		BinaryData: map[string][]byte{"blob.bin": {0xff, 0xfe}},
	}
}

// Both halves of a ConfigMap present as one flat directory.
func TestConfigMapStoreGetMergesDataAndBinaryData(t *testing.T) {
	c := fake.NewSimpleClientset(configMapFixture())

	snap, err := NewConfigMapStore(c, "my-app", "app-config").Get(context.Background())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got, _ := snap.Get("config.json"); string(got) != "{}" {
		t.Errorf("config.json = %q, want {}", got)
	}
	if got, _ := snap.Get("blob.bin"); string(got) != "\xff\xfe" {
		t.Errorf("blob.bin = %x, want fffe", got)
	}
	if len(snap.Keys()) != 2 {
		t.Errorf("Keys() = %v, want 2 entries", snap.Keys())
	}
}

// UTF-8 goes to data, non-UTF-8 to binaryData, and the other half is
// explicitly nulled so the key never lives in both — the API server rejects
// that.
func TestConfigMapStorePatchRoutesByEncoding(t *testing.T) {
	c := fake.NewSimpleClientset(configMapFixture())
	var body []byte
	c.PrependReactor("patch", "configmaps", func(a ktesting.Action) (bool, runtime.Object, error) {
		body = a.(ktesting.PatchActionImpl).Patch
		return true, configMapFixture(), nil
	})

	err := NewConfigMapStore(c, "my-app", "app-config").Patch(
		context.Background(),
		map[string][]byte{"text.txt": []byte("hello"), "raw.bin": {0xff}},
		[]string{"gone.txt"},
	)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}

	var got struct {
		Data       map[string]*string `json:"data"`
		BinaryData map[string]*string `json:"binaryData"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("patch body not JSON: %v (%s)", err, body)
	}

	if got.Data["text.txt"] == nil || *got.Data["text.txt"] != "hello" {
		t.Errorf("data[text.txt] = %v, want hello", got.Data["text.txt"])
	}
	if v, ok := got.BinaryData["text.txt"]; !ok || v != nil {
		t.Errorf("binaryData[text.txt] = %v, want explicit null", v)
	}
	if got.BinaryData["raw.bin"] == nil || *got.BinaryData["raw.bin"] != "/w==" {
		t.Errorf("binaryData[raw.bin] = %v, want base64 of 0xff", got.BinaryData["raw.bin"])
	}
	if v, ok := got.Data["raw.bin"]; !ok || v != nil {
		t.Errorf("data[raw.bin] = %v, want explicit null", v)
	}
	// A deletion must null both halves, since we do not know which held it.
	if v, ok := got.Data["gone.txt"]; !ok || v != nil {
		t.Errorf("data[gone.txt] = %v, want explicit null", v)
	}
	if v, ok := got.BinaryData["gone.txt"]; !ok || v != nil {
		t.Errorf("binaryData[gone.txt] = %v, want explicit null", v)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./pkg/objectfs/ -run TestConfigMapStore -v`
Expected: FAIL — `undefined: NewConfigMapStore`

- [ ] **Step 3: Write the implementation**

Create `pkg/objectfs/store_configmap.go`:

```go
package objectfs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

// ConfigMapStore backs a mount with a corev1.ConfigMap. A ConfigMap splits
// its map across data (UTF-8 strings) and binaryData (bytes); the mount
// presents both as one flat directory.
type ConfigMapStore struct {
	client kubernetes.Interface
	ns     string
	name   string
}

// NewConfigMapStore returns a Store backed by the named ConfigMap. The client
// must be built from the consuming pod's token.
func NewConfigMapStore(c kubernetes.Interface, ns, name string) *ConfigMapStore {
	return &ConfigMapStore{client: c, ns: ns, name: name}
}

func (s *ConfigMapStore) Describe() string { return "configmaps/" + s.name }

func (s *ConfigMapStore) Get(ctx context.Context) (*Snapshot, error) {
	// Empty GetOptions == quorum read. Do not set ResourceVersion.
	obj, err := s.client.CoreV1().ConfigMaps(s.ns).Get(ctx, s.name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return s.Decode(obj)
}

func (s *ConfigMapStore) Watch(ctx context.Context, sinceRV string) (watch.Interface, error) {
	return s.client.CoreV1().ConfigMaps(s.ns).Watch(ctx, metav1.ListOptions{
		FieldSelector:   fields.OneTermEqualSelector("metadata.name", s.name).String(),
		ResourceVersion: sinceRV,
	})
}

func (s *ConfigMapStore) Decode(obj runtime.Object) (*Snapshot, error) {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		return nil, fmt.Errorf("expected *v1.ConfigMap, got %T", obj)
	}
	data := make(map[string][]byte, len(cm.Data)+len(cm.BinaryData))
	for k, v := range cm.Data {
		data[k] = []byte(v)
	}
	for k, v := range cm.BinaryData {
		cp := make([]byte, len(v))
		copy(cp, v)
		data[k] = cp
	}
	return &Snapshot{
		Data:            data,
		ResourceVersion: cm.ResourceVersion,
		FetchedAt:       time.Now(),
	}, nil
}

func (s *ConfigMapStore) Patch(ctx context.Context, set map[string][]byte, del []string) error {
	// The API server rejects a key present in both maps, so every write nulls
	// the half it is not using. A deletion nulls both, since the caller does
	// not know which half held the key.
	data := map[string]interface{}{}
	binary := map[string]interface{}{}

	for k, v := range set {
		if utf8.Valid(v) {
			data[k] = string(v)
			binary[k] = nil
		} else {
			binary[k] = v // json.Marshal base64-encodes []byte
			data[k] = nil
		}
	}
	for _, k := range del {
		data[k] = nil
		binary[k] = nil
	}

	body, err := json.Marshal(map[string]interface{}{
		"data":       data,
		"binaryData": binary,
	})
	if err != nil {
		return err
	}
	_, err = s.client.CoreV1().ConfigMaps(s.ns).Patch(
		ctx, s.name, types.MergePatchType, body, metav1.PatchOptions{})
	return err
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./pkg/objectfs/ -run TestConfigMapStore -v`
Expected: PASS, both tests.

- [ ] **Step 5: Verify both stores satisfy the interface**

Add to `pkg/objectfs/store.go`:

```go
var (
	_ Store = (*SecretStore)(nil)
	_ Store = (*ConfigMapStore)(nil)
)
```

Run: `go build ./... && go test ./pkg/objectfs/ -v`
Expected: builds clean, all tests pass.

- [ ] **Step 6: Commit**

```bash
git add pkg/objectfs/store.go pkg/objectfs/store_configmap.go pkg/objectfs/store_configmap_test.go
git commit -m "$(cat <<'EOF'
objectfs: ConfigMap implementation of Store

A ConfigMap splits its map across data (UTF-8) and binaryData (bytes) and
the API server rejects a key present in both, so every write nulls the half
it is not using and a deletion nulls both.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
EOF
)"
```

---

### Task 6: Watch cache with staleness bound

Serves eventual reads from a watch-fed pointer, and provides the
**guaranteed-fresh** read that the lock protocol depends on.

**Files:**
- Create: `pkg/objectfs/cache.go`
- Test: `pkg/objectfs/cache_test.go`

**Interfaces:**
- Consumes: `Store` (Task 4), `Snapshot` (Task 3).
- Produces:
  - `func NewCache(s Store, bound time.Duration) *Cache`
  - `func (c *Cache) Current() *Snapshot`
  - `func (c *Cache) Fresh(ctx) (*Snapshot, error)` — always a quorum GET
  - `func (c *Cache) MaybeFresh(ctx) *Snapshot` — GET only if past the bound; never fails
  - `func (c *Cache) Set(*Snapshot)` — own-writes
  - `func (c *Cache) Run(ctx)` — watch loop, blocks until ctx done

- [ ] **Step 1: Write the failing test**

Create `pkg/objectfs/cache_test.go`:

```go
package objectfs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
)

// stubStore counts calls so tests can assert on API traffic, which is the
// whole point of the staleness bound.
type stubStore struct {
	mu       sync.Mutex
	gets     int
	getErr   error
	snap     *Snapshot
	watchCh  chan watch.Event
	watchErr error
}

func newStubStore(data map[string][]byte) *stubStore {
	return &stubStore{
		snap:    &Snapshot{Data: data, ResourceVersion: "1", FetchedAt: time.Now()},
		watchCh: make(chan watch.Event, 8),
	}
}

func (s *stubStore) Get(context.Context) (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	if s.getErr != nil {
		return nil, s.getErr
	}
	cp := *s.snap
	cp.FetchedAt = time.Now()
	return &cp, nil
}

func (s *stubStore) getCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets
}

func (s *stubStore) Watch(context.Context, string) (watch.Interface, error) {
	if s.watchErr != nil {
		return nil, s.watchErr
	}
	return watch.NewProxyWatcher(s.watchCh), nil
}

func (s *stubStore) Decode(obj runtime.Object) (*Snapshot, error) {
	return obj.(*snapObject).snap, nil
}

func (s *stubStore) Patch(context.Context, map[string][]byte, []string) error { return nil }
func (s *stubStore) Describe() string                                        { return "stub/obj" }

// snapObject smuggles a Snapshot through the runtime.Object interface so the
// stub can drive Decode without a real API type.
type snapObject struct {
	runtime.Object
	snap *Snapshot
}

func (o *snapObject) DeepCopyObject() runtime.Object { return o }

func TestCacheFreshAlwaysHitsTheAPI(t *testing.T) {
	s := newStubStore(map[string][]byte{"a": []byte("1")})
	c := NewCache(s, time.Hour) // bound is irrelevant to Fresh

	for i := 0; i < 3; i++ {
		if _, err := c.Fresh(context.Background()); err != nil {
			t.Fatalf("Fresh: %v", err)
		}
	}
	if got := s.getCount(); got != 3 {
		t.Fatalf("gets = %d, want 3 — Fresh must never serve from cache", got)
	}
}

func TestCacheMaybeFreshRespectsTheBound(t *testing.T) {
	s := newStubStore(map[string][]byte{"a": []byte("1")})
	c := NewCache(s, time.Hour)

	first := c.MaybeFresh(context.Background()) // cold: must fetch
	if first == nil {
		t.Fatal("MaybeFresh returned nil on cold cache")
	}
	if got := s.getCount(); got != 1 {
		t.Fatalf("gets = %d, want 1", got)
	}

	c.MaybeFresh(context.Background()) // warm and inside bound: no fetch
	if got := s.getCount(); got != 1 {
		t.Fatalf("gets = %d, want still 1 — inside the bound", got)
	}
}

func TestCacheMaybeFreshRefetchesPastTheBound(t *testing.T) {
	s := newStubStore(map[string][]byte{"a": []byte("1")})
	c := NewCache(s, time.Nanosecond)

	c.MaybeFresh(context.Background())
	time.Sleep(time.Millisecond)
	c.MaybeFresh(context.Background())

	if got := s.getCount(); got != 2 {
		t.Fatalf("gets = %d, want 2 — past the bound must refetch", got)
	}
}

// A failed refresh degrades to stale rather than failing the read: a stale
// credential costs a 401 and a retry, a failed read costs an outage.
func TestCacheMaybeFreshServesStaleOnError(t *testing.T) {
	s := newStubStore(map[string][]byte{"a": []byte("1")})
	c := NewCache(s, time.Nanosecond)

	warm := c.MaybeFresh(context.Background())
	if warm == nil {
		t.Fatal("cold MaybeFresh returned nil")
	}

	s.mu.Lock()
	s.getErr = errors.New("apiserver unreachable")
	s.mu.Unlock()

	time.Sleep(time.Millisecond)
	got := c.MaybeFresh(context.Background())
	if got == nil {
		t.Fatal("MaybeFresh returned nil instead of degrading to stale")
	}
	if v, _ := got.Get("a"); string(v) != "1" {
		t.Fatalf("stale value = %q, want 1", v)
	}
}

func TestCacheSetIsVisibleImmediately(t *testing.T) {
	s := newStubStore(map[string][]byte{"a": []byte("1")})
	c := NewCache(s, time.Hour)

	c.Set(&Snapshot{Data: map[string][]byte{"a": []byte("2")}, ResourceVersion: "9", FetchedAt: time.Now()})

	if v, _ := c.Current().Get("a"); string(v) != "2" {
		t.Fatalf("Current() = %q, want 2", v)
	}
}

func TestCacheRunAppliesWatchEvents(t *testing.T) {
	s := newStubStore(map[string][]byte{"a": []byte("1")})
	c := NewCache(s, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	next := &Snapshot{Data: map[string][]byte{"a": []byte("2")}, ResourceVersion: "2", FetchedAt: time.Now()}
	s.watchCh <- watch.Event{Type: watch.Modified, Object: &snapObject{snap: next}}

	deadline := time.After(2 * time.Second)
	for {
		if cur := c.Current(); cur != nil {
			if v, _ := cur.Get("a"); string(v) == "2" {
				return
			}
		}
		select {
		case <-deadline:
			t.Fatal("watch event never applied to cache")
		case <-time.After(5 * time.Millisecond):
		}
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./pkg/objectfs/ -run TestCache -v`
Expected: FAIL — `undefined: NewCache`

- [ ] **Step 3: Write the implementation**

Create `pkg/objectfs/cache.go`:

```go
package objectfs

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/watch"
)

// DefaultStalenessBound is how long the cache may go without confirmation
// from the API server before the next open() forces a quorum read. It exists
// to catch a watch that has died silently — the same job kubelet's periodic
// resync does for native projection.
const DefaultStalenessBound = 30 * time.Second

// Cache holds the current Snapshot for one mounted object.
//
// Reads are eventual, fed by a watch. The read-after-write guarantee is not
// provided here — it comes from callers invoking Fresh at the moment they
// acquire the Lease.
type Cache struct {
	store Store
	bound time.Duration

	snap atomic.Pointer[Snapshot]

	mu       sync.Mutex
	lastSync time.Time
}

// NewCache returns a Cache over store. A non-positive bound falls back to
// DefaultStalenessBound.
func NewCache(store Store, bound time.Duration) *Cache {
	if bound <= 0 {
		bound = DefaultStalenessBound
	}
	return &Cache{store: store, bound: bound}
}

// Current returns the cached Snapshot, or nil if nothing has been fetched.
func (c *Cache) Current() *Snapshot { return c.snap.Load() }

// Set installs snap as current. Called after a successful write so the
// writing node reads its own writes without a round trip.
func (c *Cache) Set(snap *Snapshot) {
	if snap == nil {
		return
	}
	c.snap.Store(snap)
	c.mu.Lock()
	c.lastSync = time.Now()
	c.mu.Unlock()
}

// Fresh performs a quorum read and installs the result. This is the
// read-after-write guarantee; it must be called on every Lease acquisition
// and must never be served from cache.
func (c *Cache) Fresh(ctx context.Context) (*Snapshot, error) {
	snap, err := c.store.Get(ctx)
	if err != nil {
		return nil, err
	}
	c.Set(snap)
	return snap, nil
}

// MaybeFresh returns the current Snapshot, refreshing first if the cache has
// gone unconfirmed for longer than the bound.
//
// It never returns an error: if the refresh fails, the stale snapshot is
// served anyway. A stale value costs the consumer a 401 and a retry; a
// failed read costs it an outage.
func (c *Cache) MaybeFresh(ctx context.Context) *Snapshot {
	cur := c.snap.Load()

	c.mu.Lock()
	age := time.Since(c.lastSync)
	c.mu.Unlock()

	if cur != nil && age < c.bound {
		return cur
	}
	if snap, err := c.Fresh(ctx); err == nil {
		return snap
	}
	return cur
}

// Run drives the watch loop until ctx is cancelled. On any disconnect it
// re-syncs with a quorum read before re-establishing the watch, so the cache
// is never trusted across a gap it cannot account for.
func (c *Cache) Run(ctx context.Context) {
	backoff := 100 * time.Millisecond
	const maxBackoff = 5 * time.Second

	for ctx.Err() == nil {
		snap, err := c.Fresh(ctx)
		if err != nil {
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}
		backoff = 100 * time.Millisecond

		if !c.watchOnce(ctx, snap.ResourceVersion) {
			return
		}
	}
}

// watchOnce runs a single watch until it closes or errors. It reports false
// only when ctx is done, so the caller can distinguish shutdown from a
// reconnect.
func (c *Cache) watchOnce(ctx context.Context, sinceRV string) bool {
	w, err := c.store.Watch(ctx, sinceRV)
	if err != nil {
		return ctx.Err() == nil
	}
	defer w.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case ev, ok := <-w.ResultChan():
			if !ok {
				return true // channel closed: reconnect via a fresh Get
			}
			switch ev.Type {
			case watch.Added, watch.Modified:
				if snap, err := c.store.Decode(ev.Object); err == nil {
					c.Set(snap)
				}
			case watch.Deleted:
				// Keep serving the last snapshot; writes will fail loudly.
				return true
			case watch.Error:
				return true
			}
		}
	}
}

// sleepCtx reports false if ctx finished first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./pkg/objectfs/ -run TestCache -race -v`
Expected: PASS, all six tests, no race warnings.

- [ ] **Step 5: Commit**

```bash
git add pkg/objectfs/cache.go pkg/objectfs/cache_test.go
git commit -m "$(cat <<'EOF'
objectfs: watch cache with a staleness bound

Reads are eventual, fed by a watch. Fresh() is the read-after-write
guarantee and always does a quorum read; MaybeFresh() refreshes only past
the bound and degrades to stale on error, because a stale credential costs
a 401 and a retry while a failed read costs an outage.

The watch loop re-syncs with a quorum Get before re-establishing, so the
cache is never trusted across a gap it cannot account for.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
EOF
)"
```

---

### Task 7: Lease manager

The sole mutual-exclusion mechanism. Renewal has no ceiling — an open handle
holds its lock as long as it wants, like a local filesystem.

**Files:**
- Create: `pkg/objectfs/lease.go`
- Test: `pkg/objectfs/lease_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `func NewLeaseManager(c kubernetes.Interface, ns, name, holder string, duration time.Duration) *LeaseManager`
  - `func (m *LeaseManager) TryAcquire(ctx) (bool, error)`
  - `func (m *LeaseManager) Acquire(ctx) error` — blocks until held or ctx done
  - `func (m *LeaseManager) Release(ctx) error`
  - `func (m *LeaseManager) Healthy() bool` — fencing check before a commit
  - `var ErrLeaseLost = errors.New(...)`

- [ ] **Step 1: Write the failing test**

Create `pkg/objectfs/lease_test.go`:

```go
package objectfs

import (
	"context"
	"testing"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

const testLeaseDur = 15 * time.Second

func TestLeaseAcquireCreatesWhenAbsent(t *testing.T) {
	c := fake.NewSimpleClientset()
	m := NewLeaseManager(c, "my-app", "roommate-oauth-credentials", "podA:1", testLeaseDur)

	ok, err := m.TryAcquire(context.Background())
	if err != nil || !ok {
		t.Fatalf("TryAcquire = %v, %v; want true, nil", ok, err)
	}

	got, err := c.CoordinationV1().Leases("my-app").
		Get(context.Background(), "roommate-oauth-credentials", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("lease not created: %v", err)
	}
	if got.Spec.HolderIdentity == nil || *got.Spec.HolderIdentity != "podA:1" {
		t.Fatalf("holderIdentity = %v, want podA:1", got.Spec.HolderIdentity)
	}
	if got.Spec.LeaseDurationSeconds == nil || *got.Spec.LeaseDurationSeconds != 15 {
		t.Fatalf("leaseDurationSeconds = %v, want 15", got.Spec.LeaseDurationSeconds)
	}
}

func TestLeaseTryAcquireFailsWhenHeldAndLive(t *testing.T) {
	held := liveLease("podB:1", testLeaseDur, time.Now())
	c := fake.NewSimpleClientset(held)
	m := NewLeaseManager(c, "my-app", "roommate-oauth-credentials", "podA:1", testLeaseDur)

	ok, err := m.TryAcquire(context.Background())
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	if ok {
		t.Fatal("acquired a lease that another holder holds")
	}
}

// A crashed holder must not block forever: past renewTime+duration the lease
// is up for grabs.
func TestLeaseTryAcquireTakesOverExpired(t *testing.T) {
	stale := liveLease("podB:1", testLeaseDur, time.Now().Add(-time.Hour))
	c := fake.NewSimpleClientset(stale)
	m := NewLeaseManager(c, "my-app", "roommate-oauth-credentials", "podA:1", testLeaseDur)

	ok, err := m.TryAcquire(context.Background())
	if err != nil || !ok {
		t.Fatalf("TryAcquire = %v, %v; want true, nil on an expired lease", ok, err)
	}

	got, _ := c.CoordinationV1().Leases("my-app").
		Get(context.Background(), "roommate-oauth-credentials", metav1.GetOptions{})
	if *got.Spec.HolderIdentity != "podA:1" {
		t.Fatalf("holderIdentity = %v, want podA:1", *got.Spec.HolderIdentity)
	}
}

func TestLeaseReacquireBySameHolderSucceeds(t *testing.T) {
	held := liveLease("podA:1", testLeaseDur, time.Now())
	c := fake.NewSimpleClientset(held)
	m := NewLeaseManager(c, "my-app", "roommate-oauth-credentials", "podA:1", testLeaseDur)

	ok, err := m.TryAcquire(context.Background())
	if err != nil || !ok {
		t.Fatalf("TryAcquire = %v, %v; want true for the existing holder", ok, err)
	}
}

func TestLeaseHealthyOnlyWhileHeld(t *testing.T) {
	c := fake.NewSimpleClientset()
	m := NewLeaseManager(c, "my-app", "roommate-oauth-credentials", "podA:1", testLeaseDur)

	if m.Healthy() {
		t.Fatal("Healthy() before acquiring")
	}
	if _, err := m.TryAcquire(context.Background()); err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	if !m.Healthy() {
		t.Fatal("not Healthy() while held")
	}
	if err := m.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if m.Healthy() {
		t.Fatal("Healthy() after Release")
	}
}

func TestLeaseReleaseClearsHolder(t *testing.T) {
	c := fake.NewSimpleClientset()
	m := NewLeaseManager(c, "my-app", "roommate-oauth-credentials", "podA:1", testLeaseDur)
	if _, err := m.TryAcquire(context.Background()); err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	if err := m.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}

	got, _ := c.CoordinationV1().Leases("my-app").
		Get(context.Background(), "roommate-oauth-credentials", metav1.GetOptions{})
	if got.Spec.HolderIdentity != nil && *got.Spec.HolderIdentity == "podA:1" {
		t.Fatal("Release left holderIdentity in place")
	}

	// Another holder can now take it immediately.
	other := NewLeaseManager(c, "my-app", "roommate-oauth-credentials", "podB:1", testLeaseDur)
	ok, err := other.TryAcquire(context.Background())
	if err != nil || !ok {
		t.Fatalf("second holder TryAcquire = %v, %v; want true", ok, err)
	}
}

func liveLease(holder string, dur time.Duration, renewed time.Time) *coordv1.Lease {
	secs := int32(dur / time.Second)
	rt := metav1.NewMicroTime(renewed)
	return &coordv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name: "roommate-oauth-credentials", Namespace: "my-app", ResourceVersion: "1",
		},
		Spec: coordv1.LeaseSpec{
			HolderIdentity:       &holder,
			LeaseDurationSeconds: &secs,
			RenewTime:            &rt,
		},
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./pkg/objectfs/ -run TestLease -v`
Expected: FAIL — `undefined: NewLeaseManager`

- [ ] **Step 3: Write the implementation**

Create `pkg/objectfs/lease.go`:

```go
package objectfs

import (
	"context"
	"errors"
	"sync"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// ErrLeaseLost means background renewal stopped succeeding, so a write can no
// longer claim to be serialised. Commits fence on this rather than proceeding
// under a lock that may already belong to someone else.
var ErrLeaseLost = errors.New("roommate: lease renewal lost")

// DefaultLeaseDuration is how long a lease survives without renewal. Renewal
// runs at a third of this.
const DefaultLeaseDuration = 15 * time.Second

// LeaseManager owns one coordination.k8s.io Lease on behalf of one open file
// handle.
//
// There is deliberately no maximum hold time: an open handle keeps its lock
// for as long as it wants, exactly as on a local filesystem. Renewal runs in
// the background until Release.
type LeaseManager struct {
	client   kubernetes.Interface
	ns       string
	name     string
	holder   string
	duration time.Duration

	mu      sync.Mutex
	held    bool
	healthy bool
	cancel  context.CancelFunc
}

// NewLeaseManager returns a manager for the named Lease. holder must be
// unique per lock holder — pod UID plus a per-handle counter. The client must
// be built from the consuming pod's token.
func NewLeaseManager(c kubernetes.Interface, ns, name, holder string, duration time.Duration) *LeaseManager {
	if duration <= 0 {
		duration = DefaultLeaseDuration
	}
	return &LeaseManager{client: c, ns: ns, name: name, holder: holder, duration: duration}
}

// Healthy reports whether the lease is held and renewal is still succeeding.
func (m *LeaseManager) Healthy() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.held && m.healthy
}

// TryAcquire attempts to take the lease once, reporting whether it succeeded.
// It is the non-blocking path behind flock(LOCK_NB).
func (m *LeaseManager) TryAcquire(ctx context.Context) (bool, error) {
	m.mu.Lock()
	if m.held {
		m.mu.Unlock()
		return true, nil
	}
	m.mu.Unlock()

	cur, err := m.client.CoordinationV1().Leases(m.ns).Get(ctx, m.name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		if err := m.create(ctx); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return false, nil // lost the race; caller retries
			}
			return false, err
		}
	case err != nil:
		return false, err
	default:
		if !m.claimable(cur) {
			return false, nil
		}
		if err := m.takeOver(ctx, cur); err != nil {
			if apierrors.IsConflict(err) {
				return false, nil // lost the race; caller retries
			}
			return false, err
		}
	}

	m.startRenewal()
	return true, nil
}

// Acquire blocks until the lease is held or ctx is done. A blocking flock has
// no timeout, matching local filesystem semantics; the caller interrupts it
// by cancelling ctx.
func (m *LeaseManager) Acquire(ctx context.Context) error {
	// Poll at a third of the duration so a released lease is picked up
	// promptly without hammering the API server.
	interval := m.duration / 3
	for {
		ok, err := m.TryAcquire(ctx)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if !sleepCtx(ctx, interval) {
			return ctx.Err()
		}
	}
}

// Release stops renewal and clears our holder identity so the next acquirer
// does not have to wait out the full duration.
func (m *LeaseManager) Release(ctx context.Context) error {
	m.mu.Lock()
	if !m.held {
		m.mu.Unlock()
		return nil
	}
	m.held, m.healthy = false, false
	cancel := m.cancel
	m.cancel = nil
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	cur, err := m.client.CoordinationV1().Leases(m.ns).Get(ctx, m.name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if cur.Spec.HolderIdentity == nil || *cur.Spec.HolderIdentity != m.holder {
		return nil // someone else already took it
	}
	cur.Spec.HolderIdentity = nil
	cur.Spec.RenewTime = nil
	_, err = m.client.CoordinationV1().Leases(m.ns).Update(ctx, cur, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// claimable reports whether the lease is free, expired, or already ours.
func (m *LeaseManager) claimable(l *coordv1.Lease) bool {
	if l.Spec.HolderIdentity == nil || *l.Spec.HolderIdentity == "" {
		return true
	}
	if *l.Spec.HolderIdentity == m.holder {
		return true
	}
	if l.Spec.RenewTime == nil {
		return true
	}
	dur := m.duration
	if l.Spec.LeaseDurationSeconds != nil {
		dur = time.Duration(*l.Spec.LeaseDurationSeconds) * time.Second
	}
	return time.Now().After(l.Spec.RenewTime.Add(dur))
}

func (m *LeaseManager) create(ctx context.Context) error {
	secs := int32(m.duration / time.Second)
	now := metav1.NowMicro()
	_, err := m.client.CoordinationV1().Leases(m.ns).Create(ctx, &coordv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: m.name, Namespace: m.ns},
		Spec: coordv1.LeaseSpec{
			HolderIdentity:       &m.holder,
			LeaseDurationSeconds: &secs,
			AcquireTime:          &now,
			RenewTime:            &now,
		},
	}, metav1.CreateOptions{})
	return err
}

// takeOver updates in place, so the Lease's own resourceVersion serialises
// two acquirers racing for an expired lease.
func (m *LeaseManager) takeOver(ctx context.Context, cur *coordv1.Lease) error {
	secs := int32(m.duration / time.Second)
	now := metav1.NowMicro()
	cur.Spec.HolderIdentity = &m.holder
	cur.Spec.LeaseDurationSeconds = &secs
	cur.Spec.AcquireTime = &now
	cur.Spec.RenewTime = &now
	_, err := m.client.CoordinationV1().Leases(m.ns).Update(ctx, cur, metav1.UpdateOptions{})
	return err
}

func (m *LeaseManager) startRenewal() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.held {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.held, m.healthy, m.cancel = true, true, cancel
	go m.renewLoop(ctx)
}

// renewLoop runs until Release. A failed renewal flips healthy to false,
// which fences commits; a later success restores it.
func (m *LeaseManager) renewLoop(ctx context.Context) {
	interval := m.duration / 3
	for {
		if !sleepCtx(ctx, interval) {
			return
		}
		err := m.renewOnce(ctx)
		m.mu.Lock()
		m.healthy = err == nil
		m.mu.Unlock()
	}
}

func (m *LeaseManager) renewOnce(ctx context.Context) error {
	cur, err := m.client.CoordinationV1().Leases(m.ns).Get(ctx, m.name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if cur.Spec.HolderIdentity == nil || *cur.Spec.HolderIdentity != m.holder {
		return ErrLeaseLost // someone took it while we slept
	}
	now := metav1.NowMicro()
	cur.Spec.RenewTime = &now
	_, err = m.client.CoordinationV1().Leases(m.ns).Update(ctx, cur, metav1.UpdateOptions{})
	return err
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./pkg/objectfs/ -run TestLease -race -v`
Expected: PASS, all six tests, no race warnings.

- [ ] **Step 5: Commit**

```bash
git add pkg/objectfs/lease.go pkg/objectfs/lease_test.go
git commit -m "$(cat <<'EOF'
objectfs: Lease manager

The sole mutual-exclusion mechanism. Renewal has no ceiling — an open handle
holds its lock as long as it wants, matching local filesystem semantics — and
a failed renewal flips Healthy() false so commits fence rather than writing
under a lock that may already belong to someone else.

Take-over of an expired lease updates in place, so the Lease's own
resourceVersion serialises two acquirers racing for it.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
EOF
)"
```

---

### Task 8: Committer — size check, fencing, patch dispatch

Thin by design. Because writes are per-key merge-patches there is no
read-modify-write, no CAS, and no retry loop to build here.

**Files:**
- Create: `pkg/objectfs/commit.go`
- Test: `pkg/objectfs/commit_test.go`

**Interfaces:**
- Consumes: `Store` (Task 4), `Cache` (Task 6), `LeaseManager` (Task 7), `Snapshot` (Task 3).
- Produces:
  - `type Committer struct{ ... }`
  - `func NewCommitter(s Store, c *Cache) *Committer`
  - `func (c *Committer) Commit(ctx, lease *LeaseManager, set map[string][]byte, del []string) error`
  - `var ErrTooLarge = errors.New(...)`

- [ ] **Step 1: Write the failing test**

Create `pkg/objectfs/commit_test.go`:

```go
package objectfs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
)

// recordingStore captures what Patch was asked to do.
type recordingStore struct {
	*stubStore
	set      map[string][]byte
	del      []string
	patchErr error
	patches  int
}

func newRecordingStore(data map[string][]byte) *recordingStore {
	return &recordingStore{stubStore: newStubStore(data)}
}

func (r *recordingStore) Patch(_ context.Context, set map[string][]byte, del []string) error {
	r.patches++
	r.set, r.del = set, del
	return r.patchErr
}

func TestCommitPatchesOnlyDirtyKeys(t *testing.T) {
	s := newRecordingStore(map[string][]byte{"a": []byte("1"), "b": []byte("2")})
	c := NewCommitter(s, NewCache(s, time.Hour))

	err := c.Commit(context.Background(), nil,
		map[string][]byte{"a": []byte("9")}, []string{"b"})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if len(s.set) != 1 || string(s.set["a"]) != "9" {
		t.Errorf("set = %v, want only a=9", s.set)
	}
	if len(s.del) != 1 || s.del[0] != "b" {
		t.Errorf("del = %v, want only [b]", s.del)
	}
}

// A merge patch has no optimistic concurrency check, so a single Patch call
// is the whole write. Any retry loop here would be a design regression.
func TestCommitDoesNotRetry(t *testing.T) {
	s := newRecordingStore(map[string][]byte{"a": []byte("1")})
	s.patchErr = errors.New("boom")
	c := NewCommitter(s, NewCache(s, time.Hour))

	if err := c.Commit(context.Background(), nil, map[string][]byte{"a": []byte("9")}, nil); err == nil {
		t.Fatal("Commit succeeded despite a Patch error")
	}
	if s.patches != 1 {
		t.Fatalf("patches = %d, want exactly 1 — no retry loop", s.patches)
	}
}

func TestCommitRefusesOversizedWrite(t *testing.T) {
	s := newRecordingStore(map[string][]byte{"a": []byte("1")})
	c := NewCommitter(s, NewCache(s, time.Hour))

	big := []byte(strings.Repeat("x", MaxObjectBytes+1))
	err := c.Commit(context.Background(), nil, map[string][]byte{"a": big}, nil)
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("Commit err = %v, want ErrTooLarge", err)
	}
	if s.patches != 0 {
		t.Fatal("oversized write reached the API server")
	}
}

// Fencing: if renewal has stopped succeeding we must not write under a lock
// that may already belong to someone else.
func TestCommitFencesOnLostLease(t *testing.T) {
	s := newRecordingStore(map[string][]byte{"a": []byte("1")})
	c := NewCommitter(s, NewCache(s, time.Hour))

	kc := fake.NewSimpleClientset()
	lease := NewLeaseManager(kc, "my-app", "roommate-oauth-credentials", "podA:1", testLeaseDur)
	// Never acquired, so Healthy() is false.

	err := c.Commit(context.Background(), lease, map[string][]byte{"a": []byte("9")}, nil)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Commit err = %v, want ErrLeaseLost", err)
	}
	if s.patches != 0 {
		t.Fatal("wrote despite an unhealthy lease")
	}
}

// After a successful write the writing node reads its own writes without a
// round trip.
func TestCommitUpdatesCacheOptimistically(t *testing.T) {
	s := newRecordingStore(map[string][]byte{"a": []byte("1")})
	cache := NewCache(s, time.Hour)
	if _, err := cache.Fresh(context.Background()); err != nil {
		t.Fatalf("warm cache: %v", err)
	}
	c := NewCommitter(s, cache)

	if err := c.Commit(context.Background(), nil, map[string][]byte{"a": []byte("9")}, nil); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if v, _ := cache.Current().Get("a"); string(v) != "9" {
		t.Fatalf("cache a = %q, want 9 immediately after commit", v)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./pkg/objectfs/ -run TestCommit -v`
Expected: FAIL — `undefined: NewCommitter`

- [ ] **Step 3: Write the implementation**

Create `pkg/objectfs/commit.go`:

```go
package objectfs

import (
	"context"
	"errors"
)

// ErrTooLarge means the write would push the object past etcd's practical
// ceiling. Surfaced as ENOSPC so the consumer sees a filesystem-shaped error
// rather than an opaque API rejection.
var ErrTooLarge = errors.New("roommate: object would exceed size limit")

// Committer turns a handle's buffered writes into a single per-key merge
// patch.
//
// It is deliberately thin. Because a merge patch mentions only the keys we
// touched, keys we never wrote are preserved by the request itself — there is
// no read-modify-write, no optimistic concurrency check, and no retry loop.
type Committer struct {
	store Store
	cache *Cache
}

// NewCommitter returns a Committer writing through store and refreshing cache.
func NewCommitter(store Store, cache *Cache) *Committer {
	return &Committer{store: store, cache: cache}
}

// Commit applies set and del as one merge patch.
//
// If lease is non-nil it is fenced first: a lease whose renewal has stopped
// succeeding refuses the write rather than committing under a lock that may
// already belong to someone else.
func (c *Committer) Commit(ctx context.Context, lease *LeaseManager, set map[string][]byte, del []string) error {
	if len(set) == 0 && len(del) == 0 {
		return nil
	}
	if lease != nil && !lease.Healthy() {
		return ErrLeaseLost
	}
	if err := c.checkSize(set, del); err != nil {
		return err
	}
	if err := c.store.Patch(ctx, set, del); err != nil {
		return err
	}
	// Read-your-own-writes locally, without a round trip.
	if cur := c.cache.Current(); cur != nil {
		c.cache.Set(cur.With(set, del))
	}
	return nil
}

// checkSize projects the write onto the current snapshot and rejects it if
// the result would exceed the ceiling. Without a cached snapshot the write is
// allowed through and the API server has the final say.
func (c *Committer) checkSize(set map[string][]byte, del []string) error {
	cur := c.cache.Current()
	if cur == nil {
		for _, v := range set {
			if len(v) > MaxObjectBytes {
				return ErrTooLarge
			}
		}
		return nil
	}
	if cur.With(set, del).Size() > MaxObjectBytes {
		return ErrTooLarge
	}
	return nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./pkg/objectfs/ -run TestCommit -race -v`
Expected: PASS, all five tests.

- [ ] **Step 5: Commit**

```bash
git add pkg/objectfs/commit.go pkg/objectfs/commit_test.go
git commit -m "$(cat <<'EOF'
objectfs: committer with size check and lease fencing

Thin by design: a merge patch mentions only the keys we touched, so keys we
never wrote are preserved by the request itself. No read-modify-write, no
CAS, no retry loop — TestCommitDoesNotRetry guards against reintroducing one.

Fences on an unhealthy lease so a write never lands under a lock that may
already belong to someone else.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
EOF
)"
```

---

### Task 9: Pod token extraction and client construction

Turns the volume context kubelet hands us into a Kubernetes client
authenticated as the consuming pod. This is where the "zero driver RBAC"
property is actually implemented.

**Files:**
- Create: `pkg/podtoken/token.go`
- Test: `pkg/podtoken/token_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `func Extract(volumeContext map[string]string) (token string, err error)`
  - `func PodIdentity(volumeContext map[string]string) (namespace, serviceAccount, uid string, err error)`
  - `func ClientFor(token string) (kubernetes.Interface, error)`
  - `const KeyTokens = "csi.storage.k8s.io/serviceAccount.tokens"` and the pod-info keys

- [ ] **Step 1: Write the failing test**

Create `pkg/podtoken/token_test.go`:

```go
package podtoken

import "testing"

func ctxWithToken(tokens string) map[string]string {
	return map[string]string{
		KeyTokens:         tokens,
		KeyPodNamespace:   "my-app",
		KeyPodServiceAcct: "session-runner",
		KeyPodUID:         "abc-123",
	}
}

func TestExtractDefaultAudience(t *testing.T) {
	got, err := Extract(ctxWithToken(`{"":{"token":"tok-abc","expirationTimestamp":"2026-08-20T12:00:00Z"}}`))
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if got != "tok-abc" {
		t.Fatalf("token = %q, want tok-abc", got)
	}
}

// CSIDriver requests audience "", but tolerate a driver configured with a
// named audience rather than failing the mount.
func TestExtractFallsBackToSoleAudience(t *testing.T) {
	got, err := Extract(ctxWithToken(`{"api":{"token":"tok-xyz","expirationTimestamp":"2026-08-20T12:00:00Z"}}`))
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if got != "tok-xyz" {
		t.Fatalf("token = %q, want tok-xyz", got)
	}
}

func TestExtractErrors(t *testing.T) {
	tests := []struct {
		name string
		ctx  map[string]string
	}{
		{"missing key", map[string]string{}},
		{"empty value", ctxWithToken("")},
		{"not json", ctxWithToken("{{{")},
		{"no audiences", ctxWithToken("{}")},
		{"empty token", ctxWithToken(`{"":{"token":""}}`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Extract(tt.ctx); err == nil {
				t.Fatal("Extract succeeded, want error")
			}
		})
	}
}

// The error must never contain the token itself — it is surfaced verbatim in
// a kubectl describe pod event.
func TestExtractErrorDoesNotLeakToken(t *testing.T) {
	_, err := Extract(ctxWithToken(`{"":{"token":"super-secret-value"`))
	if err == nil {
		t.Fatal("want error")
	}
	if contains(err.Error(), "super-secret-value") {
		t.Fatalf("error leaked the token: %v", err)
	}
}

func TestPodIdentity(t *testing.T) {
	ns, sa, uid, err := PodIdentity(ctxWithToken(`{"":{"token":"t"}}`))
	if err != nil {
		t.Fatalf("PodIdentity: %v", err)
	}
	if ns != "my-app" || sa != "session-runner" || uid != "abc-123" {
		t.Fatalf("got %q %q %q", ns, sa, uid)
	}
}

func TestPodIdentityRequiresPodInfoOnMount(t *testing.T) {
	if _, _, _, err := PodIdentity(map[string]string{}); err == nil {
		t.Fatal("want error when podInfoOnMount fields are absent")
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) &&
		(haystack == needle || len(needle) == 0 ||
			indexOf(haystack, needle) >= 0)
}

func indexOf(h, n string) int {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return i
		}
	}
	return -1
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./pkg/podtoken/ -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Write the implementation**

Create `pkg/podtoken/token.go`:

```go
// Package podtoken turns the volume context kubelet supplies into a
// Kubernetes client authenticated as the consuming pod.
//
// This is where roommate's central security property lives: the driver's own
// ServiceAccount holds no API permissions, so every call must carry a token
// kubelet minted for the pod being served. A pod can therefore only reach
// objects its own RBAC already allows.
package podtoken

import (
	"encoding/json"
	"errors"
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Volume context keys populated by kubelet. The token key requires
// CSIDriver.spec.tokenRequests; the pod-info keys require podInfoOnMount.
const (
	KeyTokens         = "csi.storage.k8s.io/serviceAccount.tokens"
	KeyPodNamespace   = "csi.storage.k8s.io/pod.namespace"
	KeyPodName        = "csi.storage.k8s.io/pod.name"
	KeyPodUID         = "csi.storage.k8s.io/pod.uid"
	KeyPodServiceAcct = "csi.storage.k8s.io/pod.service-account.name"
)

// ErrNoToken means kubelet supplied no usable token, which almost always
// means CSIDriver.spec.tokenRequests is missing or misconfigured.
var ErrNoToken = errors.New("roommate: no service account token in volume context")

type tokenEntry struct {
	Token               string `json:"token"`
	ExpirationTimestamp string `json:"expirationTimestamp"`
}

// Extract returns the pod's bearer token. It prefers the default audience
// ("" — what our CSIDriver requests) and falls back to the sole entry if the
// driver was configured with a named audience instead.
//
// Errors never include the raw payload: this message is surfaced verbatim in
// a FailedMount event.
func Extract(volumeContext map[string]string) (string, error) {
	raw, ok := volumeContext[KeyTokens]
	if !ok || raw == "" {
		return "", ErrNoToken
	}
	var byAudience map[string]tokenEntry
	if err := json.Unmarshal([]byte(raw), &byAudience); err != nil {
		return "", fmt.Errorf("%w: token payload is not valid JSON", ErrNoToken)
	}
	if entry, ok := byAudience[""]; ok && entry.Token != "" {
		return entry.Token, nil
	}
	if len(byAudience) == 1 {
		for _, entry := range byAudience {
			if entry.Token != "" {
				return entry.Token, nil
			}
		}
	}
	return "", ErrNoToken
}

// PodIdentity returns the consuming pod's namespace, ServiceAccount name, and
// UID. All three require podInfoOnMount: true on the CSIDriver object.
func PodIdentity(volumeContext map[string]string) (namespace, serviceAccount, uid string, err error) {
	namespace = volumeContext[KeyPodNamespace]
	serviceAccount = volumeContext[KeyPodServiceAcct]
	uid = volumeContext[KeyPodUID]
	if namespace == "" || serviceAccount == "" || uid == "" {
		return "", "", "", errors.New(
			"roommate: pod identity missing from volume context; " +
				"set podInfoOnMount: true on the CSIDriver object")
	}
	return namespace, serviceAccount, uid, nil
}

// ClientFor builds a client authenticated as the pod.
//
// It starts from the in-cluster config for the API server address and CA
// only, then replaces every credential field. Leaving the driver's own
// ServiceAccount token in place would silently restore the confused-deputy
// problem this package exists to prevent.
func ClientFor(token string) (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}
	cfg.BearerToken = token
	cfg.BearerTokenFile = ""
	cfg.Username, cfg.Password = "", ""
	cfg.CertFile, cfg.KeyFile = "", ""
	cfg.CertData, cfg.KeyData = nil, nil
	cfg.AuthProvider = nil
	cfg.ExecProvider = nil
	return kubernetes.NewForConfig(cfg)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./pkg/podtoken/ -v`
Expected: PASS, all tests. `ClientFor` is not unit-tested — it needs a real
in-cluster environment and is covered by e2e in Task 19.

- [ ] **Step 5: Commit**

```bash
git add pkg/podtoken
git commit -m "$(cat <<'EOF'
podtoken: build a client authenticated as the consuming pod

Where the zero-driver-RBAC property is actually implemented. ClientFor
starts from the in-cluster config for the API server address and CA only,
then clears every credential field — leaving the driver's own token in place
would silently restore the confused deputy this package exists to prevent.

Extract never includes the raw payload in an error, since that message is
surfaced verbatim in a FailedMount event.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
EOF
)"
```

---

### Task 10: Volume — one mountable unit

Bundles a store, cache, committer, and lease factory so the FUSE layer has a
single dependency, and holds the mount-time configuration.

**Files:**
- Create: `pkg/objectfs/volume.go`
- Test: `pkg/objectfs/volume_test.go`

**Interfaces:**
- Consumes: `Store`, `Cache`, `Committer`, `LeaseManager`.
- Produces:
  - `type Config struct { ObjectKind, ObjectName, LeaseName, Namespace string; FileMode, DirMode uint32; UID, GID uint32; StalenessBound, LeaseDuration time.Duration }`
  - `func ParseConfig(volumeContext map[string]string, namespace string) (Config, error)`
  - `func NewVolume(client kubernetes.Interface, cfg Config) (*Volume, error)`
  - `func (v *Volume) Run(ctx)`, `func (v *Volume) NewLease() *LeaseManager`

- [ ] **Step 1: Write the failing test**

Create `pkg/objectfs/volume_test.go`:

```go
package objectfs

import (
	"testing"
	"time"
)

func TestParseConfigDefaults(t *testing.T) {
	cfg, err := ParseConfig(map[string]string{
		"objectKind": "Secret",
		"objectName": "oauth-credentials",
	}, "my-app")
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.LeaseName != "roommate-oauth-credentials" {
		t.Errorf("LeaseName = %q, want roommate-oauth-credentials", cfg.LeaseName)
	}
	if cfg.FileMode != 0o600 {
		t.Errorf("FileMode = %o, want 600", cfg.FileMode)
	}
	if cfg.DirMode != 0o700 {
		t.Errorf("DirMode = %o, want 700", cfg.DirMode)
	}
	if cfg.StalenessBound != DefaultStalenessBound {
		t.Errorf("StalenessBound = %v, want %v", cfg.StalenessBound, DefaultStalenessBound)
	}
	if cfg.LeaseDuration != DefaultLeaseDuration {
		t.Errorf("LeaseDuration = %v, want %v", cfg.LeaseDuration, DefaultLeaseDuration)
	}
	// Namespace always comes from pod info, never from attributes.
	if cfg.Namespace != "my-app" {
		t.Errorf("Namespace = %q, want my-app", cfg.Namespace)
	}
}

func TestParseConfigOverrides(t *testing.T) {
	cfg, err := ParseConfig(map[string]string{
		"objectKind":            "ConfigMap",
		"objectName":            "app-config",
		"leaseName":             "custom-lease",
		"fileMode":              "0644",
		"dirMode":               "0755",
		"uid":                   "1000",
		"gid":                   "2000",
		"stalenessBoundSeconds": "5",
		"leaseDurationSeconds":  "30",
	}, "my-app")
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.LeaseName != "custom-lease" {
		t.Errorf("LeaseName = %q", cfg.LeaseName)
	}
	if cfg.FileMode != 0o644 || cfg.DirMode != 0o755 {
		t.Errorf("modes = %o %o", cfg.FileMode, cfg.DirMode)
	}
	if cfg.UID != 1000 || cfg.GID != 2000 {
		t.Errorf("uid/gid = %d %d", cfg.UID, cfg.GID)
	}
	if cfg.StalenessBound != 5*time.Second {
		t.Errorf("StalenessBound = %v", cfg.StalenessBound)
	}
	if cfg.LeaseDuration != 30*time.Second {
		t.Errorf("LeaseDuration = %v", cfg.LeaseDuration)
	}
}

func TestParseConfigRejectsBadInput(t *testing.T) {
	tests := []struct {
		name string
		attr map[string]string
		ns   string
	}{
		{"missing kind", map[string]string{"objectName": "x"}, "my-app"},
		{"missing name", map[string]string{"objectKind": "Secret"}, "my-app"},
		{"bad kind", map[string]string{"objectKind": "Pod", "objectName": "x"}, "my-app"},
		{"bad object name", map[string]string{"objectKind": "Secret", "objectName": "Not_Valid"}, "my-app"},
		{"bad file mode", map[string]string{"objectKind": "Secret", "objectName": "x", "fileMode": "zzz"}, "my-app"},
		{"missing namespace", map[string]string{"objectKind": "Secret", "objectName": "x"}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseConfig(tt.attr, tt.ns); err == nil {
				t.Fatal("ParseConfig succeeded, want error")
			}
		})
	}
}

// A namespace attribute must not be honoured: cross-namespace access is a
// non-goal, and silently accepting one would be a security hole.
func TestParseConfigIgnoresNamespaceAttribute(t *testing.T) {
	cfg, err := ParseConfig(map[string]string{
		"objectKind": "Secret",
		"objectName": "oauth-credentials",
		"namespace":  "kube-system",
	}, "my-app")
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	if cfg.Namespace != "my-app" {
		t.Fatalf("Namespace = %q, want my-app — attribute must be ignored", cfg.Namespace)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./pkg/objectfs/ -run TestParseConfig -v`
Expected: FAIL — `undefined: ParseConfig`

- [ ] **Step 3: Write the implementation**

Create `pkg/objectfs/volume.go`:

```go
package objectfs

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"k8s.io/client-go/kubernetes"
)

// Object kinds accepted in volumeAttributes.
const (
	KindSecret    = "Secret"
	KindConfigMap = "ConfigMap"
)

// Config is the parsed shape of an inline volume's volumeAttributes.
type Config struct {
	ObjectKind string
	ObjectName string
	LeaseName  string
	Namespace  string // always the consuming pod's; never from attributes

	FileMode uint32
	DirMode  uint32
	UID      uint32
	GID      uint32

	StalenessBound time.Duration
	LeaseDuration  time.Duration
}

// ParseConfig validates volumeAttributes and applies defaults.
//
// namespace comes from podInfoOnMount, not from attributes: honouring a
// caller-supplied namespace would allow a pod to aim the mount at another
// namespace's object, and cross-namespace access is a non-goal.
func ParseConfig(attr map[string]string, namespace string) (Config, error) {
	if namespace == "" {
		return Config{}, fmt.Errorf("roommate: pod namespace is required")
	}
	cfg := Config{
		Namespace:      namespace,
		FileMode:       0o600,
		DirMode:        0o700,
		StalenessBound: DefaultStalenessBound,
		LeaseDuration:  DefaultLeaseDuration,
	}

	cfg.ObjectKind = attr["objectKind"]
	if cfg.ObjectKind != KindSecret && cfg.ObjectKind != KindConfigMap {
		return Config{}, fmt.Errorf(
			"roommate: objectKind must be %q or %q, got %q",
			KindSecret, KindConfigMap, cfg.ObjectKind)
	}

	cfg.ObjectName = attr["objectName"]
	if !ValidKey(cfg.ObjectName) {
		return Config{}, fmt.Errorf("roommate: objectName %q is not a valid object name", cfg.ObjectName)
	}

	cfg.LeaseName = attr["leaseName"]
	if cfg.LeaseName == "" {
		cfg.LeaseName = "roommate-" + cfg.ObjectName
	}

	var err error
	if cfg.FileMode, err = parseMode(attr, "fileMode", cfg.FileMode); err != nil {
		return Config{}, err
	}
	if cfg.DirMode, err = parseMode(attr, "dirMode", cfg.DirMode); err != nil {
		return Config{}, err
	}
	if cfg.UID, err = parseUint32(attr, "uid", 0); err != nil {
		return Config{}, err
	}
	if cfg.GID, err = parseUint32(attr, "gid", 0); err != nil {
		return Config{}, err
	}
	if cfg.StalenessBound, err = parseSeconds(attr, "stalenessBoundSeconds", cfg.StalenessBound); err != nil {
		return Config{}, err
	}
	if cfg.LeaseDuration, err = parseSeconds(attr, "leaseDurationSeconds", cfg.LeaseDuration); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func parseMode(attr map[string]string, key string, def uint32) (uint32, error) {
	raw, ok := attr[key]
	if !ok || raw == "" {
		return def, nil
	}
	n, err := strconv.ParseUint(raw, 8, 32)
	if err != nil {
		return 0, fmt.Errorf("roommate: %s %q is not an octal mode", key, raw)
	}
	return uint32(n), nil
}

func parseUint32(attr map[string]string, key string, def uint32) (uint32, error) {
	raw, ok := attr[key]
	if !ok || raw == "" {
		return def, nil
	}
	n, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("roommate: %s %q is not a number", key, raw)
	}
	return uint32(n), nil
}

func parseSeconds(attr map[string]string, key string, def time.Duration) (time.Duration, error) {
	raw, ok := attr[key]
	if !ok || raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("roommate: %s %q is not a positive number of seconds", key, raw)
	}
	return time.Duration(n) * time.Second, nil
}

// Volume is one mounted object: its store, cache, committer, and the factory
// for the per-handle leases that serialise writers.
type Volume struct {
	Cfg       Config
	Store     Store
	Cache     *Cache
	Committer *Committer

	client    kubernetes.Interface
	holderSeq atomic.Uint64
	podUID    string
}

// NewVolume assembles a Volume. client must be authenticated as the
// consuming pod.
func NewVolume(client kubernetes.Interface, cfg Config, podUID string) (*Volume, error) {
	var store Store
	switch cfg.ObjectKind {
	case KindSecret:
		store = NewSecretStore(client, cfg.Namespace, cfg.ObjectName)
	case KindConfigMap:
		store = NewConfigMapStore(client, cfg.Namespace, cfg.ObjectName)
	default:
		return nil, fmt.Errorf("roommate: unsupported objectKind %q", cfg.ObjectKind)
	}
	cache := NewCache(store, cfg.StalenessBound)
	return &Volume{
		Cfg:       cfg,
		Store:     store,
		Cache:     cache,
		Committer: NewCommitter(store, cache),
		client:    client,
		podUID:    podUID,
	}, nil
}

// Run drives the watch loop until ctx is cancelled.
func (v *Volume) Run(ctx context.Context) { v.Cache.Run(ctx) }

// NewLease returns a LeaseManager for one open handle. Holder identity must
// be unique per holder, so each handle gets its own sequence number — two
// processes in the same pod are distinct lock holders.
func (v *Volume) NewLease() *LeaseManager {
	holder := fmt.Sprintf("%s:%d", v.podUID, v.holderSeq.Add(1))
	return NewLeaseManager(v.client, v.Cfg.Namespace, v.Cfg.LeaseName, holder, v.Cfg.LeaseDuration)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./pkg/objectfs/ -run TestParseConfig -v`
Expected: PASS, all four tests.

- [ ] **Step 5: Commit**

```bash
git add pkg/objectfs/volume.go pkg/objectfs/volume_test.go
git commit -m "$(cat <<'EOF'
objectfs: Volume wiring and volumeAttributes parsing

Namespace comes from podInfoOnMount and a namespace attribute is ignored
outright — honouring one would let a pod aim the mount at another
namespace's object, and cross-namespace access is a non-goal.

Each handle gets its own lease holder identity, so two processes in the same
pod are distinct lock holders.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
EOF
)"
```

---

### Task 11: FUSE read path

First task that mounts a real filesystem. Tests drive it with ordinary
syscalls, so they stay correct regardless of go-fuse's internal API shape.

**Requires `/dev/fuse`.** GitHub's `ubuntu-latest` runners provide it. If
`/dev/fuse` is absent locally, these tests skip rather than fail.

**Files:**
- Create: `pkg/objectfs/fs.go`, `pkg/objectfs/handle.go`, `pkg/objectfs/mount.go`
- Test: `pkg/objectfs/fs_read_test.go`, `pkg/objectfs/fstest_helper_test.go`

**Interfaces:**
- Consumes: `Volume` (Task 10), `Snapshot` (Task 3).
- Produces:
  - `func Mount(dir string, v *Volume) (*fuse.Server, error)`
  - `type Root struct{ fs.Inode; vol *Volume }`
  - `type File struct{ fs.Inode; vol *Volume; key string }`
  - `type handle struct{ ... }`

- [ ] **Step 1: Read the go-fuse API before writing code**

go-fuse's interface names must be matched exactly or the methods are silently
ignored — the filesystem mounts and then behaves as read-only or empty, which
is a confusing failure. Before implementing, read:

```bash
go doc github.com/hanwen/go-fuse/v2/fs Inode
go doc github.com/hanwen/go-fuse/v2/fs NodeReaddirer
go doc github.com/hanwen/go-fuse/v2/fs NodeLookuper
go doc github.com/hanwen/go-fuse/v2/fs NodeGetattrer
go doc github.com/hanwen/go-fuse/v2/fs NodeOpener
go doc github.com/hanwen/go-fuse/v2/fs FileReader
go doc github.com/hanwen/go-fuse/v2/fs Mount
go doc github.com/hanwen/go-fuse/v2/fuse MountOptions
```

Confirm each interface's exact method signature and adjust the code below to
match if the library has moved on. The tests are syscall-level and will not
need changing.

- [ ] **Step 2: Write the test helper**

Create `pkg/objectfs/fstest_helper_test.go`:

```go
package objectfs

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// mountForTest mounts a volume backed by an in-memory store on a temp dir and
// returns the mountpoint. It skips when /dev/fuse is unavailable.
func mountForTest(t *testing.T, data map[string][]byte) (string, *recordingStore) {
	t.Helper()
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("/dev/fuse unavailable; skipping FUSE test")
	}

	store := newRecordingStore(data)
	cfg := Config{
		Namespace: "my-app", ObjectKind: KindSecret, ObjectName: "oauth-credentials",
		LeaseName: "roommate-oauth-credentials",
		FileMode:  0o600, DirMode: 0o700,
		StalenessBound: time.Hour, LeaseDuration: testLeaseDur,
	}
	cache := NewCache(store, cfg.StalenessBound)
	if _, err := cache.Fresh(context.Background()); err != nil {
		t.Fatalf("warm cache: %v", err)
	}
	vol := &Volume{
		Cfg: cfg, Store: store, Cache: cache,
		Committer: NewCommitter(store, cache),
		podUID:    "test-pod-uid",
	}

	dir := t.TempDir()
	srv, err := Mount(dir, vol)
	if err != nil {
		t.Fatalf("Mount: %v", err)
	}
	t.Cleanup(func() {
		if err := srv.Unmount(); err != nil {
			t.Logf("unmount: %v", err)
		}
	})
	waitMounted(t, srv)
	return dir, store
}

func waitMounted(t *testing.T, srv *fuse.Server) {
	t.Helper()
	done := make(chan struct{})
	go func() { srv.WaitMount(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("mount did not become ready")
	}
}
```

- [ ] **Step 3: Write the failing read tests**

Create `pkg/objectfs/fs_read_test.go`:

```go
package objectfs

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestReaddirListsKeys(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{
		"config.json":       []byte("{}"),
		".credentials.json": []byte("creds"),
		"session.key":       []byte("v1"),
	})

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var got []string
	for _, e := range entries {
		got = append(got, e.Name())
	}
	sort.Strings(got)
	want := []string{".credentials.json", "config.json", "session.key"}
	if len(got) != len(want) {
		t.Fatalf("entries = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("entries = %v, want %v", got, want)
		}
	}
}

func TestReadFileContents(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"session.key": []byte("hello world")})

	got, err := os.ReadFile(filepath.Join(dir, "session.key"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "hello world" {
		t.Fatalf("contents = %q, want hello world", got)
	}
}

func TestStatReportsSizeAndMode(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"session.key": []byte("12345")})

	fi, err := os.Stat(filepath.Join(dir, "session.key"))
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size() != 5 {
		t.Errorf("Size() = %d, want 5", fi.Size())
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Errorf("Mode() = %o, want 600", perm)
	}
	if fi.IsDir() {
		t.Error("regular file reported as a directory")
	}
}

func TestStatMissingFile(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"a": []byte("1")})

	if _, err := os.Stat(filepath.Join(dir, "nope")); !os.IsNotExist(err) {
		t.Fatalf("Stat(nope) err = %v, want IsNotExist", err)
	}
}

// A handle pins its snapshot at open. A concurrent remote change must not be
// visible through it — this is what reproduces kubelet's atomic ..data flip
// and prevents a read splicing two versions together.
func TestOpenHandlePinsSnapshot(t *testing.T) {
	dir, store := mountForTest(t, map[string][]byte{"session.key": []byte("v1")})

	f, err := os.Open(filepath.Join(dir, "session.key"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	// Remote change lands after open.
	store.mu.Lock()
	store.snap = &Snapshot{Data: map[string][]byte{"session.key": []byte("v2")}, ResourceVersion: "2"}
	store.mu.Unlock()

	buf := make([]byte, 16)
	n, err := f.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(buf[:n]) != "v1" {
		t.Fatalf("read = %q, want v1 — the handle must keep its pinned snapshot", buf[:n])
	}
}

func TestSubdirectoriesAreRejected(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"a": []byte("1")})

	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o700); err == nil {
		t.Fatal("Mkdir succeeded; object keys are flat so it must fail")
	}
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `go test ./pkg/objectfs/ -run 'TestReaddir|TestRead|TestStat|TestOpen|TestSubdir' -v`
Expected: FAIL — `undefined: Mount`

- [ ] **Step 5: Write the mount entrypoint**

Create `pkg/objectfs/mount.go`:

```go
package objectfs

import (
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Mount serves v at dir and returns the running server. The caller unmounts
// via Server.Unmount.
//
// AllowOther is required because the FUSE server runs as root in the
// DaemonSet while consuming processes usually do not. Mounting as root does
// not need user_allow_other in /etc/fuse.conf.
//
// EnableLocks makes the kernel negotiate FUSE_CAP_FLOCK_LOCKS, without which
// flock never reaches the filesystem at all.
func Mount(dir string, v *Volume) (*fuse.Server, error) {
	root := &Root{vol: v}
	return fs.Mount(dir, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther:  true,
			FsName:      v.Cfg.ObjectName,
			Name:        "roommate",
			EnableLocks: true,
		},
	})
}
```

- [ ] **Step 6: Write the node types**

Create `pkg/objectfs/fs.go`:

```go
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

func (r *Root) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR | r.vol.Cfg.DirMode
	out.Owner.Uid = r.vol.Cfg.UID
	out.Owner.Gid = r.vol.Cfg.GID
	return 0
}

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

func (r *Root) fillFileAttr(attr *fuse.Attr, size int) {
	attr.Mode = fuse.S_IFREG | r.vol.Cfg.FileMode
	attr.Size = uint64(size)
	attr.Owner.Uid = r.vol.Cfg.UID
	attr.Owner.Gid = r.vol.Cfg.GID
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
	out.Owner.Uid = f.vol.Cfg.UID
	out.Owner.Gid = f.vol.Cfg.GID
	out.Nlink = 1
	return 0
}

// Open pins the current snapshot into the handle. Every read from this handle
// is served from that pinned value for its whole lifetime, reproducing the
// atomicity of kubelet's ..data flip: a handle never observes a partial
// transition, and a read can never splice two versions together.
func (f *File) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
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
```

- [ ] **Step 7: Write the handle type**

Create `pkg/objectfs/handle.go`:

```go
package objectfs

import (
	"context"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// handle is one open file description.
//
// It owns three things that must stay consistent: the snapshot pinned at
// open, the write buffer that has not yet been committed, and the Lease if
// this handle took one.
type handle struct {
	vol *Volume
	key string

	mu    sync.Mutex
	snap  *Snapshot
	buf   []byte
	dirty bool

	lease *LeaseManager
}

var _ fs.FileReader = (*handle)(nil)

func newHandle(v *Volume, key string, snap *Snapshot, value []byte) *handle {
	return &handle{vol: v, key: key, snap: snap, buf: value}
}

func (h *handle) Read(_ context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if off < 0 || off >= int64(len(h.buf)) {
		return fuse.ReadResultData(nil), 0
	}
	end := off + int64(len(dest))
	if end > int64(len(h.buf)) {
		end = int64(len(h.buf))
	}
	return fuse.ReadResultData(h.buf[off:end]), 0
}
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `go test ./pkg/objectfs/ -run 'TestReaddir|TestRead|TestStat|TestOpen|TestSubdir' -v`
Expected: PASS, all six tests. If they skip, `/dev/fuse` is missing — run in
a container with `--device /dev/fuse` or rely on CI.

- [ ] **Step 9: Commit**

```bash
git add pkg/objectfs/fs.go pkg/objectfs/handle.go pkg/objectfs/mount.go \
        pkg/objectfs/fs_read_test.go pkg/objectfs/fstest_helper_test.go
git commit -m "$(cat <<'EOF'
objectfs: FUSE read path

Root lists object keys as a flat directory; mkdir is ENOTSUP because keys
cannot nest. Open pins the current snapshot into the handle, so every read
from that handle sees one coherent version for its lifetime — reproducing
kubelet's atomic ..data flip and preventing a read from splicing two
versions together.

Tests drive real syscalls against a real mount, so they stay valid across
go-fuse API changes. They skip when /dev/fuse is unavailable.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
EOF
)"
```

---

### Task 12: FUSE write path

Writes buffer in the handle and commit on `flush`, `fsync`, or `release`.
There is no time-based writeback: a handle held open forever and never synced
never commits, exactly as on a local filesystem.

**Files:**
- Modify: `pkg/objectfs/fs.go`, `pkg/objectfs/handle.go`
- Test: `pkg/objectfs/fs_write_test.go`

**Interfaces:**
- Consumes: `handle`, `Root`, `File` (Task 11), `Committer` (Task 8).
- Produces: `Root.Create`, `Root.Unlink`, `Root.Rename`, `File.Setattr`,
  `handle.Write`, `handle.Flush`, `handle.Fsync`, `handle.Release`.

- [ ] **Step 1: Confirm the go-fuse write interfaces**

```bash
go doc github.com/hanwen/go-fuse/v2/fs NodeCreater
go doc github.com/hanwen/go-fuse/v2/fs NodeUnlinker
go doc github.com/hanwen/go-fuse/v2/fs NodeRenamer
go doc github.com/hanwen/go-fuse/v2/fs NodeSetattrer
go doc github.com/hanwen/go-fuse/v2/fs FileWriter
go doc github.com/hanwen/go-fuse/v2/fs FileFlusher
go doc github.com/hanwen/go-fuse/v2/fs FileReleaser
go doc github.com/hanwen/go-fuse/v2/fs FileFsyncer
```

- [ ] **Step 2: Write the failing tests**

Create `pkg/objectfs/fs_write_test.go`:

```go
package objectfs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteCommitsOnClose(t *testing.T) {
	dir, store := mountForTest(t, map[string][]byte{"session.key": []byte("v1")})

	if err := os.WriteFile(filepath.Join(dir, "session.key"), []byte("v2"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if store.patches == 0 {
		t.Fatal("close did not commit")
	}
	if got := string(store.set["session.key"]); got != "v2" {
		t.Fatalf("patched value = %q, want v2", got)
	}
}

// A merge patch must mention only what changed — that is what makes
// concurrent writes to different keys structurally safe.
func TestWritePatchesOnlyTheWrittenKey(t *testing.T) {
	dir, store := mountForTest(t, map[string][]byte{
		"session.key": []byte("v1"),
		"config.json": []byte("{}"),
	})

	if err := os.WriteFile(filepath.Join(dir, "session.key"), []byte("v2"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, ok := store.set["config.json"]; ok {
		t.Fatal("patch mentioned config.json, which was never written")
	}
	if len(store.set) != 1 {
		t.Fatalf("patch set = %v, want only session.key", store.set)
	}
}

func TestCreateNewFile(t *testing.T) {
	dir, store := mountForTest(t, map[string][]byte{"a": []byte("1")})

	if err := os.WriteFile(filepath.Join(dir, "new.json"), []byte("{}"), 0o600); err != nil {
		t.Fatalf("create: %v", err)
	}
	if got := string(store.set["new.json"]); got != "{}" {
		t.Fatalf("new.json = %q, want {}", got)
	}
}

func TestCreateRejectsInvalidKey(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"a": []byte("1")})

	err := os.WriteFile(filepath.Join(dir, "not valid"), []byte("x"), 0o600)
	if err == nil {
		t.Fatal("created a file whose name is not a valid object key")
	}
}

func TestUnlinkDeletesKey(t *testing.T) {
	dir, store := mountForTest(t, map[string][]byte{"a": []byte("1"), "b": []byte("2")})

	if err := os.Remove(filepath.Join(dir, "b")); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if len(store.del) != 1 || store.del[0] != "b" {
		t.Fatalf("del = %v, want [b]", store.del)
	}
	if len(store.set) != 0 {
		t.Fatalf("unlink also set keys: %v", store.set)
	}
}

func TestRenameIsCopyPlusDelete(t *testing.T) {
	dir, store := mountForTest(t, map[string][]byte{"old.json": []byte("data")})

	if err := os.Rename(filepath.Join(dir, "old.json"), filepath.Join(dir, "new.json")); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if got := string(store.set["new.json"]); got != "data" {
		t.Fatalf("new.json = %q, want data", got)
	}
	if len(store.del) != 1 || store.del[0] != "old.json" {
		t.Fatalf("del = %v, want [old.json]", store.del)
	}
}

// Modes come from mount attributes and are not stored in the object, so the
// object stays interoperable with kubelet projection and kubectl edit.
func TestChmodIsRefused(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"a": []byte("1")})

	if err := os.Chmod(filepath.Join(dir, "a"), 0o644); err == nil {
		t.Fatal("chmod succeeded; modes come from mount attributes")
	}
}

func TestWriteBeyondCeilingReturnsENOSPC(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"a": []byte("1")})

	big := []byte(strings.Repeat("x", MaxObjectBytes+1))
	err := os.WriteFile(filepath.Join(dir, "a"), big, 0o600)
	if err == nil {
		t.Fatal("oversized write succeeded")
	}
	if !strings.Contains(err.Error(), "no space") {
		t.Fatalf("err = %v, want ENOSPC", err)
	}
}

// Nothing reaches the API server until flush/close: there is no time-based
// writeback.
func TestNoWritebackBeforeClose(t *testing.T) {
	dir, store := mountForTest(t, map[string][]byte{"a": []byte("1")})

	f, err := os.OpenFile(filepath.Join(dir, "a"), os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write([]byte("2")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if store.patches != 0 {
		t.Fatal("write reached the API server before flush")
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if store.patches == 0 {
		t.Fatal("close did not commit")
	}
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test ./pkg/objectfs/ -run 'TestWrite|TestCreate|TestUnlink|TestRename|TestChmod|TestNoWriteback' -v`
Expected: FAIL — writes return `EROFS` or a missing-method error.

- [ ] **Step 4: Extend the handle with the write path**

Append to `pkg/objectfs/handle.go`:

```go
var (
	_ fs.FileWriter   = (*handle)(nil)
	_ fs.FileFlusher  = (*handle)(nil)
	_ fs.FileFsyncer  = (*handle)(nil)
	_ fs.FileReleaser = (*handle)(nil)
)

// Write buffers into the handle. Nothing reaches the API server until flush,
// fsync, or release — there is no time-based writeback, exactly as a local
// filesystem defers to its page cache.
func (h *handle) Write(_ context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if off < 0 {
		return 0, syscall.EINVAL
	}
	end := int(off) + len(data)
	if end > len(h.buf) {
		grown := make([]byte, end)
		copy(grown, h.buf)
		h.buf = grown
	}
	copy(h.buf[off:], data)
	h.dirty = true
	return uint32(len(data)), 0
}

// Truncate resizes the buffer. Called via Setattr when a file is opened with
// O_TRUNC, which is what os.WriteFile does.
func (h *handle) truncate(size uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	switch {
	case int(size) < len(h.buf):
		h.buf = h.buf[:size]
	case int(size) > len(h.buf):
		grown := make([]byte, size)
		copy(grown, h.buf)
		h.buf = grown
	default:
		return
	}
	h.dirty = true
}

func (h *handle) Flush(ctx context.Context) syscall.Errno {
	return h.commit(ctx)
}

func (h *handle) Fsync(ctx context.Context, _ uint32) syscall.Errno {
	return h.commit(ctx)
}

// Release commits any outstanding write and then releases the Lease. The
// order is load-bearing: the next holder's mandatory quorum read must be able
// to observe our write, so releasing first would reintroduce the very race
// this driver exists to close.
func (h *handle) Release(ctx context.Context) syscall.Errno {
	errno := h.commit(ctx)
	if h.lease != nil {
		_ = h.lease.Release(ctx)
		h.lease = nil
	}
	return errno
}

func (h *handle) commit(ctx context.Context) syscall.Errno {
	h.mu.Lock()
	if !h.dirty {
		h.mu.Unlock()
		return 0
	}
	value := make([]byte, len(h.buf))
	copy(value, h.buf)
	lease := h.lease
	h.mu.Unlock()

	err := h.vol.Committer.Commit(ctx, lease, map[string][]byte{h.key: value}, nil)
	if err != nil {
		return errnoFor(err)
	}

	h.mu.Lock()
	h.dirty = false
	h.mu.Unlock()
	return 0
}

// errnoFor maps a commit failure to the closest filesystem error, so a
// consumer sees a filesystem-shaped problem rather than an opaque API one.
func errnoFor(err error) syscall.Errno {
	switch {
	case errors.Is(err, ErrTooLarge):
		return syscall.ENOSPC
	case errors.Is(err, ErrLeaseLost):
		return syscall.EIO
	case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
		return syscall.EACCES
	case apierrors.IsNotFound(err):
		return syscall.ENOENT
	case apierrors.IsRequestEntityTooLargeError(err):
		return syscall.ENOSPC
	default:
		return syscall.EIO
	}
}
```

Add `"errors"` and `apierrors "k8s.io/apimachinery/pkg/api/errors"` to the
import block in `handle.go`.

- [ ] **Step 5: Extend the nodes with create, unlink, rename, setattr**

Append to `pkg/objectfs/fs.go`:

```go
var (
	_ fs.NodeCreater  = (*Root)(nil)
	_ fs.NodeUnlinker = (*Root)(nil)
	_ fs.NodeRenamer  = (*Root)(nil)
	_ fs.NodeSetattrer = (*File)(nil)
)

// Create adds a new key. The value is empty until the handle commits.
func (r *Root) Create(ctx context.Context, name string, flags, mode uint32, out *fuse.EntryOut) (
	*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	if !ValidKey(name) {
		return nil, nil, 0, syscall.EINVAL
	}
	child := &File{vol: r.vol, key: name}
	h := newHandle(r.vol, name, r.vol.Cache.Current(), nil)
	h.dirty = true // an empty create must still produce a key
	r.fillFileAttr(&out.Attr, 0)
	inode := r.NewInode(ctx, child, fs.StableAttr{Mode: fuse.S_IFREG})
	return inode, h, 0, 0
}

func (r *Root) Unlink(ctx context.Context, name string) syscall.Errno {
	if err := r.vol.Committer.Commit(ctx, nil, nil, []string{name}); err != nil {
		return errnoFor(err)
	}
	return 0
}

// Rename is a copy-key plus delete-key in one patch, since a key cannot move.
func (r *Root) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if newParent != fs.InodeEmbedder(r) {
		return syscall.EXDEV // flat namespace: there is nowhere else to go
	}
	if !ValidKey(newName) {
		return syscall.EINVAL
	}
	snap := r.vol.Cache.MaybeFresh(ctx)
	if snap == nil {
		return syscall.EIO
	}
	value, ok := snap.Get(name)
	if !ok {
		return syscall.ENOENT
	}
	err := r.vol.Committer.Commit(ctx, nil, map[string][]byte{newName: value}, []string{name})
	if err != nil {
		return errnoFor(err)
	}
	return 0
}

// Setattr handles the truncate half of O_TRUNC and refuses everything else.
// Modes and ownership come from mount attributes and are not stored in the
// object, so chmod cannot round-trip.
func (f *File) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if _, ok := in.GetMode(); ok {
		return syscall.EPERM
	}
	if _, ok := in.GetUID(); ok {
		return syscall.EPERM
	}
	if _, ok := in.GetGID(); ok {
		return syscall.EPERM
	}
	if size, ok := in.GetSize(); ok {
		h, ok := fh.(*handle)
		if !ok {
			return syscall.EINVAL
		}
		h.truncate(size)
		out.Size = size
	}
	out.Mode = fuse.S_IFREG | f.vol.Cfg.FileMode
	out.Owner.Uid = f.vol.Cfg.UID
	out.Owner.Gid = f.vol.Cfg.GID
	return 0
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test ./pkg/objectfs/ -run 'TestWrite|TestCreate|TestUnlink|TestRename|TestChmod|TestNoWriteback' -race -v`
Expected: PASS, all nine tests.

- [ ] **Step 7: Run the whole package**

Run: `go test ./... -race`
Expected: PASS. Read paths must still pass — in particular
`TestOpenHandlePinsSnapshot`, which the write buffer must not have broken.

- [ ] **Step 8: Commit**

```bash
git add pkg/objectfs/fs.go pkg/objectfs/handle.go pkg/objectfs/fs_write_test.go
git commit -m "$(cat <<'EOF'
objectfs: FUSE write path

Writes buffer in the handle and commit on flush, fsync, or release. There is
no time-based writeback: a handle held open and never synced never commits,
exactly as on a local filesystem.

Release commits before releasing the Lease. That order is load-bearing — the
next holder's mandatory quorum read must be able to observe our write, so
releasing first would reintroduce the refresh race this driver exists to
close.

chmod is EPERM because modes come from mount attributes and are not stored
in the object, keeping it interoperable with kubelet projection.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
EOF
)"
```

---

### Task 13: FUSE lock path — `flock` to Lease

The reason the project exists. Acquiring the lock forces a quorum read, which
is what makes the double-checked refresh pattern sound.

**Files:**
- Modify: `pkg/objectfs/handle.go`
- Test: `pkg/objectfs/fs_lock_test.go`

**Interfaces:**
- Consumes: `LeaseManager` (Task 7), `handle` (Task 11), `Volume.NewLease` (Task 10).
- Produces: `handle.Setlk`, `handle.Setlkw`, `handle.Getlk`.

- [ ] **Step 1: Confirm the go-fuse lock interfaces**

```bash
go doc github.com/hanwen/go-fuse/v2/fs FileSetlker
go doc github.com/hanwen/go-fuse/v2/fs FileSetlkwer
go doc github.com/hanwen/go-fuse/v2/fs FileGetlker
go doc github.com/hanwen/go-fuse/v2/fuse FileLock
```

Confirm how a flock-style request is distinguished from a POSIX range lock —
it is a bit in the `flags` argument (`fuse.FUSE_LK_FLOCK`). Confirm the
constant's exact name before using it.

- [ ] **Step 2: Write the failing tests**

Create `pkg/objectfs/fs_lock_test.go`:

```go
package objectfs

import (
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestFlockExclusiveAcquiresLease(t *testing.T) {
	dir, store := mountForTest(t, map[string][]byte{"session.key": []byte("v1")})

	f, err := os.Open(filepath.Join(dir, "session.key"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatalf("Flock: %v", err)
	}

	// Acquiring the lock must force a quorum read. That guaranteed-fresh read
	// is the entire read-after-write guarantee.
	if store.getCount() < 2 {
		t.Fatalf("gets = %d; acquiring the lock must force a fresh quorum read", store.getCount())
	}

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatalf("unlock: %v", err)
	}
}

func TestFlockNonBlockingFailsWhenHeld(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"session.key": []byte("v1")})
	path := filepath.Join(dir, "session.key")

	a, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open a: %v", err)
	}
	defer a.Close()
	if err := syscall.Flock(int(a.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatalf("Flock a: %v", err)
	}

	b, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open b: %v", err)
	}
	defer b.Close()

	err = syscall.Flock(int(b.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		t.Fatal("second LOCK_NB succeeded while the lease was held")
	}
	if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
		t.Fatalf("err = %v, want EWOULDBLOCK", err)
	}
}

func TestFlockReleasedOnClose(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"session.key": []byte("v1")})
	path := filepath.Join(dir, "session.key")

	a, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open a: %v", err)
	}
	if err := syscall.Flock(int(a.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatalf("Flock a: %v", err)
	}
	a.Close() // release without an explicit LOCK_UN

	b, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open b: %v", err)
	}
	defer b.Close()

	deadline := time.Now().Add(5 * time.Second)
	for {
		err = syscall.Flock(int(b.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("lease never released after close: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A shared lock maps to the same exclusive Lease. Over-strict, but a caller
// taking LOCK_SH is asking for "no writer is mid-write", and only the Lease
// can promise that.
func TestFlockSharedMapsToExclusive(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"session.key": []byte("v1")})
	path := filepath.Join(dir, "session.key")

	a, _ := os.Open(path)
	defer a.Close()
	if err := syscall.Flock(int(a.Fd()), syscall.LOCK_SH); err != nil {
		t.Fatalf("LOCK_SH: %v", err)
	}

	b, _ := os.Open(path)
	defer b.Close()
	if err := syscall.Flock(int(b.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err == nil {
		t.Fatal("two shared locks held at once; LOCK_SH must map to exclusive")
	}
}

// Commit must land before the Lease is released, so the next holder's
// mandatory fresh read can observe it.
func TestCommitHappensBeforeLeaseRelease(t *testing.T) {
	dir, store := mountForTest(t, map[string][]byte{"session.key": []byte("v1")})
	path := filepath.Join(dir, "session.key")

	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatalf("Flock: %v", err)
	}
	if _, err := f.Write([]byte("v2")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	f.Close()

	if store.patches == 0 {
		t.Fatal("no commit on close")
	}

	// The next acquirer must succeed and see the committed value.
	g, _ := os.Open(path)
	defer g.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := syscall.Flock(int(g.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lease never released")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Two handles racing for the lock: exactly one wins at a time.
func TestFlockSerialisesConcurrentHolders(t *testing.T) {
	dir, _ := mountForTest(t, map[string][]byte{"session.key": []byte("v1")})
	path := filepath.Join(dir, "session.key")

	var (
		mu      sync.Mutex
		holders int
		maxSeen int
		wg      sync.WaitGroup
	)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f, err := os.Open(path)
			if err != nil {
				return
			}
			defer f.Close()
			if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
				return
			}
			mu.Lock()
			holders++
			if holders > maxSeen {
				maxSeen = holders
			}
			mu.Unlock()

			time.Sleep(50 * time.Millisecond)

			mu.Lock()
			holders--
			mu.Unlock()
			_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		}()
	}
	wg.Wait()

	if maxSeen > 1 {
		t.Fatalf("saw %d concurrent lock holders, want at most 1", maxSeen)
	}
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test ./pkg/objectfs/ -run 'TestFlock|TestCommitHappens' -v`
Expected: FAIL — locks are not implemented, so either they all succeed
(no exclusion) or `Flock` returns `ENOSYS`.

- [ ] **Step 4: Implement the lock path**

Append to `pkg/objectfs/handle.go`:

```go
var (
	_ fs.FileSetlker  = (*handle)(nil)
	_ fs.FileSetlkwer = (*handle)(nil)
)

// Setlk is the non-blocking lock path: flock(LOCK_NB).
func (h *handle) Setlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	return h.setlk(ctx, lk, flags, false)
}

// Setlkw is the blocking lock path.
//
// It blocks indefinitely rather than timing out, matching local filesystem
// semantics — an open file keeps its lock as long as it wants. The kernel's
// FUSE interrupt path cancels ctx when the waiting process is signalled, so a
// signal still breaks the wait.
func (h *handle) Setlkw(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	return h.setlk(ctx, lk, flags, true)
}

func (h *handle) setlk(ctx context.Context, lk *fuse.FileLock, flags uint32, blocking bool) syscall.Errno {
	// Only whole-file flock is supported; byte-range locks are a non-goal.
	if flags&fuse.FUSE_LK_FLOCK == 0 {
		return syscall.ENOTSUP
	}

	if lk.Typ == syscall.F_UNLCK {
		return h.unlock(ctx)
	}

	// F_RDLCK (LOCK_SH) maps to the same exclusive Lease as F_WRLCK. A caller
	// taking a shared lock is asking for "no writer is mid-write", and only
	// the Lease can promise that. Over-strict, deliberately.
	h.mu.Lock()
	if h.lease != nil {
		h.mu.Unlock()
		return 0 // already ours
	}
	lease := h.vol.NewLease()
	h.mu.Unlock()

	if blocking {
		if err := lease.Acquire(ctx); err != nil {
			return syscall.EINTR
		}
	} else {
		ok, err := lease.TryAcquire(ctx)
		if err != nil {
			return errnoFor(err)
		}
		if !ok {
			return syscall.EWOULDBLOCK
		}
	}

	// The mandatory quorum read. This is the read-after-write guarantee: it
	// is what lets a consumer check "did someone already refresh?" and get a
	// truthful answer.
	if _, err := h.vol.Cache.Fresh(ctx); err != nil {
		_ = lease.Release(ctx)
		return errnoFor(err)
	}

	h.mu.Lock()
	h.lease = lease
	// Re-pin to the freshly read snapshot, and adopt its value unless this
	// handle has uncommitted writes of its own.
	if snap := h.vol.Cache.Current(); snap != nil && !h.dirty {
		h.snap = snap
		if value, ok := snap.Get(h.key); ok {
			h.buf = value
		}
	}
	h.mu.Unlock()
	return 0
}

// unlock commits before releasing, so the next holder's mandatory fresh read
// can observe this holder's write.
func (h *handle) unlock(ctx context.Context) syscall.Errno {
	errno := h.commit(ctx)

	h.mu.Lock()
	lease := h.lease
	h.lease = nil
	h.mu.Unlock()

	if lease != nil {
		_ = lease.Release(ctx)
	}
	return errno
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./pkg/objectfs/ -run 'TestFlock|TestCommitHappens' -race -v`
Expected: PASS, all six tests.

- [ ] **Step 6: Run the whole suite**

Run: `make cover`
Expected: PASS with no races.

- [ ] **Step 7: Commit**

```bash
git add pkg/objectfs/handle.go pkg/objectfs/fs_lock_test.go
git commit -m "$(cat <<'EOF'
objectfs: flock mapped to a coordination.k8s.io Lease

The reason the project exists. Acquiring the lock forces a quorum read, and
that guaranteed-fresh read is what makes the double-checked refresh pattern
sound: a consumer can ask "did someone already refresh?" and get a truthful
answer.

Unlock commits before releasing, so the next holder's fresh read observes
this holder's write. LOCK_SH maps to the same exclusive Lease — over-strict,
but a shared-lock caller is asking for "no writer is mid-write" and only the
Lease can promise it. Blocking acquisition has no timeout, matching local
filesystem semantics; FUSE's interrupt path still breaks it on a signal.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
EOF
)"
```

---

### Task 14: Mount registry and published-state file

Distinguishes a genuine first publish from a republish after a plugin
restart. Without that distinction, the never-error rule cannot be applied
safely.

**Files:**
- Create: `pkg/mounts/registry.go`
- Test: `pkg/mounts/registry_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `type Mount struct { TargetPath, ObjectKind, ObjectName, Namespace string }`
  - `type Registry struct{ ... }`; `func NewRegistry(statePath string) (*Registry, error)`
  - `func (r *Registry) Get(target string) (*Live, bool)`
  - `func (r *Registry) Put(target string, live *Live, m Mount) error`
  - `func (r *Registry) Delete(ctx, target string) error`
  - `func (r *Registry) WasPublished(target string) bool`
  - `type Live struct { Cancel context.CancelFunc; Unmount func() error; Token atomic.Pointer[string] }`

- [ ] **Step 1: Write the failing test**

Create `pkg/mounts/registry_test.go`:

```go
package mounts

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func newLive() *Live {
	return &Live{Cancel: func() {}, Unmount: func() error { return nil }}
}

func testMount(target string) Mount {
	return Mount{TargetPath: target, ObjectKind: "Secret", ObjectName: "oauth-credentials", Namespace: "my-app"}
}

func TestRegistryPutGetDelete(t *testing.T) {
	r, err := NewRegistry(filepath.Join(t.TempDir(), "published.json"))
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	if _, ok := r.Get("/t/1"); ok {
		t.Fatal("empty registry returned a mount")
	}
	if err := r.Put("/t/1", newLive(), testMount("/t/1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, ok := r.Get("/t/1"); !ok {
		t.Fatal("Get after Put missed")
	}
	if err := r.Delete(context.Background(), "/t/1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok := r.Get("/t/1"); ok {
		t.Fatal("Get after Delete hit")
	}
}

// The state file is the only thing that tells a post-restart republish from a
// genuine first publish. Getting this wrong means either erroring on
// republish (which deletes the mount point, k8s#121271) or silently
// swallowing a real authorization failure.
func TestRegistrySurvivesRestart(t *testing.T) {
	state := filepath.Join(t.TempDir(), "published.json")

	first, err := NewRegistry(state)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	if err := first.Put("/t/1", newLive(), testMount("/t/1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Simulate a plugin restart: in-memory state is gone, the file is not.
	second, err := NewRegistry(state)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, ok := second.Get("/t/1"); ok {
		t.Fatal("live mount survived a restart; only the record should")
	}
	if !second.WasPublished("/t/1") {
		t.Fatal("WasPublished false after restart; republish would wrongly error")
	}
	if second.WasPublished("/t/never") {
		t.Fatal("WasPublished true for a target never published")
	}
}

func TestRegistryDeleteRemovesFromState(t *testing.T) {
	state := filepath.Join(t.TempDir(), "published.json")
	r, _ := NewRegistry(state)
	_ = r.Put("/t/1", newLive(), testMount("/t/1"))
	if err := r.Delete(context.Background(), "/t/1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	again, _ := NewRegistry(state)
	if again.WasPublished("/t/1") {
		t.Fatal("record survived Delete")
	}
}

func TestRegistryDeleteRunsTeardown(t *testing.T) {
	r, _ := NewRegistry(filepath.Join(t.TempDir(), "published.json"))

	cancelled, unmounted := false, false
	live := &Live{
		Cancel:  func() { cancelled = true },
		Unmount: func() error { unmounted = true; return nil },
	}
	_ = r.Put("/t/1", live, testMount("/t/1"))

	if err := r.Delete(context.Background(), "/t/1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !cancelled {
		t.Error("watch context not cancelled")
	}
	if !unmounted {
		t.Error("filesystem not unmounted")
	}
}

func TestRegistryTolerAtesCorruptStateFile(t *testing.T) {
	state := filepath.Join(t.TempDir(), "published.json")
	if err := os.WriteFile(state, []byte("{{{ not json"), 0o600); err != nil {
		t.Fatal(err)
	}

	// A corrupt file must not wedge the plugin on every restart; start empty.
	r, err := NewRegistry(state)
	if err != nil {
		t.Fatalf("NewRegistry on corrupt state: %v", err)
	}
	if r.WasPublished("/t/1") {
		t.Fatal("corrupt state produced phantom records")
	}
}

func TestRegistryTokenSwapIsRaceFree(t *testing.T) {
	r, _ := NewRegistry(filepath.Join(t.TempDir(), "published.json"))
	live := newLive()
	_ = r.Put("/t/1", live, testMount("/t/1"))

	got, _ := r.Get("/t/1")
	tok := "tok-1"
	got.Token.Store(&tok)
	if v := got.Token.Load(); v == nil || *v != "tok-1" {
		t.Fatalf("Token = %v, want tok-1", v)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./pkg/mounts/ -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Write the implementation**

Create `pkg/mounts/registry.go`:

```go
// Package mounts tracks the FUSE mounts this node plugin owns.
//
// It exists to answer one question that has no other source of truth after a
// plugin restart: has this target path been published successfully before?
//
// The answer decides whether NodePublishVolume may return an error. On a
// genuine first publish an error is correct — it is how an unauthorized pod
// learns it is unauthorized. On a republish an error is destructive: kubelet
// deletes the mount point from the host filesystem and later successful calls
// cannot restore the pod's view (kubernetes/kubernetes#121271).
package mounts

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// Mount is the durable record of a published target.
type Mount struct {
	TargetPath string `json:"targetPath"`
	ObjectKind string `json:"objectKind"`
	ObjectName string `json:"objectName"`
	Namespace  string `json:"namespace"`
}

// Live is the in-memory half: the running FUSE server and its watch loop.
// It does not survive a plugin restart.
type Live struct {
	Cancel  context.CancelFunc
	Unmount func() error

	// Token holds the most recent pod token. Republish swaps it atomically,
	// roughly ten times a second, so this must never take a lock the data
	// path also wants.
	Token atomic.Pointer[string]
}

// Registry maps target paths to live mounts, backed by a JSON state file.
type Registry struct {
	statePath string

	mu      sync.Mutex
	live    map[string]*Live
	records map[string]Mount
}

// NewRegistry loads the state file if present. A missing or corrupt file
// starts empty rather than failing: refusing to start would wedge the plugin
// permanently, whereas an empty registry self-heals as republishes arrive.
func NewRegistry(statePath string) (*Registry, error) {
	r := &Registry{
		statePath: statePath,
		live:      map[string]*Live{},
		records:   map[string]Mount{},
	}
	data, err := os.ReadFile(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return r, nil
		}
		return nil, fmt.Errorf("read state: %w", err)
	}
	var records map[string]Mount
	if err := json.Unmarshal(data, &records); err != nil {
		return r, nil // corrupt: start empty
	}
	r.records = records
	return r, nil
}

// Get returns the live mount for target, if this process owns one.
func (r *Registry) Get(target string) (*Live, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	live, ok := r.live[target]
	return live, ok
}

// WasPublished reports whether target has ever been published successfully,
// including by a previous incarnation of this process.
func (r *Registry) WasPublished(target string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.live[target]; ok {
		return true
	}
	_, ok := r.records[target]
	return ok
}

// Put records a successful publish.
func (r *Registry) Put(target string, live *Live, m Mount) error {
	r.mu.Lock()
	r.live[target] = live
	r.records[target] = m
	r.mu.Unlock()
	return r.persist()
}

// Delete tears the mount down and forgets it.
func (r *Registry) Delete(_ context.Context, target string) error {
	r.mu.Lock()
	live := r.live[target]
	delete(r.live, target)
	delete(r.records, target)
	r.mu.Unlock()

	if live != nil {
		if live.Cancel != nil {
			live.Cancel()
		}
		if live.Unmount != nil {
			if err := live.Unmount(); err != nil {
				// Persist regardless: leaving a record for a mount we no
				// longer own would make a future republish do nothing.
				_ = r.persist()
				return fmt.Errorf("unmount %s: %w", target, err)
			}
		}
	}
	return r.persist()
}

// persist writes the state file atomically, so a crash mid-write cannot leave
// a half-written file that reads as corrupt on the next start.
func (r *Registry) persist() error {
	r.mu.Lock()
	data, err := json.MarshalIndent(r.records, "", "  ")
	r.mu.Unlock()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(r.statePath), 0o755); err != nil {
		return err
	}
	tmp := r.statePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, r.statePath)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./pkg/mounts/ -race -v`
Expected: PASS, all six tests.

- [ ] **Step 5: Commit**

```bash
git add pkg/mounts
git commit -m "$(cat <<'EOF'
mounts: registry with a published-state file

Answers the one question that has no other source of truth after a plugin
restart: has this target been published before? That decides whether
NodePublishVolume may return an error — correct on a first publish, and
destructive on a republish, since kubelet deletes the mount point when a
republish errors (kubernetes/kubernetes#121271).

A missing or corrupt state file starts empty rather than failing; refusing
to start would wedge the plugin permanently, while an empty registry
self-heals as republishes arrive.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
EOF
)"
```

---

### Task 15: CSI Identity and Node services

**Files:**
- Create: `pkg/driver/identity.go`, `pkg/driver/node.go`, `pkg/driver/server.go`, `pkg/driver/rbachint.go`
- Test: `pkg/driver/node_test.go`, `pkg/driver/rbachint_test.go`

**Interfaces:**
- Consumes: `podtoken` (Task 9), `objectfs.ParseConfig`/`NewVolume`/`Mount` (Tasks 10–11), `mounts.Registry` (Task 14).
- Produces:
  - `func NewNodeServer(nodeID string, reg *mounts.Registry, log *slog.Logger) *NodeServer`
  - `func NewIdentityServer(name, version string) *IdentityServer`
  - `func Serve(endpoint string, id csi.IdentityServer, node csi.NodeServer) error`
  - `func RBACHint(namespace, serviceAccount, kind, objectName, leaseName string) string`

- [ ] **Step 1: Write the failing tests**

Create `pkg/driver/rbachint_test.go`:

```go
package driver

import (
	"strings"
	"testing"
)

// This string is surfaced verbatim in a kubectl describe pod event, so it is
// the driver's entire self-service ergonomics story. Test it like an API.
func TestRBACHintIsActionable(t *testing.T) {
	got := RBACHint("my-app", "session-runner", "Secret", "oauth-credentials", "roommate-oauth-credentials")

	for _, want := range []string{
		"my-app", "session-runner", "oauth-credentials", "roommate-oauth-credentials",
		"kind: Role", "kind: RoleBinding",
		"secrets", "leases",
		`"get"`, `"watch"`, `"patch"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("hint missing %q:\n%s", want, got)
		}
	}
}

func TestRBACHintUsesConfigMapsForConfigMapKind(t *testing.T) {
	got := RBACHint("my-app", "session-runner", "ConfigMap", "app-config", "roommate-app-config")
	if !strings.Contains(got, "configmaps") {
		t.Errorf("hint does not mention configmaps:\n%s", got)
	}
	if strings.Contains(got, `resources: ["secrets"]`) {
		t.Errorf("hint mentions secrets for a ConfigMap volume:\n%s", got)
	}
}
```

Create `pkg/driver/node_test.go`:

```go
package driver

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/middlendian/roommate-csi/pkg/mounts"
)

func newTestNodeServer(t *testing.T) *NodeServer {
	t.Helper()
	reg, err := mounts.NewRegistry(filepath.Join(t.TempDir(), "published.json"))
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	return NewNodeServer("node-1", reg, slog.Default())
}

func TestNodeGetCapabilitiesIsEmpty(t *testing.T) {
	// No STAGE_UNSTAGE: podInfoOnMount and tokenRequests populate only
	// NodePublishVolume, so there is no pod identity at stage time.
	resp, err := newTestNodeServer(t).NodeGetCapabilities(context.Background(), &csi.NodeGetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("NodeGetCapabilities: %v", err)
	}
	if len(resp.GetCapabilities()) != 0 {
		t.Fatalf("capabilities = %v, want none", resp.GetCapabilities())
	}
}

func TestNodeGetInfo(t *testing.T) {
	resp, err := newTestNodeServer(t).NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
	if err != nil {
		t.Fatalf("NodeGetInfo: %v", err)
	}
	if resp.GetNodeId() != "node-1" {
		t.Fatalf("NodeId = %q, want node-1", resp.GetNodeId())
	}
}

func TestNodePublishRejectsMissingArguments(t *testing.T) {
	n := newTestNodeServer(t)
	tests := []struct {
		name string
		req  *csi.NodePublishVolumeRequest
	}{
		{"no volume id", &csi.NodePublishVolumeRequest{TargetPath: "/t"}},
		{"no target", &csi.NodePublishVolumeRequest{VolumeId: "v"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := n.NodePublishVolume(context.Background(), tt.req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
			}
		})
	}
}

// Inline ephemeral only: a PVC-backed request must be refused clearly rather
// than half-working.
func TestNodePublishRequiresEphemeral(t *testing.T) {
	_, err := newTestNodeServer(t).NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:   "v",
		TargetPath: "/t",
		VolumeContext: map[string]string{
			"objectKind": "Secret",
			"objectName": "oauth-credentials",
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument for a non-ephemeral volume", status.Code(err))
	}
}

// A first publish must fail loudly when the token is absent — that is how a
// misconfigured CSIDriver gets noticed.
func TestNodePublishFirstPublishErrorsWithoutToken(t *testing.T) {
	_, err := newTestNodeServer(t).NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:   "v",
		TargetPath: "/t",
		VolumeContext: map[string]string{
			"csi.storage.k8s.io/ephemeral":                "true",
			"csi.storage.k8s.io/pod.namespace":            "my-app",
			"csi.storage.k8s.io/pod.name":                 "p",
			"csi.storage.k8s.io/pod.uid":                  "uid",
			"csi.storage.k8s.io/pod.service-account.name": "session-runner",
			"objectKind": "Secret",
			"objectName": "oauth-credentials",
		},
	})
	if err == nil {
		t.Fatal("first publish succeeded without a token")
	}
}

// After a successful publish, republish must NEVER error: kubelet deletes the
// mount point when it does (kubernetes/kubernetes#121271). This is the single
// most important behaviour in the file.
func TestRepublishNeverErrorsAfterFirstSuccess(t *testing.T) {
	n := newTestNodeServer(t)
	target := filepath.Join(t.TempDir(), "target")

	// Record a prior successful publish without mounting anything.
	if err := n.registry.Put(target, &mounts.Live{
		Cancel: func() {}, Unmount: func() error { return nil },
	}, mounts.Mount{TargetPath: target, ObjectKind: "Secret", ObjectName: "oauth-credentials", Namespace: "my-app"}); err != nil {
		t.Fatalf("seed registry: %v", err)
	}

	// A republish carrying garbage — no token, no pod info — must still be OK.
	resp, err := n.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:      "v",
		TargetPath:    target,
		VolumeContext: map[string]string{"csi.storage.k8s.io/ephemeral": "true"},
	})
	if err != nil {
		t.Fatalf("republish returned an error, which deletes the mount point: %v", err)
	}
	if resp == nil {
		t.Fatal("nil response")
	}
}

func TestNodeUnpublishIsIdempotent(t *testing.T) {
	n := newTestNodeServer(t)
	target := filepath.Join(t.TempDir(), "target")

	for i := 0; i < 2; i++ {
		if _, err := n.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
			VolumeId: "v", TargetPath: target,
		}); err != nil {
			t.Fatalf("unpublish %d: %v", i, err)
		}
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./pkg/driver/ -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Write the RBAC hint**

Create `pkg/driver/rbachint.go`:

```go
package driver

import (
	"fmt"
	"strings"
)

// RBACHint returns the exact Role and RoleBinding needed to make a denied
// mount work.
//
// kubelet surfaces a NodePublishVolume error as a FailedMount event, so this
// text appears in `kubectl describe pod`. Making it copy-pasteable is the
// whole self-service story: the driver cannot create these objects itself
// without becoming a privilege-escalation service.
func RBACHint(namespace, serviceAccount, kind, objectName, leaseName string) string {
	resource := "secrets"
	if strings.EqualFold(kind, "ConfigMap") {
		resource = "configmaps"
	}
	return fmt.Sprintf(`serviceaccount %s/%s is not permitted to use %s/%s.

Apply this in namespace %s:

---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  namespace: %s
  name: roommate-%s
rules:
  - apiGroups: [""]
    resources: [%q]
    resourceNames: [%q]
    verbs: ["get", "watch", "patch"]
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    resourceNames: [%q]
    verbs: ["get", "create", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  namespace: %s
  name: roommate-%s
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: roommate-%s
subjects:
  - kind: ServiceAccount
    name: %s
    namespace: %s`,
		namespace, serviceAccount, resource, objectName,
		namespace,
		namespace, objectName,
		resource, objectName,
		leaseName,
		namespace, objectName,
		objectName,
		serviceAccount, namespace)
}
```

- [ ] **Step 4: Write the Identity service**

Create `pkg/driver/identity.go`:

```go
package driver

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// IdentityServer implements the CSI Identity service.
//
// It advertises no CONTROLLER_SERVICE: roommate has no controller at all,
// because inline ephemeral volumes never invoke one.
type IdentityServer struct {
	csi.UnimplementedIdentityServer
	name    string
	version string
}

func NewIdentityServer(name, version string) *IdentityServer {
	return &IdentityServer{name: name, version: version}
}

func (s *IdentityServer) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: s.name, VendorVersion: s.version}, nil
}

func (s *IdentityServer) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{}, nil
}

func (s *IdentityServer) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{Ready: wrapperspb.Bool(true)}, nil
}
```

- [ ] **Step 5: Write the gRPC listener**

Create `pkg/driver/server.go`:

```go
package driver

import (
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
)

// Serve listens on a unix:// endpoint until the process exits.
func Serve(endpoint string, id csi.IdentityServer, node csi.NodeServer) error {
	path := strings.TrimPrefix(endpoint, "unix://")
	if path == endpoint {
		return fmt.Errorf("endpoint %q must start with unix://", endpoint)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale socket: %w", err)
	}
	lis, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("listen %s: %w", path, err)
	}
	srv := grpc.NewServer()
	csi.RegisterIdentityServer(srv, id)
	csi.RegisterNodeServer(srv, node)
	return srv.Serve(lis)
}
```

- [ ] **Step 6: Write the Node service**

Create `pkg/driver/node.go`:

```go
package driver

import (
	"context"
	"log/slog"
	"os"

	"github.com/container-storage-interface/spec/lib/go/csi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/middlendian/roommate-csi/pkg/mounts"
	"github.com/middlendian/roommate-csi/pkg/objectfs"
	"github.com/middlendian/roommate-csi/pkg/podtoken"
)

// keyEphemeral marks an inline volume. roommate serves nothing else.
const keyEphemeral = "csi.storage.k8s.io/ephemeral"

// NodeServer implements the CSI Node service.
//
// NodePublishVolume is the only RPC that does work, and it is called roughly
// ten times a second per volume because the CSIDriver sets
// requiresRepublish: true. The republish path must therefore stay
// allocation-cheap: a registry lookup and an atomic token swap, nothing more.
type NodeServer struct {
	csi.UnimplementedNodeServer

	nodeID   string
	registry *mounts.Registry
	log      *slog.Logger
}

func NewNodeServer(nodeID string, reg *mounts.Registry, log *slog.Logger) *NodeServer {
	return &NodeServer{nodeID: nodeID, registry: reg, log: log}
}

// NodeGetCapabilities advertises nothing.
//
// No STAGE_UNSTAGE in particular: podInfoOnMount and tokenRequests populate
// only NodePublishVolume, so there is no pod identity available at stage
// time and a staged mount could not be authenticated as anyone.
func (n *NodeServer) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{}, nil
}

func (n *NodeServer) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{NodeId: n.nodeID}, nil
}

func (n *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	target := req.GetTargetPath()
	if req.GetVolumeId() == "" || target == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id and target_path are required")
	}
	vc := req.GetVolumeContext()

	// Republish: swap the token and return. NEVER error from here — kubelet
	// deletes the mount point when a republish fails, and later successful
	// calls cannot restore the pod's view (kubernetes/kubernetes#121271). A
	// rejected token surfaces as EACCES from the FUSE data path instead,
	// which is the correct layer to fail at: revocation still bites, the
	// mount survives, and buffered writes are not destroyed by a blip.
	if live, ok := n.registry.Get(target); ok {
		if tok, err := podtoken.Extract(vc); err == nil {
			live.Token.Store(&tok)
		}
		return &csi.NodePublishVolumeResponse{}, nil
	}

	// Known target with no live mount means the plugin restarted. Rebuild it,
	// and stay silent on failure for the same reason.
	restarting := n.registry.WasPublished(target)

	if err := n.publish(ctx, target, vc); err != nil {
		if restarting {
			n.log.Error("remount after restart failed; will retry on next republish",
				"target", target, "err", err)
			return &csi.NodePublishVolumeResponse{}, nil
		}
		return nil, err
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

// publish does the real work of a first mount. Errors returned here are
// surfaced by kubelet as a FailedMount event, so they must be actionable.
func (n *NodeServer) publish(ctx context.Context, target string, vc map[string]string) error {
	if vc[keyEphemeral] != "true" {
		return status.Error(codes.InvalidArgument,
			"roommate serves inline ephemeral volumes only; declare the volume "+
				"under pod.spec.volumes[].csi rather than via a PVC")
	}
	namespace, serviceAccount, uid, err := podtoken.PodIdentity(vc)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	cfg, err := objectfs.ParseConfig(vc, namespace)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	tok, err := podtoken.Extract(vc)
	if err != nil {
		return status.Errorf(codes.FailedPrecondition,
			"%v; set tokenRequests on the CSIDriver object", err)
	}
	client, err := podtoken.ClientFor(tok)
	if err != nil {
		return status.Errorf(codes.Internal, "build client: %v", err)
	}
	vol, err := objectfs.NewVolume(client, cfg, uid)
	if err != nil {
		return status.Errorf(codes.Internal, "volume: %v", err)
	}

	// Prove authorization before mounting, so a denied pod gets an actionable
	// event rather than a mount that fails on first read.
	if _, err := vol.Cache.Fresh(ctx); err != nil {
		switch {
		case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
			return status.Error(codes.PermissionDenied,
				RBACHint(namespace, serviceAccount, cfg.ObjectKind, cfg.ObjectName, cfg.LeaseName))
		case apierrors.IsNotFound(err):
			return status.Errorf(codes.NotFound,
				"%s %s/%s does not exist; roommate never creates the backing object",
				cfg.ObjectKind, namespace, cfg.ObjectName)
		default:
			return status.Errorf(codes.Internal, "read %s: %v", vol.Store.Describe(), err)
		}
	}

	if err := os.MkdirAll(target, os.FileMode(cfg.DirMode)); err != nil {
		return status.Errorf(codes.Internal, "mkdir target: %v", err)
	}

	watchCtx, cancel := context.WithCancel(context.Background())
	go vol.Run(watchCtx)

	server, err := objectfs.Mount(target, vol)
	if err != nil {
		cancel()
		return status.Errorf(codes.Internal, "mount fuse: %v", err)
	}

	live := &mounts.Live{Cancel: cancel, Unmount: server.Unmount}
	live.Token.Store(&tok)

	if err := n.registry.Put(target, live, mounts.Mount{
		TargetPath: target, ObjectKind: cfg.ObjectKind,
		ObjectName: cfg.ObjectName, Namespace: namespace,
	}); err != nil {
		cancel()
		_ = server.Unmount()
		return status.Errorf(codes.Internal, "record mount: %v", err)
	}
	n.log.Info("published", "target", target, "object", vol.Store.Describe(), "namespace", namespace)
	return nil
}

func (n *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	target := req.GetTargetPath()
	if req.GetVolumeId() == "" || target == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id and target_path are required")
	}
	if err := n.registry.Delete(ctx, target); err != nil {
		return nil, status.Errorf(codes.Internal, "unpublish: %v", err)
	}
	_ = os.Remove(target)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}
```

Note: `node_test.go` reaches `n.registry`, so the field must stay unexported
but package-visible — which it is.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `go test ./pkg/driver/ -race -v`
Expected: PASS, all tests.

- [ ] **Step 8: Commit**

```bash
git add pkg/driver
git commit -m "$(cat <<'EOF'
driver: CSI Identity and Node services

NodePublishVolume is the only RPC that does work, and requiresRepublish
means kubelet calls it about ten times a second per volume — so the
republish path is a registry lookup and an atomic token swap, nothing more.

It never returns an error once a target has published successfully. kubelet
deletes the mount point when a republish fails and later successful calls
cannot restore the pod's view (kubernetes/kubernetes#121271), so a rejected
token surfaces as EACCES from the FUSE data path instead. That is the right
layer: revocation still bites, the mount survives, and buffered writes are
not destroyed by a transient blip.

A denied first publish returns copy-pasteable Role and RoleBinding YAML,
which kubelet surfaces in a FailedMount event.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
EOF
)"
```

---

### Task 16: The node binary

**Files:**
- Create: `cmd/node/main.go`

**Interfaces:**
- Consumes: `driver.NewIdentityServer`, `driver.NewNodeServer`, `driver.Serve` (Task 15), `mounts.NewRegistry` (Task 14).
- Produces: the `roommate-node` binary.

- [ ] **Step 1: Write the binary**

Create `cmd/node/main.go`:

```go
// Command roommate-node is the roommate CSI node plugin.
//
// There is no controller binary. Inline ephemeral volumes never invoke the
// CSI controller service, so the entire driver is this one process.
package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/middlendian/roommate-csi/pkg/driver"
	"github.com/middlendian/roommate-csi/pkg/mounts"
)

// version is overridden at build time via -ldflags.
var version = "dev"

func main() {
	var (
		endpoint  = flag.String("endpoint", "unix:///csi/csi.sock", "CSI gRPC endpoint")
		driverName = flag.String("driver-name", "roommate.csi", "CSI driver name")
		nodeID    = flag.String("node-id", "", "node name (required)")
		statePath = flag.String("state-file",
			"/var/lib/kubelet/plugins/roommate.csi/published.json",
			"where published-target records are kept across restarts")
		logLevel = flag.String("log-level", "info", "debug, info, warn, or error")
	)
	flag.Parse()

	log := newLogger(*logLevel)

	if *nodeID == "" {
		log.Error("-node-id is required (set it from spec.nodeName via the downward API)")
		os.Exit(1)
	}

	registry, err := mounts.NewRegistry(*statePath)
	if err != nil {
		log.Error("open state file", "path", *statePath, "err", err)
		os.Exit(1)
	}

	log.Info("starting", "driver", *driverName, "version", version, "node", *nodeID)

	// Mounts are not restored here. Any target that was published before a
	// restart is rebuilt by the next republish, which arrives within about
	// 100ms because the CSIDriver sets requiresRepublish.
	err = driver.Serve(*endpoint,
		driver.NewIdentityServer(*driverName, version),
		driver.NewNodeServer(*nodeID, registry, log),
	)
	if err != nil {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}
```

- [ ] **Step 2: Verify it builds and the flags work**

Run:
```bash
make build
./bin/roommate-node -h
./bin/roommate-node 2>&1 | grep -q 'node-id is required' && echo "guard ok"
```
Expected: builds; help lists all five flags; the missing-`node-id` guard fires.

- [ ] **Step 3: Run the full gate**

Run: `make fmt-check vet tidy-check cover build`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add cmd/node
git commit -m "$(cat <<'EOF'
cmd/node: the node plugin binary

There is no controller binary — inline ephemeral volumes never invoke the
CSI controller service, so the whole driver is this one process.

Mounts are not restored at startup: any target published before a restart is
rebuilt by the next republish, which arrives within about 100ms.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
EOF
)"
```

---

### Task 17: Container image

**Files:**
- Create: `Dockerfile`, `.dockerignore`
- Modify: `Makefile`

**Interfaces:**
- Consumes: the `roommate-node` binary (Task 16).
- Produces: `make docker` building `ghcr.io/middlendian/roommate-csi:dev`.

- [ ] **Step 1: Write the Dockerfile**

Create `Dockerfile`:

```dockerfile
FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/roommate-node ./cmd/node

# fuse3 provides fusermount3, which go-fuse invokes to unmount.
FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends fuse3 ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/roommate-node /usr/local/bin/roommate-node
ENTRYPOINT ["/usr/local/bin/roommate-node"]
```

- [ ] **Step 2: Write .dockerignore**

Create `.dockerignore`:

```
.git
bin
cover.out
docs
test/e2e
```

- [ ] **Step 3: Add the docker target**

Append to `Makefile`:

```make
IMAGE   ?= ghcr.io/middlendian/roommate-csi
TAG     ?= dev

.PHONY: docker
docker:
	docker build --build-arg VERSION=$(TAG) -t $(IMAGE):$(TAG) .
```

- [ ] **Step 4: Verify the image builds and runs**

Run:
```bash
make docker
docker run --rm ghcr.io/middlendian/roommate-csi:dev -h
docker run --rm --entrypoint fusermount3 ghcr.io/middlendian/roommate-csi:dev -V
```
Expected: image builds; the binary prints help; `fusermount3` reports a
version, confirming go-fuse can unmount inside the container.

- [ ] **Step 5: Commit**

```bash
git add Dockerfile .dockerignore Makefile
git commit -m "$(cat <<'EOF'
build: container image

Static binary on debian-slim with fuse3, which supplies the fusermount3
binary go-fuse shells out to when unmounting.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
EOF
)"
```

---

### Task 18: Deployment manifests

**Files:**
- Create: `deploy/kustomize/base/{namespace,csidriver,daemonset,rbac,kustomization}.yaml`
- Create: `deploy/kustomize/base/roommate-user-clusterrole.yaml`
- Create: `examples/pod.yaml`, `examples/rbac.yaml`

**Interfaces:**
- Consumes: the image (Task 17), the driver name `roommate.csi`.
- Produces: `kubectl apply -k deploy/kustomize/base` installing the driver.

- [ ] **Step 1: Write the CSIDriver object**

Create `deploy/kustomize/base/csidriver.yaml`:

```yaml
apiVersion: storage.k8s.io/v1
kind: CSIDriver
metadata:
  name: roommate.csi
spec:
  # No controller service exists, so nothing to attach.
  attachRequired: false
  # Supplies pod.namespace, pod.name, pod.uid and pod.service-account.name to
  # NodePublishVolume. The namespace comes from here and never from
  # volumeAttributes.
  podInfoOnMount: true
  # Makes kubelet mint a token for the consuming pod's ServiceAccount and pass
  # it in the volume context. This is what lets the driver hold zero RBAC.
  tokenRequests:
    - audience: ""
  # Makes kubelet re-call NodePublishVolume about ten times a second so the
  # token stays fresh. The republish handler must therefore stay cheap and
  # must never return an error once a target has published.
  requiresRepublish: true
  fsGroupPolicy: File
  volumeLifecycleModes:
    - Ephemeral
```

- [ ] **Step 2: Write the namespace and driver RBAC**

Create `deploy/kustomize/base/namespace.yaml`:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: roommate-system
```

Create `deploy/kustomize/base/rbac.yaml`:

```yaml
# The driver's ServiceAccount holds NO Kubernetes API permissions.
#
# Every call to read, watch, patch or lock is authenticated as the consuming
# pod, using a token kubelet mints via CSIDriver.spec.tokenRequests. Adding a
# rule here would reintroduce the confused-deputy problem the design exists to
# eliminate: an already-privileged DaemonSet able to read objects on behalf of
# pods that could not read them themselves.
#
# There is deliberately no Role, ClusterRole, or binding for this account.
apiVersion: v1
kind: ServiceAccount
metadata:
  name: roommate-node
  namespace: roommate-system
```

- [ ] **Step 3: Write the ClusterRole operators bind**

Create `deploy/kustomize/base/roommate-user-clusterrole.yaml`:

```yaml
# Convenience for operators. Bind with a RoleBinding (never a
# ClusterRoleBinding) to scope it to one namespace:
#
#   kubectl create rolebinding roommate-user \
#     --clusterrole=roommate-user \
#     --serviceaccount=my-app:session-runner \
#     --namespace=my-app
#
# This grants access to every Secret and ConfigMap in that namespace. For
# per-object scoping, write a Role with resourceNames instead — see
# examples/rbac.yaml. Note that a Role using resourceNames requires the client
# to pass a metadata.name field selector on watches, which roommate does.
#
# Applying the binding carries no escalation risk: RBAC requires whoever
# creates it to already hold these permissions themselves.
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: roommate-user
rules:
  - apiGroups: [""]
    resources: ["secrets", "configmaps"]
    verbs: ["get", "watch", "patch"]
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "create", "update"]
```

- [ ] **Step 4: Write the DaemonSet**

Create `deploy/kustomize/base/daemonset.yaml`:

```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: roommate-node
  namespace: roommate-system
  labels: {app: roommate-node}
spec:
  selector:
    matchLabels: {app: roommate-node}
  template:
    metadata:
      labels: {app: roommate-node}
    spec:
      serviceAccountName: roommate-node
      hostNetwork: false
      containers:
        - name: node-driver-registrar
          image: registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.13.0
          args:
            - --csi-address=/csi/csi.sock
            - --kubelet-registration-path=/var/lib/kubelet/plugins/roommate.csi/csi.sock
          volumeMounts:
            - {name: socket-dir, mountPath: /csi}
            - {name: registration-dir, mountPath: /registration}

        - name: roommate
          image: ghcr.io/middlendian/roommate-csi:dev
          args:
            - --endpoint=unix:///csi/csi.sock
            - --node-id=$(NODE_NAME)
          env:
            - name: NODE_NAME
              valueFrom:
                fieldRef: {fieldPath: spec.nodeName}
          securityContext:
            # Required for /dev/fuse and for Bidirectional mount propagation.
            # Note what this does NOT imply: the ServiceAccount still holds no
            # Kubernetes API permissions at all.
            privileged: true
          volumeMounts:
            - {name: socket-dir, mountPath: /csi}
            - name: kubelet-dir
              mountPath: /var/lib/kubelet
              # Bidirectional so FUSE mounts made here are visible to kubelet
              # and to consuming pods.
              mountPropagation: Bidirectional
            - {name: fuse, mountPath: /dev/fuse}
          livenessProbe:
            exec:
              command: ["/bin/sh", "-c", "test -S /csi/csi.sock"]
            initialDelaySeconds: 10
            periodSeconds: 30
      volumes:
        - name: socket-dir
          hostPath:
            path: /var/lib/kubelet/plugins/roommate.csi
            type: DirectoryOrCreate
        - name: registration-dir
          hostPath:
            path: /var/lib/kubelet/plugins_registry
            type: Directory
        - name: kubelet-dir
          hostPath: {path: /var/lib/kubelet, type: Directory}
        - name: fuse
          hostPath: {path: /dev/fuse, type: CharDevice}
```

- [ ] **Step 5: Write the kustomization**

Create `deploy/kustomize/base/kustomization.yaml`:

```yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - namespace.yaml
  - rbac.yaml
  - roommate-user-clusterrole.yaml
  - csidriver.yaml
  - daemonset.yaml
images:
  - name: ghcr.io/middlendian/roommate-csi
    newTag: dev
```

- [ ] **Step 6: Write the examples**

Create `examples/rbac.yaml`:

```yaml
# Per-object grant. This is the entire authorization story: one Role and one
# RoleBinding, in the consuming pod's own namespace. There is nothing to grant
# in the driver's namespace.
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
    verbs: ["get", "create", "update"]
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

Create `examples/pod.yaml`:

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

- [ ] **Step 7: Verify the manifests render and contain no driver RBAC**

Run:
```bash
kubectl kustomize deploy/kustomize/base > /tmp/rendered.yaml
grep -c 'kind: CSIDriver' /tmp/rendered.yaml
# Assert the driver's ServiceAccount is bound to nothing.
! grep -E 'name: roommate-node' /tmp/rendered.yaml | grep -q 'RoleBinding' && echo "no driver bindings: ok"
```
Expected: renders cleanly, one CSIDriver, and no binding naming
`roommate-node`.

- [ ] **Step 8: Commit**

```bash
git add deploy examples
git commit -m "$(cat <<'EOF'
deploy: kustomize base and examples

CSIDriver sets podInfoOnMount, tokenRequests and requiresRepublish — the
three settings that together let the driver run on the consuming pod's
identity instead of its own.

rbac.yaml deliberately contains a ServiceAccount and nothing else. Adding a
rule there would reintroduce the confused deputy this design eliminates, so
the absence is documented in the file itself.

Ships roommate-user for operators who want one namespace-scoped binding, and
a per-object Role in examples/rbac.yaml for those who want resourceNames.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
EOF
)"
```

---

### Task 19: envtest suite for real API semantics

Four claims the design rests on cannot be verified against the fake
clientset, because the fake does not model them. They need a real API server.

**Files:**
- Create: `test/apisemantics/semantics_test.go`
- Modify: `Makefile`, `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: `objectfs` stores and `LeaseManager`.
- Produces: `make envtest`.

- [ ] **Step 1: Add the dependency and the Makefile target**

```bash
go get sigs.k8s.io/controller-runtime/pkg/envtest@latest
go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
go mod tidy
```

Append to `Makefile`:

```make
ENVTEST_K8S_VERSION ?= 1.31.0

.PHONY: envtest
envtest:
	KUBEBUILDER_ASSETS="$$(setup-envtest use $(ENVTEST_K8S_VERSION) -p path)" \
	go test -tags=envtest ./test/apisemantics/... -v
```

- [ ] **Step 2: Write the suite**

Create `test/apisemantics/semantics_test.go`:

```go
//go:build envtest

// Package apisemantics verifies claims the design depends on that the fake
// clientset cannot model. Each test here corresponds to a design decision
// that would fail silently in production if the API server behaved
// differently than assumed.
package apisemantics

import (
	"context"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/middlendian/roommate-csi/pkg/objectfs"
)

var cfg *rest.Config

func TestMain(m *testing.M) {
	env := &envtest.Environment{}
	var err error
	cfg, err = env.Start()
	if err != nil {
		panic("start envtest (run: setup-envtest use): " + err.Error())
	}
	code := m.Run()
	_ = env.Stop()
	os.Exit(code)
}

func setup(t *testing.T) (kubernetes.Interface, string) {
	t.Helper()
	c, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	ns := "test-" + t.Name()[:min(len(t.Name()), 20)]
	ns = sanitize(ns)
	_, err = c.CoreV1().Namespaces().Create(context.Background(),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	return c, ns
}

func sanitize(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch >= 'a' && ch <= 'z', ch >= '0' && ch <= '9', ch == '-':
			out = append(out, ch)
		case ch >= 'A' && ch <= 'Z':
			out = append(out, ch+32)
		default:
			out = append(out, '-')
		}
	}
	return string(out)
}

// CLAIM 1: a merge patch merges at the key level, leaving untouched keys
// alone. The whole write path depends on this — it is why there is no
// read-modify-write.
func TestMergePatchPreservesUntouchedKeys(t *testing.T) {
	c, ns := setup(t)
	ctx := context.Background()

	_, err := c.CoreV1().Secrets(ns).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "oauth-credentials"},
		Data:       map[string][]byte{"a": []byte("1"), "b": []byte("2")},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	store := objectfs.NewSecretStore(c, ns, "oauth-credentials")
	if err := store.Patch(ctx, map[string][]byte{"a": []byte("9")}, nil); err != nil {
		t.Fatalf("Patch: %v", err)
	}

	snap, err := store.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v, _ := snap.Get("a"); string(v) != "9" {
		t.Errorf("a = %q, want 9", v)
	}
	if v, ok := snap.Get("b"); !ok || string(v) != "2" {
		t.Errorf("b = %q (present=%v), want 2 — merge patch must not clobber it", v, ok)
	}
}

// CLAIM 2: a null value in a merge patch deletes the key.
func TestMergePatchNullDeletesKey(t *testing.T) {
	c, ns := setup(t)
	ctx := context.Background()

	_, _ = c.CoreV1().Secrets(ns).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "oauth-credentials"},
		Data:       map[string][]byte{"a": []byte("1"), "b": []byte("2")},
	}, metav1.CreateOptions{})

	store := objectfs.NewSecretStore(c, ns, "oauth-credentials")
	if err := store.Patch(ctx, nil, []string{"b"}); err != nil {
		t.Fatalf("Patch: %v", err)
	}

	snap, _ := store.Get(ctx)
	if _, ok := snap.Get("b"); ok {
		t.Error("b survived a null patch")
	}
	if _, ok := snap.Get("a"); !ok {
		t.Error("a was removed by a patch that only nulled b")
	}
}

// CLAIM 3: a quorum GET reflects a just-completed patch. This is the
// read-after-write guarantee the lock protocol sells.
func TestQuorumGetSeesJustCompletedPatch(t *testing.T) {
	c, ns := setup(t)
	ctx := context.Background()

	_, _ = c.CoreV1().Secrets(ns).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "oauth-credentials"},
		Data:       map[string][]byte{"session.key": []byte("v1")},
	}, metav1.CreateOptions{})

	store := objectfs.NewSecretStore(c, ns, "oauth-credentials")
	for i := 0; i < 20; i++ {
		want := []byte{byte('A' + i)}
		if err := store.Patch(ctx, map[string][]byte{"session.key": want}, nil); err != nil {
			t.Fatalf("Patch %d: %v", i, err)
		}
		snap, err := store.Get(ctx)
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		if got, _ := snap.Get("session.key"); string(got) != string(want) {
			t.Fatalf("iteration %d: quorum GET returned %q, want %q", i, got, want)
		}
	}
}

// CLAIM 4: RBAC resourceNames rejects an unscoped watch. This is why the
// stores set a metadata.name field selector, and it is the claim most likely
// to regress silently — a shared informer would compile and pass every unit
// test, then 403 in production.
func TestResourceNamesRequiresNameFieldSelectorOnWatch(t *testing.T) {
	admin, ns := setup(t)
	ctx := context.Background()

	_, _ = admin.CoreV1().Secrets(ns).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "oauth-credentials"},
		Data:       map[string][]byte{"a": []byte("1")},
	}, metav1.CreateOptions{})

	_, err := admin.RbacV1().Roles(ns).Create(ctx, &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "roommate-user"},
		Rules: []rbacv1.PolicyRule{{
			APIGroups:     []string{""},
			Resources:     []string{"secrets"},
			ResourceNames: []string{"oauth-credentials"},
			Verbs:         []string{"get", "watch", "patch"},
		}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create role: %v", err)
	}
	_, err = admin.RbacV1().RoleBindings(ns).Create(ctx, &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "roommate-user"},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "roommate-user"},
		Subjects: []rbacv1.Subject{{
			Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: "scoped-user",
		}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create binding: %v", err)
	}

	scoped := *cfg
	scoped.Impersonate = rest.ImpersonationConfig{UserName: "scoped-user"}
	sc, err := kubernetes.NewForConfig(&scoped)
	if err != nil {
		t.Fatalf("scoped client: %v", err)
	}

	// Without the field selector: denied.
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := sc.CoreV1().Secrets(ns).Watch(wctx, metav1.ListOptions{}); err == nil {
		t.Fatal("unscoped watch was allowed; the field-selector requirement no longer holds")
	}

	// With it, via our store: allowed.
	w, err := objectfs.NewSecretStore(sc, ns, "oauth-credentials").Watch(wctx, "")
	if err != nil {
		t.Fatalf("scoped watch denied: %v", err)
	}
	w.Stop()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
```

- [ ] **Step 3: Run the suite**

Run: `make envtest`
Expected: PASS, all four tests. If envtest binaries are missing, run
`setup-envtest use 1.31.0` first.

- [ ] **Step 4: Add it to CI**

In `.github/workflows/ci.yml`, add a step to the `check` job after `make check`:

```yaml
      - name: api semantics (envtest)
        run: |
          go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
          make envtest
```

- [ ] **Step 5: Commit**

```bash
git add test/apisemantics Makefile .github/workflows/ci.yml go.mod go.sum
git commit -m "$(cat <<'EOF'
test: envtest suite for API semantics the fake cannot model

Four claims the design rests on, each of which would fail silently in
production if the API server behaved differently:

- merge patch merges at the key level (why there is no read-modify-write)
- a null value deletes a key
- a quorum GET reflects a just-completed patch (the RAW guarantee)
- RBAC resourceNames rejects a watch without a metadata.name field selector

The last is the most valuable: a shared informer would compile, pass every
unit test, and then 403 in a real cluster.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
EOF
)"
```

---

### Task 20: kind end-to-end suite

The refresh race is the reason the project exists, so it is the test that
matters. It must prove serialisation rather than observe it by luck.

**Files:**
- Create: `hack/e2e.sh`, `hack/kind.yaml`, `test/e2e/e2e_test.go`, `test/e2e/fixtures.go`
- Modify: `Makefile`, `.github/workflows/` (new `e2e.yml`)

**Interfaces:**
- Consumes: the image (Task 17) and manifests (Task 18).
- Produces: `make e2e`.

- [ ] **Step 1: Write the kind config**

Create `hack/kind.yaml`:

```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
```

Two workers so the refresh-race test can place contenders on different nodes,
which is the case that a single-node test would not exercise.

- [ ] **Step 2: Write the harness**

Create `hack/e2e.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

CLUSTER="${CLUSTER:-roommate-e2e}"
IMAGE="ghcr.io/middlendian/roommate-csi:dev"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cleanup() {
  if [[ "${KEEP_CLUSTER:-}" != "1" ]]; then
    kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if ! kind get clusters | grep -qx "$CLUSTER"; then
  kind create cluster --name "$CLUSTER" --config "$ROOT/hack/kind.yaml"
fi

docker build --build-arg VERSION=e2e -t "$IMAGE" "$ROOT"
kind load docker-image "$IMAGE" --name "$CLUSTER"

kubectl --context "kind-$CLUSTER" apply -k "$ROOT/deploy/kustomize/base"
kubectl --context "kind-$CLUSTER" -n roommate-system rollout status ds/roommate-node --timeout=180s

KUBECONFIG_FILE="$(mktemp)"
kind get kubeconfig --name "$CLUSTER" > "$KUBECONFIG_FILE"
KUBECONFIG="$KUBECONFIG_FILE" go test -tags=e2e -timeout=20m -count=1 -v "$ROOT/test/e2e/..."
```

Then: `chmod +x hack/e2e.sh`

- [ ] **Step 3: Write the fixtures**

Create `test/e2e/fixtures.go`:

```go
//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

func mustClient(t *testing.T) kubernetes.Interface {
	t.Helper()
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		t.Fatalf("kubeconfig: %v", err)
	}
	c, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	return c
}

// setupNamespace creates a namespace, ServiceAccount, Secret, and the
// per-object Role plus binding, then cleans them up.
func setupNamespace(t *testing.T, c kubernetes.Interface, ns string, data map[string][]byte) {
	t.Helper()
	ctx := context.Background()

	create(t, "namespace", func() error {
		_, err := c.CoreV1().Namespaces().Create(ctx,
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{})
		return err
	})
	t.Cleanup(func() {
		_ = c.CoreV1().Namespaces().Delete(context.Background(), ns, metav1.DeleteOptions{})
	})

	create(t, "serviceaccount", func() error {
		_, err := c.CoreV1().ServiceAccounts(ns).Create(ctx,
			&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "session-runner"}}, metav1.CreateOptions{})
		return err
	})
	create(t, "secret", func() error {
		_, err := c.CoreV1().Secrets(ns).Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "oauth-credentials"},
			Data:       data,
		}, metav1.CreateOptions{})
		return err
	})
	create(t, "role", func() error {
		_, err := c.RbacV1().Roles(ns).Create(ctx, &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Name: "roommate-oauth-credentials"},
			Rules: []rbacv1.PolicyRule{
				{
					APIGroups:     []string{""},
					Resources:     []string{"secrets"},
					ResourceNames: []string{"oauth-credentials"},
					Verbs:         []string{"get", "watch", "patch"},
				},
				{
					APIGroups:     []string{"coordination.k8s.io"},
					Resources:     []string{"leases"},
					ResourceNames: []string{"roommate-oauth-credentials"},
					Verbs:         []string{"get", "create", "update"},
				},
			},
		}, metav1.CreateOptions{})
		return err
	})
	create(t, "rolebinding", func() error {
		_, err := c.RbacV1().RoleBindings(ns).Create(ctx, &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "roommate-oauth-credentials"},
			RoleRef: rbacv1.RoleRef{
				APIGroup: rbacv1.GroupName, Kind: "Role", Name: "roommate-oauth-credentials"},
			Subjects: []rbacv1.Subject{{
				Kind: "ServiceAccount", Name: "session-runner", Namespace: ns}},
		}, metav1.CreateOptions{})
		return err
	})
}

func create(t *testing.T, what string, fn func() error) {
	t.Helper()
	if err := fn(); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create %s: %v", what, err)
	}
}

// podSpec returns a pod mounting the shared Secret via an inline volume,
// optionally pinned to a node.
func podSpec(name, ns, node string, script string) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1.PodSpec{
			ServiceAccountName: "session-runner",
			RestartPolicy:      corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:    "app",
				Image:   "debian:bookworm-slim",
				Command: []string{"/bin/sh", "-c", script},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "creds", MountPath: "/creds"},
				},
			}},
			Volumes: []corev1.Volume{{
				Name: "creds",
				VolumeSource: corev1.VolumeSource{
					CSI: &corev1.CSIVolumeSource{
						Driver: "roommate.csi",
						VolumeAttributes: map[string]string{
							"objectKind": "Secret",
							"objectName": "oauth-credentials",
						},
					},
				},
			}},
		},
	}
	if node != "" {
		p.Spec.NodeName = node
	}
	return p
}

func waitForPhase(t *testing.T, c kubernetes.Interface, ns, name string, want corev1.PodPhase, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		p, err := c.CoreV1().Pods(ns).Get(context.Background(), name, metav1.GetOptions{})
		if err == nil && p.Status.Phase == want {
			return
		}
		if time.Now().After(deadline) {
			describePod(t, c, ns, name)
			t.Fatalf("pod %s never reached %s", name, want)
		}
		time.Sleep(time.Second)
	}
}

func describePod(t *testing.T, c kubernetes.Interface, ns, name string) {
	t.Helper()
	events, err := c.CoreV1().Events(ns).List(context.Background(), metav1.ListOptions{
		FieldSelector: fmt.Sprintf("involvedObject.name=%s", name),
	})
	if err != nil {
		return
	}
	for _, e := range events.Items {
		t.Logf("event %s/%s: %s: %s", e.Type, e.Reason, name, e.Message)
	}
}

func workerNodes(t *testing.T, c kubernetes.Interface) []string {
	t.Helper()
	list, err := c.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	var names []string
	for _, n := range list.Items {
		if _, isCP := n.Labels["node-role.kubernetes.io/control-plane"]; isCP {
			continue
		}
		names = append(names, n.Name)
	}
	if len(names) < 2 {
		t.Fatalf("need 2 worker nodes, found %d", len(names))
	}
	return names
}
```

- [ ] **Step 4: Write the suite**

Create `test/e2e/e2e_test.go`:

```go
//go:build e2e

package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestReadWriteRoundTrip(t *testing.T) {
	c := mustClient(t)
	ns := "e2e-roundtrip"
	setupNamespace(t, c, ns, map[string][]byte{"session.key": []byte("v1")})

	pod := podSpec("writer", ns, "", `
set -e
test "$(cat /creds/session.key)" = v1
printf v2 > /creds/session.key
test "$(cat /creds/session.key)" = v2
`)
	if _, err := c.CoreV1().Pods(ns).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	waitForPhase(t, c, ns, "writer", corev1.PodSucceeded, 3*time.Minute)

	sec, err := c.CoreV1().Secrets(ns).Get(context.Background(), "oauth-credentials", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get secret: %v", err)
	}
	if string(sec.Data["session.key"]) != "v2" {
		t.Fatalf("secret session.key = %q, want v2 — the write never reached the object",
			sec.Data["session.key"])
	}
}

// A pod whose ServiceAccount has no grant must fail to mount, and the event
// must tell the operator exactly what to apply.
func TestDeniedMountExplainsItself(t *testing.T) {
	c := mustClient(t)
	ns := "e2e-denied"
	setupNamespace(t, c, ns, map[string][]byte{"session.key": []byte("v1")})

	// Remove the binding so the SA has no access.
	if err := c.RbacV1().RoleBindings(ns).Delete(
		context.Background(), "roommate-oauth-credentials", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete binding: %v", err)
	}

	pod := podSpec("denied", ns, "", "sleep 60")
	if _, err := c.CoreV1().Pods(ns).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}

	deadline := time.Now().Add(3 * time.Minute)
	for {
		events, _ := c.CoreV1().Events(ns).List(context.Background(), metav1.ListOptions{})
		for _, e := range events.Items {
			if strings.Contains(e.Message, "kind: Role") &&
				strings.Contains(e.Message, "session-runner") {
				return // the hint reached kubectl describe pod
			}
		}
		if time.Now().After(deadline) {
			describePod(t, c, ns, "denied")
			t.Fatal("no FailedMount event carrying the RBAC hint")
		}
		time.Sleep(2 * time.Second)
	}
}

// THE test. Two pods on different nodes both find an expired credential and
// both try to refresh. The lock plus the mandatory fresh read must mean
// exactly one refresh happens.
//
// Determinism comes from a barrier: neither pod proceeds until a starter key
// appears, so both are guaranteed to be contending rather than arriving
// sequentially by luck.
func TestRefreshRaceProducesExactlyOneRefresh(t *testing.T) {
	c := mustClient(t)
	ns := "e2e-refresh-race"
	setupNamespace(t, c, ns, map[string][]byte{
		"session.key": []byte("EXPIRED"),
	})

	nodes := workerNodes(t, c)

	// Each contender: wait for the barrier, lock, re-read, and refresh ONLY
	// if the credential is still expired. flock(1) holds the lock for the
	// whole subshell.
	script := `
set -e
apt-get update -qq >/dev/null 2>&1 && apt-get install -y -qq util-linux >/dev/null 2>&1 || true
for i in $(seq 1 120); do
  [ -f /creds/start ] && break
  sleep 1
done
flock /creds/session.key -c '
  cur=$(cat /creds/session.key)
  if [ "$cur" = EXPIRED ]; then
    printf REFRESHED-%s > /creds/session.key
    printf 1 >> /creds/refreshes
  fi
' 
`
	for i, node := range nodes[:2] {
		name := []string{"contender-a", "contender-b"}[i]
		pod := podSpec(name, ns, node, strings.ReplaceAll(script, "%s", name))
		if _, err := c.CoreV1().Pods(ns).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}

	// Wait for both to be running and blocked on the barrier, then release.
	for _, name := range []string{"contender-a", "contender-b"} {
		waitForPhase(t, c, ns, name, corev1.PodRunning, 3*time.Minute)
	}
	time.Sleep(5 * time.Second) // let both reach the barrier loop

	patchStarter(t, c, ns)

	for _, name := range []string{"contender-a", "contender-b"} {
		waitForPhase(t, c, ns, name, corev1.PodSucceeded, 3*time.Minute)
	}

	sec, err := c.CoreV1().Secrets(ns).Get(context.Background(), "oauth-credentials", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get secret: %v", err)
	}

	refreshes := len(sec.Data["refreshes"])
	if refreshes != 1 {
		t.Fatalf("refreshes = %d, want exactly 1; value = %q. More than one means "+
			"the lock or the guaranteed-fresh read failed, which is the exact "+
			"failure this driver exists to prevent",
			refreshes, sec.Data["session.key"])
	}
	if !strings.HasPrefix(string(sec.Data["session.key"]), "REFRESHED-") {
		t.Fatalf("session.key = %q, want a REFRESHED- value", sec.Data["session.key"])
	}
}

func patchStarter(t *testing.T, c kubernetes.Interface, ns string) {
	t.Helper()
	sec, err := c.CoreV1().Secrets(ns).Get(context.Background(), "oauth-credentials", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get secret: %v", err)
	}
	if sec.Data == nil {
		sec.Data = map[string][]byte{}
	}
	sec.Data["start"] = []byte("go")
	if _, err := c.CoreV1().Secrets(ns).Update(context.Background(), sec, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("release barrier: %v", err)
	}
}
```

Add `"k8s.io/client-go/kubernetes"` to the import block.

- [ ] **Step 5: Add the Makefile target and CI workflow**

Append to `Makefile`:

```make
.PHONY: e2e
e2e:
	hack/e2e.sh
```

Create `.github/workflows/e2e.yml`:

```yaml
name: e2e
on:
  push:
    branches: [main]
  pull_request:
  workflow_dispatch:

jobs:
  e2e:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: {go-version: '1.25'}
      - uses: helm/kind-action@v1
        with: {install_only: true}
      - run: make e2e
```

- [ ] **Step 6: Run it**

Run: `make e2e`
Expected: PASS, all three tests. `TestRefreshRaceProducesExactlyOneRefresh` is
the one that matters — if it fails with `refreshes = 2`, the lock or the
guaranteed-fresh read is broken, and that is a release blocker.

- [ ] **Step 7: Commit**

```bash
git add hack test/e2e Makefile .github/workflows/e2e.yml
git commit -m "$(cat <<'EOF'
test: kind end-to-end suite

Three tests: a read/write round trip proving writes reach the object, a
denied mount proving the RBAC hint reaches kubectl describe pod, and the
refresh race.

The refresh race is the reason the project exists, so it is made
deterministic rather than left to luck: both contenders are pinned to
different nodes and blocked on a barrier key, then released together, so
they are guaranteed to be contending. Exactly one refresh must occur —
two means the lock or the guaranteed-fresh read failed.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
EOF
)"
```

---

### Task 21: README and repository documentation

**Files:**
- Modify: `README.md`
- Create: `CLAUDE.md`, `CHANGELOG.md`

**Interfaces:**
- Consumes: everything.
- Produces: the documentation a first-time reader needs.

- [ ] **Step 1: Write the README**

Replace `README.md`. It must open by saying what the driver does, because
`roommate.csi` in a pod spec tells a stranger nothing. Required sections:

1. **One-line statement** — mount a Secret or ConfigMap as a *writable*,
   cross-node shared directory, with `flock` backed by a Lease. Then the pun:
   roommates share a lease.
2. **Why** — the refresh race, in the form from the spec's Problem section.
3. **How it differs** from `secrets-store-csi-driver`, OpenShift
   `csi-driver-shared-resource`, and kubelet projection: all read-only, none
   lock.
4. **Install** — `kubectl apply -k`, then the grant, then the pod. Copy the
   YAML from `deploy/` and `examples/` verbatim so it cannot drift.
5. **The consumer contract** — the double-checked pattern. State plainly that
   a consumer which refreshes unconditionally after locking will still
   double-refresh; the driver guarantees the read is fresh, it cannot make
   the consumer look at it.
6. **Configuration** — the `volumeAttributes` table from the spec.
7. **Security** — the driver holds zero RBAC; access is the pod's own.
8. **Limitations** — copied from the spec's Limitations section verbatim,
   including that `inotify` does not fire on remote changes and that open
   descriptors break across a plugin restart.

- [ ] **Step 2: Write CLAUDE.md**

Create `CLAUDE.md` covering: repo map, `make` targets and which need
`/dev/fuse` or a cluster, the invariants an agent must not break (zero driver
RBAC; quorum GET means empty `ResourceVersion`; watches need the field
selector; republish never errors after first success; commit-then-release
ordering), and the pre-merge checklist.

- [ ] **Step 3: Start the CHANGELOG**

Create `CHANGELOG.md` in Keep a Changelog format with an `## [Unreleased]`
section listing the v1 feature set.

- [ ] **Step 4: Verify the documented commands work**

Run every command block in the README against a live kind cluster from Task
20. Any that fails is a documentation bug — fix the docs, not the test.

- [ ] **Step 5: Commit**

```bash
git add README.md CLAUDE.md CHANGELOG.md
git commit -m "$(cat <<'EOF'
docs: README, contributor guide, and changelog

The README opens with what the driver does, since roommate.csi in a pod spec
tells a stranger nothing on its own.

It states the consumer contract explicitly: the double-checked pattern is
what makes this safe, and a consumer that refreshes unconditionally after
locking will still double-refresh. The driver guarantees the post-lock read
is fresh; it cannot make the consumer look at it.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01DejgP8xmghBjNSNaogtnny
EOF
)"
```

---

## Plan self-review

Run against the spec after the plan is written.

**Spec coverage.** Every spec section maps to a task: Architecture → 1, 10,
16; data model → 3, 4, 5; filenames → 2; modes → 10, 12; read path and
consistency → 6, 11; quorum-GET definition → 4, 19; write path → 8, 12;
locking → 7, 13; mount lifecycle → 14, 15; failure modes → 8, 12, 15;
configuration → 10; authorization → 9, 15, 18, 19; security → 9, 18; testing
→ 19, 20; limitations and inotify → 21.

**Deferred deliberately, recorded here so it is a decision and not a gap:**

- `NodeGetVolumeStats` — spec open question 4 defers it from v1.
- RBAC setup tooling (kustomize component / CLI) — spec open question 5
  defers it; v1 ships the ClusterRole and a documented Role only.
- inotify — spec's inotify section defers both routes. Task 21 documents the
  limitation; no task implements it.
- Watch fan-out sharing — spec open question 2 leaves per-(pod, volume)
  watches as v1 behaviour.

**Type consistency.** Names used across task boundaries, fixed here:
`Snapshot.Get/Keys/Size/With`; `Store.Get/Watch/Decode/Patch/Describe`;
`Cache.Current/Fresh/MaybeFresh/Set/Run`; `Committer.Commit(ctx, lease, set,
del)`; `LeaseManager.TryAcquire/Acquire/Release/Healthy`;
`Volume.Cfg/Store/Cache/Committer/Run/NewLease`; `ValidKey`; `errnoFor`;
`objectfs.Mount(dir, vol) (*fuse.Server, error)`;
`mounts.Registry.Get/Put/Delete/WasPublished`; `mounts.Live.Cancel/Unmount/Token`;
`podtoken.Extract/PodIdentity/ClientFor`; `driver.RBACHint`.

**Cross-task couplings a reviewer should check:**

- Task 11 defines `handle` with `mu`, `snap`, `buf`, `dirty`, `lease`. Tasks
  12 and 13 append methods to that same struct — they must not redefine it.
- `errnoFor` is introduced in Task 12 and used by Tasks 12, 13, and 15.
- `stubStore` and `recordingStore` are defined in Tasks 6 and 8 and reused by
  the FUSE tests in Tasks 11–13. Running FUSE tests alone will not compile
  without those files present.
- Task 15's `node_test.go` reaches `n.registry`, so the field name must stay
  exactly `registry`.
