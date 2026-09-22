//go:build envtest

// Package apisemantics verifies claims the design depends on that the fake
// clientset cannot model. Each test here corresponds to a design decision
// that would fail silently in production if the API server behaved
// differently than assumed.
package apisemantics

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/middlendian/roommate-csi/pkg/objectfs"
)

var cfg *rest.Config

// TestMain boots one shared envtest API server (kube-apiserver + etcd) for
// the whole package, since each test only needs a fresh namespace, not a
// fresh cluster.
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
	// Not just present — intact. C2's fix (Cache.setFromWatch) and every
	// other place that leans on "a merge patch touches only the keys it
	// names" needs the untouched sibling's *value* preserved, not merely
	// that the key still exists.
	if v, ok := snap.Get("a"); !ok || string(v) != "1" {
		t.Errorf("a = %q (present=%v), want \"1\" — a patch that only nulled b must not touch a's value", v, ok)
	}
}

// CLAIM 3: exercises 20 rounds of patch-then-Get and asserts each Get
// reflects the immediately preceding patch. This does NOT prove quorum
// semantics, despite living next to the tests that do: envtest runs a
// single-member apiserver + etcd, where the watch cache (what a
// ResourceVersion: "0" Get would be served from) lags real writes by
// something close to zero — a multi-member, more realistically-lagged
// cluster could pass every round here while a non-quorum Get would still be
// a bug. The falsifiable guard for "this Store issues a genuine quorum
// read" is TestSecretStoreGetIsQuorumRead (pkg/objectfs/store_secret_test.go),
// which inspects the actual GetOptions.ResourceVersion sent on the wire.
// What this test is actually good for: a real, if weak, end-to-end signal
// that Patch and Get are talking to the same backing object at all.
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

// CLAIM 5: LeaseManager.takeOver's in-place Update is CAS-guarded by the
// Lease's own resourceVersion, so of two LeaseManagers racing to take over
// the same expired Lease, exactly one wins. This is the mechanism the whole
// driver leans on for cross-node mutual exclusion — a process-local mutex
// cannot substitute for it, since the racing holders are on different nodes
// entirely.
//
// The fake clientset used by pkg/objectfs's own unit tests does not enforce
// resourceVersion conflicts on Update at all (confirmed empirically during
// design work: two concurrent fake updaters against the same expired Lease
// both succeed, neither observes a conflict). So this claim has, until now,
// been verified by nothing. If takeOver ever stops passing the fetched
// object's resourceVersion through to Update — e.g. by rebuilding the Lease
// from scratch, or switching to an unconditional merge patch — both
// LeaseManagers below would race to write the same expired Lease and both
// could observe success, which this test asserts against directly.
func TestLeaseManagerCASRejectsStaleWriter(t *testing.T) {
	c, ns := setup(t)
	ctx := context.Background()

	// tinyDuration stands in for leaseDurationSeconds here. The seeded
	// Lease deliberately leaves LeaseDurationSeconds nil so claimable falls
	// back to each LeaseManager's own duration (see claimable in
	// pkg/objectfs/lease.go). It can't go below 1s: takeOver/create derive
	// spec.leaseDurationSeconds from it via int32(duration/time.Second),
	// and the real API server rejects 0 as invalid.
	const tinyDuration = time.Second

	const rounds = 25
	for i := 0; i < rounds; i++ {
		leaseName := fmt.Sprintf("race-%d", i)
		staleHolder := "long-gone-holder"
		expired := metav1.NewMicroTime(time.Now().Add(-time.Hour))
		_, err := c.CoordinationV1().Leases(ns).Create(ctx, &coordv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: leaseName, Namespace: ns},
			Spec: coordv1.LeaseSpec{
				HolderIdentity: &staleHolder,
				RenewTime:      &expired,
			},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("round %d: seed expired lease: %v", i, err)
		}

		mgrA := objectfs.NewLeaseManager(c, ns, leaseName, fmt.Sprintf("node-a:%d", i), tinyDuration)
		mgrB := objectfs.NewLeaseManager(c, ns, leaseName, fmt.Sprintf("node-b:%d", i), tinyDuration)

		// Since the clock-skew fix (final-wave item 4), a foreign lease is
		// judged expired against each LeaseManager's own local observation
		// time, not the remote RenewTime directly — so a lone TryAcquire
		// against an already-stale lease no longer takes over on first
		// sight; it only starts that manager's observation clock. Prime
		// both managers sequentially, then wait out tinyDuration, so the
		// race below exercises the CAS guard this test is actually about
		// rather than the (separately tested) observation window.
		if ok, err := mgrA.TryAcquire(ctx); err != nil || ok {
			t.Fatalf("round %d: priming TryAcquire for A = %v, %v; want false, nil on first sighting", i, ok, err)
		}
		if ok, err := mgrB.TryAcquire(ctx); err != nil || ok {
			t.Fatalf("round %d: priming TryAcquire for B = %v, %v; want false, nil on first sighting", i, ok, err)
		}
		time.Sleep(tinyDuration + 200*time.Millisecond)

		// Release both goroutines together so their TryAcquire calls'
		// Get-then-Update windows overlap on the real API server as often as
		// possible; the assertion below must hold regardless of how much
		// they actually overlap on any given round.
		start := make(chan struct{})
		var wg sync.WaitGroup
		var okA, okB bool
		var errA, errB error
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			okA, errA = mgrA.TryAcquire(ctx)
		}()
		go func() {
			defer wg.Done()
			<-start
			okB, errB = mgrB.TryAcquire(ctx)
		}()
		close(start)
		wg.Wait()

		if errA != nil || errB != nil {
			t.Fatalf("round %d: TryAcquire error: A=%v B=%v", i, errA, errB)
		}

		wins := 0
		if okA {
			wins++
		}
		if okB {
			wins++
		}
		if wins != 1 {
			t.Fatalf("round %d: %d of 2 racing acquirers won the same expired lease (want exactly 1); A=%v B=%v — takeOver is no longer CAS-guarded by resourceVersion", i, wins, okA, okB)
		}

		winner, loser := mgrA, mgrB
		if okB {
			winner, loser = mgrB, mgrA
		}
		if !winner.Healthy() {
			t.Fatalf("round %d: winner does not believe it holds the lock", i)
		}
		if loser.Healthy() {
			t.Fatalf("round %d: loser believes it holds the lock", i)
		}
		if err := winner.Release(ctx); err != nil {
			t.Fatalf("round %d: release winner: %v", i, err)
		}
	}
}
