//go:build e2e

// Package e2e drives the CSI driver through a real kubelet against a kind
// cluster: it creates namespaces, RBAC, Secrets, and Pods with inline CSI
// volumes, and asserts on Pod phase, Kubernetes events, and Secret contents.
// It is excluded from `go test ./...` by the e2e build tag; run it via
// `make e2e` / `make e2e-nfs`, which boot the cluster first.
package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
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
	// whole subshell. debian:bookworm-slim already ships flock via
	// util-linux (an essential package); the apt-get is a defensive
	// fallback, only hit if that ever changes.
	script := `
set -e
if ! command -v flock >/dev/null 2>&1; then
  apt-get update -qq >/dev/null 2>&1 && apt-get install -y -qq util-linux >/dev/null 2>&1 || true
fi
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
