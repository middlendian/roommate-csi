//go:build e2e

// Package e2e drives the CSI driver through a real kubelet against a kind
// cluster: it creates namespaces, RBAC, Secrets, and Pods with inline CSI
// volumes, and asserts on Pod phase, Kubernetes events, and Secret contents.
// It is excluded from `go test ./...` by the e2e build tag; run it via
// `make e2e` / `make e2e-nfs`, which boot the cluster first.
package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
// Determinism comes from a barrier: neither pod proceeds past its own
// waiting-<name> marker until a starter key appears, and the test does not
// release that starter until it has observed both markers — so both
// contenders are guaranteed to be genuinely contending (blocked on the same
// Setlkw from different nodes) rather than arriving sequentially by luck.
func TestRefreshRaceProducesExactlyOneRefresh(t *testing.T) {
	c := mustClient(t)
	ns := "e2e-refresh-race"
	setupNamespace(t, c, ns, map[string][]byte{
		"session.key": []byte("EXPIRED"),
	})

	nodes := workerNodes(t, c)
	names := []string{"contender-a", "contender-b"}

	// Each contender: record that it has reached the barrier, wait for the
	// barrier to release, lock, re-read, and refresh ONLY if the credential
	// is still expired. flock(1) holds the lock for the whole subshell.
	// debian:bookworm-slim already ships flock via util-linux (an essential
	// package); the apt-get is a defensive fallback, only hit if that ever
	// changes.
	//
	// Each contender records its own outcome under its own key
	// (refreshed-<name>) instead of appending to one shared counter key.
	// Commits are per-key merge patches with no CAS, no read-modify-write,
	// and no retry (pkg/objectfs/commit.go), and the two contenders run on
	// different nodes with independent caches. A shared counter key is
	// exactly the write a lost update hides behind: if the lock or the
	// mandatory fresh read failed and both contenders were granted
	// concurrently, both would read the counter as empty and both write the
	// same one-byte value — the test would see length 1 even though both
	// refreshed. Two distinct keys cannot suffer that: a merge patch only
	// ever touches the key(s) it names, so both contenders' patches land
	// regardless of ordering (proved against a real API server by
	// TestMergePatchPreservesUntouchedKeys in test/apisemantics). Counting
	// which marker keys exist therefore tells us how many contenders
	// actually refreshed, not just a length that a correct and a buggy run
	// can produce identically.
	script := `
set -e
if ! command -v flock >/dev/null 2>&1; then
  apt-get update -qq >/dev/null 2>&1 && apt-get install -y -qq util-linux >/dev/null 2>&1 || true
fi
printf '' > /creds/waiting-%s
for i in $(seq 1 120); do
  [ -f /creds/start ] && break
  sleep 1
done
flock /creds/session.key -c '
  cur=$(cat /creds/session.key)
  if [ "$cur" = EXPIRED ]; then
    printf REFRESHED-%s > /creds/session.key
    printf "" > /creds/refreshed-%s
  fi
'
`
	for i, node := range nodes[:2] {
		name := names[i]
		pod := podSpec(name, ns, node, strings.ReplaceAll(script, "%s", name))
		if _, err := c.CoreV1().Pods(ns).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}

	// Wait for both to be running, then for both to have actually reached
	// the barrier poll loop. PodRunning only proves the container started;
	// waiting for each contender's own waiting-<name> key is a real
	// rendezvous, not a guess that a fixed sleep gave both scripts enough
	// time to get there.
	for _, name := range names {
		waitForPhase(t, c, ns, name, corev1.PodRunning, 3*time.Minute)
	}
	waiting := make([]string, len(names))
	for i, name := range names {
		waiting[i] = "waiting-" + name
	}
	waitForSecretKeys(t, c, ns, waiting, 3*time.Minute)

	patchStarter(t, c, ns)

	for _, name := range names {
		waitForPhase(t, c, ns, name, corev1.PodSucceeded, 3*time.Minute)
	}

	sec, err := c.CoreV1().Secrets(ns).Get(context.Background(), "oauth-credentials", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get secret: %v", err)
	}

	var winners []string
	for _, name := range names {
		if _, ok := sec.Data["refreshed-"+name]; ok {
			winners = append(winners, name)
		}
	}
	if len(winners) != 1 {
		t.Fatalf("contenders that refreshed = %v (want exactly 1); session.key = %q. "+
			"More than one means the lock or the guaranteed-fresh read failed, which is "+
			"the exact failure this driver exists to prevent; zero means neither ever "+
			"observed EXPIRED",
			winners, sec.Data["session.key"])
	}
	// The invariant is "exactly one contender refreshed" — name which one
	// and assert session.key agrees, rather than accepting either value.
	want := "REFRESHED-" + winners[0]
	if string(sec.Data["session.key"]) != want {
		t.Fatalf("session.key = %q, want %q — %s's own marker says it won the race",
			sec.Data["session.key"], want, winners[0])
	}
}

// TestNodePluginRestartRecoversMount is C3's regression guard: spec:664
// names node-plugin restart recovery as a required e2e scenario, and it was
// missing — the restart test "would very likely have caught C3"
// (final-review-findings.md).
//
// It force-kills (GracePeriodSeconds: 0) the roommate-node pod on the node a
// long-lived consumer pod is scheduled on, deliberately bypassing the
// graceful SIGTERM shutdown path entirely: the assertion below rests on
// publish()'s DetachStale fix (a hard crash, the case Shutdown cannot help
// with), not on Registry.Shutdown's best-effort unmount.
//
// It does NOT assert that the survivor pod's own already-mounted view
// recovers: kubelet's bind-mount of the CSI target into that pod's mount
// namespace is a snapshot taken when the pod started, and replacing the
// FUSE mount underneath it at the host level does not retroactively fix an
// already-established bind-mount reference elsewhere — confirmed
// empirically in CI (ENOTCONN persisted in the consumer's namespace even
// after the driver's own remount succeeded). That is the design's own
// documented, accepted limitation ("open descriptors break across a
// node-plugin restart" — spec's failure-mode table), not what C3 fixes.
// What C3 fixes, and what this test actually proves, is that the DRIVER
// SIDE recovers: the pre-existing target gets a fresh, successful
// NodePublishVolume (visible as a "published" log line for that exact
// target from the *new* driver process — before the fix this never
// happens, republish just swallows the mkdir/mount failure forever) and
// the pod can still be torn down cleanly afterward rather than sticking in
// Terminating on a mount kubelet can never clean up.
func TestNodePluginRestartRecoversMount(t *testing.T) {
	c := mustClient(t)
	ns := "e2e-restart"
	setupNamespace(t, c, ns, map[string][]byte{"session.key": []byte("v1")})

	pod := podSpec("survivor", ns, "", `
set -e
test "$(cat /creds/session.key)" = v1
sleep 600
`)
	if _, err := c.CoreV1().Pods(ns).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	t.Cleanup(func() {
		_ = c.CoreV1().Pods(ns).Delete(context.Background(), "survivor", metav1.DeleteOptions{})
	})

	waitForPhase(t, c, ns, "survivor", corev1.PodRunning, 3*time.Minute)

	survivor, err := c.CoreV1().Pods(ns).Get(context.Background(), "survivor", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get survivor pod: %v", err)
	}
	node := survivor.Spec.NodeName
	// The exact target path kubelet mounts this inline CSI volume at,
	// matching podSpec's volume name "creds" — this is the pre-existing,
	// about-to-go-stale mount C3's fix must successfully remount.
	target := fmt.Sprintf("/var/lib/kubelet/pods/%s/volumes/kubernetes.io~csi/creds/mount", survivor.UID)

	driverPod := driverPodOnNode(t, c, node)

	grace := int64(0)
	if err := c.CoreV1().Pods("roommate-system").Delete(context.Background(), driverPod.Name,
		metav1.DeleteOptions{GracePeriodSeconds: &grace}); err != nil {
		t.Fatalf("force-delete driver pod %s: %v", driverPod.Name, err)
	}

	// Wait for the DaemonSet to replace it with a new, running pod on the
	// same node.
	var newDriverPod *corev1.Pod
	deadline := time.Now().Add(3 * time.Minute)
	for {
		newDriverPod = driverPodOnNodeOrNil(t, c, node)
		if newDriverPod != nil && newDriverPod.UID != driverPod.UID && newDriverPod.Status.Phase == corev1.PodRunning {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("DaemonSet never replaced the killed driver pod with a running one")
		}
		time.Sleep(time.Second)
	}

	// Prove the remount actually happened: the new process's own log must
	// carry a fresh "published" line for survivor's pre-existing target.
	// The new container's log starts empty, so any match here is
	// necessarily post-restart. Before the fix this line never appears —
	// MkdirAll's Stat sees ENOTCONN forever, and the republish path
	// swallows that error silently by design (see node.go's `restarting`
	// branch), so nothing else would ever surface the failure.
	deadline = time.Now().Add(3 * time.Minute)
	for {
		logs, err := getPodLogs(context.Background(), c, "roommate-system", newDriverPod.Name, "roommate", false)
		if err == nil && strings.Contains(logs, "published") && strings.Contains(logs, target) {
			break
		}
		if time.Now().After(deadline) {
			describePod(t, c, ns, "survivor")
			t.Fatalf("new driver process %s never logged a successful remount of %s", newDriverPod.Name, target)
		}
		time.Sleep(2 * time.Second)
	}

	// The pod must also delete cleanly afterward: Registry.Delete's
	// stale-mount detach must not leave it stuck in Terminating.
	if err := c.CoreV1().Pods(ns).Delete(context.Background(), "survivor", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete survivor pod: %v", err)
	}
	deadline = time.Now().Add(2 * time.Minute)
	for {
		_, err := c.CoreV1().Pods(ns).Get(context.Background(), "survivor", metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("survivor pod stuck after delete — the mount left behind by the restart did not clean up")
		}
		time.Sleep(time.Second)
	}
}

// driverPodOnNode returns the roommate-node DaemonSet pod scheduled on node,
// failing the test if there isn't exactly one.
func driverPodOnNode(t *testing.T, c kubernetes.Interface, node string) *corev1.Pod {
	t.Helper()
	pods, err := c.CoreV1().Pods("roommate-system").List(context.Background(), metav1.ListOptions{
		LabelSelector: "app=roommate-node",
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", node),
	})
	if err != nil {
		t.Fatalf("list driver pods on %s: %v", node, err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("driver pods on %s = %d, want exactly 1", node, len(pods.Items))
	}
	return &pods.Items[0]
}

// driverPodOnNodeOrNil is driverPodOnNode without failing the test, for use
// inside a polling loop where "not there yet" is an expected transient state.
func driverPodOnNodeOrNil(t *testing.T, c kubernetes.Interface, node string) *corev1.Pod {
	t.Helper()
	pods, err := c.CoreV1().Pods("roommate-system").List(context.Background(), metav1.ListOptions{
		LabelSelector: "app=roommate-node",
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", node),
	})
	if err != nil || len(pods.Items) == 0 {
		return nil
	}
	return &pods.Items[0]
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
