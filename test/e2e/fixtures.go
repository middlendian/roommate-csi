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
					Verbs:         []string{"get", "update"},
				},
				// RBAC cannot scope "create" by resourceNames (the object's
				// name isn't known at authorization time), so a
				// resourceNames-restricted create rule silently denies
				// every create — this must be a separate, unscoped rule.
				{
					APIGroups: []string{"coordination.k8s.io"},
					Resources: []string{"leases"},
					Verbs:     []string{"create"},
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
		// A terminal phase other than the one we're waiting for will never
		// change on its own — fail immediately with diagnostics instead of
		// burning the rest of the deadline polling a phase that is done
		// changing. This is what makes a genuine failure (as opposed to a
		// slow-starting pod) show up with a timestamp close to the actual
		// event instead of the full timeout later.
		if err == nil && (p.Status.Phase == corev1.PodFailed || p.Status.Phase == corev1.PodSucceeded) && p.Status.Phase != want {
			describePod(t, c, ns, name)
			t.Fatalf("pod %s is %s (terminal), want %s", name, p.Status.Phase, want)
		}
		if time.Now().After(deadline) {
			describePod(t, c, ns, name)
			t.Fatalf("pod %s never reached %s", name, want)
		}
		time.Sleep(time.Second)
	}
}

// waitForSecretKeys blocks until oauth-credentials carries every key in
// want, or d elapses. It is a rendezvous primitive for tests that need to
// know several independent processes have each reached a specific point —
// each process proves it by writing its own key through the mount — rather
// than guessing via a fixed sleep after a phase transition that only proves
// the container started.
func waitForSecretKeys(t *testing.T, c kubernetes.Interface, ns string, want []string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		sec, err := c.CoreV1().Secrets(ns).Get(context.Background(), "oauth-credentials", metav1.GetOptions{})
		if err == nil {
			var missing []string
			for _, k := range want {
				if _, ok := sec.Data[k]; !ok {
					missing = append(missing, k)
				}
			}
			if len(missing) == 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("oauth-credentials never gained keys %v (still missing %v after %s)", want, missing, d)
			}
		} else if time.Now().After(deadline) {
			t.Fatalf("get secret: %v", err)
		}
		time.Sleep(time.Second)
	}
}

// describePod logs everything useful for post-mortem: the pod's events, its
// container statuses (exit code / reason / message), and both the current
// and any previous-incarnation container logs, plus the roommate-node
// driver's own logs from the node the pod ran on. It never fails the test
// itself — it is called just before a t.Fatalf that already knows why the
// wait didn't succeed; the point here is *why the pod's own process*
// didn't reach the expected phase.
func describePod(t *testing.T, c kubernetes.Interface, ns, name string) {
	t.Helper()
	ctx := context.Background()

	events, err := c.CoreV1().Events(ns).List(ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("involvedObject.name=%s", name),
	})
	if err == nil {
		for _, e := range events.Items {
			t.Logf("event %s/%s: %s: %s", e.Type, e.Reason, name, e.Message)
		}
	}

	pod, err := c.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Logf("describePod: get pod %s: %v", name, err)
		return
	}
	for _, cs := range pod.Status.ContainerStatuses {
		t.Logf("container %s state: %+v", cs.Name, cs.State)
		if cs.LastTerminationState.Terminated != nil {
			t.Logf("container %s last termination: %+v", cs.Name, cs.LastTerminationState.Terminated)
		}
	}

	for _, container := range pod.Spec.Containers {
		if logs, err := getPodLogs(ctx, c, ns, name, container.Name, false); err == nil && logs != "" {
			t.Logf("logs %s/%s:\n%s", name, container.Name, logs)
		}
		if logs, err := getPodLogs(ctx, c, ns, name, container.Name, true); err == nil && logs != "" {
			t.Logf("previous logs %s/%s:\n%s", name, container.Name, logs)
		}
	}

	if pod.Spec.NodeName != "" {
		dumpDriverLogs(t, c, pod.Spec.NodeName)
	}
}

func getPodLogs(ctx context.Context, c kubernetes.Interface, ns, pod, container string, previous bool) (string, error) {
	req := c.CoreV1().Pods(ns).GetLogs(pod, &corev1.PodLogOptions{Container: container, Previous: previous})
	raw, err := req.DoRaw(ctx)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// dumpDriverLogs logs the roommate-node driver container's output from the
// DaemonSet pod scheduled on nodeName, so a mount-time failure shows the
// server side of the story alongside the client-side pod logs above.
func dumpDriverLogs(t *testing.T, c kubernetes.Interface, nodeName string) {
	t.Helper()
	ctx := context.Background()
	const driverNS = "roommate-system"

	pods, err := c.CoreV1().Pods(driverNS).List(ctx, metav1.ListOptions{
		LabelSelector: "app=roommate-node",
		FieldSelector: fmt.Sprintf("spec.nodeName=%s", nodeName),
	})
	if err != nil || len(pods.Items) == 0 {
		t.Logf("dumpDriverLogs: no roommate-node pod found on %s: %v", nodeName, err)
		return
	}
	driverPod := pods.Items[0].Name
	if logs, err := getPodLogs(ctx, c, driverNS, driverPod, "roommate", false); err == nil {
		t.Logf("driver logs %s (node %s):\n%s", driverPod, nodeName, logs)
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
