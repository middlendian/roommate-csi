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
