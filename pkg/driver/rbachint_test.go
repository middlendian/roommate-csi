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

// RBAC cannot restrict a create request by resourceNames — the object's
// name isn't known at authorization time, so the API server's RBAC
// authorizer checks resourceNames against an empty name and the rule never
// matches. A leases rule that grants "create" alongside a resourceNames
// list therefore silently denies every create, which is exactly the
// mount-time failure this regression guards: the first pod to take the
// Lease for a given object gets Permission denied on its very first flock,
// because the Lease does not exist yet and nothing is allowed to create it.
func TestRBACHintLeaseCreateIsNotNameScoped(t *testing.T) {
	got := RBACHint("my-app", "session-runner", "Secret", "oauth-credentials", "roommate-oauth-credentials")

	createRule := `- apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["create"]`
	if !strings.Contains(got, createRule) {
		t.Fatalf("hint must grant \"create\" on leases as its own rule with no resourceNames "+
			"(RBAC cannot scope create by resourceNames):\n%s", got)
	}

	getUpdateRule := `- apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    resourceNames: ["roommate-oauth-credentials"]
    verbs: ["get", "update"]`
	if !strings.Contains(got, getUpdateRule) {
		t.Fatalf("hint must keep \"get\"/\"update\" on leases scoped by resourceNames:\n%s", got)
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
