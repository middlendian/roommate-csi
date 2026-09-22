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

// TestRBACHintBindingNameIncludesServiceAccount is I7's regression guard.
// Before the fix, the RoleBinding was named just "roommate-<objectName>",
// with no ServiceAccount component. Two ServiceAccounts mounting the same
// Secret/ConfigMap would each be told to apply a binding with that same
// name — and applying a RoleBinding replaces its subjects list wholesale
// rather than merging, so the second apply silently drops the first
// ServiceAccount's subject. Its next API call 403s, its watch dies, and it
// degrades to a frozen cache with EACCES writes, with nothing pointing at
// the cause.
func TestRBACHintBindingNameIncludesServiceAccount(t *testing.T) {
	gotA := RBACHint("my-app", "service-a", "Secret", "oauth-credentials", "roommate-oauth-credentials")
	gotB := RBACHint("my-app", "service-b", "Secret", "oauth-credentials", "roommate-oauth-credentials")

	if !strings.Contains(gotA, "name: roommate-oauth-credentials-service-a") {
		t.Errorf("hint for service-a does not name a per-ServiceAccount binding:\n%s", gotA)
	}
	if !strings.Contains(gotB, "name: roommate-oauth-credentials-service-b") {
		t.Errorf("hint for service-b does not name a per-ServiceAccount binding:\n%s", gotB)
	}

	// The two ServiceAccounts' hints must not tell them to apply a
	// RoleBinding under the exact same name — that is precisely the
	// collision this fix closes.
	bindingName := func(hint string) string {
		i := strings.Index(hint, "kind: RoleBinding")
		if i < 0 {
			t.Fatalf("hint has no RoleBinding:\n%s", hint)
		}
		rest := hint[i:]
		j := strings.Index(rest, "name: ")
		if j < 0 {
			t.Fatalf("RoleBinding has no name:\n%s", hint)
		}
		rest = rest[j+len("name: "):]
		if k := strings.IndexAny(rest, "\r\n"); k >= 0 {
			rest = rest[:k]
		}
		return rest
	}
	if bindingName(gotA) == bindingName(gotB) {
		t.Fatalf("service-a and service-b are told to apply the same RoleBinding name %q — "+
			"applying the second would silently replace the first's subject", bindingName(gotA))
	}

	// The Role — shared per-object, not per-ServiceAccount — is unaffected:
	// re-applying identical rules is idempotent and loses nothing, unlike a
	// RoleBinding's subjects list.
	if !strings.Contains(gotA, "name: roommate-oauth-credentials\n") {
		t.Errorf("Role name changed unexpectedly:\n%s", gotA)
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
