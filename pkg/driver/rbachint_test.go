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
