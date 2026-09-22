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
    verbs: ["get", "update"]
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["create"]
---
# Named per-ServiceAccount, not just per-object: a RoleBinding's subjects
# list is replaced wholesale on apply, not merged with what's already
# there. If another ServiceAccount already has a working binding for
# %s/%s, add a subject to that one instead of applying this — applying
# this alongside one named just "roommate-%s" would silently drop the
# other ServiceAccount's subject and 403 its next API call.
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  namespace: %s
  name: roommate-%s-%s
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
		kind, objectName, objectName,
		namespace, objectName, serviceAccount,
		objectName,
		serviceAccount, namespace)
}
