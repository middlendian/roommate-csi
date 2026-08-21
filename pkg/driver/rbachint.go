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
