package podtoken

import "testing"

// TestVolumeContextKeyLiterals pins every exported Key* constant to its
// literal wire value, per the CSI pod-info spec:
// https://kubernetes-csi.github.io/docs/token-requests.html and
// https://kubernetes-csi.github.io/docs/pod-info.html
//
// This intentionally does NOT compare against the Key* constants — it
// hardcodes the expected string on the right-hand side. A test built from
// the constant itself (want: KeyPodServiceAcct, or a fixture map keyed by
// KeyPodServiceAcct) is tautological: it passes no matter what the constant
// is set to, because both sides drift together. That is exactly how
// KeyPodServiceAcct shipped as "csi.storage.k8s.io/pod.service-account.name"
// — a key kubelet never sets — while every existing test, built from the
// constant, stayed green. The whole point of this test is to fail if a
// future edit changes a constant's value without the wire contract having
// changed. Do not "simplify" this back to referencing the constants.
func TestVolumeContextKeyLiterals(t *testing.T) {
	tests := []struct {
		name        string
		got         string
		wantLiteral string
	}{
		{"KeyTokens", KeyTokens, "csi.storage.k8s.io/serviceAccount.tokens"},
		{"KeyPodNamespace", KeyPodNamespace, "csi.storage.k8s.io/pod.namespace"},
		{"KeyPodName", KeyPodName, "csi.storage.k8s.io/pod.name"},
		{"KeyPodUID", KeyPodUID, "csi.storage.k8s.io/pod.uid"},
		{"KeyPodServiceAcct", KeyPodServiceAcct, "csi.storage.k8s.io/serviceAccount.name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.wantLiteral {
				t.Fatalf("%s = %q, want %q", tt.name, tt.got, tt.wantLiteral)
			}
		})
	}
}
