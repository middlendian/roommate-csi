package objectfs

import (
	"context"
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func secretFixture() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: "oauth-credentials", Namespace: "my-app", ResourceVersion: "42",
		},
		Data: map[string][]byte{"session.key": []byte("v1"), "config.json": []byte("{}")},
	}
}

func TestSecretStoreGet(t *testing.T) {
	c := fake.NewSimpleClientset(secretFixture())
	s := NewSecretStore(c, "my-app", "oauth-credentials")

	snap, err := s.Get(context.Background())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got, _ := snap.Get("session.key"); string(got) != "v1" {
		t.Errorf("session.key = %q, want v1", got)
	}
	if snap.ResourceVersion != "42" {
		t.Errorf("ResourceVersion = %q, want 42", snap.ResourceVersion)
	}
	if snap.FetchedAt.IsZero() {
		t.Error("FetchedAt not stamped")
	}
}

// A quorum read requires ResourceVersion to be empty. Passing "0" would be
// served from the API server's watch cache and silently break the
// read-after-write guarantee, so assert on the actual GetOptions.
func TestSecretStoreGetIsQuorumRead(t *testing.T) {
	c := fake.NewSimpleClientset(secretFixture())
	var seen metav1.GetOptions
	c.PrependReactor("get", "secrets", func(a ktesting.Action) (bool, runtime.Object, error) {
		seen = a.(ktesting.GetActionImpl).GetOptions
		return false, nil, nil
	})

	if _, err := NewSecretStore(c, "my-app", "oauth-credentials").Get(context.Background()); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if seen.ResourceVersion != "" {
		t.Fatalf("GetOptions.ResourceVersion = %q, want empty (quorum read)", seen.ResourceVersion)
	}
}

// RBAC resourceNames rejects an unscoped watch with 403, so the field
// selector is a correctness requirement, not an optimisation.
func TestSecretStoreWatchSetsNameFieldSelector(t *testing.T) {
	c := fake.NewSimpleClientset(secretFixture())
	var seen string
	c.PrependWatchReactor("secrets", func(a ktesting.Action) (bool, watch.Interface, error) {
		seen = a.(ktesting.WatchActionImpl).WatchRestrictions.Fields.String()
		return false, nil, nil
	})

	w, err := NewSecretStore(c, "my-app", "oauth-credentials").Watch(context.Background(), "42")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	defer w.Stop()

	if seen != "metadata.name=oauth-credentials" {
		t.Fatalf("field selector = %q, want metadata.name=oauth-credentials", seen)
	}
}

func TestSecretStorePatchBody(t *testing.T) {
	c := fake.NewSimpleClientset(secretFixture())
	var body []byte
	c.PrependReactor("patch", "secrets", func(a ktesting.Action) (bool, runtime.Object, error) {
		body = a.(ktesting.PatchActionImpl).Patch
		return true, secretFixture(), nil
	})

	err := NewSecretStore(c, "my-app", "oauth-credentials").Patch(
		context.Background(),
		map[string][]byte{"session.key": []byte("v2")},
		[]string{"config.json"},
	)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}

	var got struct {
		Data map[string]*string `json:"data"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("patch body not JSON: %v (%s)", err, body)
	}
	// Secret data values are base64 in JSON.
	if got.Data["session.key"] == nil || *got.Data["session.key"] != "djI=" {
		t.Errorf("session.key = %v, want base64 of v2", got.Data["session.key"])
	}
	if v, ok := got.Data["config.json"]; !ok || v != nil {
		t.Errorf("config.json = %v, want explicit null for deletion", v)
	}
	// A merge patch must not mention keys we did not touch.
	if _, ok := got.Data["untouched"]; ok {
		t.Error("patch mentioned a key it should not have")
	}
}
