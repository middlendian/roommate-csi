package objectfs

import (
	"context"
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func configMapFixture() *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "app-config", Namespace: "my-app", ResourceVersion: "7",
		},
		Data:       map[string]string{"config.json": "{}"},
		BinaryData: map[string][]byte{"blob.bin": {0xff, 0xfe}},
	}
}

// Both halves of a ConfigMap present as one flat directory.
func TestConfigMapStoreGetMergesDataAndBinaryData(t *testing.T) {
	c := fake.NewSimpleClientset(configMapFixture())

	snap, err := NewConfigMapStore(c, "my-app", "app-config").Get(context.Background())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got, _ := snap.Get("config.json"); string(got) != "{}" {
		t.Errorf("config.json = %q, want {}", got)
	}
	if got, _ := snap.Get("blob.bin"); string(got) != "\xff\xfe" {
		t.Errorf("blob.bin = %x, want fffe", got)
	}
	if len(snap.Keys()) != 2 {
		t.Errorf("Keys() = %v, want 2 entries", snap.Keys())
	}
}

// UTF-8 goes to data, non-UTF-8 to binaryData, and the other half is
// explicitly nulled so the key never lives in both — the API server rejects
// that.
func TestConfigMapStorePatchRoutesByEncoding(t *testing.T) {
	c := fake.NewSimpleClientset(configMapFixture())
	var body []byte
	c.PrependReactor("patch", "configmaps", func(a ktesting.Action) (bool, runtime.Object, error) {
		body = a.(ktesting.PatchActionImpl).Patch
		return true, configMapFixture(), nil
	})

	err := NewConfigMapStore(c, "my-app", "app-config").Patch(
		context.Background(),
		map[string][]byte{"text.txt": []byte("hello"), "raw.bin": {0xff}},
		[]string{"gone.txt"},
	)
	if err != nil {
		t.Fatalf("Patch: %v", err)
	}

	var got struct {
		Data       map[string]*string `json:"data"`
		BinaryData map[string]*string `json:"binaryData"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("patch body not JSON: %v (%s)", err, body)
	}

	if got.Data["text.txt"] == nil || *got.Data["text.txt"] != "hello" {
		t.Errorf("data[text.txt] = %v, want hello", got.Data["text.txt"])
	}
	if v, ok := got.BinaryData["text.txt"]; !ok || v != nil {
		t.Errorf("binaryData[text.txt] = %v, want explicit null", v)
	}
	if got.BinaryData["raw.bin"] == nil || *got.BinaryData["raw.bin"] != "/w==" {
		t.Errorf("binaryData[raw.bin] = %v, want base64 of 0xff", got.BinaryData["raw.bin"])
	}
	if v, ok := got.Data["raw.bin"]; !ok || v != nil {
		t.Errorf("data[raw.bin] = %v, want explicit null", v)
	}
	// A deletion must null both halves, since we do not know which held it.
	if v, ok := got.Data["gone.txt"]; !ok || v != nil {
		t.Errorf("data[gone.txt] = %v, want explicit null", v)
	}
	if v, ok := got.BinaryData["gone.txt"]; !ok || v != nil {
		t.Errorf("binaryData[gone.txt] = %v, want explicit null", v)
	}
}
