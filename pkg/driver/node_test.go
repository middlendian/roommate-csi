package driver

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/middlendian/roommate-csi/pkg/mounts"
)

func newTestNodeServer(t *testing.T) *NodeServer {
	t.Helper()
	reg, err := mounts.NewRegistry(filepath.Join(t.TempDir(), "published.json"))
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	return NewNodeServer("node-1", reg, slog.Default())
}

func TestNodeGetCapabilitiesIsEmpty(t *testing.T) {
	// No STAGE_UNSTAGE: podInfoOnMount and tokenRequests populate only
	// NodePublishVolume, so there is no pod identity at stage time.
	resp, err := newTestNodeServer(t).NodeGetCapabilities(context.Background(), &csi.NodeGetCapabilitiesRequest{})
	if err != nil {
		t.Fatalf("NodeGetCapabilities: %v", err)
	}
	if len(resp.GetCapabilities()) != 0 {
		t.Fatalf("capabilities = %v, want none", resp.GetCapabilities())
	}
}

func TestNodeGetInfo(t *testing.T) {
	resp, err := newTestNodeServer(t).NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
	if err != nil {
		t.Fatalf("NodeGetInfo: %v", err)
	}
	if resp.GetNodeId() != "node-1" {
		t.Fatalf("NodeId = %q, want node-1", resp.GetNodeId())
	}
}

func TestNodePublishRejectsMissingArguments(t *testing.T) {
	n := newTestNodeServer(t)
	tests := []struct {
		name string
		req  *csi.NodePublishVolumeRequest
	}{
		{"no volume id", &csi.NodePublishVolumeRequest{TargetPath: "/t"}},
		{"no target", &csi.NodePublishVolumeRequest{VolumeId: "v"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := n.NodePublishVolume(context.Background(), tt.req)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
			}
		})
	}
}

// Inline ephemeral only: a PVC-backed request must be refused clearly rather
// than half-working.
func TestNodePublishRequiresEphemeral(t *testing.T) {
	_, err := newTestNodeServer(t).NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:   "v",
		TargetPath: "/t",
		VolumeContext: map[string]string{
			"objectKind": "Secret",
			"objectName": "oauth-credentials",
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument for a non-ephemeral volume", status.Code(err))
	}
}

// A first publish must fail loudly when the token is absent — that is how a
// misconfigured CSIDriver gets noticed.
func TestNodePublishFirstPublishErrorsWithoutToken(t *testing.T) {
	_, err := newTestNodeServer(t).NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:   "v",
		TargetPath: "/t",
		VolumeContext: map[string]string{
			"csi.storage.k8s.io/ephemeral":                "true",
			"csi.storage.k8s.io/pod.namespace":            "my-app",
			"csi.storage.k8s.io/pod.name":                 "p",
			"csi.storage.k8s.io/pod.uid":                  "uid",
			"csi.storage.k8s.io/pod.service-account.name": "session-runner",
			"objectKind": "Secret",
			"objectName": "oauth-credentials",
		},
	})
	if err == nil {
		t.Fatal("first publish succeeded without a token")
	}
}

// After a successful publish, republish must NEVER error: kubelet deletes the
// mount point when it does (kubernetes/kubernetes#121271). This is the single
// most important behaviour in the file.
func TestRepublishNeverErrorsAfterFirstSuccess(t *testing.T) {
	n := newTestNodeServer(t)
	target := filepath.Join(t.TempDir(), "target")

	// Record a prior successful publish without mounting anything.
	if err := n.registry.Put(target, &mounts.Live{
		Cancel: func() {}, Unmount: func() error { return nil },
	}, mounts.Mount{TargetPath: target, ObjectKind: "Secret", ObjectName: "oauth-credentials", Namespace: "my-app"}); err != nil {
		t.Fatalf("seed registry: %v", err)
	}

	// A republish carrying garbage — no token, no pod info — must still be OK.
	resp, err := n.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{
		VolumeId:      "v",
		TargetPath:    target,
		VolumeContext: map[string]string{"csi.storage.k8s.io/ephemeral": "true"},
	})
	if err != nil {
		t.Fatalf("republish returned an error, which deletes the mount point: %v", err)
	}
	if resp == nil {
		t.Fatal("nil response")
	}
}

func TestNodeUnpublishIsIdempotent(t *testing.T) {
	n := newTestNodeServer(t)
	target := filepath.Join(t.TempDir(), "target")

	for i := 0; i < 2; i++ {
		if _, err := n.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{
			VolumeId: "v", TargetPath: target,
		}); err != nil {
			t.Fatalf("unpublish %d: %v", i, err)
		}
	}
}
