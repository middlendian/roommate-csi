// Package driver implements the CSI Identity and Node gRPC services.
//
// There is no controller: inline ephemeral volumes never invoke one, so
// NodePublishVolume is the only RPC that does real work. Every Kubernetes
// API call it makes runs through pkg/podtoken as the consuming pod — the
// driver's own ServiceAccount holds no RBAC.
package driver

import (
	"context"
	"log/slog"
	"os"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/middlendian/roommate-csi/pkg/mounts"
	"github.com/middlendian/roommate-csi/pkg/objectfs"
	"github.com/middlendian/roommate-csi/pkg/podtoken"
)

// keyEphemeral marks an inline volume. roommate serves nothing else.
const keyEphemeral = "csi.storage.k8s.io/ephemeral"

// NodeServer implements the CSI Node service.
//
// NodePublishVolume is the only RPC that does work, and it is called roughly
// ten times a second per volume because the CSIDriver sets
// requiresRepublish: true. The republish path must therefore stay
// allocation-cheap: a registry lookup and an atomic token swap, nothing more.
type NodeServer struct {
	csi.UnimplementedNodeServer

	nodeID   string
	registry *mounts.Registry
	log      *slog.Logger
}

// NewNodeServer returns a NodeServer that identifies as nodeID and records
// published mounts in reg.
func NewNodeServer(nodeID string, reg *mounts.Registry, log *slog.Logger) *NodeServer {
	return &NodeServer{nodeID: nodeID, registry: reg, log: log}
}

// NodeGetCapabilities advertises nothing.
//
// No STAGE_UNSTAGE in particular: podInfoOnMount and tokenRequests populate
// only NodePublishVolume, so there is no pod identity available at stage
// time and a staged mount could not be authenticated as anyone.
func (n *NodeServer) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{}, nil
}

// NodeGetInfo reports the node ID this plugin instance runs on.
func (n *NodeServer) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{NodeId: n.nodeID}, nil
}

// NodePublishVolume mounts (or, on every republish, refreshes) one target.
//
// A target already in the registry is a republish: it swaps the token
// atomically and returns immediately, without erroring, no matter what the
// request carries — see the type doc for why an error here is destructive.
// A target absent from the registry but present in the state file is a
// remount after a plugin restart, which fails the same way: silently, with
// a retry on the next republish. Only a genuine first publish is allowed to
// return an error.
func (n *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	target := req.GetTargetPath()
	if req.GetVolumeId() == "" || target == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id and target_path are required")
	}
	vc := req.GetVolumeContext()

	// Republish: swap the token and return. NEVER error from here — kubelet
	// deletes the mount point when a republish fails, and later successful
	// calls cannot restore the pod's view (kubernetes/kubernetes#121271). A
	// rejected token surfaces as EACCES from the FUSE data path instead,
	// which is the correct layer to fail at: revocation still bites, the
	// mount survives, and buffered writes are not destroyed by a blip.
	if live, ok := n.registry.Get(target); ok {
		if tok, err := podtoken.Extract(vc); err == nil {
			live.Token.Store(&tok)
		}
		return &csi.NodePublishVolumeResponse{}, nil
	}

	// Known target with no live mount means the plugin restarted. Rebuild it,
	// and stay silent on failure for the same reason.
	restarting := n.registry.WasPublished(target)

	if err := n.publish(ctx, target, vc); err != nil {
		if restarting {
			n.log.Error("remount after restart failed; will retry on next republish",
				"target", target, "err", err)
			return &csi.NodePublishVolumeResponse{}, nil
		}
		return nil, err
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

// publish does the real work of a first mount. Errors returned here are
// surfaced by kubelet as a FailedMount event, so they must be actionable.
func (n *NodeServer) publish(ctx context.Context, target string, vc map[string]string) error {
	if vc[keyEphemeral] != "true" {
		return status.Error(codes.InvalidArgument,
			"roommate serves inline ephemeral volumes only; declare the volume "+
				"under pod.spec.volumes[].csi rather than via a PVC")
	}
	namespace, serviceAccount, uid, err := podtoken.PodIdentity(vc)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	cfg, err := objectfs.ParseConfig(vc, namespace)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	tok, err := podtoken.Extract(vc)
	if err != nil {
		return status.Errorf(codes.FailedPrecondition,
			"%v; set tokenRequests on the CSIDriver object", err)
	}
	client, err := podtoken.ClientFor(tok)
	if err != nil {
		return status.Errorf(codes.Internal, "build client: %v", err)
	}
	vol, err := objectfs.NewVolume(client, cfg, uid)
	if err != nil {
		return status.Errorf(codes.Internal, "volume: %v", err)
	}

	// Prove authorization before mounting, so a denied pod gets an actionable
	// event rather than a mount that fails on first read.
	if _, err := vol.Cache.Fresh(ctx); err != nil {
		switch {
		case apierrors.IsForbidden(err), apierrors.IsUnauthorized(err):
			return status.Error(codes.PermissionDenied,
				RBACHint(namespace, serviceAccount, cfg.ObjectKind, cfg.ObjectName, cfg.LeaseName))
		case apierrors.IsNotFound(err):
			return status.Errorf(codes.NotFound,
				"%s %s/%s does not exist; roommate never creates the backing object",
				cfg.ObjectKind, namespace, cfg.ObjectName)
		default:
			return status.Errorf(codes.Internal, "read %s: %v", vol.Store.Describe(), err)
		}
	}

	if err := os.MkdirAll(target, os.FileMode(cfg.DirMode)); err != nil {
		return status.Errorf(codes.Internal, "mkdir target: %v", err)
	}

	watchCtx, cancel := context.WithCancel(context.Background())
	go vol.Run(watchCtx)

	server, err := objectfs.Mount(target, vol)
	if err != nil {
		cancel()
		return status.Errorf(codes.Internal, "mount fuse: %v", err)
	}

	live := &mounts.Live{Cancel: cancel, Unmount: server.Unmount}
	live.Token.Store(&tok)

	if err := n.registry.Put(target, live, mounts.Mount{
		TargetPath: target, ObjectKind: cfg.ObjectKind,
		ObjectName: cfg.ObjectName, Namespace: namespace,
	}); err != nil {
		cancel()
		_ = server.Unmount()
		return status.Errorf(codes.Internal, "record mount: %v", err)
	}
	n.log.Info("published", "target", target, "object", vol.Store.Describe(), "namespace", namespace)
	return nil
}

// NodeUnpublishVolume tears down target and forgets it. It is idempotent: a
// target that was never published, or already torn down, returns success.
func (n *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	target := req.GetTargetPath()
	if req.GetVolumeId() == "" || target == "" {
		return nil, status.Error(codes.InvalidArgument, "volume_id and target_path are required")
	}
	if err := n.registry.Delete(ctx, target); err != nil {
		return nil, status.Errorf(codes.Internal, "unpublish: %v", err)
	}
	_ = os.Remove(target)
	return &csi.NodeUnpublishVolumeResponse{}, nil
}
