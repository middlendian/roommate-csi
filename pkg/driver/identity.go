package driver

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// IdentityServer implements the CSI Identity service.
//
// It advertises no CONTROLLER_SERVICE: roommate has no controller at all,
// because inline ephemeral volumes never invoke one.
type IdentityServer struct {
	csi.UnimplementedIdentityServer
	name    string
	version string
}

// NewIdentityServer returns an IdentityServer reporting the given plugin
// name and version.
func NewIdentityServer(name, version string) *IdentityServer {
	return &IdentityServer{name: name, version: version}
}

// GetPluginInfo reports the plugin's name and version.
func (s *IdentityServer) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: s.name, VendorVersion: s.version}, nil
}

// GetPluginCapabilities reports no capabilities: no CONTROLLER_SERVICE, no
// VOLUME_ACCESSIBILITY_CONSTRAINTS.
func (s *IdentityServer) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{}, nil
}

// Probe always reports ready: there is no external dependency to check at
// startup, since every API call is authenticated per-request as the
// consuming pod rather than through a driver-held connection.
func (s *IdentityServer) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{Ready: wrapperspb.Bool(true)}, nil
}
