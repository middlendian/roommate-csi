package driver

import (
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
)

// Serve listens on a unix:// endpoint until the process exits.
func Serve(endpoint string, id csi.IdentityServer, node csi.NodeServer) error {
	path := strings.TrimPrefix(endpoint, "unix://")
	if path == endpoint {
		return fmt.Errorf("endpoint %q must start with unix://", endpoint)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale socket: %w", err)
	}
	lis, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("listen %s: %w", path, err)
	}
	srv := grpc.NewServer()
	csi.RegisterIdentityServer(srv, id)
	csi.RegisterNodeServer(srv, node)
	return srv.Serve(lis)
}
