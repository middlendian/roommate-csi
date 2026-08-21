// Command roommate-node is the roommate CSI node plugin.
//
// There is no controller binary. Inline ephemeral volumes never invoke the
// CSI controller service, so the entire driver is this one process.
package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/middlendian/roommate-csi/pkg/driver"
	"github.com/middlendian/roommate-csi/pkg/mounts"
)

// version is overridden at build time via -ldflags.
var version = "dev"

func main() {
	var (
		endpoint   = flag.String("endpoint", "unix:///csi/csi.sock", "CSI gRPC endpoint")
		driverName = flag.String("driver-name", "roommate.csi", "CSI driver name")
		nodeID     = flag.String("node-id", "", "node name (required)")
		statePath  = flag.String("state-file",
			"/var/lib/kubelet/plugins/roommate.csi/published.json",
			"where published-target records are kept across restarts")
		logLevel = flag.String("log-level", "info", "debug, info, warn, or error")
	)
	flag.Parse()

	log := newLogger(*logLevel)

	if *nodeID == "" {
		log.Error("-node-id is required (set it from spec.nodeName via the downward API)")
		os.Exit(1)
	}

	registry, err := mounts.NewRegistry(*statePath)
	if err != nil {
		log.Error("open state file", "path", *statePath, "err", err)
		os.Exit(1)
	}

	log.Info("starting", "driver", *driverName, "version", version, "node", *nodeID)

	// Mounts are not restored here. Any target that was published before a
	// restart is rebuilt by the next republish, which arrives within about
	// 100ms because the CSIDriver sets requiresRepublish.
	err = driver.Serve(*endpoint,
		driver.NewIdentityServer(*driverName, version),
		driver.NewNodeServer(*nodeID, registry, log),
	)
	if err != nil {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}

// newLogger builds a JSON slog.Logger writing to stderr at the given level.
// An unparsable level falls back to info.
func newLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}
