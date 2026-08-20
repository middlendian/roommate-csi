package objectfs

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

// KindSecret is the objectKind value selecting a Secret-backed Volume.
const KindSecret = "Secret"

// KindConfigMap is the objectKind value selecting a ConfigMap-backed Volume.
const KindConfigMap = "ConfigMap"

// Config is the parsed shape of an inline volume's volumeAttributes.
type Config struct {
	// ObjectKind is KindSecret or KindConfigMap.
	ObjectKind string
	// ObjectName is the name of the Secret or ConfigMap to mount.
	ObjectName string
	// LeaseName is the coordination.k8s.io Lease that serialises writers.
	LeaseName string
	// Namespace is always the consuming pod's; never from attributes.
	Namespace string

	// FileMode is the permission bits reported for each entry file.
	FileMode uint32
	// DirMode is the permission bits reported for the mount root.
	DirMode uint32
	// UID is the owner reported for every entry.
	UID uint32
	// GID is the group reported for every entry.
	GID uint32

	// StalenessBound is how long the cache may go without confirmation
	// before reads are refused.
	StalenessBound time.Duration
	// LeaseDuration is how long an acquired Lease survives without renewal.
	LeaseDuration time.Duration
}

// ParseConfig validates volumeAttributes and applies defaults.
//
// namespace comes from podInfoOnMount, not from attributes: honouring a
// caller-supplied namespace would allow a pod to aim the mount at another
// namespace's object, and cross-namespace access is a non-goal.
func ParseConfig(attr map[string]string, namespace string) (Config, error) {
	if namespace == "" {
		return Config{}, fmt.Errorf("roommate: pod namespace is required")
	}
	cfg := Config{
		Namespace:      namespace,
		FileMode:       0o600,
		DirMode:        0o700,
		StalenessBound: DefaultStalenessBound,
		LeaseDuration:  DefaultLeaseDuration,
	}

	cfg.ObjectKind = attr["objectKind"]
	if cfg.ObjectKind != KindSecret && cfg.ObjectKind != KindConfigMap {
		return Config{}, fmt.Errorf(
			"roommate: objectKind must be %q or %q, got %q",
			KindSecret, KindConfigMap, cfg.ObjectKind)
	}

	cfg.ObjectName = attr["objectName"]
	// Secret/ConfigMap names are Kubernetes object names (RFC 1123
	// subdomain), a stricter rule than ValidKey's data-map key check —
	// ValidKey permits mixed case and underscores that the API server
	// would reject on the object name itself.
	if errs := validation.IsDNS1123Subdomain(cfg.ObjectName); len(errs) > 0 {
		return Config{}, fmt.Errorf("roommate: objectName %q is not a valid object name: %s", cfg.ObjectName, errs[0])
	}

	cfg.LeaseName = attr["leaseName"]
	if cfg.LeaseName == "" {
		cfg.LeaseName = "roommate-" + cfg.ObjectName
	}

	var err error
	if cfg.FileMode, err = parseMode(attr, "fileMode", cfg.FileMode); err != nil {
		return Config{}, err
	}
	if cfg.DirMode, err = parseMode(attr, "dirMode", cfg.DirMode); err != nil {
		return Config{}, err
	}
	if cfg.UID, err = parseUint32(attr, "uid", 0); err != nil {
		return Config{}, err
	}
	if cfg.GID, err = parseUint32(attr, "gid", 0); err != nil {
		return Config{}, err
	}
	if cfg.StalenessBound, err = parseSeconds(attr, "stalenessBoundSeconds", cfg.StalenessBound); err != nil {
		return Config{}, err
	}
	if cfg.LeaseDuration, err = parseSeconds(attr, "leaseDurationSeconds", cfg.LeaseDuration); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func parseMode(attr map[string]string, key string, def uint32) (uint32, error) {
	raw, ok := attr[key]
	if !ok || raw == "" {
		return def, nil
	}
	n, err := strconv.ParseUint(raw, 8, 32)
	if err != nil {
		return 0, fmt.Errorf("roommate: %s %q is not an octal mode", key, raw)
	}
	return uint32(n), nil
}

func parseUint32(attr map[string]string, key string, def uint32) (uint32, error) {
	raw, ok := attr[key]
	if !ok || raw == "" {
		return def, nil
	}
	n, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("roommate: %s %q is not a number", key, raw)
	}
	return uint32(n), nil
}

func parseSeconds(attr map[string]string, key string, def time.Duration) (time.Duration, error) {
	raw, ok := attr[key]
	if !ok || raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("roommate: %s %q is not a positive number of seconds", key, raw)
	}
	return time.Duration(n) * time.Second, nil
}

// Volume is one mounted object: its store, cache, committer, and the factory
// for the per-handle leases that serialise writers.
type Volume struct {
	// Cfg is the parsed mount-time configuration.
	Cfg Config
	// Store is the backing Secret or ConfigMap accessor.
	Store Store
	// Cache is the in-memory view refreshed by Run.
	Cache *Cache
	// Committer applies local writes back to Store through Cache.
	Committer *Committer

	client    kubernetes.Interface
	holderSeq atomic.Uint64
	podUID    string
}

// NewVolume assembles a Volume. client must be authenticated as the
// consuming pod.
func NewVolume(client kubernetes.Interface, cfg Config, podUID string) (*Volume, error) {
	var store Store
	switch cfg.ObjectKind {
	case KindSecret:
		store = NewSecretStore(client, cfg.Namespace, cfg.ObjectName)
	case KindConfigMap:
		store = NewConfigMapStore(client, cfg.Namespace, cfg.ObjectName)
	default:
		return nil, fmt.Errorf("roommate: unsupported objectKind %q", cfg.ObjectKind)
	}
	cache := NewCache(store, cfg.StalenessBound)
	return &Volume{
		Cfg:       cfg,
		Store:     store,
		Cache:     cache,
		Committer: NewCommitter(store, cache),
		client:    client,
		podUID:    podUID,
	}, nil
}

// Run drives the watch loop until ctx is cancelled.
func (v *Volume) Run(ctx context.Context) { v.Cache.Run(ctx) }

// NewLease returns a LeaseManager for one open handle. Holder identity must
// be unique per holder, so each handle gets its own sequence number — two
// processes in the same pod are distinct lock holders.
func (v *Volume) NewLease() *LeaseManager {
	holder := fmt.Sprintf("%s:%d", v.podUID, v.holderSeq.Add(1))
	return NewLeaseManager(v.client, v.Cfg.Namespace, v.Cfg.LeaseName, holder, v.Cfg.LeaseDuration)
}
