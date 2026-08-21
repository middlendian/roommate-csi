// Package mounts tracks the FUSE mounts this node plugin owns.
//
// It exists to answer one question that has no other source of truth after a
// plugin restart: has this target path been published successfully before?
//
// The answer decides whether NodePublishVolume may return an error. On a
// genuine first publish an error is correct — it is how an unauthorized pod
// learns it is unauthorized. On a republish an error is destructive: kubelet
// deletes the mount point from the host filesystem and later successful calls
// cannot restore the pod's view (kubernetes/kubernetes#121271).
package mounts

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// Mount is the durable record of a published target.
type Mount struct {
	// TargetPath is the mount point.
	TargetPath string `json:"targetPath"`
	// ObjectKind is "Secret" or "ConfigMap".
	ObjectKind string `json:"objectKind"`
	// ObjectName is the name of the Secret or ConfigMap.
	ObjectName string `json:"objectName"`
	// Namespace is the Kubernetes namespace.
	Namespace string `json:"namespace"`
}

// Live is the in-memory half: the running FUSE server and its watch loop.
// It does not survive a plugin restart.
type Live struct {
	// Cancel cancels the watch context.
	Cancel context.CancelFunc
	// Unmount tears down the FUSE mount.
	Unmount func() error

	// Token holds the most recent pod token. Republish swaps it atomically,
	// roughly ten times a second, so this must never take a lock the data
	// path also wants.
	Token atomic.Pointer[string]
}

// Registry maps target paths to live mounts, backed by a JSON state file.
type Registry struct {
	statePath string

	mu      sync.Mutex
	live    map[string]*Live
	records map[string]Mount
}

// NewRegistry loads the state file if present. A missing or corrupt file
// starts empty rather than failing: refusing to start would wedge the plugin
// permanently, whereas an empty registry self-heals as republishes arrive.
func NewRegistry(statePath string) (*Registry, error) {
	r := &Registry{
		statePath: statePath,
		live:      map[string]*Live{},
		records:   map[string]Mount{},
	}
	data, err := os.ReadFile(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return r, nil
		}
		return nil, fmt.Errorf("read state: %w", err)
	}
	var records map[string]Mount
	if err := json.Unmarshal(data, &records); err != nil {
		return r, nil // corrupt: start empty
	}
	if records == nil {
		records = map[string]Mount{}
	}
	r.records = records
	return r, nil
}

// Get returns the live mount for target, if this process owns one.
func (r *Registry) Get(target string) (*Live, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	live, ok := r.live[target]
	return live, ok
}

// WasPublished reports whether target has ever been published successfully,
// including by a previous incarnation of this process.
func (r *Registry) WasPublished(target string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.live[target]; ok {
		return true
	}
	_, ok := r.records[target]
	return ok
}

// Put records a successful publish.
func (r *Registry) Put(target string, live *Live, m Mount) error {
	r.mu.Lock()
	r.live[target] = live
	r.records[target] = m
	r.mu.Unlock()
	return r.persist()
}

// Delete tears the mount down and forgets it.
func (r *Registry) Delete(_ context.Context, target string) error {
	r.mu.Lock()
	live := r.live[target]
	delete(r.live, target)
	delete(r.records, target)
	r.mu.Unlock()

	if live != nil {
		if live.Cancel != nil {
			live.Cancel()
		}
		if live.Unmount != nil {
			if err := live.Unmount(); err != nil {
				// Persist regardless: leaving a record for a mount we no
				// longer own would make a future republish do nothing.
				_ = r.persist()
				return fmt.Errorf("unmount %s: %w", target, err)
			}
		}
	}
	return r.persist()
}

// persist writes the state file atomically, so a crash mid-write cannot leave
// a half-written file that reads as corrupt on the next start. The mutex is
// held across the entire operation (marshal, write, rename) to prevent
// concurrent persist calls from racing and losing updates: without this,
// two concurrent Put/Delete operations could lead to one's changes being
// silently overwritten by the other's stale snapshot.
func (r *Registry) persist() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	data, err := json.MarshalIndent(r.records, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(r.statePath), 0o755); err != nil {
		return err
	}
	tmp := r.statePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		// Clean up stale temp file on error.
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, r.statePath)
}
