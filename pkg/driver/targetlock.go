package driver

// targetLock is a reference-counted, per-target mutex used to serialize the
// check-then-act sequence in NodePublishVolume's first-publish path: Get
// miss, then WasPublished, then publish, then Put.
//
// It closes a narrow race: the CSI CO may retry NodePublishVolume for the
// same target after a client-side timeout while an earlier call for that
// target is still running (objectfs.Mount succeeded but Registry.Put
// hasn't landed yet). Without this lock, two concurrent Get misses could
// both decide the target has never published and both call publish(); the
// loser could then fail for a target that, a moment earlier, had already
// published successfully — the same mount-point-deletion disaster
// (kubernetes/kubernetes#121271) the registry-hit path exists to avoid.
// Kubelet's ~10 Hz republish traffic always hits the registry directly and
// never touches this lock, so it does not affect that path's cost.
//
// It is implemented over a channel rather than sync.Mutex so a blocked
// acquirer is deterministically observable with testing/synctest, whose
// Wait tracks channel and condition-variable blocking but explicitly does
// not track sync.Mutex contention.
type targetLock struct {
	ch   chan struct{} // buffered 1; holding the token means unlocked
	refs int           // guarded by NodeServer.locksMu, not by ch
}

func newTargetLock() *targetLock {
	tl := &targetLock{ch: make(chan struct{}, 1)}
	tl.ch <- struct{}{}
	return tl
}

// Lock acquires tl, blocking until it is available.
func (tl *targetLock) Lock() { <-tl.ch }

// Unlock releases tl.
func (tl *targetLock) Unlock() { tl.ch <- struct{}{} }

// lockTarget returns target's lock, held. It creates the lock on first use
// and removes it again once the last holder releases it, so the driver does
// not accumulate one lock per target path for the life of the process.
func (n *NodeServer) lockTarget(target string) *targetLock {
	n.locksMu.Lock()
	tl, ok := n.locks[target]
	if !ok {
		tl = newTargetLock()
		n.locks[target] = tl
	}
	tl.refs++
	n.locksMu.Unlock()

	tl.Lock()
	return tl
}

// unlockTarget releases tl, previously returned by lockTarget for target.
func (n *NodeServer) unlockTarget(target string, tl *targetLock) {
	tl.Unlock()

	n.locksMu.Lock()
	tl.refs--
	if tl.refs == 0 {
		delete(n.locks, target)
	}
	n.locksMu.Unlock()
}
