package objectfs

import (
	"context"
	"errors"
	"sync"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// ErrLeaseLost means background renewal stopped succeeding, so a write can no
// longer claim to be serialised. Commits fence on this rather than proceeding
// under a lock that may already belong to someone else.
var ErrLeaseLost = errors.New("roommate: lease renewal lost")

// DefaultLeaseDuration is how long a lease survives without renewal. Renewal
// runs at a third of this.
const DefaultLeaseDuration = 15 * time.Second

// LeaseManager owns one coordination.k8s.io Lease on behalf of one open file
// handle.
//
// There is deliberately no maximum hold time: an open handle keeps its lock
// for as long as it wants, exactly as on a local filesystem. Renewal runs in
// the background until Release.
type LeaseManager struct {
	client   kubernetes.Interface
	ns       string
	name     string
	holder   string
	duration time.Duration

	// local, if set, is shared by every LeaseManager for the same Volume
	// (wired up by Volume.NewLease) and serialises each one's Get-then-write
	// attempt in TryAcquire. Two handles in the same pod get distinct holder
	// identities, so nothing else stops them from both observing the same
	// Lease as unclaimed and both writing themselves in as holder: a real API
	// server catches that via a resourceVersion conflict on the loser's
	// write, but nothing about this type requires the backing store to
	// enforce that — closing the window locally removes the dependency.
	// TryAcquire only ever *tries* this lock (TryLock, never Lock): it backs
	// both the blocking Acquire poll loop and the non-blocking flock(LOCK_NB)
	// path, and LOCK_NB must fail fast rather than stall — with no bound
	// independent of the caller's ctx — behind another handle's in-flight API
	// call for this Volume. Left nil for a bare NewLeaseManager (as in this
	// package's tests), which never races itself.
	local *sync.Mutex

	mu      sync.Mutex
	held    bool
	healthy bool
	cancel  context.CancelFunc

	// obsMu guards obs, the local-clock observation of the last foreign
	// (holderIdentity, renewTime) pair claimable has seen. It is separate
	// from mu because claimable runs without mu held (see TryAcquire).
	obsMu sync.Mutex
	obs   leaseObservation
}

// leaseObservation records, using our own clock only, when we first saw the
// current holder+renewTime pair on a foreign Lease. claimable measures
// expiry against observedAt, never against the remote RenewTime directly —
// see claimable's comment for why.
type leaseObservation struct {
	holder    string
	renewTime time.Time
	at        time.Time
	valid     bool
}

// NewLeaseManager returns a manager for the named Lease. holder must be
// unique per lock holder — pod UID plus a per-handle counter. The client must
// be built from the consuming pod's token.
func NewLeaseManager(c kubernetes.Interface, ns, name, holder string, duration time.Duration) *LeaseManager {
	if duration <= 0 {
		duration = DefaultLeaseDuration
	}
	return &LeaseManager{client: c, ns: ns, name: name, holder: holder, duration: duration}
}

// Healthy reports whether the lease is held and renewal is still succeeding.
func (m *LeaseManager) Healthy() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.held && m.healthy
}

// TryAcquire attempts to take the lease once, reporting whether it succeeded.
// It is the non-blocking path behind flock(LOCK_NB).
func (m *LeaseManager) TryAcquire(ctx context.Context) (bool, error) {
	m.mu.Lock()
	if m.held {
		m.mu.Unlock()
		return true, nil
	}
	m.mu.Unlock()

	// local is taken non-blockingly: TryAcquire backs both the non-blocking
	// flock(LOCK_NB) path and Acquire's blocking poll loop, and LOCK_NB must
	// fail fast rather than stall behind another handle's in-flight API
	// call for this Volume. If someone else is already mid-attempt, treat
	// that exactly like losing the race on the Lease itself: report not-ok
	// and let the caller decide whether to retry (Acquire) or return
	// EWOULDBLOCK (a bare TryAcquire).
	if m.local != nil {
		if !m.local.TryLock() {
			return false, nil
		}
		defer m.local.Unlock()
	}

	cur, err := m.client.CoordinationV1().Leases(m.ns).Get(ctx, m.name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		if err := m.create(ctx); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return false, nil // lost the race; caller retries
			}
			return false, err
		}
	case err != nil:
		return false, err
	default:
		if !m.claimable(cur) {
			return false, nil
		}
		if err := m.takeOver(ctx, cur); err != nil {
			if apierrors.IsConflict(err) {
				return false, nil // lost the race; caller retries
			}
			return false, err
		}
	}

	m.startRenewal()
	return true, nil
}

// Acquire blocks until the lease is held or ctx is done. A blocking flock has
// no timeout, matching local filesystem semantics; the caller interrupts it
// by cancelling ctx.
func (m *LeaseManager) Acquire(ctx context.Context) error {
	// Poll at a third of the duration so a released lease is picked up
	// promptly without hammering the API server.
	interval := m.duration / 3
	for {
		ok, err := m.TryAcquire(ctx)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if !sleepCtx(ctx, interval) {
			return ctx.Err()
		}
	}
}

// Release stops renewal and clears our holder identity so the next acquirer
// does not have to wait out the full duration.
func (m *LeaseManager) Release(ctx context.Context) error {
	m.mu.Lock()
	if !m.held {
		m.mu.Unlock()
		return nil
	}
	m.held, m.healthy = false, false
	cancel := m.cancel
	m.cancel = nil
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	cur, err := m.client.CoordinationV1().Leases(m.ns).Get(ctx, m.name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if cur.Spec.HolderIdentity == nil || *cur.Spec.HolderIdentity != m.holder {
		return nil // someone else already took it
	}
	cur.Spec.HolderIdentity = nil
	cur.Spec.RenewTime = nil
	_, err = m.client.CoordinationV1().Leases(m.ns).Update(ctx, cur, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// claimable reports whether the lease is free, expired, or already ours.
//
// A lease with no holder, no RenewTime, or held by us needs no clock at all
// and is claimable immediately — that also mitigates the observation-window
// cost below for the cases where it isn't needed. A genuinely foreign,
// live-looking lease is judged expired against OUR OWN clock's record of
// when we first observed its current (holderIdentity, renewTime) pair,
// never against the remote RenewTime directly.
//
// This matters because RenewTime is stamped by the remote holder's clock.
// Comparing it against our local time.Now() (the old behaviour) means a
// local clock running more than leaseDurationSeconds ahead of the holder's
// judges a lease the holder just renewed as expired. The CAS then succeeds
// — there is no conflict, just a stale view — and both sides believe they
// hold the lock. client-go's leaderelection avoids exactly this by
// recording a local observedTime whenever the record changes and measuring
// expiry from that; this mirrors it.
//
// Consequence, deliberately accepted: reclaiming a crashed holder's lease
// now takes up to ~2x leaseDurationSeconds instead of 1x, because the first
// sighting of the crashed holder's now-stale record only starts our clock;
// we still wait a further full duration of continued silence before
// treating it as ours to take. A single, isolated TryAcquire (the
// flock(LOCK_NB) path) against an already-expired foreign lease will now
// return false on first attempt, since it has no observation history yet —
// legitimately EWOULDBLOCK, but a real behavioural change from before.
func (m *LeaseManager) claimable(l *coordv1.Lease) bool {
	if l.Spec.HolderIdentity == nil || *l.Spec.HolderIdentity == "" {
		return true
	}
	if *l.Spec.HolderIdentity == m.holder {
		return true
	}
	if l.Spec.RenewTime == nil {
		return true
	}
	dur := m.duration
	if l.Spec.LeaseDurationSeconds != nil {
		dur = time.Duration(*l.Spec.LeaseDurationSeconds) * time.Second
	}

	holder := *l.Spec.HolderIdentity
	renewTime := l.Spec.RenewTime.Time

	m.obsMu.Lock()
	defer m.obsMu.Unlock()

	if !m.obs.valid || m.obs.holder != holder || !m.obs.renewTime.Equal(renewTime) {
		// First sighting of this (holder, renewTime) pair — including the
		// very first sighting of this lease at all, and every time the
		// holder renews. A holder that keeps renewing therefore never
		// becomes claimable no matter how stale our previous observation
		// was, regardless of clock skew.
		m.obs = leaseObservation{holder: holder, renewTime: renewTime, at: time.Now(), valid: true}
		return false
	}
	return time.Since(m.obs.at) > dur
}

func (m *LeaseManager) create(ctx context.Context) error {
	secs := int32(m.duration / time.Second)
	now := metav1.NowMicro()
	_, err := m.client.CoordinationV1().Leases(m.ns).Create(ctx, &coordv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: m.name, Namespace: m.ns},
		Spec: coordv1.LeaseSpec{
			HolderIdentity:       &m.holder,
			LeaseDurationSeconds: &secs,
			AcquireTime:          &now,
			RenewTime:            &now,
		},
	}, metav1.CreateOptions{})
	return err
}

// takeOver updates in place, so the Lease's own resourceVersion serialises
// two acquirers racing for an expired lease.
func (m *LeaseManager) takeOver(ctx context.Context, cur *coordv1.Lease) error {
	secs := int32(m.duration / time.Second)
	now := metav1.NowMicro()
	cur.Spec.HolderIdentity = &m.holder
	cur.Spec.LeaseDurationSeconds = &secs
	cur.Spec.AcquireTime = &now
	cur.Spec.RenewTime = &now
	_, err := m.client.CoordinationV1().Leases(m.ns).Update(ctx, cur, metav1.UpdateOptions{})
	return err
}

func (m *LeaseManager) startRenewal() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.held {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.held, m.healthy, m.cancel = true, true, cancel
	go m.renewLoop(ctx)
}

// renewLoop runs until Release. A failed renewal flips healthy to false,
// which fences commits; a later success restores it.
func (m *LeaseManager) renewLoop(ctx context.Context) {
	interval := m.duration / 3
	for {
		if !sleepCtx(ctx, interval) {
			return
		}
		err := m.renewOnce(ctx)
		m.mu.Lock()
		m.healthy = err == nil
		m.mu.Unlock()
	}
}

func (m *LeaseManager) renewOnce(ctx context.Context) error {
	cur, err := m.client.CoordinationV1().Leases(m.ns).Get(ctx, m.name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if cur.Spec.HolderIdentity == nil || *cur.Spec.HolderIdentity != m.holder {
		return ErrLeaseLost // someone took it while we slept
	}
	now := metav1.NowMicro()
	cur.Spec.RenewTime = &now
	_, err = m.client.CoordinationV1().Leases(m.ns).Update(ctx, cur, metav1.UpdateOptions{})
	return err
}
