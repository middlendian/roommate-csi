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

	mu      sync.Mutex
	held    bool
	healthy bool
	cancel  context.CancelFunc
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
	return time.Now().After(l.Spec.RenewTime.Add(dur))
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
