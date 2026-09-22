package objectfs

import (
	"context"
	"sync"
	"testing"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

const testLeaseDur = 15 * time.Second

func TestLeaseAcquireCreatesWhenAbsent(t *testing.T) {
	c := fake.NewSimpleClientset()
	m := NewLeaseManager(c, "my-app", "roommate-oauth-credentials", "podA:1", testLeaseDur)

	ok, err := m.TryAcquire(context.Background())
	if err != nil || !ok {
		t.Fatalf("TryAcquire = %v, %v; want true, nil", ok, err)
	}

	got, err := c.CoordinationV1().Leases("my-app").
		Get(context.Background(), "roommate-oauth-credentials", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("lease not created: %v", err)
	}
	if got.Spec.HolderIdentity == nil || *got.Spec.HolderIdentity != "podA:1" {
		t.Fatalf("holderIdentity = %v, want podA:1", got.Spec.HolderIdentity)
	}
	if got.Spec.LeaseDurationSeconds == nil || *got.Spec.LeaseDurationSeconds != 15 {
		t.Fatalf("leaseDurationSeconds = %v, want 15", got.Spec.LeaseDurationSeconds)
	}
}

func TestLeaseTryAcquireFailsWhenHeldAndLive(t *testing.T) {
	held := liveLease("podB:1", testLeaseDur, time.Now())
	c := fake.NewSimpleClientset(held)
	m := NewLeaseManager(c, "my-app", "roommate-oauth-credentials", "podA:1", testLeaseDur)

	ok, err := m.TryAcquire(context.Background())
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	if ok {
		t.Fatal("acquired a lease that another holder holds")
	}
}

// A crashed holder must not block forever, but per the clock-skew fix
// (claimable now measures expiry against our own observation time, never
// the remote RenewTime directly — see claimable's doc comment) a single
// TryAcquire against an already-stale lease does NOT take over on first
// sight: that first sighting only starts our local observation clock. Only
// after we've continued to observe the same (holder, renewTime) pair for a
// further full duration do we take over. This test backdates the
// observation directly to simulate that further wait without an actual
// sleep.
func TestLeaseTryAcquireTakesOverExpired(t *testing.T) {
	stale := liveLease("podB:1", testLeaseDur, time.Now().Add(-time.Hour))
	c := fake.NewSimpleClientset(stale)
	m := NewLeaseManager(c, "my-app", "roommate-oauth-credentials", "podA:1", testLeaseDur)

	ok, err := m.TryAcquire(context.Background())
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	if ok {
		t.Fatal("TryAcquire took over on first sight of a stale lease — the observation window was skipped")
	}

	m.obsMu.Lock()
	m.obs.at = time.Now().Add(-testLeaseDur - time.Second)
	m.obsMu.Unlock()

	ok, err = m.TryAcquire(context.Background())
	if err != nil || !ok {
		t.Fatalf("TryAcquire = %v, %v; want true once the observation window has elapsed", ok, err)
	}

	got, _ := c.CoordinationV1().Leases("my-app").
		Get(context.Background(), "roommate-oauth-credentials", metav1.GetOptions{})
	if *got.Spec.HolderIdentity != "podA:1" {
		t.Fatalf("holderIdentity = %v, want podA:1", *got.Spec.HolderIdentity)
	}
}

// TestLeaseClaimableNotClaimableOnFirstSighting is the direct regression
// test for the clock-skew fix: a foreign, live-looking lease whose
// RenewTime is long past renewTime+duration must NOT be claimable the very
// first time we see it — only once we've observed it, unchanged, for a
// full duration on OUR OWN clock. Against the pre-fix implementation
// (`time.Now().After(l.Spec.RenewTime.Add(dur))`, comparing directly to the
// remote clock) this lease — renewed an hour ago against a 15s duration —
// would be judged claimable immediately; this assertion fails there.
func TestLeaseClaimableNotClaimableOnFirstSighting(t *testing.T) {
	m := NewLeaseManager(fake.NewSimpleClientset(), "my-app", "roommate-oauth-credentials", "podA:1", testLeaseDur)
	stale := liveLease("podB:1", testLeaseDur, time.Now().Add(-time.Hour))

	if m.claimable(stale) {
		t.Fatal("claimable = true on first sighting of a foreign lease; " +
			"want false — a single sighting must not be enough to take over, " +
			"regardless of how stale the remote RenewTime looks")
	}
}

// TestLeaseClaimableAfterObservationWindow confirms the other half: once
// we've continued to observe the same foreign (holder, renewTime) pair for
// a full duration, it becomes claimable.
func TestLeaseClaimableAfterObservationWindow(t *testing.T) {
	m := NewLeaseManager(fake.NewSimpleClientset(), "my-app", "roommate-oauth-credentials", "podA:1", testLeaseDur)
	stale := liveLease("podB:1", testLeaseDur, time.Now().Add(-time.Hour))

	if m.claimable(stale) {
		t.Fatal("claimable = true on first sighting; want false")
	}

	m.obsMu.Lock()
	m.obs.at = time.Now().Add(-testLeaseDur - time.Second)
	m.obsMu.Unlock()

	if !m.claimable(stale) {
		t.Fatal("claimable = false after the observation window elapsed; want true")
	}
}

// TestLeaseClaimableResetsWhenRenewTimeAdvances proves the core safety
// property: a holder that keeps renewing is never stolen from, no matter
// how far the local clock has apparently drifted (simulated here by
// backdating our observation arbitrarily far into the past — the same
// state a real clock-skewed node would reach). A renewal changes
// RenewTime, which must reset the observation clock rather than let stale
// backdating carry over.
//
// Both RenewTimes here are themselves deliberately in the past — 2h and 1h
// ago respectively, well beyond testLeaseDur — modelling a local clock that
// runs far enough ahead of the holder's for every renewal to still look
// stale by a direct comparison. t2 is simply later than t1, exactly as it
// would be after one real renewal by a healthy holder. This is what makes
// the test discriminate: against the pre-fix implementation
// (time.Now().After(RenewTime.Add(dur)), comparing straight to the remote
// clock) t2 alone is judged expired and claimable — this test's final
// assertion fails there, proving it exercises the property it claims to.
func TestLeaseClaimableResetsWhenRenewTimeAdvances(t *testing.T) {
	m := NewLeaseManager(fake.NewSimpleClientset(), "my-app", "roommate-oauth-credentials", "podA:1", testLeaseDur)
	t1 := liveLease("podB:1", testLeaseDur, time.Now().Add(-2*time.Hour))

	if m.claimable(t1) {
		t.Fatal("claimable = true on first sighting; want false")
	}

	// Simulate a wildly skewed local clock: our observation looks ancient,
	// far past the point where an unreset observation would be claimable.
	m.obsMu.Lock()
	m.obs.at = time.Now().Add(-24 * time.Hour)
	m.obsMu.Unlock()

	// The holder renews — a later RenewTime on the same lease, still an
	// hour in the past by direct comparison, but strictly newer than t1.
	t2 := liveLease("podB:1", testLeaseDur, time.Now().Add(-time.Hour))

	if m.claimable(t2) {
		t.Fatal("claimable = true right after the holder renewed; " +
			"want false — the observation must reset on a renewal, " +
			"not be judged against the old (skewed) observation time")
	}
}

func TestLeaseReacquireBySameHolderSucceeds(t *testing.T) {
	held := liveLease("podA:1", testLeaseDur, time.Now())
	c := fake.NewSimpleClientset(held)
	m := NewLeaseManager(c, "my-app", "roommate-oauth-credentials", "podA:1", testLeaseDur)

	ok, err := m.TryAcquire(context.Background())
	if err != nil || !ok {
		t.Fatalf("TryAcquire = %v, %v; want true for the existing holder", ok, err)
	}
}

func TestLeaseHealthyOnlyWhileHeld(t *testing.T) {
	c := fake.NewSimpleClientset()
	m := NewLeaseManager(c, "my-app", "roommate-oauth-credentials", "podA:1", testLeaseDur)

	if m.Healthy() {
		t.Fatal("Healthy() before acquiring")
	}
	if _, err := m.TryAcquire(context.Background()); err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	if !m.Healthy() {
		t.Fatal("not Healthy() while held")
	}
	if err := m.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if m.Healthy() {
		t.Fatal("Healthy() after Release")
	}
}

func TestLeaseReleaseClearsHolder(t *testing.T) {
	c := fake.NewSimpleClientset()
	m := NewLeaseManager(c, "my-app", "roommate-oauth-credentials", "podA:1", testLeaseDur)
	if _, err := m.TryAcquire(context.Background()); err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	if err := m.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}

	got, _ := c.CoordinationV1().Leases("my-app").
		Get(context.Background(), "roommate-oauth-credentials", metav1.GetOptions{})
	if got.Spec.HolderIdentity != nil && *got.Spec.HolderIdentity == "podA:1" {
		t.Fatal("Release left holderIdentity in place")
	}

	// Another holder can now take it immediately.
	other := NewLeaseManager(c, "my-app", "roommate-oauth-credentials", "podB:1", testLeaseDur)
	ok, err := other.TryAcquire(context.Background())
	if err != nil || !ok {
		t.Fatalf("second holder TryAcquire = %v, %v; want true", ok, err)
	}
}

// TestTryAcquireLocalIsNonBlocking guards the fix for a real regression a
// review caught: the process-local mutex that serialises concurrent local
// acquisition attempts (LeaseManager.local, wired up by Volume.NewLease)
// must never turn TryAcquire's non-blocking contract into a blocking one.
// LOCK_NB has to fail fast even if another handle on the same Volume is
// mid-attempt against a slow or hung API call — that's exactly the scenario
// this reproduces with a reactor that blocks the Lease Get until released.
//
// This is deliberately a LeaseManager-level test, not a FUSE-mounted one:
// it exercises precisely the code the regression was in (TryAcquire's use
// of local), with a controllable, unbounded delay instead of a fixed sleep,
// so it can't pass by accident the way a fixed-timing test could.
func TestTryAcquireLocalIsNonBlocking(t *testing.T) {
	c := fake.NewSimpleClientset()
	release := make(chan struct{})
	c.PrependReactor("get", "leases", func(ktesting.Action) (bool, runtime.Object, error) {
		<-release              // held open until the test explicitly lets it through
		return false, nil, nil // fall through to the default tracker-backed reaction
	})

	var local sync.Mutex
	a := NewLeaseManager(c, "my-app", "roommate-oauth-credentials", "podA:1", testLeaseDur)
	a.local = &local
	b := NewLeaseManager(c, "my-app", "roommate-oauth-credentials", "podA:2", testLeaseDur)
	b.local = &local

	aDone := make(chan struct{})
	go func() {
		defer close(aDone)
		_, _ = a.TryAcquire(context.Background())
	}()

	// Give a's goroutine a chance to actually enter TryAcquire and take
	// local before b attempts — a has nothing to do beforehand but acquire
	// the mutex and call Get (which then blocks on the reactor), so this is
	// not a tight race.
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	ok, err := b.TryAcquire(context.Background())
	elapsed := time.Since(start)
	close(release) // let a's Get proceed so its goroutine can finish
	<-aDone

	if err != nil {
		t.Fatalf("b.TryAcquire err = %v, want nil", err)
	}
	if ok {
		t.Fatal("b.TryAcquire = true; want false — local was held by a")
	}
	// The reactor holds a's Get open until the test releases it, which only
	// happens after this measurement — so any wait here is b stalling on
	// local, not on network latency. A regression that reverts to a
	// blocking Lock would hang this call until close(release), well over
	// this bound.
	if elapsed > 200*time.Millisecond {
		t.Fatalf("b.TryAcquire took %v while local was held by a's in-flight API call; "+
			"want a prompt not-ok, not a stall behind unrelated local contention", elapsed)
	}
}

func liveLease(holder string, dur time.Duration, renewed time.Time) *coordv1.Lease {
	secs := int32(dur / time.Second)
	rt := metav1.NewMicroTime(renewed)
	return &coordv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name: "roommate-oauth-credentials", Namespace: "my-app", ResourceVersion: "1",
		},
		Spec: coordv1.LeaseSpec{
			HolderIdentity:       &holder,
			LeaseDurationSeconds: &secs,
			RenewTime:            &rt,
		},
	}
}
