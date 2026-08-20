package objectfs

import (
	"context"
	"testing"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
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

// A crashed holder must not block forever: past renewTime+duration the lease
// is up for grabs.
func TestLeaseTryAcquireTakesOverExpired(t *testing.T) {
	stale := liveLease("podB:1", testLeaseDur, time.Now().Add(-time.Hour))
	c := fake.NewSimpleClientset(stale)
	m := NewLeaseManager(c, "my-app", "roommate-oauth-credentials", "podA:1", testLeaseDur)

	ok, err := m.TryAcquire(context.Background())
	if err != nil || !ok {
		t.Fatalf("TryAcquire = %v, %v; want true, nil on an expired lease", ok, err)
	}

	got, _ := c.CoordinationV1().Leases("my-app").
		Get(context.Background(), "roommate-oauth-credentials", metav1.GetOptions{})
	if *got.Spec.HolderIdentity != "podA:1" {
		t.Fatalf("holderIdentity = %v, want podA:1", *got.Spec.HolderIdentity)
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
