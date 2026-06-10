//nolint:testpackage // white-box
package election

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"git.horse/vapronva/concave/router/bigbrain/k8sclient"
	"git.horse/vapronva/concave/router/bigbrain/registry"
)

func emptyDiscoveryController(t *testing.T, debounce int) (*Controller, *registry.Registry, *deploymentState) {
	t.Helper()
	k8s := k8sclient.NewFromInterface(fake.NewClientset(), "convex")
	reg := registry.New()
	reg.EnsureDeployment("dev", "convex-dev")
	c := New(Config{EmptyDiscoveryDebounce: new(debounce)}, k8s, nil, reg, quietLogger())
	return c, reg, c.deploymentState("dev")
}

func TestReconcile_EmptyDiscoveryDebouncePublishesLeaderless(t *testing.T) {
	t.Parallel()
	const debounce = 3
	c, reg, _ := emptyDiscoveryController(t, debounce)
	ctx := context.Background()
	for i := 1; i < debounce; i++ {
		c.reconcile(ctx, "dev")
		if reg.Published("dev") {
			t.Fatalf("tick %d < debounce %d: must not publish yet", i, debounce)
		}
	}
	c.reconcile(ctx, "dev")
	if !reg.Published("dev") || !reg.AllPublished() {
		t.Fatal("persistently empty discovery must publish explicit leaderless")
	}
	if _, _, _, ok := reg.Leader("dev"); ok {
		t.Fatal("published state must be leaderless")
	}
	if _, _, seq, _ := reg.Leader("dev"); seq == 0 {
		t.Fatal("leaderless publish must carry a non-zero seq")
	}
}

func TestReconcile_EmptyDiscoveryDebounceClearsRetainedLeader(t *testing.T) {
	t.Parallel()
	const debounce = 3
	c, reg, st := emptyDiscoveryController(t, debounce)
	ctx := context.Background()
	c.setLeader("dev", st, "backend-0", "http://10.0.0.1:3210")
	for range debounce {
		c.reconcile(ctx, "dev")
	}
	if pod, ok := leaderOf(t, reg, "dev"); ok {
		t.Fatalf("stale leader %q must be cleared after the empty-discovery debounce", pod)
	}
	if !reg.AllPublished() {
		t.Fatal("the leaderless publish must keep the deployment published")
	}
	c.mu.Lock()
	incumbent, streak := st.incumbentPod, st.leaderlessStreak
	c.mu.Unlock()
	if incumbent != "" || streak != 0 {
		t.Fatalf("retained leader state must be cleared, got incumbent=%q leaderlessStreak=%d", incumbent, streak)
	}
	if _, empty, _ := c.commitState(st, decision{liveLeaderCount: 1}, false, time.Now()); empty != 0 {
		t.Fatalf("non-empty discovery must reset the empty streak, got %d", empty)
	}
}

func TestReconcile_EmptyDiscoveryBlipKeepsLastKnownLeader(t *testing.T) {
	t.Parallel()
	const debounce = 5
	c, reg, st := emptyDiscoveryController(t, debounce)
	ctx := context.Background()
	c.setLeader("dev", st, "backend-0", "http://10.0.0.1:3210")
	for i := 1; i < debounce; i++ {
		c.reconcile(ctx, "dev")
		if pod, ok := leaderOf(t, reg, "dev"); !ok || pod != "backend-0" {
			t.Fatalf("tick %d < debounce %d: last-known leader must be retained, got %q ok=%v",
				i, debounce, pod, ok)
		}
	}
}

func TestReconcile_DiscoveryErrorsNeverTriggerEmptyDebounce(t *testing.T) {
	t.Parallel()
	const debounce = 2
	cs := fake.NewClientset()
	cs.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("apiserver down")
	})
	k8s := k8sclient.NewFromInterface(cs, "convex")
	reg := registry.New()
	reg.EnsureDeployment("dev", "convex-dev")
	c := New(Config{EmptyDiscoveryDebounce: new(debounce)}, k8s, nil, reg, quietLogger())
	st := c.deploymentState("dev")
	ctx := context.Background()
	c.setLeader("dev", st, "backend-0", "http://10.0.0.1:3210")
	for range debounce * 3 {
		c.reconcile(ctx, "dev")
	}
	if pod, ok := leaderOf(t, reg, "dev"); !ok || pod != "backend-0" {
		t.Fatalf("discovery errors must keep the last-known leader, got %q ok=%v", pod, ok)
	}
	c.mu.Lock()
	empty := st.emptyStreak
	c.mu.Unlock()
	if empty != 0 {
		t.Fatalf("discovery errors must not advance the empty streak, got %d", empty)
	}
}
