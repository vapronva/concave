//nolint:testpackage // white-box
package election

import (
	"context"
	"testing"
	"time"

	"git.horse/vapronva/concave/router/bigbrain/registry"
)

func leaderOf(t *testing.T, reg *registry.Registry, name string) (string, bool) {
	t.Helper()
	pod, _, _, ok := reg.Leader(name)
	return pod, ok
}

func TestActLeaderless_RetainsLastKnownLeaderDuringHysteresis(t *testing.T) {
	t.Parallel()
	reg := registry.New()
	reg.EnsureDeployment("dev", "convex-dev")
	const debounce = 3
	c := New(Config{PromoteDebounce: new(debounce)}, nil, nil, reg, quietLogger())
	st := c.deploymentState("dev")
	ctx := context.Background()
	c.setLeader("dev", st, "backend-0", "http://10.0.0.1:3210")
	if pod, ok := leaderOf(t, reg, "dev"); !ok || pod != "backend-0" {
		t.Fatalf("setup: want leader backend-0, got %q ok=%v", pod, ok)
	}
	leaderless := decision{liveLeaderCount: 0}
	for streak := 1; streak < debounce; streak++ {
		c.act(ctx, "dev", st, leaderless, streak, false)
		if pod, ok := leaderOf(t, reg, "dev"); !ok || pod != "backend-0" {
			t.Fatalf("streak %d < debounce %d: last-known leader must be retained, got %q ok=%v",
				streak, debounce, pod, ok)
		}
	}
	c.act(ctx, "dev", st, leaderless, debounce, false)
	if _, ok := leaderOf(t, reg, "dev"); ok {
		t.Fatalf("streak == debounce %d: leader must be cleared", debounce)
	}
}

func TestActLeaderless_RetainsUnreachableIncumbentWithinGrace(t *testing.T) {
	t.Parallel()
	reg := registry.New()
	reg.EnsureDeployment("dev", "convex-dev")
	const debounce = 3
	c := New(Config{PromoteDebounce: new(debounce)}, nil, nil, reg, quietLogger())
	st := c.deploymentState("dev")
	c.setLeader("dev", st, "backend-0", "http://10.0.0.1:3210")
	leaderless := decision{liveLeaderCount: 0, incumbentUnreachable: true, promoteTarget: &observation{}}
	c.act(context.Background(), "dev", st, leaderless, debounce, true)
	if pod, ok := leaderOf(t, reg, "dev"); !ok || pod != "backend-0" {
		t.Fatalf("unreachable incumbent within grace must be retained even at debounce, got %q ok=%v", pod, ok)
	}
}

func TestCommitState_UnreachableIncumbentGraceExpiry(t *testing.T) {
	t.Parallel()
	c := New(Config{UnreachableLeaderGrace: new(60 * time.Second)}, nil, nil, registry.New(), quietLogger())
	st := c.deploymentState("dev")
	base := time.Now()
	d := decision{liveLeaderCount: 0, incumbentUnreachable: true}
	if _, _, retain := c.commitState(st, d, false, base); !retain {
		t.Fatal("unreachable incumbent must be retained at the start of the grace window")
	}
	if _, _, retain := c.commitState(st, d, false, base.Add(61*time.Second)); retain {
		t.Fatal("unreachable incumbent must NOT be retained past the grace window")
	}
	reachable := decision{liveLeaderCount: 0, incumbentUnreachable: false}
	if _, _, retain := c.commitState(st, reachable, false, base.Add(62*time.Second)); retain {
		t.Fatal("a reachable/absent incumbent must reset retain")
	}
}

func TestCommitState_EmptyDiscoveryDoesNotRestartUnreachableGrace(t *testing.T) {
	t.Parallel()
	c := New(Config{UnreachableLeaderGrace: new(60 * time.Second)}, nil, nil, registry.New(), quietLogger())
	st := c.deploymentState("dev")
	base := time.Now()
	unreachable := decision{liveLeaderCount: 0, incumbentUnreachable: true}
	if _, _, retain := c.commitState(st, unreachable, false, base); !retain {
		t.Fatal("grace must start on the first unreachable observation")
	}
	c.commitState(st, decision{liveLeaderCount: 0}, true, base.Add(10*time.Second))
	if _, _, retain := c.commitState(st, unreachable, false, base.Add(61*time.Second)); retain {
		t.Fatal(
			"an empty-discovery blip must not restart the grace anchor; grace must still expire 60s after the first unreachable observation",
		)
	}
}

func TestActLeaderless_EmptyListDoesNotAdvanceStreak(t *testing.T) {
	t.Parallel()
	reg := registry.New()
	reg.EnsureDeployment("dev", "convex-dev")
	c := New(Config{PromoteDebounce: new(3)}, nil, nil, reg, quietLogger())
	st := c.deploymentState("dev")
	emptyDecision := decision{liveLeaderCount: 0}
	for range 10 {
		if got, _, _ := c.commitState(st, emptyDecision, true, time.Now()); got != 0 {
			t.Fatalf("empty backend list must not advance the leaderless streak, got %d", got)
		}
	}
}

func TestCommitState_TransitioningDoesNotAdvanceStreak(t *testing.T) {
	t.Parallel()
	c := New(Config{PromoteDebounce: new(3)}, nil, nil, registry.New(), quietLogger())
	st := c.deploymentState("dev")
	transitioning := decision{liveLeaderCount: 0, hasTransitioning: true}
	for range 10 {
		if got, _, _ := c.commitState(st, transitioning, false, time.Now()); got != 0 {
			t.Fatalf("a tick with a mid-drain pod must not advance the leaderless streak, got %d", got)
		}
	}
	if got, _, _ := c.commitState(st, decision{liveLeaderCount: 0}, false, time.Now()); got != 1 {
		t.Fatalf("a genuinely-leaderless tick must resume the streak at 1, got %d", got)
	}
}

func TestCommitState_LeaderfulClearsStreak(t *testing.T) {
	t.Parallel()
	c := New(Config{}, nil, nil, registry.New(), quietLogger())
	st := c.deploymentState("dev")
	st.leaderlessStreak = 5
	if got, _, _ := c.commitState(st, decision{liveLeaderCount: 1}, false, time.Now()); got != 0 {
		t.Fatalf("a live leader must reset the streak to 0, got %d", got)
	}
}

func TestActLeaderless_RetainsLeaderWhenPromotionBlockedByInflight(t *testing.T) {
	t.Parallel()
	reg := registry.New()
	reg.EnsureDeployment("prod", "convex-prod")
	const debounce = 3
	c := New(Config{PromoteDebounce: new(debounce)}, nil, nil, reg, quietLogger())
	st := c.deploymentState("prod")
	c.setLeader("prod", st, "backend-0", "http://10.0.0.1:3210")
	st.promoting = true
	d := decision{liveLeaderCount: 0, promoteTarget: &observation{}}
	c.actLeaderless(context.Background(), "prod", st, d, debounce, false)
	if pod, ok := leaderOf(t, reg, "prod"); !ok || pod != "backend-0" {
		t.Fatalf("promotion blocked by inflight must retain last-known leader, got %q ok=%v", pod, ok)
	}
}
