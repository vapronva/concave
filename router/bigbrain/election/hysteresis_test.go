//nolint:testpackage // white-box
package election

import (
	"context"
	"testing"

	"git.horse/vapronva/concave/router/bigbrain/registry"
)

func leaderOf(t *testing.T, reg *registry.Registry, name string) (string, bool) {
	t.Helper()
	pod, _, ok := reg.Leader(name)
	return pod, ok
}

func TestActLeaderless_RetainsLeaderUntilHysteresis(t *testing.T) {
	t.Parallel()
	reg := registry.New()
	reg.EnsureDeployment("dev", "convex-dev")
	const debounce = 3
	c := New(Config{PromoteDebounce: debounce}, nil, nil, reg, quietLogger())
	st := c.deploymentState("dev")
	ctx := context.Background()
	states := []registry.BackendState{{Pod: "backend-0", URL: "http://10.0.0.1:3210"}}
	c.setLeader("dev", st, states, "backend-0", "http://10.0.0.1:3210")
	if pod, ok := leaderOf(t, reg, "dev"); !ok || pod != "backend-0" {
		t.Fatalf("setup: want leader backend-0, got %q ok=%v", pod, ok)
	}
	leaderless := decision{liveLeaderCount: 0}
	for streak := 1; streak < debounce; streak++ {
		c.act(ctx, "dev", st, states, leaderless, streak)
		pod, ok := leaderOf(t, reg, "dev")
		if !ok || pod != "backend-0" {
			t.Fatalf("streak %d < debounce %d: leader must be retained (backend-0), got %q ok=%v",
				streak, debounce, pod, ok)
		}
	}
	c.act(ctx, "dev", st, states, leaderless, debounce)
	if _, ok := leaderOf(t, reg, "dev"); ok {
		t.Fatalf("streak == debounce %d: leader must be cleared", debounce)
	}
}

func TestActLeaderless_EmptyListDoesNotAdvanceStreak(t *testing.T) {
	t.Parallel()
	reg := registry.New()
	reg.EnsureDeployment("dev", "convex-dev")
	c := New(Config{PromoteDebounce: 3}, nil, nil, reg, quietLogger())
	st := c.deploymentState("dev")
	snap := c.snapshotState(st)
	emptyDecision := decision{liveLeaderCount: 0}
	for range 10 {
		if got := c.commitState(st, snap, emptyDecision, true); got != 0 {
			t.Fatalf("empty backend list must not advance the leaderless streak, got %d", got)
		}
		snap = c.snapshotState(st)
	}
}

func TestCommitState_LeaderfulClearsUnpromotableAndStreak(t *testing.T) {
	t.Parallel()
	c := New(Config{}, nil, nil, registry.New(), quietLogger())
	st := c.deploymentState("dev")
	c.markUnpromotable(st, "backend-9")
	snap := c.snapshotState(st)
	snap.leaderlessStreak = 5
	if got := c.commitState(st, snap, decision{liveLeaderCount: 1}, false); got != 0 {
		t.Fatalf("a live leader must reset the streak to 0, got %d", got)
	}
	if snap2 := c.snapshotState(st); len(snap2.unpromotable) != 0 {
		t.Fatalf("a live leader must clear the un-promotable set, got %+v", snap2.unpromotable)
	}
}
