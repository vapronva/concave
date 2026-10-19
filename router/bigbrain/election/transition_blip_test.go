package election

import (
	"testing"
	"time"
)

func TestRememberTransitions_UnreachableMidPromotionStaysTransitioning(t *testing.T) {
	t.Parallel()
	c := New(DefaultConfig(), nil, nil, nil, quietLogger())
	st := c.deploymentState("dev")
	now := time.Now()
	standby := obs("backend-2", false, 80, -1)
	c.markTransitioning(st, "backend-1")
	blip := []observation{unreachable("backend-1"), standby}
	c.rememberTransitions(st, blip, now.Add(2*time.Second))
	if !anyTransitioning(blip) {
		t.Fatal("a pod whose promote was just accepted must keep freezing promotion while unreachable")
	}
	if d := decide(blip, sticky("")); d.promoteTarget != nil {
		t.Fatalf("a blip during a promotion must not promote a second standby, got %+v", d.promoteTarget)
	}
	seen := []observation{withRole(obs("backend-1", false, 90, -1), "promoting"), standby}
	c.rememberTransitions(st, seen, now.Add(4*time.Second))
	back := []observation{obs("backend-1", false, 95, -1), standby}
	c.rememberTransitions(st, back, now.Add(6*time.Second))
	if len(st.transitioningSeen) != 0 {
		t.Fatalf("a pod observed settled must drop its transition memory, got %v", st.transitioningSeen)
	}
	c.rememberTransitions(st, seen, now.Add(8*time.Second))
	gone := []observation{unreachable("backend-1"), standby}
	c.rememberTransitions(st, gone, now.Add(8*time.Second+unreachableLeaderGrace))
	if anyTransitioning(gone) {
		t.Fatal("past the grace an unreachable pod is gone, not transitioning")
	}
}

func TestRememberTransitions_StalePollCannotClearNewerSeed(t *testing.T) {
	t.Parallel()
	c := New(DefaultConfig(), nil, nil, nil, quietLogger())
	st := c.deploymentState("dev")
	pollStart := time.Now()
	c.markTransitioning(st, "backend-1")
	settled := []observation{obs("backend-1", false, 90, -1), obs("backend-2", false, 80, -1)}
	c.rememberTransitions(st, settled, pollStart)
	if _, ok := st.transitioningSeen["backend-1"]; !ok {
		t.Fatal("an observation older than the seed must not clear it")
	}
	if c.tryAcquirePromote(st, pollStart) {
		t.Fatal("a decision from a poll older than the seed must not actuate")
	}
	if !c.tryAcquirePromote(st, time.Now()) {
		t.Fatal("a decision from a poll newer than the seed actuates")
	}
	c.rememberTransitions(st, settled, time.Now())
	if len(st.transitioningSeen) != 0 {
		t.Fatalf("an observation newer than the seed clears it, got %v", st.transitioningSeen)
	}
}
