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
	c.markTransitioning(st, obs("backend-1", false, 90, -1).be)
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
	restarted := []observation{unreachable("backend-1"), standby}
	restarted[0].be.Restarts = 1
	c.rememberTransitions(st, restarted, now.Add(10*time.Second))
	if anyTransitioning(restarted) {
		t.Fatal("a pod whose container restarted is no longer transitioning")
	}
	c.rememberTransitions(st, seen, now.Add(12*time.Second))
	gone := []observation{unreachable("backend-1"), standby}
	c.rememberTransitions(st, gone, now.Add(12*time.Second+unreachableLeaderGrace))
	if anyTransitioning(gone) {
		t.Fatal("past the grace an unreachable pod is gone, not transitioning")
	}
}
