//nolint:testpackage // white-box
package election

import (
	"testing"
	"time"

	"git.horse/vapronva/concave/router/bigbrain/backend"
	"git.horse/vapronva/concave/router/bigbrain/k8sclient"
)

const (
	testStabilityWindow = 15 * time.Second
	testWarmthLag       = uint64(5)
)

func obs(pod string, isLeader bool, latestTS uint64, lease int64) observation {
	o := observation{
		be:     k8sclient.Backend{Pod: pod, URL: "http://" + pod + ":3210"},
		status: backend.Leadership{IsLeader: isLeader, LatestTS: latestTS},
		reach:  true,
	}
	if lease >= 0 {
		ts := uint64(lease)
		o.status.LeaseTS = &ts
	}
	return o
}

func withPriority(o observation, prio int) observation {
	o.be.Priority = prio
	return o
}

func unreachable(pod string) observation {
	return observation{be: k8sclient.Backend{Pod: pod, URL: "http://" + pod + ":3210"}, reach: false}
}

func sticky(incumbent string) decideParams {
	return decideParams{incumbent: incumbent}
}

func TestDecide_SteadyState_SingleLeaderAdopted(t *testing.T) {
	t.Parallel()
	in := []observation{
		obs("backend-0", true, 100, 100),
		obs("backend-1", false, 98, -1),
	}
	d := decide(in, sticky("backend-0"))
	if d.leaderPod != "backend-0" {
		t.Fatalf("want leader backend-0, got %q", d.leaderPod)
	}
	if d.liveLeaderCount != 1 {
		t.Fatalf("want 1 live leader, got %d", d.liveLeaderCount)
	}
	if len(d.demotes) != 0 {
		t.Fatalf("want no demotes, got %d", len(d.demotes))
	}
	if d.promoteTarget != nil {
		t.Fatalf("want no promote target in steady state")
	}
}

func TestDecide_AdoptSoleLeader_NoIncumbentMatch(t *testing.T) {
	t.Parallel()
	in := []observation{
		obs("backend-0", true, 200, 200),
		obs("backend-1", false, 199, -1),
	}
	d := decide(in, sticky("backend-1"))
	if d.leaderPod != "backend-0" {
		t.Fatalf("want adopt backend-0, got %q", d.leaderPod)
	}
}

func TestDecide_StickyIncumbent_DemotesOther(t *testing.T) {
	t.Parallel()
	in := []observation{
		obs("backend-0", true, 100, 100),
		obs("backend-1", true, 100, 100),
	}
	d := decide(in, sticky("backend-1"))
	if d.leaderPod != "backend-1" {
		t.Fatalf("sticky: want incumbent backend-1 kept, got %q", d.leaderPod)
	}
	if len(d.demotes) != 1 || d.demotes[0].pod != "backend-0" {
		t.Fatalf("want backend-0 demoted, got %+v", d.demotes)
	}
}

func TestDecide_SplitBrain_FreshestLeaseWins(t *testing.T) {
	t.Parallel()
	in := []observation{
		obs("backend-0", true, 100, 50),
		obs("backend-1", true, 100, 150),
	}
	d := decide(in, sticky("backend-0"))
	if d.leaderPod != "backend-1" {
		t.Fatalf("split-brain: want fresher-lease backend-1, got %q", d.leaderPod)
	}
	if len(d.demotes) != 1 || d.demotes[0].pod != "backend-0" {
		t.Fatalf("want zombie backend-0 demoted, got %+v", d.demotes)
	}
}

func TestDecide_Leaderless_PromotesWarmest(t *testing.T) {
	t.Parallel()
	in := []observation{
		obs("backend-0", false, 90, -1),
		obs("backend-1", false, 120, -1),
		unreachable("backend-2"),
	}
	d := decide(in, sticky("backend-0"))
	if d.liveLeaderCount != 0 {
		t.Fatalf("want 0 live leaders, got %d", d.liveLeaderCount)
	}
	if d.promoteTarget == nil {
		t.Fatalf("want a promote target")
	}
	if d.promoteTarget.be.Pod != "backend-1" {
		t.Fatalf("want warmest reachable standby backend-1, got %q", d.promoteTarget.be.Pod)
	}
}

func TestDecide_Leaderless_NoReachableStandby(t *testing.T) {
	t.Parallel()
	in := []observation{
		unreachable("backend-0"),
		unreachable("backend-1"),
	}
	d := decide(in, sticky("backend-1"))
	if d.promoteTarget != nil {
		t.Fatalf("want no promote target when no standby is reachable")
	}
}

func TestDecide_Leaderless_NotReadyButReachableFollowerIsPromotable(t *testing.T) {
	t.Parallel()
	in := []observation{
		unreachable("backend-0"),
		obs("backend-1", false, 120, -1),
	}
	d := decide(in, sticky("backend-0"))
	if d.promoteTarget == nil || d.promoteTarget.be.Pod != "backend-1" {
		t.Fatalf("a reachable warm follower must be promotable regardless of k8s PodReady, got %+v", d.promoteTarget)
	}
}

func TestDecide_TransitioningPodExcludedFromPromotion(t *testing.T) {
	t.Parallel()
	in := []observation{
		withPriority(obs("draining", false, 200, 500), 100),
		withPriority(obs("standby", false, 120, -1), 0),
	}
	d := decide(in, sticky("draining"))
	if !d.hasTransitioning {
		t.Fatalf("a reachable lease-holding non-leader must mark the decision transitioning")
	}
	if d.promoteTarget != nil {
		t.Fatalf(
			"no promotion may happen while a pod is mid-drain (steal-mid-drain), got %+v",
			d.promoteTarget,
		)
	}
}

func TestDecide_TransitioningOnlyPodIsNotPromoted(t *testing.T) {
	t.Parallel()
	in := []observation{
		obs("draining", false, 200, 500),
	}
	d := decide(in, sticky("draining"))
	if d.promoteTarget != nil {
		t.Fatalf("a sole mid-demotion pod must not be promoted, got %q", d.promoteTarget.be.Pod)
	}
	if !d.hasTransitioning {
		t.Fatalf("decision must carry the transitioning marker so the streak freezes")
	}
}

func TestDecide_SplitBrain_ThreeClaimants(t *testing.T) {
	t.Parallel()
	in := []observation{
		obs("backend-0", true, 100, 10),
		obs("backend-1", true, 100, 99),
		obs("backend-2", true, 100, 30),
	}
	d := decide(in, sticky("backend-2"))
	if d.leaderPod != "backend-1" {
		t.Fatalf("want freshest backend-1, got %q", d.leaderPod)
	}
	if len(d.demotes) != 2 {
		t.Fatalf("want 2 zombies demoted, got %d", len(d.demotes))
	}
}

func TestPickLeader_TieBreakByPodName(t *testing.T) {
	t.Parallel()
	claims := []observation{
		obs("backend-9", true, 100, 100),
		obs("backend-3", true, 100, 100),
	}
	got, ok := pickLeader(claims, "")
	if !ok || got.be.Pod != "backend-3" {
		t.Fatalf("want lowest pod name backend-3, got %q (ok=%v)", got.be.Pod, ok)
	}
}

func TestPickLeader_IncumbentZombieDefersToFresh(t *testing.T) {
	t.Parallel()
	claims := []observation{
		obs("backend-0", true, 100, 5),
		obs("backend-1", true, 100, 80),
	}
	got, _ := pickLeader(claims, "backend-0")
	if got.be.Pod != "backend-1" {
		t.Fatalf("zombie incumbent must defer to fresher lease holder, got %q", got.be.Pod)
	}
}

func TestClaimedLeaders_IgnoresUnreachableAndFollowers(t *testing.T) {
	t.Parallel()
	in := []observation{
		obs("backend-0", true, 100, 100),
		obs("backend-1", false, 99, -1),
		unreachable("backend-2"),
	}
	if got := claimedLeaders(in); len(got) != 1 || got[0].be.Pod != "backend-0" {
		t.Fatalf("want only backend-0 as claimed leader, got %+v", got)
	}
}

func fb(now time.Time, prior failbackState) failbackParams {
	return failbackParams{
		enabled:         true,
		stabilityWindow: testStabilityWindow,
		warmthLagNs:     testWarmthLag,
		now:             now,
		prior:           prior,
	}
}

func TestDecide_Leaderless_PromotesHighestPriorityNotWarmest(t *testing.T) {
	t.Parallel()
	in := []observation{
		withPriority(obs("primary", false, 90, -1), 100),
		withPriority(obs("standby", false, 120, -1), 0),
	}
	d := decide(in, sticky(""))
	if d.promoteTarget == nil {
		t.Fatalf("want a promote target")
	}
	if d.promoteTarget.be.Pod != "primary" {
		t.Fatalf("leaderless must promote highest-PRIORITY pod 'primary', got %q", d.promoteTarget.be.Pod)
	}
}

func TestDecide_Leaderless_EqualPriorityBreaksByWarmth(t *testing.T) {
	t.Parallel()
	in := []observation{
		withPriority(obs("a", false, 90, -1), 50),
		withPriority(obs("b", false, 120, -1), 50),
	}
	d := decide(in, sticky(""))
	if d.promoteTarget == nil || d.promoteTarget.be.Pod != "b" {
		t.Fatalf("equal priority -> warmest 'b' should win, got %+v", d.promoteTarget)
	}
}

func TestDecide_Failback_FiresWhenPrimaryWarmStablePastWindow(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	in := []observation{
		withPriority(obs("standby", true, 100, 100), 0),
		withPriority(obs("primary", false, 99, -1), 100),
	}
	prior := failbackState{candidate: "primary", eligibleSince: now.Add(-30 * time.Second)}
	d := decide(in, decideParams{incumbent: "standby", failback: fb(now, prior)})
	if d.leaderPod != "standby" {
		t.Fatalf("incumbent stays the registry leader until the promote lands, got %q", d.leaderPod)
	}
	if d.failbackTarget == nil || d.failbackTarget.be.Pod != "primary" {
		t.Fatalf("want failback target 'primary', got %+v", d.failbackTarget)
	}
	if len(d.demotes) != 0 {
		t.Fatalf("failback must NOT demote the incumbent, got %d demotes", len(d.demotes))
	}
}

func TestDecide_Failback_NoFailbackWithinWindow(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	in := []observation{
		withPriority(obs("standby", true, 100, 100), 0),
		withPriority(obs("primary", false, 99, -1), 100),
	}
	d := decide(in, decideParams{incumbent: "standby", failback: fb(now, failbackState{})})
	if d.failbackTarget != nil {
		t.Fatalf("must NOT fail back within the stability window, got target %q", d.failbackTarget.be.Pod)
	}
	if d.failbackState.candidate != "primary" || !d.failbackState.eligibleSince.Equal(now) {
		t.Fatalf("want eligibleSince started at now for 'primary', got %+v", d.failbackState)
	}
}

func TestDecide_Failback_NoFailbackWhenNotWarm(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	in := []observation{
		withPriority(obs("standby", true, 100, 100), 0),
		withPriority(obs("primary", false, 80, -1), 100),
	}
	prior := failbackState{candidate: "primary", eligibleSince: now.Add(-1 * time.Hour)}
	d := decide(in, decideParams{incumbent: "standby", failback: fb(now, prior)})
	if d.failbackTarget != nil {
		t.Fatalf("must NOT fail back to a cold (lagging) primary, got %q", d.failbackTarget.be.Pod)
	}
	if d.failbackState != (failbackState{}) {
		t.Fatalf("a not-warm candidate must RESET the clock, got %+v", d.failbackState)
	}
}

func TestDecide_Failback_DemotedNotReadyExLeaderIsEligible(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	in := []observation{
		withPriority(obs("standby", true, 100, 100), 0),
		withPriority(obs("primary", false, 99, -1), 100),
	}
	prior := failbackState{candidate: "primary", eligibleSince: now.Add(-1 * time.Hour)}
	d := decide(in, decideParams{incumbent: "standby", failback: fb(now, prior)})
	if d.failbackTarget == nil || d.failbackTarget.be.Pod != "primary" {
		t.Fatalf("a demoted-in-place primary (NotReady by role-conformance readyz) must be failback-eligible, got %+v",
			d.failbackTarget)
	}
}

func TestDecide_Failback_TransitioningPrimaryNotEligible(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	in := []observation{
		withPriority(obs("standby", true, 100, 100), 0),
		withPriority(obs("primary", false, 99, 90), 100),
	}
	prior := failbackState{candidate: "primary", eligibleSince: now.Add(-1 * time.Hour)}
	d := decide(in, decideParams{incumbent: "standby", failback: fb(now, prior)})
	if d.failbackTarget != nil {
		t.Fatalf(
			"a mid-drain primary (lease still held) must NOT be failback-eligible, got %q",
			d.failbackTarget.be.Pod,
		)
	}
	if d.failbackState != (failbackState{}) {
		t.Fatalf("a transitioning candidate must RESET the clock, got %+v", d.failbackState)
	}
	if !d.hasTransitioning {
		t.Fatalf("decision must carry the transitioning marker")
	}
}

func TestDecide_Failback_FlappingResetsEligibleSince(t *testing.T) {
	t.Parallel()
	leader := withPriority(obs("standby", true, 100, 100), 0)
	t0 := time.Unix(1000, 0)
	in0 := []observation{leader, withPriority(obs("primary", false, 99, -1), 100)}
	d0 := decide(in0, decideParams{incumbent: "standby", failback: fb(t0, failbackState{})})
	if d0.failbackTarget != nil {
		t.Fatalf("t0: should not fire immediately")
	}
	if !d0.failbackState.eligibleSince.Equal(t0) {
		t.Fatalf("t0: clock should start at t0, got %v", d0.failbackState.eligibleSince)
	}
	t1 := t0.Add(10 * time.Second)
	in1 := []observation{leader, unreachable("primary")}
	d1 := decide(in1, decideParams{incumbent: "standby", failback: fb(t1, d0.failbackState)})
	if d1.failbackState != (failbackState{}) {
		t.Fatalf("t1: flap must reset the clock, got %+v", d1.failbackState)
	}
	t2 := t1.Add(1 * time.Second)
	in2 := []observation{leader, withPriority(obs("primary", false, 99, -1), 100)}
	d2 := decide(in2, decideParams{incumbent: "standby", failback: fb(t2, d1.failbackState)})
	if d2.failbackTarget != nil {
		t.Fatalf("t2: a freshly-recovered primary must NOT fail back immediately after a flap")
	}
	if !d2.failbackState.eligibleSince.Equal(t2) {
		t.Fatalf("t2: clock must RESTART at t2 after the flap, got %v", d2.failbackState.eligibleSince)
	}
}

func TestDecide_Failback_DisabledIsPureSticky(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	in := []observation{
		withPriority(obs("standby", true, 100, 100), 0),
		withPriority(obs("primary", false, 100, -1), 100),
	}
	prior := failbackState{candidate: "primary", eligibleSince: now.Add(-1 * time.Hour)}
	d := decide(
		in,
		decideParams{incumbent: "standby", failback: failbackParams{enabled: false, now: now, prior: prior}},
	)
	if d.leaderPod != "standby" {
		t.Fatalf("disabled: incumbent kept, got %q", d.leaderPod)
	}
	if d.failbackTarget != nil {
		t.Fatalf("disabled: must NOT fail back, got %q", d.failbackTarget.be.Pod)
	}
	if d.failbackState != (failbackState{}) {
		t.Fatalf("disabled: clock must stay reset, got %+v", d.failbackState)
	}
}

func TestDecide_Failback_EqualPriorityNoFailback(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	in := []observation{
		withPriority(obs("backend-0", true, 100, 100), 100),
		withPriority(obs("backend-1", false, 100, -1), 100),
	}
	prior := failbackState{candidate: "backend-1", eligibleSince: now.Add(-1 * time.Hour)}
	d := decide(in, decideParams{incumbent: "backend-0", failback: fb(now, prior)})
	if d.failbackTarget != nil {
		t.Fatalf("equal priority: must NOT fail back, got %q", d.failbackTarget.be.Pod)
	}
	if d.failbackState != (failbackState{}) {
		t.Fatalf("equal priority: no eligible candidate -> clock reset, got %+v", d.failbackState)
	}
}

func TestDecide_Failback_SplitBrainResolvesFirst(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)
	in := []observation{
		withPriority(obs("backend-0", true, 100, 150), 0),
		withPriority(obs("backend-1", true, 100, 50), 0),
		withPriority(obs("primary", false, 100, -1), 100),
	}
	prior := failbackState{candidate: "primary", eligibleSince: now.Add(-1 * time.Hour)}
	d := decide(in, decideParams{incumbent: "backend-0", failback: fb(now, prior)})
	if d.leaderPod != "backend-0" {
		t.Fatalf("split-brain: freshest-lease backend-0 must be leader, got %q", d.leaderPod)
	}
	if len(d.demotes) != 1 || d.demotes[0].pod != "backend-1" {
		t.Fatalf("split-brain: want zombie backend-1 demoted, got %+v", d.demotes)
	}
	if d.failbackTarget != nil {
		t.Fatalf("split-brain takes precedence: must NOT also fail back, got %q", d.failbackTarget.be.Pod)
	}
}

func TestWarmEnough_Boundary(t *testing.T) {
	t.Parallel()
	if !warmEnough(95, 100, 5) {
		t.Fatalf("lag exactly == bound (5) must be warm")
	}
	if warmEnough(94, 100, 5) {
		t.Fatalf("lag 6 > bound 5 must be cold")
	}
	if !warmEnough(130, 100, 5) {
		t.Fatalf("a candidate AHEAD of the leader is always warm")
	}
}
