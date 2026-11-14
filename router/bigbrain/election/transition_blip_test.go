package election

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"git.horse/vapronva/concave/router/bigbrain/backend"
	"git.horse/vapronva/concave/router/bigbrain/k8sclient"
	"git.horse/vapronva/concave/router/bigbrain/registry"
)

func TestRememberTransitions_UnreachableMidPromotionStaysTransitioning(t *testing.T) {
	t.Parallel()
	c := New(DefaultConfig(), nil, nil, nil, quietLogger())
	st := &deploymentState{}
	now := time.Now()
	standby := obs("backend-2", false, 80, -1)
	st.markTransitioning("backend-1", now)
	blip := []observation{unreachable("backend-1"), standby}
	c.rememberTransitions(st, blip, now.Add(2*time.Second))
	if !anyTransitioning(blip) {
		t.Fatal("a pod whose promote was just accepted must keep freezing promotion while unreachable")
	}
	if d := decide(blip, sticky("")); d.promoteTarget != nil {
		t.Fatalf("a blip during a promotion must not promote a second standby, got %+v", d.promoteTarget)
	}
	seen := []observation{promoting(obs("backend-1", false, 90, -1)), standby}
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

func TestPollAll_RefusedReleasesTransitionMemoryAndIncumbentHold(t *testing.T) {
	t.Parallel()
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()
	alive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer alive.Close()
	c := New(DefaultConfig(), nil, backend.New(nil), registry.New(), quietLogger())
	st := &deploymentState{}
	st.markTransitioning("alive", time.Now())
	st.markTransitioning("dead", time.Now())
	pods := []k8sclient.Backend{{Pod: "alive", URL: alive.URL}, {Pod: "dead", URL: dead.URL}}
	got := c.pollAll(context.Background(), "dev", pods)
	c.rememberTransitions(st, got, time.Now())
	if got[0].refused || !got[0].wasTransitioning {
		t.Fatalf("a live process failing its poll keeps the transition memory, got %+v", got[0])
	}
	if !got[1].refused || got[1].wasTransitioning {
		t.Fatalf("a refused poll means the accepting process is gone and releases the memory, got %+v", got[1])
	}
	if !decide(got[:1], sticky("alive")).incumbentUnreachable {
		t.Fatal("a live incumbent failing its poll keeps the unreachable-incumbent hold")
	}
	if decide(got[1:], sticky("dead")).incumbentUnreachable {
		t.Fatal("a refused incumbent poll releases the unreachable-incumbent hold")
	}
}
