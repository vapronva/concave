package election

import (
	"slices"
	"time"
)

type action struct {
	pod string
	url string
}

type decision struct {
	leaderPod            string
	leaderURL            string
	liveLeaderCount      int
	incumbentUnreachable bool
	hasTransitioning     bool
	promoteTarget        *observation
	demotes              []action
	failbackTarget       *observation
	failbackState        failbackState
}

type failbackState struct {
	candidate     string
	eligibleSince time.Time
}

func claimedLeaders(obs []observation) []observation {
	var out []observation
	for _, o := range obs {
		if o.reach && o.status.IsLeader {
			out = append(out, o)
		}
	}
	return out
}

func isTransitioning(o observation) bool {
	return o.reach && !o.status.IsLeader && o.status.LeaseTS != nil
}

func anyTransitioning(obs []observation) bool {
	return slices.ContainsFunc(obs, isTransitioning)
}

func incumbentDiscoveredUnreachable(obs []observation, incumbent string) bool {
	if incumbent == "" {
		return false
	}
	for _, o := range obs {
		if o.be.Pod == incumbent {
			return !o.reach
		}
	}
	return false
}

func pickLeader(claims []observation, incumbent string) observation {
	best := claims[0]
	for _, o := range claims[1:] {
		if betterLeader(o, best) {
			best = o
		}
	}
	for _, o := range claims {
		if o.be.Pod != incumbent {
			continue
		}
		bestLease, bestHasLease := leaseOf(best)
		incumbentLease, incumbentHasLease := leaseOf(o)
		if bestHasLease && (!incumbentHasLease || incumbentLease < bestLease) {
			break
		}
		return o
	}
	return best
}

func betterLeader(a, b observation) bool {
	at, aok := leaseOf(a)
	bt, bok := leaseOf(b)
	switch {
	case aok && bok && at != bt:
		return at > bt
	case aok != bok:
		return aok
	default:
		return a.be.Pod < b.be.Pod
	}
}

func leaseOf(o observation) (uint64, bool) {
	if o.status.LeaseTS == nil {
		return 0, false
	}
	return *o.status.LeaseTS, true
}

func bestCandidate(obs []observation) (observation, bool) {
	var best observation
	var found bool
	for _, o := range obs {
		if !o.reach || o.status.IsLeader || isTransitioning(o) {
			continue
		}
		if !found || morePreferredCandidate(o, best) {
			best, found = o, true
		}
	}
	return best, found
}

func morePreferredCandidate(a, b observation) bool {
	switch {
	case a.be.Priority != b.be.Priority:
		return a.be.Priority > b.be.Priority
	case a.status.LatestTS != b.status.LatestTS:
		return a.status.LatestTS > b.status.LatestTS
	default:
		return a.be.Pod < b.be.Pod
	}
}

type failbackParams struct {
	enabled         bool
	stabilityWindow time.Duration
	warmthLagNs     uint64
	now             time.Time
	prior           failbackState
}

type decideParams struct {
	incumbent string
	failback  failbackParams
}

func decide(obs []observation, p decideParams) decision {
	claims := claimedLeaders(obs)
	d := decision{liveLeaderCount: len(claims), hasTransitioning: anyTransitioning(obs)}
	if len(claims) == 0 {
		d.incumbentUnreachable = incumbentDiscoveredUnreachable(obs, p.incumbent)
		if d.hasTransitioning {
			return d
		}
		if cand, ok := bestCandidate(obs); ok {
			d.promoteTarget = &cand
		}
		return d
	}
	leader := pickLeader(claims, p.incumbent)
	d.leaderPod, d.leaderURL = leader.be.Pod, leader.be.URL
	for _, o := range claims {
		if o.be.Pod != leader.be.Pod {
			d.demotes = append(d.demotes, action{pod: o.be.Pod, url: o.be.URL})
		}
	}
	if len(claims) == 1 {
		fb, st := evaluateFailback(obs, leader, p.failback)
		d.failbackTarget = fb
		d.failbackState = st
	}
	return d
}

func evaluateFailback(
	obs []observation,
	leader observation,
	p failbackParams,
) (*observation, failbackState) {
	if !p.enabled {
		return nil, failbackState{}
	}
	cand, ok := bestFailbackCandidate(obs, leader, p.warmthLagNs)
	if !ok {
		return nil, failbackState{}
	}
	since := p.now
	if p.prior.candidate == cand.be.Pod && !p.prior.eligibleSince.IsZero() {
		since = p.prior.eligibleSince
	}
	st := failbackState{candidate: cand.be.Pod, eligibleSince: since}
	if p.now.Sub(since) >= p.stabilityWindow {
		return &cand, st
	}
	return nil, st
}

func bestFailbackCandidate(
	obs []observation,
	leader observation,
	warmthLagNs uint64,
) (observation, bool) {
	var best observation
	var found bool
	for _, o := range obs {
		if !o.reach || o.status.IsLeader || isTransitioning(o) {
			continue
		}
		if o.be.Pod == leader.be.Pod {
			continue
		}
		if o.be.Priority <= leader.be.Priority {
			continue
		}
		if !warmEnough(o.status.LatestTS, leader.status.LatestTS, warmthLagNs) {
			continue
		}
		if !found || morePreferredCandidate(o, best) {
			best, found = o, true
		}
	}
	return best, found
}

func warmEnough(candTS, leaderTS, lagNs uint64) bool {
	if candTS >= leaderTS {
		return true
	}
	return leaderTS-candTS <= lagNs
}
