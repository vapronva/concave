package election

import (
	"slices"
	"time"
)

type demoteTarget struct {
	pod     string
	url     string
	leaseTS *uint64
}

type decision struct {
	leaderPod            string
	leaderURL            string
	liveLeaderCount      int
	incumbentUnreachable bool
	hasTransitioning     bool
	promoteTarget        *observation
	demotes              []demoteTarget
	failbackTarget       *observation
	failbackState        failbackState
}

type failbackState struct {
	candidate     string
	eligibleSince time.Time
}

func isLiveLeaderClaim(o observation, grace time.Duration) bool {
	if !o.reach || !o.status.IsLeader {
		return false
	}
	if grace <= 0 || o.status.LeaseUnverifiedSecs == nil {
		return true
	}
	return *o.status.LeaseUnverifiedSecs <= uint64(grace/time.Second)
}

func claimedLeaders(obs []observation, grace time.Duration) []observation {
	var out []observation
	for _, o := range obs {
		if isLiveLeaderClaim(o, grace) {
			out = append(out, o)
		}
	}
	return out
}

func isTransitioning(o observation) bool {
	if !o.reach || o.status.IsLeader {
		return false
	}
	return o.status.LeaseTS != nil || o.status.Role == "promoting"
}

func anyTransitioning(obs []observation) bool {
	return slices.ContainsFunc(obs, isTransitioning)
}

func isPromotable(o observation) bool {
	return o.reach && !o.status.IsLeader && !isTransitioning(o)
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

func pickLeader(claims []observation) observation {
	best := claims[0]
	for _, o := range claims[1:] {
		if betterLeader(o, best) {
			best = o
		}
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
		if !isPromotable(o) {
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
	incumbent            string
	leaseUnverifiedGrace time.Duration
	failback             failbackParams
}

func decide(obs []observation, p decideParams) decision {
	claims := claimedLeaders(obs, p.leaseUnverifiedGrace)
	d := decision{liveLeaderCount: len(claims), hasTransitioning: anyTransitioning(obs)}
	if len(claims) == 0 {
		d.incumbentUnreachable = incumbentDiscoveredUnreachable(obs, p.incumbent)
		d.failbackState = retainedFailback(obs, p.failback.prior)
		if d.hasTransitioning {
			return d
		}
		if cand, ok := bestCandidate(obs); ok {
			d.promoteTarget = &cand
		}
		return d
	}
	leader := pickLeader(claims)
	d.leaderPod, d.leaderURL = leader.be.Pod, leader.be.URL
	for _, o := range claims {
		if o.be.Pod != leader.be.Pod {
			d.demotes = append(d.demotes, demoteTarget{pod: o.be.Pod, url: o.be.URL, leaseTS: o.status.LeaseTS})
		}
	}
	if len(claims) == 1 {
		fb, st := evaluateFailback(obs, leader, p.failback)
		d.failbackState = st
		if !d.hasTransitioning {
			d.failbackTarget = fb
		}
	}
	return d
}

func retainedFailback(obs []observation, prior failbackState) failbackState {
	isCandidate := func(o observation) bool { return o.be.Pod == prior.candidate && isPromotable(o) }
	if slices.ContainsFunc(obs, isCandidate) {
		return prior
	}
	return failbackState{}
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
		if !isPromotable(o) {
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
