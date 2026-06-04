package election

import "time"

type action struct {
	pod  string
	url  string
	kind actionKind
}

type actionKind int

const (
	actPromote actionKind = iota
	actDemote
)

type decision struct {
	leaderPod       string
	leaderURL       string
	liveLeaderCount int
	promoteTarget   *observation
	demotes         []action
	failbackTarget  *observation
	failbackState   failbackState
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

func freshestLeaseTS(claims []observation) (uint64, bool) {
	var freshest uint64
	var found bool
	for _, o := range claims {
		if o.status.LeaseTS == nil {
			continue
		}
		if !found || *o.status.LeaseTS > freshest {
			freshest, found = *o.status.LeaseTS, true
		}
	}
	return freshest, found
}

func pickLeader(claims []observation, incumbent string) (observation, bool) {
	if len(claims) == 0 {
		return observation{}, false
	}
	freshTS, haveFresh := freshestLeaseTS(claims)
	for _, o := range claims {
		if o.be.Pod != incumbent {
			continue
		}
		if haveFresh && (o.status.LeaseTS == nil || *o.status.LeaseTS < freshTS) {
			break
		}
		return o, true
	}
	best := claims[0]
	for _, o := range claims[1:] {
		if betterLeader(o, best) {
			best = o
		}
	}
	return best, true
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

func bestCandidate(obs []observation, skip map[string]bool) (observation, bool) {
	var best observation
	var found bool
	for _, o := range obs {
		if !o.reach || !o.be.Ready || o.status.IsLeader {
			continue
		}
		if skip[o.be.Pod] {
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
	incumbent   string
	skipPromote map[string]bool
	failback    failbackParams
}

func decide(obs []observation, p decideParams) decision {
	claims := claimedLeaders(obs)
	d := decision{liveLeaderCount: len(claims)}
	if len(claims) == 0 {
		if cand, ok := bestCandidate(obs, p.skipPromote); ok {
			c := cand
			d.promoteTarget = &c
		}
		return d
	}
	leader, ok := pickLeader(claims, p.incumbent)
	if !ok {
		return d
	}
	d.leaderPod, d.leaderURL = leader.be.Pod, leader.be.URL
	for _, o := range claims {
		if o.be.Pod != leader.be.Pod {
			d.demotes = append(d.demotes, action{pod: o.be.Pod, url: o.be.URL, kind: actDemote})
		}
	}
	if len(claims) == 1 {
		fb, st := evaluateFailback(obs, leader, p.skipPromote, p.failback)
		d.failbackTarget = fb
		d.failbackState = st
	}
	return d
}

func evaluateFailback(
	obs []observation,
	leader observation,
	skip map[string]bool,
	p failbackParams,
) (*observation, failbackState) {
	if !p.enabled {
		return nil, failbackState{}
	}
	cand, ok := bestFailbackCandidate(obs, leader, skip, p.warmthLagNs)
	if !ok {
		return nil, failbackState{}
	}
	since := p.now
	if p.prior.candidate == cand.be.Pod && !p.prior.eligibleSince.IsZero() {
		since = p.prior.eligibleSince
	}
	st := failbackState{candidate: cand.be.Pod, eligibleSince: since}
	if p.now.Sub(since) >= p.stabilityWindow {
		c := cand
		return &c, st
	}
	return nil, st
}

func bestFailbackCandidate(
	obs []observation,
	leader observation,
	skip map[string]bool,
	warmthLagNs uint64,
) (observation, bool) {
	var best observation
	var found bool
	for _, o := range obs {
		if !o.reach || !o.be.Ready || o.status.IsLeader {
			continue
		}
		if o.be.Pod == leader.be.Pod {
			continue
		}
		if skip[o.be.Pod] {
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
