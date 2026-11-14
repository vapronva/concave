package election

import (
	"context"
	"net/http"
	"time"
)

func (c *Controller) act(
	ctx context.Context,
	name string,
	st *deploymentState,
	d decision,
	streak int,
	retain bool,
	now time.Time,
) {
	if d.liveLeaderCount == 0 {
		c.actLeaderless(ctx, name, st, d, streak, retain, now)
		return
	}
	if len(d.demotes) > 0 {
		c.log.WarnContext(ctx, "election: extra leader(s) claimed; demoting",
			"deployment", name, "leader", d.leaderPod, "claimedLeaders", d.liveLeaderCount, "demotes", len(d.demotes))
	}
	c.setLeader(name, st, d.leaderPod, d.leaderURL)
	c.demoteAll(ctx, name, d.demotes)
	if d.failbackTarget != nil {
		target := *d.failbackTarget
		st.failback.eligibleSince = now
		c.log.InfoContext(ctx, "election: failback: promoting higher-priority pod over incumbent",
			"deployment", name, "promoting", target.be.Pod, "over", d.leaderPod,
			"candidatePriority", target.be.Priority, "candidateLatestTs", target.status.LatestTS)
		c.promote(ctx, name, "failback promote", st, target)
	}
}

func (c *Controller) actEmptyDiscovery(ctx context.Context, name, ns string, st *deploymentState, streak int) {
	switch {
	case streak < c.cfg.EmptyDiscoveryDebounce:
		c.log.WarnContext(ctx, "election: discovery returned no backend pods; retaining last-known leader",
			"deployment", name, "namespace", ns, "streak", streak, "need", c.cfg.EmptyDiscoveryDebounce)
		return
	case streak == c.cfg.EmptyDiscoveryDebounce:
		c.log.WarnContext(ctx, "election: discovery persistently empty; publishing leaderless",
			"deployment", name, "namespace", ns, "ticks", streak)
	}
	c.setLeader(name, st, "", "")
}

func (c *Controller) actLeaderless(
	ctx context.Context,
	name string,
	st *deploymentState,
	d decision,
	streak int,
	retain bool,
	now time.Time,
) {
	if streak < c.cfg.PromoteDebounce {
		return
	}
	if retain {
		c.log.WarnContext(ctx, "election: incumbent unreachable but still discovered; retaining leader within grace",
			"deployment", name, "streak", streak, "grace", c.cfg.UnreachableLeaderGrace)
		return
	}
	if d.promoteTarget == nil {
		if !d.incumbentUnreachable {
			c.setLeader(name, st, "", "")
		}
		if d.hasTransitioning {
			c.log.InfoContext(ctx, "election: leaderless; a pod is mid-transition",
				"deployment", name, "streak", streak)
			return
		}
		c.log.WarnContext(ctx, "election: leaderless and no promotable standby",
			"deployment", name, "streak", streak, "keptUnreachableIncumbent", d.incumbentUnreachable)
		return
	}
	c.setLeader(name, st, "", "")
	target := *d.promoteTarget
	st.recordPromoteAttempt(target.be.Pod, now)
	c.log.InfoContext(ctx, "election: promoting preferred warm standby",
		"deployment", name, "pod", target.be.Pod,
		"priority", target.be.Priority, "latestTs", target.status.LatestTS)
	c.promote(ctx, name, "promote", st, target)
}

func (c *Controller) promote(ctx context.Context, name, kind string, st *deploymentState, target observation) {
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.ActuationTimeout)
	defer cancel()
	code, sent, err := c.backend.Promote(actx, name, target.be.URL)
	if err != nil {
		if sent {
			st.markTransitioning(target.be.Pod, time.Now())
		}
		c.log.ErrorContext(ctx, "election: "+kind+" failed", "deployment", name, "pod", target.be.Pod, "err", err)
		return
	}
	switch code {
	case http.StatusOK, http.StatusAccepted:
		st.markTransitioning(target.be.Pod, time.Now())
		c.log.InfoContext(ctx, "election: "+kind+" accepted",
			"deployment", name, "pod", target.be.Pod, "status", code)
	case http.StatusForbidden:
		c.log.ErrorContext(ctx, "election: "+kind+" forbidden; control-plane token mismatch",
			"deployment", name, "pod", target.be.Pod)
	default:
		c.log.WarnContext(ctx, "election: "+kind+" unexpected status",
			"deployment", name, "pod", target.be.Pod, "status", code)
	}
}

func (c *Controller) demoteAll(ctx context.Context, name string, targets []demoteTarget) {
	for _, a := range targets {
		code, err := c.demoteOne(ctx, name, a)
		if err != nil {
			c.log.ErrorContext(ctx, "election: demote failed", "deployment", name, "pod", a.pod, "err", err)
			continue
		}
		switch code {
		case http.StatusAccepted:
			c.log.InfoContext(ctx, "election: demote accepted", "deployment", name, "pod", a.pod, "status", code)
		case http.StatusConflict:
			c.log.InfoContext(ctx, "election: demote declined; backend is mid-transition or its lease changed",
				"deployment", name, "pod", a.pod, "status", code)
		case http.StatusForbidden:
			c.log.ErrorContext(ctx, "election: demote forbidden; control-plane token mismatch",
				"deployment", name, "pod", a.pod)
		default:
			c.log.WarnContext(ctx, "election: demote unexpected status",
				"deployment", name, "pod", a.pod, "status", code)
		}
	}
}

func (c *Controller) demoteOne(ctx context.Context, name string, target demoteTarget) (int, error) {
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.ActuationTimeout)
	defer cancel()
	return c.backend.Demote(actx, name, target.url, target.leaseTS)
}

func (c *Controller) setLeader(name string, st *deploymentState, pod, url string) {
	if st.incumbentPod != pod {
		if pod == "" {
			c.log.Warn("election: deployment is now leaderless", "deployment", name, "was", st.incumbentPod)
		} else {
			c.log.Info("election: leader set", "deployment", name, "pod", pod, "was", st.incumbentPod)
		}
		st.incumbentPod = pod
	}
	c.reg.Update(name, pod, url)
}
