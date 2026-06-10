package election

import (
	"context"
	"net/http"
)

func (c *Controller) act(
	ctx context.Context,
	name string,
	st *deploymentState,
	d decision,
	streak int,
	retain bool,
) {
	if d.liveLeaderCount == 0 {
		c.actLeaderless(ctx, name, st, d, streak, retain)
		return
	}
	if len(d.demotes) > 0 {
		c.log.WarnContext(ctx, "election: extra leader(s) claimed; demoting",
			"deployment", name, "leader", d.leaderPod, "claimedLeaders", d.liveLeaderCount, "demotes", len(d.demotes))
	}
	c.setLeader(name, st, d.leaderPod, d.leaderURL)
	c.runActions(ctx, name, st, d.demotes)
	if d.failbackTarget != nil {
		c.actFailback(ctx, name, st, d.leaderPod, *d.failbackTarget)
	}
}

func (c *Controller) actFailback(
	ctx context.Context,
	name string,
	st *deploymentState,
	incumbent string,
	target observation,
) {
	if !c.tryAcquire(st) {
		return
	}
	base := context.WithoutCancel(ctx)
	c.actWG.Go(func() {
		defer c.release(st)
		c.log.InfoContext(base, "election: failback: promoting higher-priority pod over incumbent",
			"deployment", name, "promoting", target.be.Pod, "over", incumbent,
			"candidatePriority", target.be.Priority, "candidateLatestTs", target.status.LatestTS)
		c.promoteAndLog(base, name, "failback promote", target)
	})
}

func (c *Controller) actEmptyDiscovery(ctx context.Context, name, ns string, st *deploymentState, streak int) {
	switch {
	case streak < c.cfg.emptyDiscoveryDebounce:
		c.log.WarnContext(ctx, "election: discovery returned no backend pods; retaining last-known leader",
			"deployment", name, "namespace", ns, "streak", streak, "need", c.cfg.emptyDiscoveryDebounce)
		return
	case streak == c.cfg.emptyDiscoveryDebounce:
		c.log.WarnContext(ctx, "election: discovery persistently empty; publishing leaderless",
			"deployment", name, "namespace", ns, "ticks", streak)
	default:
		c.log.DebugContext(ctx, "election: discovery still empty",
			"deployment", name, "namespace", ns, "streak", streak)
	}
	c.mu.Lock()
	st.leaderlessStreak = 0
	c.mu.Unlock()
	c.setLeader(name, st, "", "")
}

func (c *Controller) actLeaderless(
	ctx context.Context,
	name string,
	st *deploymentState,
	d decision,
	streak int,
	retain bool,
) {
	if streak < c.cfg.promoteDebounce {
		c.log.DebugContext(ctx, "election: leaderless, retaining last-known leader during hysteresis",
			"deployment", name, "streak", streak, "need", c.cfg.promoteDebounce)
		return
	}
	if retain {
		c.log.WarnContext(ctx, "election: incumbent unreachable but still discovered; retaining leader within grace",
			"deployment", name, "streak", streak, "grace", c.cfg.unreachableLeaderGrace)
		return
	}
	if d.promoteTarget == nil {
		c.setLeader(name, st, "", "")
		c.log.WarnContext(ctx, "election: leaderless and no promotable standby",
			"deployment", name, "streak", streak)
		return
	}
	if !c.tryAcquire(st) {
		return
	}
	c.setLeader(name, st, "", "")
	target := *d.promoteTarget
	base := context.WithoutCancel(ctx)
	c.actWG.Go(func() {
		defer c.release(st)
		c.log.InfoContext(base, "election: promoting preferred warm standby",
			"deployment", name, "pod", target.be.Pod,
			"priority", target.be.Priority, "latestTs", target.status.LatestTS)
		c.promoteAndLog(base, name, "promote", target)
	})
}

func (c *Controller) promoteAndLog(base context.Context, name, kind string, target observation) {
	actx, cancel := context.WithTimeout(base, c.cfg.actuationTimeout)
	defer cancel()
	code, err := c.backend.Promote(actx, name, target.be.URL)
	if err != nil {
		c.log.ErrorContext(base, "election: "+kind+" failed", "deployment", name, "pod", target.be.Pod, "err", err)
		return
	}
	switch code {
	case http.StatusOK, http.StatusAccepted:
		c.log.InfoContext(base, "election: "+kind+" accepted",
			"deployment", name, "pod", target.be.Pod, "status", code)
	case http.StatusConflict:
		c.log.InfoContext(base, "election: "+kind+" deferred; backend is mid-transition, will retry",
			"deployment", name, "pod", target.be.Pod, "status", code)
	case http.StatusForbidden:
		c.log.ErrorContext(base, "election: "+kind+" forbidden; control-plane token mismatch",
			"deployment", name, "pod", target.be.Pod)
	default:
		c.log.WarnContext(base, "election: "+kind+" unexpected status",
			"deployment", name, "pod", target.be.Pod, "status", code)
	}
}

func (c *Controller) runActions(ctx context.Context, name string, st *deploymentState, actions []action) {
	if len(actions) == 0 {
		return
	}
	if !c.tryAcquire(st) {
		return
	}
	base := context.WithoutCancel(ctx)
	c.actWG.Go(func() {
		defer c.release(st)
		actx, cancel := context.WithTimeout(base, c.cfg.actuationTimeout)
		defer cancel()
		for _, a := range actions {
			code, err := c.backend.Demote(actx, name, a.url)
			if err != nil {
				c.log.ErrorContext(base, "election: demote failed", "deployment", name, "pod", a.pod, "err", err)
				continue
			}
			switch code {
			case http.StatusOK, http.StatusAccepted:
				c.log.InfoContext(base, "election: demote accepted", "deployment", name, "pod", a.pod, "status", code)
			case http.StatusConflict:
				c.log.InfoContext(base, "election: demote deferred; backend is mid-transition, will retry",
					"deployment", name, "pod", a.pod, "status", code)
			case http.StatusInternalServerError:
				c.log.WarnContext(base, "election: demote self-fenced; backend is restarting as follower",
					"deployment", name, "pod", a.pod)
			case http.StatusForbidden:
				c.log.ErrorContext(base, "election: demote forbidden; control-plane token mismatch",
					"deployment", name, "pod", a.pod)
			default:
				c.log.WarnContext(base, "election: demote unexpected status",
					"deployment", name, "pod", a.pod, "status", code)
			}
		}
	})
}

func (c *Controller) setLeader(name string, st *deploymentState, pod, url string) {
	c.mu.Lock()
	defer c.mu.Unlock()
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

func (c *Controller) tryAcquire(st *deploymentState) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if st.inflight {
		return false
	}
	st.inflight = true
	return true
}

func (c *Controller) release(st *deploymentState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st.inflight = false
}
