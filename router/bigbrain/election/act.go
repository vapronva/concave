package election

import (
	"context"
	"net/http"
	"time"
)

const actuationTimeout = 30 * time.Second

func (c *Controller) act(
	ctx context.Context,
	name string,
	st *deploymentState,
	d decision,
	streak int,
) {
	if d.liveLeaderCount == 0 {
		c.actLeaderless(ctx, name, st, d, streak)
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
		actx, cancel := context.WithTimeout(base, actuationTimeout)
		defer cancel()
		code, err := c.backend.Promote(actx, name, target.be.URL)
		if err != nil {
			c.log.ErrorContext(base, "election: failback promote failed",
				"deployment", name, "pod", target.be.Pod, "err", err)
			return
		}
		switch code {
		case http.StatusOK, http.StatusAccepted:
			c.log.InfoContext(base, "election: failback promote accepted",
				"deployment", name, "pod", target.be.Pod, "status", code)
		case http.StatusForbidden:
			c.log.ErrorContext(base, "election: failback promote forbidden; control-plane token mismatch",
				"deployment", name, "pod", target.be.Pod)
		default:
			c.log.WarnContext(base, "election: failback promote unexpected status",
				"deployment", name, "pod", target.be.Pod, "status", code)
		}
	})
}

func (c *Controller) actLeaderless(
	ctx context.Context,
	name string,
	st *deploymentState,
	d decision,
	streak int,
) {
	if streak < c.cfg.PromoteDebounce {
		c.log.InfoContext(ctx, "election: leaderless, retaining last-known leader during hysteresis",
			"deployment", name, "streak", streak, "need", c.cfg.PromoteDebounce)
		return
	}
	c.setLeader(name, st, "", "")
	if d.promoteTarget == nil {
		c.log.WarnContext(ctx, "election: leaderless and no promotable standby",
			"deployment", name, "streak", streak)
		return
	}
	if !c.tryAcquire(st) {
		return
	}
	target := *d.promoteTarget
	base := context.WithoutCancel(ctx)
	c.actWG.Go(func() {
		defer c.release(st)
		c.log.InfoContext(
			base,
			"election: promoting preferred warm standby",
			"deployment",
			name,
			"pod",
			target.be.Pod,
			"priority",
			target.be.Priority,
			"latestTs",
			target.status.LatestTS,
		)
		actx, cancel := context.WithTimeout(base, actuationTimeout)
		defer cancel()
		code, err := c.backend.Promote(actx, name, target.be.URL)
		if err != nil {
			c.log.ErrorContext(base, "election: promote failed", "deployment", name, "pod", target.be.Pod, "err", err)
			return
		}
		switch code {
		case http.StatusOK, http.StatusAccepted:
			c.log.InfoContext(base, "election: promote accepted",
				"deployment", name, "pod", target.be.Pod, "status", code)
		case http.StatusForbidden:
			c.log.ErrorContext(base, "election: promote forbidden; control-plane token mismatch",
				"deployment", name, "pod", target.be.Pod)
		default:
			c.log.WarnContext(base, "election: promote unexpected status",
				"deployment", name, "pod", target.be.Pod, "status", code)
		}
	})
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
		for _, a := range actions {
			actx, cancel := context.WithTimeout(base, actuationTimeout)
			code, err := c.backend.Demote(actx, name, a.url)
			cancel()
			if err != nil {
				c.log.ErrorContext(base, "election: demote failed", "deployment", name, "pod", a.pod, "err", err)
				continue
			}
			switch code {
			case http.StatusOK, http.StatusAccepted, http.StatusConflict:
				c.log.InfoContext(base, "election: demote accepted", "deployment", name, "pod", a.pod, "status", code)
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
	if st.incumbentPod != pod {
		if pod == "" {
			c.log.Warn("election: deployment is now leaderless", "deployment", name, "was", st.incumbentPod)
		} else {
			c.log.Info("election: leader set", "deployment", name, "pod", pod, "was", st.incumbentPod)
		}
		st.incumbentPod = pod
	}
	c.mu.Unlock()
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
