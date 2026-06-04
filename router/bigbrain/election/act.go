package election

import (
	"context"
	"net/http"
	"time"

	"git.horse/vapronva/concave/router/bigbrain/registry"
)

const actuationTimeout = 30 * time.Second

func (c *Controller) act(
	ctx context.Context,
	name string,
	st *deploymentState,
	states []registry.BackendState,
	d decision,
	streak int,
) {
	if d.liveLeaderCount == 0 {
		c.actLeaderless(ctx, name, st, states, d, streak)
		return
	}
	if len(d.demotes) > 0 {
		c.log.WarnContext(ctx, "election: extra leader(s) claimed; demoting",
			"deployment", name, "leader", d.leaderPod, "claimedLeaders", d.liveLeaderCount, "demotes", len(d.demotes))
	}
	c.setLeader(name, st, states, d.leaderPod, d.leaderURL)
	c.runActions(ctx, name, st, d.demotes)
	if d.failbackTarget != nil && len(d.demotes) == 0 {
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
	go func() {
		defer c.release(st)
		c.log.InfoContext(ctx, "election: failback: promoting higher-priority pod over incumbent",
			"deployment", name, "promoting", target.be.Pod, "over", incumbent,
			"candidatePriority", target.be.Priority, "candidateLatestTs", target.status.LatestTS)
		actx, cancel := context.WithTimeout(base, actuationTimeout)
		defer cancel()
		code, err := c.backend.Promote(actx, target.be.URL)
		if err != nil {
			c.log.ErrorContext(ctx, "election: failback promote failed",
				"deployment", name, "pod", target.be.Pod, "err", err)
			return
		}
		switch code {
		case http.StatusOK, http.StatusAccepted, http.StatusConflict:
			c.log.InfoContext(ctx, "election: failback promote accepted",
				"deployment", name, "pod", target.be.Pod, "status", code)
		case http.StatusServiceUnavailable:
			c.log.WarnContext(ctx, "election: failback promote lease-contended (will retry)",
				"deployment", name, "pod", target.be.Pod)
		case http.StatusNotFound:
			c.markUnpromotable(st, target.be.Pod)
			c.log.WarnContext(ctx, "election: failback promote returned 404; no promote route, marking un-promotable",
				"deployment", name, "pod", target.be.Pod)
		default:
			c.log.WarnContext(ctx, "election: failback promote unexpected status",
				"deployment", name, "pod", target.be.Pod, "status", code)
		}
	}()
}

func (c *Controller) actLeaderless(
	ctx context.Context,
	name string,
	st *deploymentState,
	states []registry.BackendState,
	d decision,
	streak int,
) {
	if streak < c.cfg.PromoteDebounce {
		retained := c.retainLeader(name, st, states)
		c.log.InfoContext(ctx, "election: leaderless, retaining last-known leader (hysteresis)",
			"deployment", name, "streak", streak, "need", c.cfg.PromoteDebounce, "leader", retained)
		return
	}
	c.setLeader(name, st, states, "", "")
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
	go func() {
		defer c.release(st)
		c.log.InfoContext(ctx, "election: promoting warmest standby",
			"deployment", name, "pod", target.be.Pod, "latestTs", target.status.LatestTS)
		actx, cancel := context.WithTimeout(base, actuationTimeout)
		defer cancel()
		code, err := c.backend.Promote(actx, target.be.URL)
		if err != nil {
			c.log.ErrorContext(ctx, "election: promote failed", "deployment", name, "pod", target.be.Pod, "err", err)
			return
		}
		switch code {
		case http.StatusOK, http.StatusAccepted, http.StatusConflict:
			c.log.InfoContext(ctx, "election: promote accepted",
				"deployment", name, "pod", target.be.Pod, "status", code)
		case http.StatusServiceUnavailable:
			c.log.WarnContext(ctx, "election: promote lease-contended (will retry)",
				"deployment", name, "pod", target.be.Pod)
		case http.StatusNotFound:
			c.markUnpromotable(st, target.be.Pod)
			c.log.WarnContext(ctx, "election: promote returned 404; pod has no promote route, marking un-promotable",
				"deployment", name, "pod", target.be.Pod)
		default:
			c.log.WarnContext(ctx, "election: promote unexpected status",
				"deployment", name, "pod", target.be.Pod, "status", code)
		}
	}()
}

func (c *Controller) runActions(ctx context.Context, name string, st *deploymentState, actions []action) {
	if len(actions) == 0 {
		return
	}
	if !c.tryAcquire(st) {
		return
	}
	base := context.WithoutCancel(ctx)
	go func() {
		defer c.release(st)
		for _, a := range actions {
			actx, cancel := context.WithTimeout(base, actuationTimeout)
			code, err := c.backend.Demote(actx, a.url)
			cancel()
			if err != nil {
				c.log.ErrorContext(ctx, "election: demote failed", "deployment", name, "pod", a.pod, "err", err)
				continue
			}
			switch code {
			case http.StatusOK, http.StatusConflict:
				c.log.InfoContext(ctx, "election: demote accepted", "deployment", name, "pod", a.pod, "status", code)
			default:
				c.log.WarnContext(ctx, "election: demote unexpected status",
					"deployment", name, "pod", a.pod, "status", code)
			}
		}
	}()
}

func (c *Controller) setLeader(name string, st *deploymentState, states []registry.BackendState, pod, url string) {
	c.mu.Lock()
	if st.incumbentPod != pod {
		if pod == "" {
			c.log.Warn("election: deployment is now leaderless", "deployment", name, "was", st.incumbentPod)
		} else {
			c.log.Info("election: leader set", "deployment", name, "pod", pod, "was", st.incumbentPod)
		}
		st.incumbentPod = pod
	}
	st.incumbentURL = url
	c.mu.Unlock()
	c.reg.Update(name, states, pod, url)
}

func (c *Controller) retainLeader(name string, st *deploymentState, states []registry.BackendState) string {
	c.mu.Lock()
	pod, url := st.incumbentPod, st.incumbentURL
	c.mu.Unlock()
	c.reg.Update(name, states, pod, url)
	return pod
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

func (c *Controller) markUnpromotable(st *deploymentState, pod string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if st.unpromotable == nil {
		st.unpromotable = make(map[string]bool)
	}
	st.unpromotable[pod] = true
}
