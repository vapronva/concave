package election

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"syscall"
	"time"

	"git.horse/vapronva/concave/router/bigbrain/backend"
	"git.horse/vapronva/concave/router/bigbrain/k8sclient"
	"git.horse/vapronva/concave/router/bigbrain/registry"
)

type Config struct {
	Interval               time.Duration
	PromoteDebounce        int
	FailbackEnabled        bool
	FailbackStability      time.Duration
	FailbackWarmthLagNs    uint64
	UnreachableLeaderGrace time.Duration
	LeaseUnverifiedGrace   time.Duration
	EmptyDiscoveryDebounce int
	ActuationTimeout       time.Duration
}

const (
	DefaultInterval                   = 2 * time.Second
	DefaultPromoteDebounce            = 3
	DefaultFailbackStability          = 15 * time.Second
	DefaultFailbackWarmthLagNs uint64 = 5_000_000_000
	DefaultActuationTimeout           = 30 * time.Second
	unreachableLeaderGrace            = 60 * time.Second
	leaseUnverifiedGrace              = 60 * time.Second
	emptyDiscoveryDebounce            = 5
	discoverTimeout                   = 5 * time.Second
	pollTimeout                       = 2 * time.Second
)

func DefaultConfig() Config {
	return Config{
		Interval:               DefaultInterval,
		PromoteDebounce:        DefaultPromoteDebounce,
		FailbackEnabled:        true,
		FailbackStability:      DefaultFailbackStability,
		FailbackWarmthLagNs:    DefaultFailbackWarmthLagNs,
		UnreachableLeaderGrace: unreachableLeaderGrace,
		LeaseUnverifiedGrace:   leaseUnverifiedGrace,
		EmptyDiscoveryDebounce: emptyDiscoveryDebounce,
		ActuationTimeout:       DefaultActuationTimeout,
	}
}

type Controller struct {
	cfg     Config
	k8s     *k8sclient.Client
	backend *backend.Client
	reg     *registry.Registry
	log     *slog.Logger
}

type deploymentState struct {
	leaderlessStreak          int
	emptyStreak               int
	incumbentPod              string
	incumbentUnreachableSince time.Time
	incumbentLiveSince        time.Time
	discoveryDown             bool
	failback                  failbackState
	transitioningSeen         map[string]time.Time
	promoteAttempts           map[string]time.Time
	pollFailing               map[string]struct{}
}

func New(cfg Config, k8s *k8sclient.Client, b *backend.Client, reg *registry.Registry, log *slog.Logger) *Controller {
	if log == nil {
		log = slog.Default()
	}
	return &Controller{cfg: cfg, k8s: k8s, backend: b, reg: reg, log: log}
}

func (c *Controller) Run(ctx context.Context) {
	c.log.InfoContext(ctx, "election: controller starting",
		"deployments", len(c.reg.Names()), "interval", c.cfg.Interval.String(),
		"promoteDebounce", c.cfg.PromoteDebounce, "emptyDiscoveryDebounce", c.cfg.EmptyDiscoveryDebounce,
		"failbackEnabled", c.cfg.FailbackEnabled, "failbackStability", c.cfg.FailbackStability.String(),
		"failbackWarmthLag", c.cfg.FailbackWarmthLagNs,
		"unreachableLeaderGrace", c.cfg.UnreachableLeaderGrace.String(),
		"leaseUnverifiedGrace", c.cfg.LeaseUnverifiedGrace.String())
	var wg sync.WaitGroup
	for _, name := range c.reg.Names() {
		wg.Go(func() {
			c.runDeployment(ctx, name)
		})
	}
	wg.Wait()
}

func (c *Controller) runDeployment(ctx context.Context, name string) {
	c.log.InfoContext(ctx, "election: starting reconcile loop", "deployment", name)
	defer c.log.InfoContext(ctx, "election: stopping reconcile loop", "deployment", name)
	st := &deploymentState{}
	tick := time.NewTicker(c.cfg.Interval)
	defer tick.Stop()
	c.reconcileSafe(ctx, name, st)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			c.reconcileSafe(ctx, name, st)
		}
	}
}

func (c *Controller) reconcileSafe(ctx context.Context, name string, st *deploymentState) {
	defer func() {
		if r := recover(); r != nil {
			c.log.ErrorContext(ctx, "election: reconcile panicked; retrying next tick",
				"deployment", name, "panic", r)
		}
	}()
	c.reconcile(ctx, name, st)
}

type observation struct {
	be               k8sclient.Backend
	status           backend.Leadership
	reach            bool
	refused          bool
	err              error
	wasTransitioning bool
}

func (c *Controller) reconcile(ctx context.Context, name string, st *deploymentState) {
	ns, ok := c.reg.Namespace(name)
	if !ok {
		return
	}
	dctx, cancel := context.WithTimeout(ctx, discoverTimeout)
	pods, err := c.k8s.DiscoverBackends(dctx, ns, name)
	cancel()
	if err != nil {
		if st.setDiscoveryDown(true) {
			c.log.WarnContext(ctx, "election: discovery failed", "deployment", name, "err", err)
		}
		return
	}
	if st.setDiscoveryDown(false) {
		c.log.InfoContext(ctx, "election: discovery recovered", "deployment", name)
	}
	obs := c.pollAll(ctx, name, pods)
	now := time.Now()
	c.logPollTransitions(ctx, name, st, obs)
	c.rememberTransitions(st, obs, now)
	dec := decide(obs, decideParams{
		incumbent:            st.incumbentPod,
		published:            c.reg.Published(name),
		leaseUnverifiedGrace: c.cfg.LeaseUnverifiedGrace,
		attempted:            st.promoteAttempts,
		failback: failbackParams{
			enabled:         c.cfg.FailbackEnabled,
			stabilityWindow: c.cfg.FailbackStability,
			warmthLagNs:     c.cfg.FailbackWarmthLagNs,
			now:             now,
			prior:           st.failback,
		},
	})
	streak, emptyStreak, retain := c.commitState(st, dec, len(pods) == 0, now)
	if len(pods) == 0 {
		c.actEmptyDiscovery(ctx, name, ns, st, emptyStreak)
		return
	}
	c.act(ctx, name, st, dec, streak, retain, now)
}

func (st *deploymentState) setDiscoveryDown(down bool) bool {
	if down {
		st.failback = failbackState{}
		st.incumbentLiveSince = time.Time{}
		st.emptyStreak = 0
	}
	if st.discoveryDown == down {
		return false
	}
	st.discoveryDown = down
	return true
}

func (c *Controller) logPollTransitions(ctx context.Context, name string, st *deploymentState, obs []observation) {
	failing := make(map[string]struct{})
	for _, o := range obs {
		_, was := st.pollFailing[o.be.Pod]
		switch {
		case !o.reach:
			failing[o.be.Pod] = struct{}{}
			if !was {
				c.log.WarnContext(ctx, "election: leadership poll failed",
					"deployment", name, "pod", o.be.Pod, "url", o.be.URL, "refused", o.refused, "err", o.err)
			}
		case was:
			c.log.InfoContext(ctx, "election: leadership poll recovered", "deployment", name, "pod", o.be.Pod)
		}
	}
	st.pollFailing = failing
}

func (c *Controller) rememberTransitions(st *deploymentState, obs []observation, now time.Time) {
	for pod, seen := range st.transitioningSeen {
		if now.Sub(seen) >= c.cfg.UnreachableLeaderGrace {
			delete(st.transitioningSeen, pod)
		}
	}
	for i := range obs {
		o := &obs[i]
		switch {
		case o.refused:
			delete(st.transitioningSeen, o.be.Pod)
		case !o.reach:
			_, o.wasTransitioning = st.transitioningSeen[o.be.Pod]
		case isTransitioning(*o):
			st.markTransitioning(o.be.Pod, now)
		default:
			delete(st.transitioningSeen, o.be.Pod)
		}
	}
}

func (st *deploymentState) markTransitioning(pod string, at time.Time) {
	if st.transitioningSeen == nil {
		st.transitioningSeen = make(map[string]time.Time)
	}
	st.transitioningSeen[pod] = at
}

func (st *deploymentState) recordPromoteAttempt(pod string, at time.Time) {
	if st.promoteAttempts == nil {
		st.promoteAttempts = make(map[string]time.Time)
	}
	st.promoteAttempts[pod] = at
}

func (c *Controller) commitState(st *deploymentState, dec decision, emptyList bool, now time.Time) (int, int, bool) {
	st.failback = dec.failbackState
	if emptyList {
		st.emptyStreak++
		if st.emptyStreak >= c.cfg.EmptyDiscoveryDebounce {
			st.leaderlessStreak = 0
		}
	} else {
		st.emptyStreak = 0
		switch {
		case dec.liveLeaderCount > 0:
			st.leaderlessStreak = 0
			st.promoteAttempts = nil
		case !dec.hasTransitioning:
			st.leaderlessStreak++
		}
	}
	retain := false
	switch {
	case dec.incumbentUnreachable:
		st.incumbentLiveSince = time.Time{}
		if st.incumbentUnreachableSince.IsZero() {
			st.incumbentUnreachableSince = now
		}
		retain = now.Sub(st.incumbentUnreachableSince) < c.cfg.UnreachableLeaderGrace
	case emptyList:
		st.incumbentLiveSince = time.Time{}
	case dec.liveLeaderCount > 0 && dec.leaderPod == st.incumbentPod:
		if st.incumbentLiveSince.IsZero() {
			st.incumbentLiveSince = now
		}
		if now.Sub(st.incumbentLiveSince) >= c.cfg.UnreachableLeaderGrace {
			st.incumbentUnreachableSince = time.Time{}
		}
	default:
		st.incumbentUnreachableSince, st.incumbentLiveSince = time.Time{}, time.Time{}
	}
	return st.leaderlessStreak, st.emptyStreak, retain
}

func (c *Controller) pollAll(ctx context.Context, name string, pods []k8sclient.Backend) []observation {
	obs := make([]observation, len(pods))
	var wg sync.WaitGroup
	for i := range pods {
		wg.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					c.log.ErrorContext(ctx, "election: leadership poll panicked",
						"deployment", name, "pod", pods[i].Pod, "panic", r)
					obs[i] = observation{be: pods[i]}
				}
			}()
			pctx, cancel := context.WithTimeout(ctx, pollTimeout)
			defer cancel()
			l, err := c.backend.Leadership(pctx, pods[i].URL)
			obs[i] = observation{
				be:      pods[i],
				status:  l,
				reach:   err == nil,
				refused: errors.Is(err, syscall.ECONNREFUSED),
				err:     err,
			}
		})
	}
	wg.Wait()
	return obs
}
