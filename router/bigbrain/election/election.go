package election

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"git.horse/vapronva/concave/router/bigbrain/backend"
	"git.horse/vapronva/concave/router/bigbrain/k8sclient"
	"git.horse/vapronva/concave/router/bigbrain/registry"
)

type Config struct {
	Interval               *time.Duration
	PromoteDebounce        *int
	FailbackEnabled        *bool
	FailbackStability      *time.Duration
	FailbackWarmthLagNs    *uint64
	UnreachableLeaderGrace *time.Duration
	EmptyDiscoveryDebounce *int
	ActuationTimeout       *time.Duration
}

type settings struct {
	interval               time.Duration
	promoteDebounce        int
	failbackEnabled        bool
	failbackStability      time.Duration
	failbackWarmthLagNs    uint64
	unreachableLeaderGrace time.Duration
	emptyDiscoveryDebounce int
	actuationTimeout       time.Duration
}

const (
	DefaultInterval                      = 2 * time.Second
	DefaultPromoteDebounce               = 3
	DefaultFailbackStability             = 15 * time.Second
	DefaultFailbackWarmthLagNs    uint64 = 5_000_000_000
	DefaultUnreachableLeaderGrace        = 60 * time.Second
	DefaultEmptyDiscoveryDebounce        = 5
	DefaultActuationTimeout              = 30 * time.Second
	discoverTimeout                      = 5 * time.Second
	pollTimeout                          = 2 * time.Second
)

func (c Config) withDefaults() settings {
	s := settings{
		interval:               DefaultInterval,
		promoteDebounce:        DefaultPromoteDebounce,
		failbackEnabled:        true,
		failbackStability:      DefaultFailbackStability,
		failbackWarmthLagNs:    DefaultFailbackWarmthLagNs,
		unreachableLeaderGrace: DefaultUnreachableLeaderGrace,
		emptyDiscoveryDebounce: DefaultEmptyDiscoveryDebounce,
		actuationTimeout:       DefaultActuationTimeout,
	}
	if c.Interval != nil && *c.Interval > 0 {
		s.interval = *c.Interval
	}
	if c.PromoteDebounce != nil && *c.PromoteDebounce >= 0 {
		s.promoteDebounce = *c.PromoteDebounce
	}
	if c.FailbackEnabled != nil {
		s.failbackEnabled = *c.FailbackEnabled
	}
	if c.FailbackStability != nil && *c.FailbackStability >= 0 {
		s.failbackStability = *c.FailbackStability
	}
	if c.FailbackWarmthLagNs != nil {
		s.failbackWarmthLagNs = *c.FailbackWarmthLagNs
	}
	if c.UnreachableLeaderGrace != nil && *c.UnreachableLeaderGrace >= 0 {
		s.unreachableLeaderGrace = *c.UnreachableLeaderGrace
	}
	if c.EmptyDiscoveryDebounce != nil && *c.EmptyDiscoveryDebounce >= 0 {
		s.emptyDiscoveryDebounce = *c.EmptyDiscoveryDebounce
	}
	if c.ActuationTimeout != nil && *c.ActuationTimeout > 0 {
		s.actuationTimeout = *c.ActuationTimeout
	}
	return s
}

type Controller struct {
	cfg     settings
	k8s     *k8sclient.Client
	backend *backend.Client
	reg     *registry.Registry
	log     *slog.Logger
	mu      sync.Mutex
	state   map[string]*deploymentState
	actWG   sync.WaitGroup
}

type deploymentState struct {
	leaderlessStreak          int
	emptyStreak               int
	incumbentPod              string
	incumbentUnreachableSince time.Time
	promoting                 bool
	demoting                  bool
	failback                  failbackState
}

func New(cfg Config, k8s *k8sclient.Client, b *backend.Client, reg *registry.Registry, log *slog.Logger) *Controller {
	if log == nil {
		log = slog.Default()
	}
	return &Controller{
		cfg:     cfg.withDefaults(),
		k8s:     k8s,
		backend: b,
		reg:     reg,
		log:     log,
		state:   make(map[string]*deploymentState),
	}
}

func (c *Controller) ActuationTimeout() time.Duration {
	return c.cfg.actuationTimeout
}

func (c *Controller) Run(ctx context.Context) {
	c.log.InfoContext(ctx, "election: controller starting",
		"deployments", len(c.reg.Names()), "interval", c.cfg.interval.String(),
		"promoteDebounce", c.cfg.promoteDebounce, "emptyDiscoveryDebounce", c.cfg.emptyDiscoveryDebounce,
		"failbackEnabled", c.cfg.failbackEnabled, "failbackStability", c.cfg.failbackStability.String(),
		"failbackWarmthLag", c.cfg.failbackWarmthLagNs,
		"unreachableLeaderGrace", c.cfg.unreachableLeaderGrace.String())
	var wg sync.WaitGroup
	for _, name := range c.reg.Names() {
		wg.Go(func() {
			c.runDeployment(ctx, name)
		})
	}
	wg.Wait()
	c.actWG.Wait()
}

func (c *Controller) runDeployment(ctx context.Context, name string) {
	c.log.InfoContext(ctx, "election: starting reconcile loop", "deployment", name)
	defer c.log.InfoContext(ctx, "election: stopping reconcile loop", "deployment", name)
	tick := time.NewTicker(c.cfg.interval)
	defer tick.Stop()
	c.reconcileSafe(ctx, name)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			c.reconcileSafe(ctx, name)
		}
	}
}

func (c *Controller) reconcileSafe(ctx context.Context, name string) {
	defer func() {
		if r := recover(); r != nil {
			c.log.ErrorContext(ctx, "election: reconcile panicked; retrying next tick",
				"deployment", name, "panic", r)
		}
	}()
	c.reconcile(ctx, name)
}

func (c *Controller) deploymentState(name string) *deploymentState {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.state[name]
	if st == nil {
		st = &deploymentState{}
		c.state[name] = st
	}
	return st
}

type observation struct {
	be     k8sclient.Backend
	status backend.Leadership
	reach  bool
}

type stateSnapshot struct {
	incumbentPod string
	failback     failbackState
}

func (c *Controller) snapshotState(st *deploymentState) stateSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return stateSnapshot{
		incumbentPod: st.incumbentPod,
		failback:     st.failback,
	}
}

func (c *Controller) reconcile(ctx context.Context, name string) {
	st := c.deploymentState(name)
	ns, ok := c.reg.Namespace(name)
	if !ok {
		return
	}
	dctx, cancel := context.WithTimeout(ctx, discoverTimeout)
	pods, err := c.k8s.DiscoverBackends(dctx, ns, name)
	cancel()
	if err != nil {
		c.log.WarnContext(ctx, "election: discovery failed", "deployment", name, "err", err)
		return
	}
	snap := c.snapshotState(st)
	obs := c.pollAll(ctx, name, pods)
	now := time.Now()
	dec := decide(obs, decideParams{
		incumbent: snap.incumbentPod,
		failback: failbackParams{
			enabled:         c.cfg.failbackEnabled,
			stabilityWindow: c.cfg.failbackStability,
			warmthLagNs:     c.cfg.failbackWarmthLagNs,
			now:             now,
			prior:           snap.failback,
		},
	})
	streak, emptyStreak, retain := c.commitState(st, dec, len(pods) == 0, now)
	if len(pods) == 0 {
		c.actEmptyDiscovery(ctx, name, ns, st, emptyStreak)
		return
	}
	c.act(ctx, name, st, dec, streak, retain)
}

func (c *Controller) commitState(st *deploymentState, dec decision, emptyList bool, now time.Time) (int, int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st.failback = dec.failbackState
	if emptyList {
		st.emptyStreak++
	} else {
		st.emptyStreak = 0
	}
	if dec.liveLeaderCount == 0 {
		if !emptyList && !dec.hasTransitioning {
			st.leaderlessStreak++
		}
	} else {
		st.leaderlessStreak = 0
	}
	retain := false
	if dec.incumbentUnreachable {
		if st.incumbentUnreachableSince.IsZero() {
			st.incumbentUnreachableSince = now
		}
		retain = now.Sub(st.incumbentUnreachableSince) < c.cfg.unreachableLeaderGrace
	} else if !emptyList {
		st.incumbentUnreachableSince = time.Time{}
	}
	return st.leaderlessStreak, st.emptyStreak, retain
}

func (c *Controller) pollAll(ctx context.Context, name string, pods []k8sclient.Backend) []observation {
	obs := make([]observation, len(pods))
	var wg sync.WaitGroup
	for i := range pods {
		wg.Go(func() {
			pctx, cancel := context.WithTimeout(ctx, pollTimeout)
			defer cancel()
			l, err := c.backend.Leadership(pctx, name, pods[i].URL)
			if err != nil {
				c.log.DebugContext(ctx, "election: leadership poll failed",
					"deployment", name, "pod", pods[i].Pod, "url", pods[i].URL, "err", err)
			}
			obs[i] = observation{be: pods[i], status: l, reach: err == nil}
		})
	}
	wg.Wait()
	return obs
}
