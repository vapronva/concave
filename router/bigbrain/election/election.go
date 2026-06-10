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
	Interval               time.Duration
	PromoteDebounce        int
	FailbackEnabled        bool
	FailbackStability      time.Duration
	FailbackWarmthLagNs    uint64
	UnreachableLeaderGrace time.Duration
}

const (
	DefaultInterval                      = 2 * time.Second
	DefaultPromoteDebounce               = 3
	DefaultFailbackStability             = 15 * time.Second
	DefaultFailbackWarmthLagNs    uint64 = 5_000_000_000
	DefaultUnreachableLeaderGrace        = 60 * time.Second
	discoverTimeout                      = 5 * time.Second
	pollTimeout                          = 2 * time.Second
)

func (c Config) withDefaults() Config {
	if c.Interval <= 0 {
		c.Interval = DefaultInterval
	}
	if c.PromoteDebounce <= 0 {
		c.PromoteDebounce = DefaultPromoteDebounce
	}
	if c.FailbackStability <= 0 {
		c.FailbackStability = DefaultFailbackStability
	}
	if c.FailbackWarmthLagNs == 0 {
		c.FailbackWarmthLagNs = DefaultFailbackWarmthLagNs
	}
	if c.UnreachableLeaderGrace <= 0 {
		c.UnreachableLeaderGrace = DefaultUnreachableLeaderGrace
	}
	return c
}

type Controller struct {
	cfg     Config
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
	incumbentPod              string
	incumbentUnreachableSince time.Time
	inflight                  bool
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

func (c *Controller) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, name := range c.reg.Names() {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			c.runDeployment(ctx, name)
		}(name)
	}
	wg.Wait()
	c.actWG.Wait()
}

func (c *Controller) runDeployment(ctx context.Context, name string) {
	c.log.InfoContext(ctx, "election: starting reconcile loop", "deployment", name)
	defer c.log.InfoContext(ctx, "election: stopping reconcile loop", "deployment", name)
	tick := time.NewTicker(c.cfg.Interval)
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
	if len(pods) == 0 {
		c.log.WarnContext(ctx, "election: discovery returned no backend pods",
			"deployment", name, "namespace", ns)
	}
	snap := c.snapshotState(st)
	obs := c.pollAll(ctx, name, pods)
	dec := decide(obs, decideParams{
		incumbent: snap.incumbentPod,
		failback: failbackParams{
			enabled:         c.cfg.FailbackEnabled,
			stabilityWindow: c.cfg.FailbackStability,
			warmthLagNs:     c.cfg.FailbackWarmthLagNs,
			now:             time.Now(),
			prior:           snap.failback,
		},
	})
	streak, retain := c.commitState(st, dec, len(pods) == 0, time.Now())
	c.act(ctx, name, st, dec, streak, retain)
}

func (c *Controller) commitState(st *deploymentState, dec decision, emptyList bool, now time.Time) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	st.failback = dec.failbackState
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
		retain = now.Sub(st.incumbentUnreachableSince) < c.cfg.UnreachableLeaderGrace
	} else {
		st.incumbentUnreachableSince = time.Time{}
	}
	return st.leaderlessStreak, retain
}

func (c *Controller) pollAll(ctx context.Context, name string, pods []k8sclient.Backend) []observation {
	obs := make([]observation, len(pods))
	var wg sync.WaitGroup
	for i := range pods {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, pollTimeout)
			defer cancel()
			l, err := c.backend.Leadership(pctx, name, pods[i].URL)
			if err != nil {
				c.log.DebugContext(ctx, "election: leadership poll failed",
					"deployment", name, "pod", pods[i].Pod, "url", pods[i].URL, "err", err)
			}
			obs[i] = observation{be: pods[i], status: l, reach: err == nil}
		}(i)
	}
	wg.Wait()
	return obs
}
