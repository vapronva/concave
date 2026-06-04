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
	Interval            time.Duration
	PromoteDebounce     int
	FailbackEnabled     bool
	FailbackStability   time.Duration
	FailbackWarmthLagNs uint64
}

const (
	defaultInterval                   = 2 * time.Second
	defaultPromoteDebounce            = 3
	discoverTimeout                   = 5 * time.Second
	pollTimeout                       = 2 * time.Second
	defaultFailbackStability          = 15 * time.Second
	defaultFailbackWarmthLagNs uint64 = 5_000_000_000
)

func (c Config) withDefaults() Config {
	if c.Interval <= 0 {
		c.Interval = defaultInterval
	}
	if c.PromoteDebounce <= 0 {
		c.PromoteDebounce = defaultPromoteDebounce
	}
	if c.FailbackStability <= 0 {
		c.FailbackStability = defaultFailbackStability
	}
	if c.FailbackWarmthLagNs == 0 {
		c.FailbackWarmthLagNs = defaultFailbackWarmthLagNs
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
}

type deploymentState struct {
	leaderlessStreak int
	incumbentPod     string
	incumbentURL     string
	inflight         bool
	unpromotable     map[string]bool
	failback         failbackState
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
}

func (c *Controller) runDeployment(ctx context.Context, name string) {
	c.log.InfoContext(ctx, "election: starting reconcile loop", "deployment", name)
	defer c.log.InfoContext(ctx, "election: stopping reconcile loop", "deployment", name)
	tick := time.NewTicker(c.cfg.Interval)
	defer tick.Stop()
	c.reconcile(ctx, name)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			c.reconcile(ctx, name)
		}
	}
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
	incumbentPod     string
	incumbentURL     string
	leaderlessStreak int
	unpromotable     map[string]bool
	failback         failbackState
}

func (c *Controller) snapshotState(st *deploymentState) stateSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	skip := make(map[string]bool, len(st.unpromotable))
	for pod := range st.unpromotable {
		skip[pod] = true
	}
	return stateSnapshot{
		incumbentPod:     st.incumbentPod,
		incumbentURL:     st.incumbentURL,
		leaderlessStreak: st.leaderlessStreak,
		unpromotable:     skip,
		failback:         st.failback,
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
	obs := c.pollAll(ctx, pods)
	states := backendStates(obs)
	dec := decide(obs, decideParams{
		incumbent:   snap.incumbentPod,
		skipPromote: snap.unpromotable,
		failback: failbackParams{
			enabled:         c.cfg.FailbackEnabled,
			stabilityWindow: c.cfg.FailbackStability,
			warmthLagNs:     c.cfg.FailbackWarmthLagNs,
			now:             time.Now(),
			prior:           snap.failback,
		},
	})
	streak := c.commitState(st, snap, dec, len(pods) == 0)
	c.act(ctx, name, st, states, dec, streak)
}

func (c *Controller) commitState(st *deploymentState, snap stateSnapshot, dec decision, emptyList bool) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	st.failback = dec.failbackState
	if dec.liveLeaderCount == 0 {
		if !emptyList {
			st.leaderlessStreak = snap.leaderlessStreak + 1
		}
	} else {
		st.leaderlessStreak = 0
		st.unpromotable = nil
	}
	return st.leaderlessStreak
}

func backendStates(obs []observation) []registry.BackendState {
	states := make([]registry.BackendState, 0, len(obs))
	now := time.Now()
	for _, o := range obs {
		states = append(states, registry.BackendStateFrom(o.be, o.status, o.reach, now))
	}
	return states
}

func (c *Controller) pollAll(ctx context.Context, pods []k8sclient.Backend) []observation {
	obs := make([]observation, len(pods))
	var wg sync.WaitGroup
	for i := range pods {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, pollTimeout)
			defer cancel()
			l, err := c.backend.Leadership(pctx, pods[i].URL)
			obs[i] = observation{be: pods[i], status: l, reach: err == nil}
		}(i)
	}
	wg.Wait()
	return obs
}
