package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"git.horse/vapronva/concave/router/bigbrain/apiserver"
	"git.horse/vapronva/concave/router/bigbrain/backend"
	"git.horse/vapronva/concave/router/bigbrain/election"
	"git.horse/vapronva/concave/router/bigbrain/insights"
	"git.horse/vapronva/concave/router/bigbrain/k8sclient"
	"git.horse/vapronva/concave/router/bigbrain/registry"
	"git.horse/vapronva/concave/router/bigbrain/server"
)

const (
	defaultAddr            = ":8081"
	defaultInsightsRingCap = 50_000
	readHeaderTimeout      = 10 * time.Second
	readTimeout            = 15 * time.Second
	idleTimeout            = 60 * time.Second
	maxHeaderBytes         = 64 * 1024
	shutdownTimeout        = 10 * time.Second
	actuationDrainSlack    = 5 * time.Second

	defaultMetricsAPIServerPort    = 6443
	defaultMetricsAPIServerCertDir = "/var/run/bigbrain-apiserver"
	defaultMetricsScrapeInterval   = 5 * time.Second
)

func main() {
	os.Exit(run())
}

func run() int {
	addr := flag.String("addr", env("BIGBRAIN_ADDR", defaultAddr), "HTTP listen address")
	kubeconfig := flag.String("kubeconfig", env("KUBECONFIG", ""), "path to kubeconfig (empty is in-cluster)")
	labelPrefix := flag.String(
		"label-prefix",
		env("BIGBRAIN_LABEL_PREFIX", k8sclient.DefaultLabelPrefix),
		"k8s label-key prefix for discovery (<prefix>/instance, <prefix>/role, <prefix>/component, <prefix>/leader-priority)",
	)
	bootstrap := flag.String(
		"deployments",
		env("BIGBRAIN_DEPLOYMENTS", ""),
		"comma-separated name=namespace pairs to register at boot (e.g., convex-dev=convex-dev,convex-prod=convex-prod)",
	)
	electionCfg := electionFlags(flag.CommandLine)
	ringCapDefault, _ := envInt("INSIGHTS_RING_CAP", defaultInsightsRingCap)
	insightsRingCap := flag.Int("insights-ring-cap", ringCapDefault,
		"in-memory insights ring-buffer capacity (rows retained per deployment)")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)
	reg, controlPlaneTokens, usageTokens, err := buildRegistry(*bootstrap, log)
	if err != nil {
		return 1
	}
	k8s, err := k8sclient.New(*kubeconfig, *labelPrefix)
	if err != nil {
		log.Error("bigbrain: kubernetes client unavailable", "err", err)
		return 1
	}
	cfg := electionCfg()
	if err = validateRuntimeConfig(cfg, *insightsRingCap, log); err != nil {
		return 1
	}
	ctrl := election.New(cfg, k8s, backend.New(controlPlaneTokens), reg, log)
	actuation := ctrl.ActuationTimeout()
	ins := insights.New(*insightsRingCap)
	srv := server.New(reg, ins, usageTokens, log)
	if len(usageTokens) == 0 {
		log.Warn("bigbrain: insights disabled because no deployment usage tokens are configured")
	} else {
		log.Info("bigbrain: insights enabled", "deployments", len(usageTokens), "ringCap", *insightsRingCap)
	}
	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctrlDone := runController(ctx, ctrl)
	metricsDone := startMetricsAPIServer(ctx, stop, k8s, reg, *labelPrefix, log)
	var listenFailed atomic.Bool
	go func() {
		log.Info("bigbrain: listening", "addr", *addr)
		if serr := httpSrv.ListenAndServe(); serr != nil && !errors.Is(serr, http.ErrServerClosed) {
			log.Error("bigbrain: http server failed", "err", serr)
			listenFailed.Store(true)
			stop()
		}
	}()
	<-ctx.Done()
	shutdown(httpSrv, srv, ctrlDone, metricsDone, actuation+actuationDrainSlack, log)
	if listenFailed.Load() {
		return 1
	}
	return 0
}

func electionFlags(fs *flag.FlagSet) func() election.Config {
	intervalDefault, intervalSet := envDuration("BIGBRAIN_INTERVAL", election.DefaultInterval)
	interval := fs.Duration("interval", intervalDefault, "reconcile interval")
	debounceDefault, debounceSet := envInt("BIGBRAIN_PROMOTE_DEBOUNCE", election.DefaultPromoteDebounce)
	debounce := fs.Int("promote-debounce", debounceDefault, "leaderless polls before promoting")
	emptyDefault, emptySet := envInt("BIGBRAIN_EMPTY_DISCOVERY_DEBOUNCE", election.DefaultEmptyDiscoveryDebounce)
	emptyDebounce := fs.Int("empty-discovery-debounce", emptyDefault,
		"consecutive empty discovery results before publishing leaderless")
	fbEnabledDefault, fbEnabledSet := envBool("BIGBRAIN_FAILBACK_ENABLED", true)
	failbackEnabled := fs.Bool("failback-enabled", fbEnabledDefault,
		"fail back to a recovered higher-priority pod (the primary) once it is warm and stable")
	fbStabilityDefault, fbStabilitySet := envDuration("BIGBRAIN_FAILBACK_STABILITY", election.DefaultFailbackStability)
	failbackStability := fs.Duration("failback-stability", fbStabilityDefault,
		"how long the higher-priority pod must stay warm and stable before failback")
	fbWarmthDefault, fbWarmthSet := envUint64("BIGBRAIN_FAILBACK_WARMTH_LAG", election.DefaultFailbackWarmthLagNs)
	failbackWarmthLag := fs.Uint64("failback-warmth-lag", fbWarmthDefault,
		"max latest_ts lag for the candidate to count as warm/caught-up")
	graceDefault, graceSet := envDuration("BIGBRAIN_UNREACHABLE_LEADER_GRACE", election.DefaultUnreachableLeaderGrace)
	unreachableGrace := fs.Duration("unreachable-leader-grace", graceDefault,
		"how long an unreachable incumbent keeps its lease before being treated as gone")
	leaseGraceDefault, leaseGraceSet := envDuration(
		"BIGBRAIN_LEASE_UNVERIFIED_GRACE", election.DefaultLeaseUnverifiedGrace,
	)
	leaseUnverifiedGrace := fs.Duration("lease-unverified-grace", leaseGraceDefault,
		"how long a reachable leader may fail to verify its own lease before losing its claim (0 disables)")
	actuationDefault, actuationSet := envDuration("BIGBRAIN_ACTUATION_TIMEOUT", election.DefaultActuationTimeout)
	actuationTimeout := fs.Duration("actuation-timeout", actuationDefault,
		"budget for one promote/demote actuation batch per deployment")
	return func() election.Config {
		passed := make(map[string]bool)
		fs.Visit(func(f *flag.Flag) { passed[f.Name] = true })
		return election.Config{
			Interval:               knob(intervalSet, "interval", passed, interval),
			PromoteDebounce:        knob(debounceSet, "promote-debounce", passed, debounce),
			EmptyDiscoveryDebounce: knob(emptySet, "empty-discovery-debounce", passed, emptyDebounce),
			FailbackEnabled:        knob(fbEnabledSet, "failback-enabled", passed, failbackEnabled),
			FailbackStability:      knob(fbStabilitySet, "failback-stability", passed, failbackStability),
			FailbackWarmthLagNs:    knob(fbWarmthSet, "failback-warmth-lag", passed, failbackWarmthLag),
			UnreachableLeaderGrace: knob(graceSet, "unreachable-leader-grace", passed, unreachableGrace),
			LeaseUnverifiedGrace:   knob(leaseGraceSet, "lease-unverified-grace", passed, leaseUnverifiedGrace),
			ActuationTimeout:       knob(actuationSet, "actuation-timeout", passed, actuationTimeout),
		}
	}
}

func knob[T any](envSet bool, name string, passed map[string]bool, v *T) *T {
	if envSet || passed[name] {
		return v
	}
	return nil
}

func buildRegistry(
	bootstrap string,
	log *slog.Logger,
) (*registry.Registry, map[string]string, map[string]string, error) {
	reg := registry.New()
	deployments, err := parseDeployments(bootstrap)
	if err != nil {
		log.Error("bigbrain: invalid --deployments/BIGBRAIN_DEPLOYMENTS", "err", err)
		return nil, nil, nil, err
	}
	controlPlaneTokens, usageTokens, err := loadDeploymentTokens(reg, deployments, log)
	if err != nil {
		log.Error("bigbrain: missing deployment control-plane token", "err", err)
		return nil, nil, nil, err
	}
	if len(reg.Names()) == 0 {
		log.Error("bigbrain: refusing to start without registered deployments")
		return nil, nil, nil, errors.New("no registered deployments")
	}
	return reg, controlPlaneTokens, usageTokens, nil
}

func validateRuntimeConfig(cfg election.Config, insightsRingCap int, log *slog.Logger) error {
	if insightsRingCap <= 0 {
		err := fmt.Errorf("insights ring cap must be > 0, got %d", insightsRingCap)
		log.Error("bigbrain: invalid --insights-ring-cap/INSIGHTS_RING_CAP", "err", err)
		return err
	}
	if err := cfg.Validate(); err != nil {
		log.Error("bigbrain: invalid election config", "err", err)
		return err
	}
	return nil
}

func shutdown(
	httpSrv *http.Server,
	srv *server.Server,
	ctrlDone, metricsDone <-chan struct{},
	actuationDrain time.Duration,
	log *slog.Logger,
) {
	log.Info("bigbrain: shutting down")
	srv.Shutdown()
	shutCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if serr := httpSrv.Shutdown(shutCtx); serr != nil {
		log.Error("bigbrain: http shutdown failed", "err", serr)
	}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), actuationDrain)
	defer drainCancel()
	var wg sync.WaitGroup
	wg.Go(func() {
		select {
		case <-ctrlDone:
		case <-drainCtx.Done():
			log.Error("bigbrain: election shutdown timed out", "err", drainCtx.Err())
		}
	})
	if metricsDone != nil {
		wg.Go(func() {
			select {
			case <-metricsDone:
			case <-drainCtx.Done():
			}
		})
	}
	wg.Wait()
}

func runController(ctx context.Context, ctrl *election.Controller) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctrl.Run(ctx)
	}()
	return done
}

func startMetricsAPIServer(
	ctx context.Context,
	stop context.CancelFunc,
	k8s *k8sclient.Client,
	reg *registry.Registry,
	labelPrefix string,
	log *slog.Logger,
) <-chan struct{} {
	if enabled, _ := envBool("BIGBRAIN_METRICS_APISERVER_ENABLED", false); !enabled {
		return nil
	}
	prov := apiserver.NewProvider()
	seen := make(map[string]struct{}, len(reg.Names()))
	namespaces := make([]string, 0, len(reg.Names()))
	for _, name := range reg.Names() {
		ns, ok := reg.Namespace(name)
		if !ok {
			continue
		}
		if _, dup := seen[ns]; dup {
			continue
		}
		seen[ns] = struct{}{}
		namespaces = append(namespaces, ns)
	}
	scrapeInterval, _ := envDuration("BIGBRAIN_METRICS_SCRAPE_INTERVAL", defaultMetricsScrapeInterval)
	if scrapeInterval <= 0 {
		badEnv("BIGBRAIN_METRICS_SCRAPE_INTERVAL", scrapeInterval.String(), errors.New("must be > 0"))
	}
	securePort, _ := envInt("BIGBRAIN_METRICS_APISERVER_PORT", defaultMetricsAPIServerPort)
	if securePort < 1 || securePort > 65535 {
		badEnv("BIGBRAIN_METRICS_APISERVER_PORT", strconv.Itoa(securePort), errors.New("must be in 1..65535"))
	}
	cfg := apiserver.Config{
		SecurePort: securePort,
		CertDir:    env("BIGBRAIN_METRICS_APISERVER_CERT_DIR", defaultMetricsAPIServerCertDir),
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		apiserver.NewScraper(k8s.Clientset(), labelPrefix, namespaces, prov, scrapeInterval, log).Run(ctx)
	})
	wg.Go(func() {
		if aerr := apiserver.Run(ctx, cfg, prov, log); aerr != nil {
			log.ErrorContext(ctx, "bigbrain: custom-metrics apiserver exited", "err", aerr)
			stop()
		}
	})
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	log.InfoContext(ctx, "bigbrain: custom-metrics apiserver enabled",
		"port", cfg.SecurePort, "scrapeInterval", scrapeInterval.String())
	return done
}

func loadDeploymentTokens(
	reg *registry.Registry,
	deployments []deploymentRef,
	log *slog.Logger,
) (map[string]string, map[string]string, error) {
	controlPlaneTokens := make(map[string]string, len(deployments))
	usageTokens := make(map[string]string, len(deployments))
	for i, d := range deployments {
		reg.EnsureDeployment(d.name, d.namespace)
		token := env(fmt.Sprintf("BIGBRAIN_CONTROL_PLANE_TOKEN_%d", i), "")
		if token == "" {
			return nil, nil, fmt.Errorf("deployment %s (index %d) has no BIGBRAIN_CONTROL_PLANE_TOKEN_%d", d.name, i, i)
		}
		controlPlaneTokens[d.name] = token
		if usage := env(fmt.Sprintf("BIGBRAIN_USAGE_TOKEN_%d", i), ""); usage != "" {
			usageTokens[d.name] = usage
		}
		log.Info("bigbrain: registered bootstrap deployment", "name", d.name, "namespace", d.namespace)
	}
	return controlPlaneTokens, usageTokens, nil
}

type deploymentRef struct{ name, namespace string }

func parseDeployments(s string) ([]deploymentRef, error) {
	var out []deploymentRef
	seen := make(map[string]struct{})
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, ns, ok := strings.Cut(part, "=")
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, fmt.Errorf("deployment entry %q has an empty name", part)
		}
		ns = strings.TrimSpace(ns)
		if !ok || ns == "" {
			ns = name
		}
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("duplicate deployment name %q", name)
		}
		seen[name] = struct{}{}
		out = append(out, deploymentRef{name: name, namespace: ns})
	}
	return out, nil
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envInt(k string, d int) (int, bool) {
	v := os.Getenv(k)
	if v == "" {
		return d, false
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		badEnv(k, v, err)
	}
	return n, true
}

func envDuration(k string, d time.Duration) (time.Duration, bool) {
	v := os.Getenv(k)
	if v == "" {
		return d, false
	}
	dur, err := time.ParseDuration(v)
	if err != nil {
		badEnv(k, v, err)
	}
	return dur, true
}

func envUint64(k string, d uint64) (uint64, bool) {
	v := os.Getenv(k)
	if v == "" {
		return d, false
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		badEnv(k, v, err)
	}
	return n, true
}

func envBool(k string, d bool) (bool, bool) {
	v := os.Getenv(k)
	if v == "" {
		return d, false
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		badEnv(k, v, err)
	}
	return b, true
}

func badEnv(k, v string, err error) {
	fmt.Fprintf(os.Stderr, "bigbrain: invalid %s=%q: %v\n", k, v, err)
	os.Exit(1)
}
