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
	actuationDrainTimeout  = 15 * time.Second

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
	interval := flag.Duration(
		"interval",
		envDuration("BIGBRAIN_INTERVAL", election.DefaultInterval),
		"reconcile interval",
	)
	debounce := flag.Int(
		"promote-debounce",
		envInt("BIGBRAIN_PROMOTE_DEBOUNCE", election.DefaultPromoteDebounce),
		"leaderless polls before promoting",
	)
	failbackEnabled := flag.Bool("failback-enabled", envBool("BIGBRAIN_FAILBACK_ENABLED", true),
		"fail back to a recovered higher-priority pod (the primary) once it is warm and stable")
	failbackStability := flag.Duration("failback-stability",
		envDuration("BIGBRAIN_FAILBACK_STABILITY", election.DefaultFailbackStability),
		"how long the higher-priority pod must stay warm and ready before failback")
	failbackWarmthLag := flag.Uint64("failback-warmth-lag",
		envUint64("BIGBRAIN_FAILBACK_WARMTH_LAG", election.DefaultFailbackWarmthLagNs),
		"max latest_ts lag for the candidate to count as warm/caught-up")
	unreachableGrace := flag.Duration("unreachable-leader-grace",
		envDuration("BIGBRAIN_UNREACHABLE_LEADER_GRACE", election.DefaultUnreachableLeaderGrace),
		"how long an unreachable incumbent keeps its lease before being treated as gone")
	insightsRingCap := flag.Int("insights-ring-cap", envInt("INSIGHTS_RING_CAP", defaultInsightsRingCap),
		"in-memory insights ring-buffer capacity (rows retained per deployment)")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)
	reg := registry.New()
	deployments := parseDeployments(*bootstrap)
	controlPlaneTokens, usageTokens, err := loadDeploymentTokens(reg, deployments, log)
	if err != nil {
		log.Error("bigbrain: missing deployment control-plane token", "err", err)
		return 1
	}
	if len(reg.Names()) == 0 {
		log.Error("bigbrain: refusing to start without registered deployments")
		return 1
	}
	k8s, err := k8sclient.New(*kubeconfig, *labelPrefix)
	if err != nil {
		log.Error("bigbrain: kubernetes client unavailable", "err", err)
		return 1
	}
	ctrl := election.New(election.Config{
		Interval:               *interval,
		PromoteDebounce:        *debounce,
		FailbackEnabled:        *failbackEnabled,
		FailbackStability:      *failbackStability,
		FailbackWarmthLagNs:    *failbackWarmthLag,
		UnreachableLeaderGrace: *unreachableGrace,
	}, k8s, backend.New(controlPlaneTokens), reg, log)
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
	log.Info("bigbrain: election controller started",
		"deployments", len(reg.Names()), "interval", interval.String(), "promoteDebounce", *debounce,
		"failbackEnabled", *failbackEnabled, "failbackStability", failbackStability.String(),
		"failbackWarmthLag", *failbackWarmthLag, "unreachableLeaderGrace", unreachableGrace.String())
	startMetricsAPIServer(ctx, stop, k8s, reg, *labelPrefix, log)
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
	shutdown(httpSrv, srv, ctrlDone, log)
	if listenFailed.Load() {
		return 1
	}
	return 0
}

func shutdown(httpSrv *http.Server, srv *server.Server, ctrlDone <-chan struct{}, log *slog.Logger) {
	log.Info("bigbrain: shutting down")
	srv.Shutdown()
	shutCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if serr := httpSrv.Shutdown(shutCtx); serr != nil {
		log.Error("bigbrain: http shutdown failed", "err", serr)
	}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), actuationDrainTimeout)
	defer drainCancel()
	select {
	case <-ctrlDone:
	case <-drainCtx.Done():
		log.Error("bigbrain: election shutdown timed out", "err", drainCtx.Err())
	}
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
) {
	if !envBool("BIGBRAIN_METRICS_APISERVER_ENABLED", false) {
		return
	}
	prov := apiserver.NewProvider()
	seen := make(map[string]struct{}, len(reg.Names()))
	namespaces := make([]string, 0, len(reg.Names()))
	for _, name := range reg.Names() {
		if ns, ok := reg.Namespace(name); ok {
			if _, dup := seen[ns]; !dup {
				seen[ns] = struct{}{}
				namespaces = append(namespaces, ns)
			}
		}
	}
	scrapeInterval := envDuration("BIGBRAIN_METRICS_SCRAPE_INTERVAL", defaultMetricsScrapeInterval)
	go apiserver.NewScraper(k8s.Clientset(), labelPrefix, namespaces, prov, scrapeInterval, log).Run(ctx)
	cfg := apiserver.Config{
		SecurePort: envInt("BIGBRAIN_METRICS_APISERVER_PORT", defaultMetricsAPIServerPort),
		CertDir:    env("BIGBRAIN_METRICS_APISERVER_CERT_DIR", defaultMetricsAPIServerCertDir),
	}
	go func() {
		if aerr := apiserver.Run(ctx, cfg, prov, log); aerr != nil {
			log.ErrorContext(ctx, "bigbrain: custom-metrics apiserver exited", "err", aerr)
			stop()
		}
	}()
	log.InfoContext(ctx, "bigbrain: custom-metrics apiserver enabled",
		"port", cfg.SecurePort, "scrapeInterval", scrapeInterval.String())
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

func parseDeployments(s string) []deploymentRef {
	var out []deploymentRef
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, ns, ok := strings.Cut(part, "=")
		name = strings.TrimSpace(name)
		if !ok || strings.TrimSpace(ns) == "" {
			ns = name
		}
		out = append(out, deploymentRef{name: name, namespace: strings.TrimSpace(ns)})
	}
	return out
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envInt(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return d
}

func envDuration(k string, d time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if dur, err := time.ParseDuration(v); err == nil {
			return dur
		}
	}
	return d
}

func envUint64(k string, d uint64) uint64 {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			return n
		}
	}
	return d
}

func envBool(k string, d bool) bool {
	if v := os.Getenv(k); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return d
}
