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
	insightsRingCap := flag.Int("insights-ring-cap", envInt("INSIGHTS_RING_CAP", defaultInsightsRingCap),
		"in-memory insights ring-buffer capacity (rows retained per process)")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)
	reg := registry.New()
	deployments := parseDeployments(*bootstrap)
	controlPlaneTokens, usageTokens := loadDeploymentTokens(reg, deployments, log)
	if len(reg.Names()) == 0 {
		log.Error("bigbrain: refusing to start without registered deployments")
		return 1
	}
	var ctrl *election.Controller
	k8s, err := k8sclient.New(*kubeconfig, *labelPrefix)
	if err != nil {
		log.Error("bigbrain: kubernetes client unavailable, running WITHOUT election", "err", err)
	} else {
		be := backend.New(controlPlaneTokens)
		ctrl = election.New(election.Config{
			Interval:            *interval,
			PromoteDebounce:     *debounce,
			FailbackEnabled:     *failbackEnabled,
			FailbackStability:   *failbackStability,
			FailbackWarmthLagNs: *failbackWarmthLag,
		}, k8s, be, reg, log)
	}
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
	var ctrlDone chan struct{}
	if ctrl != nil {
		ctrlDone = make(chan struct{})
		go func() {
			defer close(ctrlDone)
			ctrl.Run(ctx)
		}()
		log.Info("bigbrain: election controller started",
			"deployments", len(reg.Names()), "interval", interval.String(), "promoteDebounce", *debounce,
			"failbackEnabled", *failbackEnabled, "failbackStability", failbackStability.String(),
			"failbackWarmthLag", *failbackWarmthLag)
	}
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
	if ctrlDone == nil {
		return
	}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), actuationDrainTimeout)
	defer drainCancel()
	select {
	case <-ctrlDone:
	case <-drainCtx.Done():
		log.Error("bigbrain: election shutdown timed out", "err", drainCtx.Err())
	}
}

func loadDeploymentTokens(
	reg *registry.Registry,
	deployments []deploymentRef,
	log *slog.Logger,
) (map[string]string, map[string]string) {
	controlPlaneTokens := make(map[string]string, len(deployments))
	usageTokens := make(map[string]string, len(deployments))
	for i, d := range deployments {
		reg.EnsureDeployment(d.name, d.namespace)
		token := env(fmt.Sprintf("BIGBRAIN_CONTROL_PLANE_TOKEN_%d", i), "")
		if token == "" {
			log.Error("bigbrain: missing deployment control-plane token", "name", d.name, "index", i)
			os.Exit(1)
		}
		controlPlaneTokens[d.name] = token
		if usage := env(fmt.Sprintf("BIGBRAIN_USAGE_TOKEN_%d", i), ""); usage != "" {
			usageTokens[d.name] = usage
		}
		log.Info("bigbrain: registered bootstrap deployment", "name", d.name, "namespace", d.namespace)
	}
	return controlPlaneTokens, usageTokens
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
