package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"git.horse/vapronva/concave/router/bigbrain/backend"
	"git.horse/vapronva/concave/router/bigbrain/election"
	"git.horse/vapronva/concave/router/bigbrain/k8sclient"
	"git.horse/vapronva/concave/router/bigbrain/registry"
	"git.horse/vapronva/concave/router/bigbrain/server"
)

const (
	defaultAddr              = ":8081"
	defaultLabelPrefix       = "convex"
	defaultInterval          = 2 * time.Second
	defaultPromoteDebounce   = 3
	defaultFailbackStability = 15 * time.Second
	defaultFailbackWarmthLag = 5_000_000_000
	readHeaderTimeout        = 10 * time.Second
	shutdownTimeout          = 10 * time.Second
)

func main() {
	addr := flag.String("addr", env("BIGBRAIN_ADDR", defaultAddr), "HTTP listen address")
	kubeconfig := flag.String("kubeconfig", env("KUBECONFIG", ""), "path to kubeconfig (empty is in-cluster)")
	labelPrefix := flag.String(
		"label-prefix",
		env("BIGBRAIN_LABEL_PREFIX", defaultLabelPrefix),
		"k8s label-key prefix for discovery (<prefix>/instance, <prefix>/role, <prefix>/component, <prefix>/leader-priority)",
	)
	bootstrap := flag.String(
		"deployments",
		env("BIGBRAIN_DEPLOYMENTS", ""),
		"comma-separated name=namespace pairs to register at boot (e.g., convex-dev=convex-dev,convex-prod=convex-prod)",
	)
	interval := flag.Duration("interval", envDuration("BIGBRAIN_INTERVAL", defaultInterval), "reconcile interval")
	debounce := flag.Int(
		"promote-debounce",
		envInt("BIGBRAIN_PROMOTE_DEBOUNCE", defaultPromoteDebounce),
		"leaderless polls before promoting",
	)
	failbackEnabled := flag.Bool("failback-enabled", envBool("BIGBRAIN_FAILBACK_ENABLED", true),
		"fail back to a recovered higher-priority pod (the primary) once it is warm and stable")
	failbackStability := flag.Duration("failback-stability",
		envDuration("BIGBRAIN_FAILBACK_STABILITY", defaultFailbackStability),
		"how long the higher-priority pod must stay warm and ready before failback")
	failbackWarmthLag := flag.Uint64("failback-warmth-lag",
		envUint64("BIGBRAIN_FAILBACK_WARMTH_LAG", defaultFailbackWarmthLag),
		"max latest_ts lag for the candidate to count as warm/caught-up")
	controlPlaneToken := flag.String("control-plane-token", env("BIGBRAIN_CONTROL_PLANE_TOKEN", ""),
		"if set, sent as the X-Convex-Control-Plane-Token header on promote/demote requests")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)
	reg := registry.New()
	for _, d := range parseDeployments(*bootstrap) {
		reg.EnsureDeployment(d.name, d.namespace)
		log.Info("bigbrain: registered bootstrap deployment", "name", d.name, "namespace", d.namespace)
	}
	var ctrl *election.Controller
	k8s, err := k8sclient.New(*kubeconfig, *labelPrefix)
	if err != nil {
		log.Error("bigbrain: kubernetes client unavailable, running WITHOUT election", "err", err)
	} else {
		be := backend.New(*controlPlaneToken)
		ctrl = election.New(election.Config{
			Interval:            *interval,
			PromoteDebounce:     *debounce,
			FailbackEnabled:     *failbackEnabled,
			FailbackStability:   *failbackStability,
			FailbackWarmthLagNs: *failbackWarmthLag,
		}, k8s, be, reg, log)
	}
	srv := server.New(reg, log)
	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: readHeaderTimeout,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if ctrl != nil {
		go ctrl.Run(ctx)
		log.Info("bigbrain: election controller started",
			"interval", interval.String(), "promoteDebounce", *debounce,
			"failbackEnabled", *failbackEnabled, "failbackStability", failbackStability.String(),
			"failbackWarmthLag", *failbackWarmthLag)
	}
	go func() {
		log.Info("bigbrain: listening", "addr", *addr)
		if serr := httpSrv.ListenAndServe(); serr != nil && !errors.Is(serr, http.ErrServerClosed) {
			log.Error("bigbrain: http server failed", "err", serr)
			stop()
		}
	}()
	<-ctx.Done()
	log.Info("bigbrain: shutting down")
	shutCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	_ = httpSrv.Shutdown(shutCtx)
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
