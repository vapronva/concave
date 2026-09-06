package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
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
	metricsScrapeInterval          = 5 * time.Second
)

func main() {
	os.Exit(run())
}

func run() int {
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)
	addr := env("BIGBRAIN_ADDR", defaultAddr)
	labelPrefix := env("BIGBRAIN_LABEL_PREFIX", k8sclient.DefaultLabelPrefix)
	reg, controlPlaneTokens, usageTokens, err := buildRegistry(env("BIGBRAIN_DEPLOYMENTS", ""), log)
	if err != nil {
		return 1
	}
	k8s, err := k8sclient.New(labelPrefix)
	if err != nil {
		log.Error("bigbrain: kubernetes client unavailable", "err", err)
		return 1
	}
	cfg := electionConfigFromEnv()
	insightsRingCap := envInt("INSIGHTS_RING_CAP", defaultInsightsRingCap)
	if err = validateRuntimeConfig(cfg, insightsRingCap, log); err != nil {
		return 1
	}
	ctrl := election.New(cfg, k8s, backend.New(controlPlaneTokens), reg, log)
	ins := insights.New(insightsRingCap)
	srv := server.New(reg, ins, usageTokens, log)
	if len(usageTokens) == 0 {
		log.Warn("bigbrain: insights disabled because no deployment usage tokens are configured")
	} else {
		log.Info("bigbrain: insights enabled", "deployments", len(usageTokens), "ringCap", insightsRingCap)
	}
	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	metricsDone, apiserverErr := startMetricsAPIServer(ctx, stop, k8s, reg, labelPrefix, log)
	ctrlDone := runController(ctx, ctrl)
	serveErr := make(chan error, 1)
	go func() {
		log.Info("bigbrain: listening", "addr", addr)
		serr := httpSrv.ListenAndServe()
		if serr != nil && !errors.Is(serr, http.ErrServerClosed) {
			log.Error("bigbrain: http server failed", "err", serr)
			serveErr <- serr
			stop()
			return
		}
		serveErr <- nil
	}()
	<-ctx.Done()
	shutdown(httpSrv, srv, ctrlDone, metricsDone, cfg.ActuationTimeout+actuationDrainSlack, log)
	if <-serveErr != nil {
		return 1
	}
	select {
	case <-apiserverErr:
		return 1
	default:
	}
	return 0
}

func electionConfigFromEnv() election.Config {
	cfg := election.DefaultConfig()
	cfg.Interval = envDuration("BIGBRAIN_INTERVAL", cfg.Interval)
	cfg.PromoteDebounce = envInt("BIGBRAIN_PROMOTE_DEBOUNCE", cfg.PromoteDebounce)
	cfg.FailbackEnabled = envBool("BIGBRAIN_FAILBACK_ENABLED", cfg.FailbackEnabled)
	cfg.FailbackStability = envDuration("BIGBRAIN_FAILBACK_STABILITY", cfg.FailbackStability)
	cfg.FailbackWarmthLagNs = envUint64("BIGBRAIN_FAILBACK_WARMTH_LAG", cfg.FailbackWarmthLagNs)
	cfg.ActuationTimeout = envDuration("BIGBRAIN_ACTUATION_TIMEOUT", cfg.ActuationTimeout)
	return cfg
}

func buildRegistry(
	bootstrap string,
	log *slog.Logger,
) (*registry.Registry, map[string]string, map[string]string, error) {
	deployments, err := parseDeployments(bootstrap)
	if err != nil {
		log.Error("bigbrain: invalid BIGBRAIN_DEPLOYMENTS", "err", err)
		return nil, nil, nil, err
	}
	if len(deployments) == 0 {
		log.Error("bigbrain: refusing to start without registered deployments")
		return nil, nil, nil, errors.New("no registered deployments")
	}
	reg := registry.New()
	for _, d := range deployments {
		reg.EnsureDeployment(d.name, d.namespace)
		log.Info("bigbrain: registered bootstrap deployment", "name", d.name, "namespace", d.namespace)
	}
	controlPlaneTokens, usageTokens, err := loadDeploymentTokens(deployments)
	if err != nil {
		log.Error("bigbrain: missing deployment control-plane token", "err", err)
		return nil, nil, nil, err
	}
	return reg, controlPlaneTokens, usageTokens, nil
}

func validateRuntimeConfig(cfg election.Config, insightsRingCap int, log *slog.Logger) error {
	if insightsRingCap <= 0 {
		err := fmt.Errorf("insights ring cap must be > 0, got %d", insightsRingCap)
		log.Error("bigbrain: invalid INSIGHTS_RING_CAP", "err", err)
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

func startMetricsAPIServer(
	ctx context.Context,
	stop context.CancelFunc,
	k8s *k8sclient.Client,
	reg *registry.Registry,
	labelPrefix string,
	log *slog.Logger,
) (<-chan struct{}, <-chan error) {
	if !envBool("BIGBRAIN_METRICS_APISERVER_ENABLED", false) {
		return nil, nil
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
	securePort := envInt("BIGBRAIN_METRICS_APISERVER_PORT", defaultMetricsAPIServerPort)
	if securePort < 1 || securePort > 65535 {
		badEnv("BIGBRAIN_METRICS_APISERVER_PORT", strconv.Itoa(securePort), errors.New("must be in 1..65535"))
	}
	cfg := apiserver.Config{
		SecurePort: securePort,
		CertDir:    env("BIGBRAIN_METRICS_APISERVER_CERT_DIR", defaultMetricsAPIServerCertDir),
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		apiserver.NewScraper(k8s.Clientset(), labelPrefix, namespaces, prov, metricsScrapeInterval, log).Run(ctx)
	})
	apiserverErr := make(chan error, 1)
	wg.Go(func() {
		if aerr := apiserver.Run(ctx, cfg, prov, log); aerr != nil {
			log.ErrorContext(ctx, "bigbrain: custom-metrics apiserver exited", "err", aerr)
			apiserverErr <- aerr
			stop()
		}
	})
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	log.InfoContext(ctx, "bigbrain: custom-metrics apiserver enabled", "port", cfg.SecurePort)
	return done, apiserverErr
}

func runController(ctx context.Context, ctrl *election.Controller) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctrl.Run(ctx)
	}()
	return done
}

func loadDeploymentTokens(deployments []deploymentRef) (map[string]string, map[string]string, error) {
	controlPlaneTokens := make(map[string]string, len(deployments))
	usageTokens := make(map[string]string, len(deployments))
	for i, d := range deployments {
		token := env(fmt.Sprintf("BIGBRAIN_CONTROL_PLANE_TOKEN_%d", i), "")
		if token == "" {
			return nil, nil, fmt.Errorf("deployment %s (index %d) has no BIGBRAIN_CONTROL_PLANE_TOKEN_%d", d.name, i, i)
		}
		controlPlaneTokens[d.name] = token
		if usage := env(fmt.Sprintf("BIGBRAIN_USAGE_TOKEN_%d", i), ""); usage != "" {
			usageTokens[d.name] = usage
		}
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

func envInt(k string, d int) int {
	v := os.Getenv(k)
	if v == "" {
		return d
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		badEnv(k, v, err)
	}
	return n
}

func envDuration(k string, d time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return d
	}
	dur, err := time.ParseDuration(v)
	if err != nil {
		badEnv(k, v, err)
	}
	return dur
}

func envUint64(k string, d uint64) uint64 {
	v := os.Getenv(k)
	if v == "" {
		return d
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		badEnv(k, v, err)
	}
	return n
}

func envBool(k string, d bool) bool {
	v := os.Getenv(k)
	if v == "" {
		return d
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		badEnv(k, v, err)
	}
	return b
}

func badEnv(k, v string, err error) {
	fmt.Fprintf(os.Stderr, "bigbrain: invalid %s=%q: %v\n", k, v, err)
	os.Exit(1)
}
