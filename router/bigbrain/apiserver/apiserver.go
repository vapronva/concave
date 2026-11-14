package apiserver

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/spf13/pflag"
	"sigs.k8s.io/custom-metrics-apiserver/pkg/cmd"
)

const (
	adapterName  = "bigbrain-custom-metrics"
	retryInitial = time.Second
	retryCap     = time.Minute
)

type Config struct {
	SecurePort int
	CertDir    string
}

func Run(ctx context.Context, cfg Config, prov *FunrunProvider, log *slog.Logger) {
	args := []string{
		fmt.Sprintf("--secure-port=%d", cfg.SecurePort),
		fmt.Sprintf("--cert-dir=%s", cfg.CertDir),
	}
	log.InfoContext(ctx, "bigbrain: custom-metrics apiserver starting",
		"securePort", cfg.SecurePort, "metric", MetricName)
	for backoff := retryInitial; ; backoff = min(backoff+backoff, retryCap) {
		err := serve(ctx, args, prov)
		if ctx.Err() != nil {
			return
		}
		log.ErrorContext(ctx, "bigbrain: custom-metrics apiserver failed; retrying",
			"err", err, "retryIn", backoff.String())
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}

func serve(ctx context.Context, args []string, prov *FunrunProvider) error {
	base := &cmd.AdapterBase{Name: adapterName, FlagSet: pflag.NewFlagSet(adapterName, pflag.ContinueOnError)}
	base.WithCustomMetrics(prov)
	if err := base.Flags().Parse(args); err != nil {
		return fmt.Errorf("custom-metrics apiserver flags: %w", err)
	}
	defer func() {
		if ln := base.SecureServing.Listener; ln != nil {
			_ = ln.Close()
		}
	}()
	return base.Run(ctx)
}
