package apiserver

import (
	"context"
	"fmt"
	"log/slog"

	"k8s.io/component-base/logs"
	"sigs.k8s.io/custom-metrics-apiserver/pkg/cmd"
)

type Config struct {
	SecurePort int
	CertDir    string
}

func Run(ctx context.Context, cfg Config, prov *FunrunProvider, log *slog.Logger) error {
	logs.InitLogs()
	defer logs.FlushLogs()
	base := &cmd.AdapterBase{Name: "bigbrain-custom-metrics"}
	base.WithCustomMetrics(prov)
	args := []string{
		fmt.Sprintf("--secure-port=%d", cfg.SecurePort),
		fmt.Sprintf("--cert-dir=%s", cfg.CertDir),
	}
	if err := base.Flags().Parse(args); err != nil {
		return fmt.Errorf("custom-metrics apiserver flags: %w", err)
	}
	log.InfoContext(ctx, "bigbrain: custom-metrics apiserver starting",
		"securePort", cfg.SecurePort, "metric", MetricName)
	if err := base.Run(ctx); err != nil {
		return fmt.Errorf("custom-metrics apiserver: %w", err)
	}
	return nil
}
