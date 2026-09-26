package election

import (
	"log/slog"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func withConfig(mutate func(*Config)) Config {
	cfg := DefaultConfig()
	mutate(&cfg)
	return cfg
}
