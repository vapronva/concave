package election

import (
	"log/slog"
	"testing"
	"time"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func withConfig(mutate func(*Config)) Config {
	cfg := DefaultConfig()
	mutate(&cfg)
	return cfg
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		cfg  Config
		ok   bool
	}{
		{"defaults", DefaultConfig(), true},
		{"zero interval", withConfig(func(c *Config) { c.Interval = 0 }), false},
		{"zero debounce", withConfig(func(c *Config) { c.PromoteDebounce = 0 }), true},
		{"negative debounce", withConfig(func(c *Config) { c.PromoteDebounce = -1 }), false},
		{"zero stability", withConfig(func(c *Config) { c.FailbackStability = 0 }), true},
		{"negative stability", withConfig(func(c *Config) { c.FailbackStability = -time.Second }), false},
		{"zero actuation timeout", withConfig(func(c *Config) { c.ActuationTimeout = 0 }), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.cfg.Validate()
			if tc.ok && err != nil {
				t.Fatalf("want valid, got %v", err)
			}
			if !tc.ok && err == nil {
				t.Fatal("want a validation error")
			}
		})
	}
}
