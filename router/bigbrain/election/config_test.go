package election

import (
	"testing"
	"time"
)

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
		{"zero unreachable grace", withConfig(func(c *Config) { c.UnreachableLeaderGrace = 0 }), true},
		{"negative unreachable grace", withConfig(func(c *Config) { c.UnreachableLeaderGrace = -time.Second }), false},
		{
			"sub-second lease grace rejected",
			withConfig(func(c *Config) { c.LeaseUnverifiedGrace = 500 * time.Millisecond }),
			false,
		},
		{"zero lease grace disables", withConfig(func(c *Config) { c.LeaseUnverifiedGrace = 0 }), true},
		{"negative empty debounce", withConfig(func(c *Config) { c.EmptyDiscoveryDebounce = -1 }), false},
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

func TestSetDiscoveryDown_TransitionsLogOnce(t *testing.T) {
	t.Parallel()
	c := New(DefaultConfig(), nil, nil, nil, nil)
	st := &deploymentState{}
	if !c.setDiscoveryDown(st, true) {
		t.Fatal("first failure must report a transition")
	}
	if c.setDiscoveryDown(st, true) {
		t.Fatal("repeated failure must not report a transition")
	}
	if !c.setDiscoveryDown(st, false) {
		t.Fatal("recovery must report a transition")
	}
	if c.setDiscoveryDown(st, false) {
		t.Fatal("repeated success must not report a transition")
	}
}
