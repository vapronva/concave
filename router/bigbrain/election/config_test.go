//nolint:testpackage // white-box
package election

import (
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		cfg  Config
		ok   bool
	}{
		{"empty", Config{}, true},
		{"zero interval", Config{Interval: new(time.Duration(0))}, false},
		{"positive interval", Config{Interval: new(time.Second)}, true},
		{"negative debounce", Config{PromoteDebounce: new(-1)}, false},
		{"zero debounce", Config{PromoteDebounce: new(0)}, true},
		{"negative stability", Config{FailbackStability: new(-time.Second)}, false},
		{"zero stability", Config{FailbackStability: new(time.Duration(0))}, true},
		{"negative unreachable grace", Config{UnreachableLeaderGrace: new(-time.Second)}, false},
		{"zero unreachable grace", Config{UnreachableLeaderGrace: new(time.Duration(0))}, true},
		{"sub-second lease grace rejected", Config{LeaseUnverifiedGrace: new(500 * time.Millisecond)}, false},
		{"one-second lease grace", Config{LeaseUnverifiedGrace: new(time.Second)}, true},
		{"zero lease grace disables", Config{LeaseUnverifiedGrace: new(time.Duration(0))}, true},
		{"negative empty debounce", Config{EmptyDiscoveryDebounce: new(-1)}, false},
		{"zero actuation timeout", Config{ActuationTimeout: new(time.Duration(0))}, false},
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

func TestWithDefaults_NilFillsOnly(t *testing.T) {
	t.Parallel()
	s := Config{}.withDefaults()
	if s.interval != DefaultInterval || s.leaseUnverifiedGrace != DefaultLeaseUnverifiedGrace {
		t.Fatalf("nil config must yield defaults, got %+v", s)
	}
	explicit := Config{
		PromoteDebounce:      new(0),
		FailbackStability:    new(time.Duration(0)),
		LeaseUnverifiedGrace: new(time.Duration(0)),
	}.withDefaults()
	if explicit.promoteDebounce != 0 || explicit.failbackStability != 0 || explicit.leaseUnverifiedGrace != 0 {
		t.Fatalf("explicit zeros must survive withDefaults, got %+v", explicit)
	}
}

func TestSetDiscoveryDown_TransitionsLogOnce(t *testing.T) {
	t.Parallel()
	c := New(Config{}, nil, nil, nil, nil)
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
