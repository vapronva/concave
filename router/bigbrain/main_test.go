package main

import (
	"flag"
	"log/slog"
	"testing"
	"time"

	"git.horse/vapronva/concave/router/bigbrain/registry"
)

func clearElectionEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"BIGBRAIN_INTERVAL", "BIGBRAIN_PROMOTE_DEBOUNCE", "BIGBRAIN_EMPTY_DISCOVERY_DEBOUNCE",
		"BIGBRAIN_FAILBACK_ENABLED", "BIGBRAIN_FAILBACK_STABILITY", "BIGBRAIN_FAILBACK_WARMTH_LAG",
		"BIGBRAIN_UNREACHABLE_LEADER_GRACE",
	} {
		t.Setenv(k, "")
	}
}

func TestElectionFlags_ExplicitZeroAndUnsetEnv(t *testing.T) {
	clearElectionEnv(t)
	t.Setenv("BIGBRAIN_PROMOTE_DEBOUNCE", "0")
	t.Setenv("BIGBRAIN_EMPTY_DISCOVERY_DEBOUNCE", "7")
	t.Setenv("BIGBRAIN_FAILBACK_ENABLED", "false")
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfgFn := electionFlags(fs)
	if err := fs.Parse(nil); err != nil {
		t.Fatalf("parse: %v", err)
	}
	cfg := cfgFn()
	if cfg.PromoteDebounce == nil || *cfg.PromoteDebounce != 0 {
		t.Fatalf("an explicit zero env must reach the config, got %v", cfg.PromoteDebounce)
	}
	if cfg.EmptyDiscoveryDebounce == nil || *cfg.EmptyDiscoveryDebounce != 7 {
		t.Fatalf("set env must reach the config, got %v", cfg.EmptyDiscoveryDebounce)
	}
	if cfg.FailbackEnabled == nil || *cfg.FailbackEnabled {
		t.Fatalf("explicit false env must reach the config, got %v", cfg.FailbackEnabled)
	}
	if cfg.Interval != nil || cfg.FailbackStability != nil ||
		cfg.FailbackWarmthLagNs != nil || cfg.UnreachableLeaderGrace != nil {
		t.Fatalf("unset envs must leave fields nil for withDefaults to fill, got %+v", cfg)
	}
}

func TestElectionFlags_FlagOverridesEnv(t *testing.T) {
	clearElectionEnv(t)
	t.Setenv("BIGBRAIN_PROMOTE_DEBOUNCE", "9")
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfgFn := electionFlags(fs)
	if err := fs.Parse([]string{"-promote-debounce=2", "-unreachable-leader-grace=5s"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	cfg := cfgFn()
	if cfg.PromoteDebounce == nil || *cfg.PromoteDebounce != 2 {
		t.Fatalf("a passed flag must override the env, got %v", cfg.PromoteDebounce)
	}
	if cfg.UnreachableLeaderGrace == nil || *cfg.UnreachableLeaderGrace != 5*time.Second {
		t.Fatalf("a passed flag must count as set, got %v", cfg.UnreachableLeaderGrace)
	}
}

func TestParseDeployments_Valid(t *testing.T) {
	t.Parallel()
	got, err := parseDeployments(" a=ns-a , b ,, c=ns-c ")
	if err != nil {
		t.Fatalf("parseDeployments: %v", err)
	}
	want := []deploymentRef{
		{name: "a", namespace: "ns-a"},
		{name: "b", namespace: "b"},
		{name: "c", namespace: "ns-c"},
	}
	if len(got) != len(want) {
		t.Fatalf("want %d deployments, got %+v", len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("entry %d: want %+v, got %+v", i, want[i], got[i])
		}
	}
}

func TestParseDeployments_RejectsEmptyName(t *testing.T) {
	t.Parallel()
	for _, in := range []string{"=ns", "a=1, =2", " = ", "="} {
		if _, err := parseDeployments(in); err == nil {
			t.Fatalf("input %q: want error for empty deployment name", in)
		}
	}
}

func TestParseDeployments_RejectsDuplicateName(t *testing.T) {
	t.Parallel()
	for _, in := range []string{"a=1,a=2", "a,a", "a=x,b=y,a=z"} {
		if _, err := parseDeployments(in); err == nil {
			t.Fatalf("input %q: want error for duplicate deployment name", in)
		}
	}
}

func TestLoadDeploymentTokens_PairsTokensByIndex(t *testing.T) {
	t.Setenv("BIGBRAIN_CONTROL_PLANE_TOKEN_0", "cp-a")
	t.Setenv("BIGBRAIN_CONTROL_PLANE_TOKEN_1", "cp-b")
	t.Setenv("BIGBRAIN_USAGE_TOKEN_0", "")
	t.Setenv("BIGBRAIN_USAGE_TOKEN_1", "usage-b")
	deployments, err := parseDeployments("a=ns-a,b=ns-b")
	if err != nil {
		t.Fatalf("parseDeployments: %v", err)
	}
	reg := registry.New()
	cp, usage, err := loadDeploymentTokens(reg, deployments, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("loadDeploymentTokens: %v", err)
	}
	if cp["a"] != "cp-a" || cp["b"] != "cp-b" {
		t.Fatalf("control-plane token pairing broken: %v", cp)
	}
	if len(usage) != 1 || usage["b"] != "usage-b" {
		t.Fatalf("usage token pairing broken: %v", usage)
	}
	if ns, ok := reg.Namespace("a"); !ok || ns != "ns-a" {
		t.Fatalf("deployment a not registered with its namespace: %q ok=%v", ns, ok)
	}
}

func TestLoadDeploymentTokens_MissingTokenErrors(t *testing.T) {
	t.Setenv("BIGBRAIN_CONTROL_PLANE_TOKEN_0", "cp-a")
	t.Setenv("BIGBRAIN_CONTROL_PLANE_TOKEN_1", "")
	deployments, err := parseDeployments("a,b")
	if err != nil {
		t.Fatalf("parseDeployments: %v", err)
	}
	if _, _, lerr := loadDeploymentTokens(registry.New(), deployments, slog.New(slog.DiscardHandler)); lerr == nil {
		t.Fatal("missing BIGBRAIN_CONTROL_PLANE_TOKEN_1 must error")
	}
}
