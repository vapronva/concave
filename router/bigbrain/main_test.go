package main

import (
	"log/slog"
	"testing"

	"git.horse/vapronva/concave/router/bigbrain/election"
)

func TestParseDeployments(t *testing.T) {
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
	for _, in := range []string{"=ns", "a=1, =2", " = ", "a=1,a=2", "a,a", "a=x,b=y,a=z"} {
		if _, perr := parseDeployments(in); perr == nil {
			t.Fatalf("input %q: want an error for an empty or duplicate name", in)
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
	cp, usage, err := loadDeploymentTokens(deployments)
	if err != nil {
		t.Fatalf("loadDeploymentTokens: %v", err)
	}
	if cp["a"] != "cp-a" || cp["b"] != "cp-b" {
		t.Fatalf("control-plane token pairing broken: %v", cp)
	}
	if len(usage) != 1 || usage["b"] != "usage-b" {
		t.Fatalf("usage token pairing broken: %v", usage)
	}
	t.Setenv("BIGBRAIN_CONTROL_PLANE_TOKEN_1", "")
	if _, _, lerr := loadDeploymentTokens(deployments); lerr == nil {
		t.Fatal("missing BIGBRAIN_CONTROL_PLANE_TOKEN_1 must error")
	}
	if _, _, _, berr := buildRegistry("", slog.New(slog.DiscardHandler)); berr == nil {
		t.Fatal("an empty deployment set must refuse to start")
	}
}

func TestElectionConfigFromEnv(t *testing.T) {
	for _, k := range []string{
		"BIGBRAIN_INTERVAL", "BIGBRAIN_PROMOTE_DEBOUNCE", "BIGBRAIN_FAILBACK_ENABLED",
		"BIGBRAIN_FAILBACK_STABILITY", "BIGBRAIN_FAILBACK_WARMTH_LAG", "BIGBRAIN_ACTUATION_TIMEOUT",
	} {
		t.Setenv(k, "")
	}
	if got := electionConfigFromEnv(); got != election.DefaultConfig() {
		t.Fatalf("unset envs must yield the election defaults, got %+v", got)
	}
	t.Setenv("BIGBRAIN_PROMOTE_DEBOUNCE", "0")
	t.Setenv("BIGBRAIN_FAILBACK_ENABLED", "false")
	got := electionConfigFromEnv()
	if got.PromoteDebounce != 0 || got.FailbackEnabled {
		t.Fatalf("explicit zero and false must reach the config, got %+v", got)
	}
	if got.Interval != election.DefaultInterval || got.ActuationTimeout != election.DefaultActuationTimeout {
		t.Fatalf("untouched knobs must keep their defaults, got %+v", got)
	}
}
