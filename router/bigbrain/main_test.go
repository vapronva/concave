package main

import (
	"log/slog"
	"testing"

	"git.horse/vapronva/concave/router/bigbrain/election"
)

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
}

func TestLoadDeploymentTokens_MissingTokenErrors(t *testing.T) {
	t.Setenv("BIGBRAIN_CONTROL_PLANE_TOKEN_0", "cp-a")
	t.Setenv("BIGBRAIN_CONTROL_PLANE_TOKEN_1", "")
	deployments, err := parseDeployments("a,b")
	if err != nil {
		t.Fatalf("parseDeployments: %v", err)
	}
	if _, _, lerr := loadDeploymentTokens(deployments); lerr == nil {
		t.Fatal("missing BIGBRAIN_CONTROL_PLANE_TOKEN_1 must error")
	}
}

func TestElectionConfigFromEnv(t *testing.T) {
	for _, k := range []string{
		"BIGBRAIN_INTERVAL", "BIGBRAIN_PROMOTE_DEBOUNCE", "BIGBRAIN_EMPTY_DISCOVERY_DEBOUNCE",
		"BIGBRAIN_FAILBACK_ENABLED", "BIGBRAIN_FAILBACK_STABILITY", "BIGBRAIN_FAILBACK_WARMTH_LAG",
		"BIGBRAIN_UNREACHABLE_LEADER_GRACE", "BIGBRAIN_LEASE_UNVERIFIED_GRACE", "BIGBRAIN_ACTUATION_TIMEOUT",
	} {
		t.Setenv(k, "")
	}
	if got := electionConfigFromEnv(); got != election.DefaultConfig() {
		t.Fatalf("unset envs must yield the election defaults, got %+v", got)
	}
	t.Setenv("BIGBRAIN_PROMOTE_DEBOUNCE", "0")
	t.Setenv("BIGBRAIN_EMPTY_DISCOVERY_DEBOUNCE", "7")
	t.Setenv("BIGBRAIN_FAILBACK_ENABLED", "false")
	t.Setenv("BIGBRAIN_LEASE_UNVERIFIED_GRACE", "0s")
	got := electionConfigFromEnv()
	if got.PromoteDebounce != 0 || got.LeaseUnverifiedGrace != 0 {
		t.Fatalf("explicit zeros must reach the config, got %+v", got)
	}
	if got.EmptyDiscoveryDebounce != 7 || got.FailbackEnabled {
		t.Fatalf("set envs must reach the config, got %+v", got)
	}
	if got.Interval != election.DefaultInterval || got.ActuationTimeout != election.DefaultActuationTimeout {
		t.Fatalf("untouched knobs must keep their defaults, got %+v", got)
	}
}

func TestBuildRegistry_RegistersNamespaces(t *testing.T) {
	t.Setenv("BIGBRAIN_CONTROL_PLANE_TOKEN_0", "cp-a")
	t.Setenv("BIGBRAIN_CONTROL_PLANE_TOKEN_1", "cp-b")
	reg, cp, usage, err := buildRegistry("a=ns-a,b", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("buildRegistry: %v", err)
	}
	for name, want := range map[string]string{"a": "ns-a", "b": "b"} {
		if ns, ok := reg.Namespace(name); !ok || ns != want {
			t.Fatalf("deployment %s: namespace %q ok=%v, want %q", name, ns, ok, want)
		}
	}
	if len(cp) != 2 || len(usage) != 0 {
		t.Fatalf("tokens: control-plane %v usage %v", cp, usage)
	}
	if _, _, _, berr := buildRegistry("", slog.New(slog.DiscardHandler)); berr == nil {
		t.Fatal("an empty deployment set must refuse to start")
	}
}
