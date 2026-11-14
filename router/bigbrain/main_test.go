package main

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"
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
	cp, usage, err := loadDeploymentTokens(deployments, slog.New(slog.DiscardHandler))
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
	if _, _, lerr := loadDeploymentTokens(deployments, slog.New(slog.DiscardHandler)); lerr == nil {
		t.Fatal("missing BIGBRAIN_CONTROL_PLANE_TOKEN_1 must error")
	}
	if _, _, _, berr := buildRegistry("", slog.New(slog.DiscardHandler)); berr == nil {
		t.Fatal("an empty deployment set must refuse to start")
	}
}

func TestLoadDeploymentTokens_SharedTokens(t *testing.T) {
	deployments, err := parseDeployments("a,b,c")
	if err != nil {
		t.Fatalf("parseDeployments: %v", err)
	}
	for i, token := range []string{"cp-a", "cp-b", "cp-c"} {
		t.Setenv(fmt.Sprintf("BIGBRAIN_CONTROL_PLANE_TOKEN_%d", i), token)
	}
	for i, token := range []string{"shared", "usage-b", "shared"} {
		t.Setenv(fmt.Sprintf("BIGBRAIN_USAGE_TOKEN_%d", i), token)
	}
	_, usage, err := loadDeploymentTokens(deployments, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("a shared usage token must not refuse boot: %v", err)
	}
	if len(usage) != 1 || usage["b"] != "usage-b" {
		t.Fatalf("deployments sharing a usage token must lose it, got %v", usage)
	}
	t.Setenv("BIGBRAIN_CONTROL_PLANE_TOKEN_2", "cp-a")
	if _, _, err = loadDeploymentTokens(deployments, slog.New(slog.DiscardHandler)); err == nil ||
		!strings.Contains(err.Error(), "[a c]") {
		t.Fatalf("a shared control-plane token must refuse boot naming the deployments, got %v", err)
	}
}
