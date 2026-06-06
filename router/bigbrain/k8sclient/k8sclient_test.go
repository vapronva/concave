package k8sclient_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"git.horse/vapronva/concave/router/bigbrain/k8sclient"
)

func pod(prefix, name, role, component, priority, ip string, ready bool) *corev1.Pod {
	labels := map[string]string{prefix + "/instance": "acme"}
	if role != "" {
		labels[prefix+"/role"] = role
	}
	if component != "" {
		labels[prefix+"/component"] = component
	}
	if priority != "" {
		labels[prefix+"/leader-priority"] = priority
	}
	cond := corev1.ConditionFalse
	if ready {
		cond = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "acme", Labels: labels},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			PodIP:      ip,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: cond}},
		},
	}
}

func TestDiscoverBackends_CustomPrefix(t *testing.T) {
	t.Parallel()
	const prefix = "example.com"
	cs := fake.NewClientset(
		pod(prefix, "backend-0", "leader", "backend", "100", "10.0.0.1", true),
		pod(prefix, "backend-1", "follower", "", "", "10.0.0.2", false),
		pod(prefix, "dashboard-0", "", "", "", "10.0.0.3", true),
		pod(prefix, "funrun-0", "follower", "funrun", "", "10.0.0.4", true),
		pod("convex", "other-0", "leader", "backend", "100", "10.0.0.5", true),
	)
	c := k8sclient.NewFromInterface(cs, prefix)
	out, err := c.DiscoverBackends(context.Background(), "acme", "acme")
	if err != nil {
		t.Fatalf("DiscoverBackends: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("want 2 backends (role-labelled, component=backend or unset), got %d: %+v", len(out), out)
	}
	if out[0].Pod != "backend-0" || out[1].Pod != "backend-1" {
		t.Fatalf("want backend-0,backend-1 sorted, got %q,%q", out[0].Pod, out[1].Pod)
	}
	if out[0].URL != "http://10.0.0.1:3210" {
		t.Fatalf("unexpected URL %q", out[0].URL)
	}
	if out[0].Priority != 100 || !out[0].Ready {
		t.Fatalf("unexpected leader fields: %+v", out[0])
	}
	if out[1].Ready {
		t.Fatalf("backend-1 is not Ready; want Ready=false, got %+v", out[1])
	}
}

func TestDiscoverBackends_DefaultPrefix(t *testing.T) {
	t.Parallel()
	cs := fake.NewClientset(
		pod("convex", "backend-0", "leader", "backend", "100", "10.0.0.1", true),
		pod("example.com", "ignore-0", "leader", "backend", "100", "10.0.0.9", true),
	)
	for _, prefix := range []string{"", "convex"} {
		c := k8sclient.NewFromInterface(cs, prefix)
		out, err := c.DiscoverBackends(context.Background(), "acme", "acme")
		if err != nil {
			t.Fatalf("prefix %q: DiscoverBackends: %v", prefix, err)
		}
		if len(out) != 1 || out[0].Pod != "backend-0" {
			t.Fatalf("prefix %q: want only convex-labelled backend-0, got %+v", prefix, out)
		}
	}
}

func TestDiscoverBackends_SkipsNonBackendComponentAndNoIP(t *testing.T) {
	t.Parallel()
	const prefix = "convex"
	cs := fake.NewClientset(
		pod(prefix, "backend-0", "leader", "backend", "100", "10.0.0.1", true),
		pod(prefix, "sidecar-0", "follower", "router", "", "10.0.0.2", true),
		pod(prefix, "funrun-0", "", "funrun", "", "10.0.0.3", true),
		pod(prefix, "pending-0", "follower", "backend", "", "", true),
	)
	c := k8sclient.NewFromInterface(cs, prefix)
	out, err := c.DiscoverBackends(context.Background(), "acme", "acme")
	if err != nil {
		t.Fatalf("DiscoverBackends: %v", err)
	}
	if len(out) != 1 || out[0].Pod != "backend-0" {
		t.Fatalf("want only backend-0 (skip non-backend component and no-IP), got %+v", out)
	}
}
