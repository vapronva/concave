//nolint:testpackage // white-box
package apiserver

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func funrunPod(ns, name string, port int) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    map[string]string{"convex/component": "funrun"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Ports: []corev1.ContainerPort{{Name: healthPortName, ContainerPort: int32(port)}},
			}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			PodIP:      "127.0.0.1",
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

func metricsServerPort(t *testing.T, body string) int {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	_, portStr, err := net.SplitHostPort(srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}
	return port
}

func gaugesServerPort(t *testing.T) int {
	t.Helper()
	return metricsServerPort(t, "convex_funrun_isolate_busy_threads 12\nconvex_funrun_isolate_total_threads 24\n")
}

func cachedPods(prov *FunrunProvider, names ...types.NamespacedName) map[types.NamespacedName]bool {
	out := make(map[types.NamespacedName]bool, len(names))
	for _, name := range names {
		_, err := prov.GetMetricByName(context.Background(), name, metricInfo(), nil)
		out[name] = err == nil
	}
	return out
}

func TestScrapeAll_MergesAcrossNamespaces(t *testing.T) {
	t.Parallel()
	port := gaugesServerPort(t)
	cs := fake.NewClientset(funrunPod("ns1", "funrun-a", port), funrunPod("ns2", "funrun-b", port))
	prov := NewProvider()
	s := NewScraper(cs, "convex", []string{"ns1", "ns2"}, prov, time.Hour, slog.New(slog.DiscardHandler))
	s.scrapeAll(context.Background())
	a := types.NamespacedName{Namespace: "ns1", Name: "funrun-a"}
	b := types.NamespacedName{Namespace: "ns2", Name: "funrun-b"}
	if got := cachedPods(prov, a, b); !got[a] || !got[b] {
		t.Fatalf("both namespaces must be merged into the cache, got %v", got)
	}
	v, err := prov.GetMetricByName(context.Background(), a, metricInfo(), nil)
	if err != nil {
		t.Fatalf("GetMetricByName: %v", err)
	}
	if v.Value.MilliValue() != 50_000 {
		t.Fatalf("want 50%% utilization (50000m), got %s", v.Value.String())
	}
}

func TestScrapeAll_ListFailureDropsNamespaceFromCache(t *testing.T) {
	t.Parallel()
	port := gaugesServerPort(t)
	cs := fake.NewClientset(funrunPod("ns1", "funrun-a", port), funrunPod("ns2", "funrun-b", port))
	prov := NewProvider()
	s := NewScraper(cs, "convex", []string{"ns1", "ns2"}, prov, time.Hour, slog.New(slog.DiscardHandler))
	s.scrapeAll(context.Background())
	a := types.NamespacedName{Namespace: "ns1", Name: "funrun-a"}
	b := types.NamespacedName{Namespace: "ns2", Name: "funrun-b"}
	if got := cachedPods(prov, a, b); !got[a] || !got[b] {
		t.Fatalf("setup: both pods must be cached, got %v", got)
	}
	cs.PrependReactor("list", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetNamespace() == "ns2" {
			return true, nil, errors.New("list blew up")
		}
		return false, nil, nil
	})
	s.scrapeAll(context.Background())
	got := cachedPods(prov, a, b)
	if !got[a] {
		t.Fatalf("healthy namespace must stay cached, got %v", got)
	}
	if got[b] {
		t.Fatal("failed-list namespace must be absent from the replaced cache, never kept stale")
	}
}

func TestScrapeAll_ZeroTotalThreadsDropsPodFromCache(t *testing.T) {
	t.Parallel()
	port := metricsServerPort(t, "convex_funrun_isolate_busy_threads 0\nconvex_funrun_isolate_total_threads 0\n")
	cs := fake.NewClientset(funrunPod("ns1", "funrun-cold", port))
	prov := NewProvider()
	s := NewScraper(cs, "convex", []string{"ns1"}, prov, time.Hour, slog.New(slog.DiscardHandler))
	s.scrapeAll(context.Background())
	cold := types.NamespacedName{Namespace: "ns1", Name: "funrun-cold"}
	if got := cachedPods(prov, cold); got[cold] {
		t.Fatal("a pod reporting total_threads=0 must be dropped from the cache (div-by-zero guard), not published")
	}
}
