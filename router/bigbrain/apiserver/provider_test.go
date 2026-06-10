//nolint:testpackage // white-box
package apiserver

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/custom-metrics-apiserver/pkg/provider"
)

func metricInfo() provider.CustomMetricInfo {
	return provider.CustomMetricInfo{
		GroupResource: schema.GroupResource{Resource: podsResource},
		Namespaced:    true,
		Metric:        MetricName,
	}
}

func TestProviderReplaceWithEmptyClearsCache(t *testing.T) {
	t.Parallel()
	p := NewProvider()
	pod := types.NamespacedName{Namespace: "ns", Name: "funrun-0"}
	p.replace(map[types.NamespacedName]podSample{
		pod: {labels: labels.Set{}, value: milliPercent(42), ts: time.Now()},
	})
	if _, err := p.GetMetricByName(context.Background(), pod, metricInfo(), nil); err != nil {
		t.Fatalf("fresh sample must be served: %v", err)
	}
	p.replace(map[types.NamespacedName]podSample{})
	if _, err := p.GetMetricByName(context.Background(), pod, metricInfo(), nil); err == nil {
		t.Fatal("a fully failed scrape cycle must clear the cache, not freeze it")
	}
	list, err := p.GetMetricBySelector(context.Background(), "ns", labels.Everything(), metricInfo(), nil)
	if err != nil {
		t.Fatalf("selector query: %v", err)
	}
	if len(list.Items) != 0 {
		t.Fatalf("cleared cache must serve no items, got %d", len(list.Items))
	}
}

func TestProviderRejectsUnknownMetric(t *testing.T) {
	t.Parallel()
	p := NewProvider()
	info := provider.CustomMetricInfo{Metric: "nope"}
	if _, err := p.GetMetricByName(context.Background(), types.NamespacedName{}, info, nil); err == nil ||
		!strings.Contains(err.Error(), "nope") {
		t.Fatalf("unknown metric must be rejected, got %v", err)
	}
}

func TestProviderRejectsNonPodResource(t *testing.T) {
	t.Parallel()
	p := NewProvider()
	pod := types.NamespacedName{Namespace: "ns", Name: "funrun-0"}
	p.replace(map[types.NamespacedName]podSample{
		pod: {labels: labels.Set{}, value: milliPercent(42), ts: time.Now()},
	})
	info := provider.CustomMetricInfo{
		GroupResource: schema.GroupResource{Resource: "services"},
		Namespaced:    true,
		Metric:        MetricName,
	}
	if _, err := p.GetMetricByName(context.Background(), pod, info, nil); err == nil {
		t.Fatal("GetMetricByName must reject non-pod resources")
	}
	if _, err := p.GetMetricBySelector(context.Background(), "ns", labels.Everything(), info, nil); err == nil {
		t.Fatal("GetMetricBySelector must reject non-pod resources")
	}
}

func TestHealthPortPrefersNamedContainerPort(t *testing.T) {
	t.Parallel()
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Ports: []corev1.ContainerPort{
					{Name: "grpc", ContainerPort: 8090},
					{Name: "health", ContainerPort: 9999},
				},
			}},
		},
	}
	if got := healthPort(pod); got != 9999 {
		t.Fatalf("named health port: want 9999, got %d", got)
	}
	if got := healthPort(&corev1.Pod{}); got != funrunHealthPort {
		t.Fatalf("fallback: want %d, got %d", funrunHealthPort, got)
	}
}

func TestParsePromGauges(t *testing.T) {
	t.Parallel()
	body := `# HELP convex_funrun_isolate_busy_threads busy
convex_funrun_isolate_busy_threads{instance="x"} 12
convex_funrun_isolate_total_threads 256
`
	busy, total, err := parsePromGauges(strings.NewReader(body))
	if err != nil || busy != 12 || total != 256 {
		t.Fatalf("want 12/256, got %v/%v (%v)", busy, total, err)
	}
	if _, _, err = parsePromGauges(strings.NewReader("unrelated 1\n")); err == nil {
		t.Fatal("missing gauges must error")
	}
}
