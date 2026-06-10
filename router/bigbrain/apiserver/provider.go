package apiserver

import (
	"context"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/metrics/pkg/apis/custom_metrics"
	"sigs.k8s.io/custom-metrics-apiserver/pkg/provider"
)

const (
	MetricName      = "funrun_isolate_utilization"
	podsResource    = "pods"
	milliPerPercent = 1000
)

type FunrunProvider struct {
	mu    sync.RWMutex
	cache map[types.NamespacedName]podSample
}

type podSample struct {
	labels labels.Set
	value  resource.Quantity
	ts     time.Time
}

func NewProvider() *FunrunProvider {
	return &FunrunProvider{cache: make(map[types.NamespacedName]podSample)}
}

func (p *FunrunProvider) GetMetricByName(
	_ context.Context,
	name types.NamespacedName,
	info provider.CustomMetricInfo,
	_ labels.Selector,
) (*custom_metrics.MetricValue, error) {
	if info.Metric != MetricName || info.GroupResource.Resource != podsResource {
		return nil, provider.NewMetricNotFoundError(info.GroupResource, info.Metric)
	}
	p.mu.RLock()
	s, ok := p.cache[name]
	p.mu.RUnlock()
	if !ok {
		return nil, provider.NewMetricNotFoundForError(info.GroupResource, info.Metric, name.Name)
	}
	v := metricFor(name, s)
	return &v, nil
}

func (p *FunrunProvider) GetMetricBySelector(
	_ context.Context,
	namespace string,
	selector labels.Selector,
	info provider.CustomMetricInfo,
	_ labels.Selector,
) (*custom_metrics.MetricValueList, error) {
	if info.Metric != MetricName || info.GroupResource.Resource != podsResource {
		return nil, provider.NewMetricNotFoundError(info.GroupResource, info.Metric)
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	items := make([]custom_metrics.MetricValue, 0, len(p.cache))
	for name, s := range p.cache {
		if name.Namespace != namespace {
			continue
		}
		if selector != nil && !selector.Empty() && !selector.Matches(s.labels) {
			continue
		}
		items = append(items, metricFor(name, s))
	}
	return &custom_metrics.MetricValueList{Items: items}, nil
}

func (p *FunrunProvider) ListAllMetrics() []provider.CustomMetricInfo {
	return []provider.CustomMetricInfo{{
		GroupResource: schema.GroupResource{Resource: podsResource},
		Namespaced:    true,
		Metric:        MetricName,
	}}
}

func (p *FunrunProvider) replace(fresh map[types.NamespacedName]podSample) {
	p.mu.Lock()
	p.cache = fresh
	p.mu.Unlock()
}

func milliPercent(pct float64) resource.Quantity {
	return *resource.NewMilliQuantity(int64(pct*milliPerPercent), resource.DecimalSI)
}

func metricFor(name types.NamespacedName, s podSample) custom_metrics.MetricValue {
	return custom_metrics.MetricValue{
		DescribedObject: custom_metrics.ObjectReference{
			Kind:       "Pod",
			APIVersion: "v1",
			Namespace:  name.Namespace,
			Name:       name.Name,
		},
		Metric:    custom_metrics.MetricIdentifier{Name: MetricName},
		Timestamp: metav1.NewTime(s.ts),
		Value:     s.value,
	}
}
