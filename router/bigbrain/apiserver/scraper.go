package apiserver

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"git.horse/vapronva/concave/router/bigbrain/k8sclient"
)

const (
	funrunHealthPort    = 8091
	healthPortName      = "health"
	busyGauge           = "convex_funrun_isolate_busy_threads"
	totalGauge          = "convex_funrun_isolate_total_threads"
	scrapeTimeout       = 2 * time.Second
	scrapeBufferInitial = 64 * 1024
	scrapeBufferMax     = 1 << 20
	metricLineMinFields = 2
)

type Scraper struct {
	cs             kubernetes.Interface
	componentLabel string
	namespaces     []string
	prov           *FunrunProvider
	interval       time.Duration
	httpc          *http.Client
	log            *slog.Logger
}

func NewScraper(
	cs kubernetes.Interface,
	labelPrefix string,
	namespaces []string,
	prov *FunrunProvider,
	interval time.Duration,
	log *slog.Logger,
) *Scraper {
	if labelPrefix == "" {
		labelPrefix = k8sclient.DefaultLabelPrefix
	}
	return &Scraper{
		cs:             cs,
		componentLabel: labelPrefix + "/component",
		namespaces:     namespaces,
		prov:           prov,
		interval:       interval,
		httpc:          &http.Client{Timeout: scrapeTimeout},
		log:            log,
	}
}

func (s *Scraper) Run(ctx context.Context) {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	s.scrapeAll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.scrapeAll(ctx)
		}
	}
}

func (s *Scraper) scrapeAll(ctx context.Context) {
	fresh := make(map[types.NamespacedName]podSample)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, ns := range s.namespaces {
		wg.Add(1)
		go func(ns string) {
			defer wg.Done()
			samples := s.scrapeNamespace(ctx, ns)
			mu.Lock()
			maps.Copy(fresh, samples)
			mu.Unlock()
		}(ns)
	}
	wg.Wait()
	s.prov.replace(fresh)
}

func (s *Scraper) scrapeNamespace(ctx context.Context, ns string) map[types.NamespacedName]podSample {
	sel := fmt.Sprintf("%s=funrun", s.componentLabel)
	pods, err := s.cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: sel})
	if err != nil {
		s.log.WarnContext(ctx, "scraper: list funrun pods failed", "namespace", ns, "err", err)
		return nil
	}
	out := make(map[types.NamespacedName]podSample, len(pods.Items))
	var mu sync.Mutex
	var wg sync.WaitGroup
	now := time.Now()
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.DeletionTimestamp != nil || p.Status.PodIP == "" || !k8sclient.PodReady(p) {
			continue
		}
		wg.Add(1)
		go func(p *corev1.Pod) {
			defer wg.Done()
			busy, total, perr := s.scrapePod(ctx, p.Status.PodIP, healthPort(p))
			if perr != nil {
				s.log.DebugContext(ctx, "scraper: pod scrape failed",
					"namespace", ns, "pod", p.Name, "err", perr)
				return
			}
			if total == 0 {
				return
			}
			mu.Lock()
			out[types.NamespacedName{Namespace: p.Namespace, Name: p.Name}] = podSample{
				labels: labels.Set(p.Labels),
				value:  milliPercent(100 * busy / total),
				ts:     now,
			}
			mu.Unlock()
		}(p)
	}
	wg.Wait()
	return out
}

func healthPort(p *corev1.Pod) int {
	for i := range p.Spec.Containers {
		for _, port := range p.Spec.Containers[i].Ports {
			if port.Name == healthPortName {
				return int(port.ContainerPort)
			}
		}
	}
	return funrunHealthPort
}

func (s *Scraper) scrapePod(ctx context.Context, podIP string, port int) (float64, float64, error) {
	addr := net.JoinHostPort(podIP, strconv.Itoa(port))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/metrics", nil)
	if err != nil {
		return 0, 0, err
	}
	resp, err := s.httpc.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("status %d", resp.StatusCode)
	}
	return parsePromGauges(resp.Body)
}

func parsePromGauges(r io.Reader) (float64, float64, error) {
	var busy, total float64
	var haveBusy, haveTotal bool
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, scrapeBufferInitial), scrapeBufferMax)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' {
			continue
		}
		name, valStr, ok := cutMetricLine(line)
		if !ok {
			continue
		}
		switch name {
		case busyGauge:
			if v, err := strconv.ParseFloat(valStr, 64); err == nil {
				busy, haveBusy = v, true
			}
		case totalGauge:
			if v, err := strconv.ParseFloat(valStr, 64); err == nil {
				total, haveTotal = v, true
			}
		}
	}
	if err := sc.Err(); err != nil {
		return 0, 0, err
	}
	if !haveBusy || !haveTotal {
		return 0, 0, errors.New("funrun isolate gauges missing from /metrics")
	}
	return busy, total, nil
}

func cutMetricLine(line string) (string, string, bool) {
	fields := strings.Fields(line)
	if len(fields) < metricLineMinFields {
		return "", "", false
	}
	name := fields[0]
	if i := strings.IndexByte(name, '{'); i >= 0 {
		name = name[:i]
	}
	return name, fields[1], true
}
