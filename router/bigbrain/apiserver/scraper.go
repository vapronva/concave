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
)

const (
	funrunHealthPort    = 8091
	busyGauge           = "convex_funrun_isolate_busy_threads"
	totalGauge          = "convex_funrun_isolate_total_threads"
	scrapeTimeout       = 2 * time.Second
	scrapeBufferInitial = 64 * 1024
	scrapeBufferMax     = 1 << 20
	metricLineMinFields = 2
)

type Deployment struct {
	Name      string
	Namespace string
}

type Scraper struct {
	cs             kubernetes.Interface
	componentLabel string
	deployments    []Deployment
	prov           *FunrunProvider
	interval       time.Duration
	httpc          *http.Client
	log            *slog.Logger
}

func NewScraper(
	cs kubernetes.Interface,
	labelPrefix string,
	deps []Deployment,
	prov *FunrunProvider,
	interval time.Duration,
	log *slog.Logger,
) *Scraper {
	if labelPrefix == "" {
		labelPrefix = "convex"
	}
	return &Scraper{
		cs:             cs,
		componentLabel: labelPrefix + "/component",
		deployments:    deps,
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
	for _, d := range s.deployments {
		wg.Add(1)
		go func(d Deployment) {
			defer wg.Done()
			samples := s.scrapeDeployment(ctx, d)
			mu.Lock()
			maps.Copy(fresh, samples)
			mu.Unlock()
		}(d)
	}
	wg.Wait()
	if len(fresh) == 0 {
		return
	}
	s.prov.replace(fresh)
}

func (s *Scraper) scrapeDeployment(ctx context.Context, d Deployment) map[types.NamespacedName]podSample {
	sel := fmt.Sprintf("%s=funrun", s.componentLabel)
	pods, err := s.cs.CoreV1().Pods(d.Namespace).List(ctx, metav1.ListOptions{LabelSelector: sel})
	if err != nil {
		s.log.WarnContext(ctx, "scraper: list funrun pods failed",
			"deployment", d.Name, "namespace", d.Namespace, "err", err)
		return nil
	}
	out := make(map[types.NamespacedName]podSample, len(pods.Items))
	now := time.Now()
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.DeletionTimestamp != nil || p.Status.PodIP == "" || !podReady(p) {
			continue
		}
		busy, total, perr := s.scrapePod(ctx, p.Status.PodIP)
		if perr != nil {
			s.log.DebugContext(ctx, "scraper: pod scrape failed",
				"deployment", d.Name, "pod", p.Name, "err", perr)
			continue
		}
		if total == 0 {
			continue
		}
		out[types.NamespacedName{Namespace: p.Namespace, Name: p.Name}] = podSample{
			labels: labels.Set(p.Labels),
			value:  milliPercent(100 * busy / total),
			ts:     now,
		}
	}
	return out
}

func (s *Scraper) scrapePod(ctx context.Context, podIP string) (float64, float64, error) {
	addr := net.JoinHostPort(podIP, strconv.Itoa(funrunHealthPort))
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

func podReady(p *corev1.Pod) bool {
	if p.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, cond := range p.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}
