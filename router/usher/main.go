package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	bodyReadLimit        = 4096
	streamBufferMax      = 1 << 16
	httpClientTimeout    = 3 * time.Second
	backendQueryTimeout  = 2 * time.Second
	resolveTimeout       = 3 * time.Second
	resolveInterval      = 5 * time.Second
	readHeaderTimeout    = 10 * time.Second
	idleTimeout          = 120 * time.Second
	shutdownTimeout      = 10 * time.Second
	streamBackoffInitial = 1 * time.Second
	streamBackoffCap     = 15 * time.Second
	instancePrefix       = "/instance"
	sitePort             = "3211"
	routesPerDeployment  = 2
)

type deploymentCfg struct {
	Host     string   `json:"host"`
	SiteHost string   `json:"siteHost"`
	Name     string   `json:"name"`
	Backends []string `json:"backends"`
}

type config struct {
	Deployments []deploymentCfg `json:"deployments"`
}

type leadership struct {
	Role     string `json:"role"`
	IsLeader bool   `json:"is_leader"`
	LatestTS int64  `json:"latest_ts"`
}

type leaderResponse struct {
	Name      string `json:"name"`
	LeaderPod string `json:"leaderPod"`
	LeaderURL string `json:"leaderUrl"`
}

type route struct {
	tracker *tracker
	site    bool
}

type tracker struct {
	host        string
	siteHost    string
	name        string
	backends    []string
	bigbrainURL string
	client      *http.Client
	mu          sync.RWMutex
	leaderURL   string
	proxy       *httputil.ReverseProxy
	siteProxy   *httputil.ReverseProxy
}

func newReverseProxy(u *url.URL) *httputil.ReverseProxy {
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(u)
		},
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			log.Printf("usher: upstream error: %v", err)
			http.Error(w, "bad gateway", http.StatusBadGateway)
		},
	}
	return rp
}

func (t *tracker) setLeader(leaderURL string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if leaderURL == t.leaderURL {
		return false
	}
	if leaderURL == "" {
		t.leaderURL, t.proxy, t.siteProxy = "", nil, nil
		log.Printf("usher: %s leader cleared (none available)", t.host)
		return true
	}
	u, err := url.Parse(leaderURL)
	if err != nil {
		log.Printf("usher: %s bad leader url %q: %v", t.host, leaderURL, err)
		return false
	}
	t.leaderURL, t.proxy = leaderURL, newReverseProxy(u)
	if t.siteHost != "" {
		su := *u
		su.Host = net.JoinHostPort(u.Hostname(), sitePort)
		t.siteProxy = newReverseProxy(&su)
		log.Printf("usher: %s leader -> %s (site %s -> %s)", t.host, leaderURL, t.siteHost, su.String())
	} else {
		log.Printf("usher: %s leader -> %s", t.host, leaderURL)
	}
	return true
}

func (t *tracker) currentLeader(site bool) *httputil.ReverseProxy {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if site {
		return t.siteProxy
	}
	return t.proxy
}

func (t *tracker) resolveOnce(ctx context.Context) {
	if t.bigbrainURL != "" {
		if leader, ok := t.queryBigbrain(ctx); ok {
			t.setLeader(leader)
			return
		}
	}
	if leader, ok := t.pollBackends(ctx); ok {
		t.setLeader(leader)
	}
}

func (t *tracker) queryBigbrain(ctx context.Context) (string, bool) {
	u := strings.TrimRight(t.bigbrainURL, "/") + "/registry/deployments/" + t.name + "/leader"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", false
	}
	resp, err := t.client.Do(req) //nolint:bodyclose // closed via drainClose
	if err != nil {
		return "", false
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", false
	}
	var lr leaderResponse
	if json.NewDecoder(io.LimitReader(resp.Body, bodyReadLimit)).Decode(&lr) != nil || lr.LeaderURL == "" {
		return "", false
	}
	return lr.LeaderURL, true
}

func (t *tracker) pollBackends(ctx context.Context) (string, bool) {
	best, bestTS := "", int64(-1)
	for _, b := range t.backends {
		l, ok := fetchLeadership(ctx, t.client, b)
		if ok && l.IsLeader && l.LatestTS > bestTS {
			best, bestTS = b, l.LatestTS
		}
	}
	return best, best != ""
}

func fetchLeadership(ctx context.Context, client *http.Client, base string) (leadership, bool) {
	rctx, cancel := context.WithTimeout(ctx, backendQueryTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodGet, base+"/instance/leadership", nil)
	if err != nil {
		return leadership{}, false
	}
	resp, err := client.Do(req) //nolint:bodyclose // closed via drainClose
	if err != nil {
		return leadership{}, false
	}
	defer drainClose(resp.Body)
	var l leadership
	if json.NewDecoder(io.LimitReader(resp.Body, bodyReadLimit)).Decode(&l) != nil {
		return leadership{}, false
	}
	return l, true
}

func (t *tracker) streamBigbrain(ctx context.Context) {
	if t.bigbrainURL == "" {
		return
	}
	u := strings.TrimRight(t.bigbrainURL, "/") + "/registry/deployments/" + t.name + "/leader-stream"
	backoff := streamBackoffInitial
	for {
		if ctx.Err() != nil {
			return
		}
		t.consumeStream(ctx, u)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < streamBackoffCap {
			backoff *= 2
		}
	}
}

func (t *tracker) consumeStream(ctx context.Context, u string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := (&http.Client{}).Do(req) //nolint:bodyclose // closed via drainClose
	if err != nil {
		return
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, bodyReadLimit), streamBufferMax)
	for sc.Scan() {
		line := sc.Text()
		data, ok := strings.CutPrefix(line, "data: ")
		if !ok {
			continue
		}
		var ev leaderResponse
		if json.Unmarshal([]byte(data), &ev) != nil {
			continue
		}
		t.setLeader(ev.LeaderURL)
	}
}

func isBlockedControlPath(p string) bool {
	c := path.Clean("/" + strings.TrimSuffix(p, "/"))
	return c == instancePrefix || strings.HasPrefix(c, instancePrefix+"/") || strings.HasPrefix(c, instancePrefix+"_")
}

func (t *tracker) serveHTTP(w http.ResponseWriter, r *http.Request, site bool) {
	if !site && isBlockedControlPath(r.URL.Path) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	p := t.currentLeader(site)
	if p == nil {
		log.Printf("usher: %s no leader available", t.host)
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	mw := &misdirectWriter{ResponseWriter: w}
	p.ServeHTTP(mw, r)
	if mw.misdirected {
		log.Printf("usher: %s upstream 421; re-resolving leader", t.host)
		ctx, cancel := context.WithTimeout(context.Background(), resolveTimeout)
		t.resolveOnce(ctx)
		cancel()
	}
}

type misdirectWriter struct {
	http.ResponseWriter

	misdirected bool
	wroteHeader bool
}

func (m *misdirectWriter) WriteHeader(code int) {
	if m.wroteHeader {
		return
	}
	m.wroteHeader = true
	if code == http.StatusMisdirectedRequest {
		m.misdirected = true
		m.ResponseWriter.Header().Set("Retry-After", "1")
		m.ResponseWriter.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	m.ResponseWriter.WriteHeader(code)
}

func (m *misdirectWriter) Write(b []byte) (int, error) {
	if !m.wroteHeader {
		m.WriteHeader(http.StatusOK)
	}
	if m.misdirected {
		return len(b), nil
	}
	return m.ResponseWriter.Write(b)
}

func startTracker(ctx context.Context, client *http.Client, bigbrainURL string, d deploymentCfg) *tracker {
	name := d.Name
	if name == "" {
		name = firstLabel(d.Host)
	}
	t := &tracker{
		host:        d.Host,
		siteHost:    d.SiteHost,
		name:        name,
		backends:    d.Backends,
		bigbrainURL: bigbrainURL,
		client:      client,
	}
	initCtx, cancel := context.WithTimeout(ctx, resolveTimeout)
	t.resolveOnce(initCtx)
	cancel()
	go t.resolveLoop(ctx)
	go t.streamBigbrain(ctx)
	log.Printf(
		"usher: configured host=%s siteHost=%q name=%s bigbrain=%q fallback=%v",
		d.Host, d.SiteHost, name, bigbrainURL, d.Backends,
	)
	return t
}

func (t *tracker) resolveLoop(ctx context.Context) {
	tick := time.NewTicker(resolveInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			rctx, rcancel := context.WithTimeout(ctx, resolveTimeout)
			t.resolveOnce(rctx)
			rcancel()
		}
	}
}

func main() {
	cfgPath := flag.String("config", env("USHER_CONFIG", "/etc/usher/config.json"), "config file path")
	addr := flag.String("addr", env("USHER_ADDR", ":8080"), "listen address")
	bigbrainURL := flag.String(
		"bigbrain",
		env("USHER_BIGBRAIN_URL", ""),
		"bigbrain base URL (e.g. http://bigbrain.convex-system.svc.cluster.local:8081)",
	)
	flag.Parse()
	data, err := os.ReadFile(*cfgPath)
	if err != nil {
		log.Fatalf("read config %s: %v", *cfgPath, err)
	}
	var cfg config
	if err = json.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("parse config: %v", err)
	}
	client := &http.Client{Timeout: httpClientTimeout}
	routes := make(map[string]route, len(cfg.Deployments)*routesPerDeployment)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	for _, d := range cfg.Deployments {
		t := startTracker(ctx, client, *bigbrainURL, d)
		routes[d.Host] = route{tracker: t}
		if d.SiteHost != "" {
			routes[d.SiteHost] = route{tracker: t, site: true}
		}
	}
	mux := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/usher/healthz" {
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "ok")
			return
		}
		host := stripPort(r.Host)
		rt, ok := routes[host]
		if !ok {
			http.Error(w, "usher: unknown deployment host "+host, http.StatusNotFound)
			return
		}
		rt.tracker.serveHTTP(w, r, rt.site)
	})
	log.Printf("usher listening on %s, %d deployment(s), bigbrain=%q", *addr, len(cfg.Deployments), *bigbrainURL)
	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: readHeaderTimeout, IdleTimeout: idleTimeout}
	go func() {
		if serr := srv.ListenAndServe(); serr != nil && !errors.Is(serr, http.ErrServerClosed) {
			log.Printf("usher: serve: %v", serr)
			stop()
		}
	}()
	<-ctx.Done()
	log.Print("usher: shutting down")
	shutCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if serr := srv.Shutdown(shutCtx); serr != nil {
		log.Printf("usher: shutdown: %v", serr)
	}
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func stripPort(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}
	return strings.TrimSpace(host)
}

func firstLabel(host string) string {
	h := stripPort(host)
	if before, _, ok := strings.Cut(h, "."); ok {
		return before
	}
	return h
}

func drainClose(rc io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(rc, bodyReadLimit))
	_ = rc.Close()
}
