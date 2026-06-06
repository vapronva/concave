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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	bodyReadLimit         = 4096
	maxRequestBodyBytes   = 128 * 1024 * 1024
	streamBufferMax       = 1 << 16
	httpClientTimeout     = 3 * time.Second
	resolveTimeout        = 3 * time.Second
	resolveInterval       = 60 * time.Second
	forcedResolveInterval = 1 * time.Second
	readHeaderTimeout     = 10 * time.Second
	idleTimeout           = 120 * time.Second
	shutdownTimeout       = 10 * time.Second
	streamBackoffInitial  = 1 * time.Second
	streamBackoffCap      = 15 * time.Second
	streamHealthyAfter    = 30 * time.Second
	instancePrefix        = "/instance"
	sitePort              = "3211"
	routesPerDeployment   = 2
	maxHeaderBytes        = 64 * 1024
)

type deploymentCfg struct {
	Host     string `json:"host"`
	SiteHost string `json:"siteHost"`
	Name     string `json:"name"`
}

type config struct {
	Deployments []deploymentCfg `json:"deployments"`
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
	host           string
	siteHost       string
	name           string
	bigbrainURL    string
	client         *http.Client
	streamClient   *http.Client
	proxyTransport *http.Transport
	resolveCh      chan struct{}
	resolveMu      sync.Mutex
	nextResolve    time.Time
	mu             sync.RWMutex
	leaderURL      string
	proxy          *httputil.ReverseProxy
	siteProxy      *httputil.ReverseProxy
}

func clonedDefaultTransport() *http.Transport {
	if dt, ok := http.DefaultTransport.(*http.Transport); ok {
		return dt.Clone()
	}
	return &http.Transport{}
}

func newProxyTransport() *http.Transport {
	tr := clonedDefaultTransport()
	tr.Proxy = nil
	tr.MaxIdleConns = 256
	tr.MaxIdleConnsPerHost = 64
	return tr
}

func newReverseProxy(u *url.URL, nudge func(), tr *http.Transport) *httputil.ReverseProxy {
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(u)
			pr.SetXForwarded()
			pr.Out.Host = pr.In.Host
		},
		Transport:     tr,
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			log.Printf("usher: upstream error: %v", err)
			var maxBytesError *http.MaxBytesError
			if errors.As(err, &maxBytesError) {
				http.Error(w, "request entity too large", http.StatusRequestEntityTooLarge)
				return
			}
			if r.Context().Err() == nil {
				nudge()
			}
			http.Error(w, "bad gateway", http.StatusBadGateway)
		},
		ModifyResponse: func(resp *http.Response) error {
			if resp.StatusCode != http.StatusMisdirectedRequest {
				return nil
			}
			_ = resp.Body.Close()
			body := []byte("service unavailable\n")
			resp.StatusCode = http.StatusServiceUnavailable
			resp.Status = http.StatusText(http.StatusServiceUnavailable)
			resp.Header = make(http.Header)
			resp.Header.Set("Content-Type", "text/plain; charset=utf-8")
			resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
			resp.Header.Set("Retry-After", "1")
			resp.ContentLength = int64(len(body))
			resp.Body = io.NopCloser(strings.NewReader(string(body)))
			resp.Trailer = nil
			resp.TransferEncoding = nil
			nudge()
			return nil
		},
	}
	return rp
}

func (t *tracker) setLeader(leaderURL string) bool {
	var u *url.URL
	var err error
	if leaderURL != "" {
		u, err = url.ParseRequestURI(leaderURL)
		if err != nil || u.Scheme == "" || u.Host == "" {
			log.Printf("usher: %s bad leader url %q: %v", t.host, leaderURL, err)
			return false
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if leaderURL == t.leaderURL {
		return false
	}
	if t.proxyTransport == nil {
		t.proxyTransport = newProxyTransport()
	}
	t.proxyTransport.CloseIdleConnections()
	if leaderURL == "" {
		t.leaderURL, t.proxy, t.siteProxy = "", nil, nil
		log.Printf("usher: %s leader cleared (none available)", t.host)
		return true
	}
	t.leaderURL, t.proxy = leaderURL, newReverseProxy(u, t.nudgeResolve, t.proxyTransport)
	if t.siteHost != "" {
		su := *u
		su.Host = net.JoinHostPort(u.Hostname(), sitePort)
		t.siteProxy = newReverseProxy(&su, t.nudgeResolve, t.proxyTransport)
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
	if leader, ok := t.queryBigbrain(ctx); ok {
		t.setLeader(leader)
	}
}

func (t *tracker) queryBigbrain(ctx context.Context) (string, bool) {
	u := strings.TrimRight(t.bigbrainURL, "/") + "/registry/deployments/" + t.name + "/leader"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", false
	}
	resp, err := t.client.Do(req) //nolint:bodyclose // drainClose drains and closes the body
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

func (t *tracker) streamBigbrain(ctx context.Context) {
	u := strings.TrimRight(t.bigbrainURL, "/") + "/registry/deployments/" + t.name + "/leader-stream"
	backoff := streamBackoffInitial
	for {
		if ctx.Err() != nil {
			return
		}
		healthy := t.consumeStream(ctx, u)
		if healthy {
			backoff = streamBackoffInitial
		}
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

func (t *tracker) consumeStream(ctx context.Context, u string) bool {
	started := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return false
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := t.streamClient.Do(req) //nolint:bodyclose // drainClose drains and closes the body
	if err != nil {
		return false
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return false
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
	return time.Since(started) >= streamHealthyAfter
}

func isBlockedControlPath(p string) bool {
	c := strings.ToLower(path.Clean("/" + strings.TrimSuffix(p, "/")))
	return c == instancePrefix || strings.HasPrefix(c, instancePrefix+"/")
}

func (t *tracker) serveHTTP(w http.ResponseWriter, r *http.Request, site bool) {
	if !site && isBlockedControlPath(r.URL.Path) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if !allowedMethod(r.Method) {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.ContentLength > maxRequestBodyBytes {
		http.Error(w, "request entity too large", http.StatusRequestEntityTooLarge)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	p := t.currentLeader(site)
	if p == nil {
		log.Printf("usher: %s no leader available", t.host)
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	p.ServeHTTP(w, r)
}

func allowedMethod(method string) bool {
	switch method {
	case http.MethodGet,
		http.MethodHead,
		http.MethodPost,
		http.MethodPut,
		http.MethodPatch,
		http.MethodDelete,
		http.MethodOptions:
		return true
	default:
		return false
	}
}

func startTracker(ctx context.Context, client *http.Client, bigbrainURL string, d deploymentCfg) *tracker {
	name := d.Name
	if name == "" {
		name = firstLabel(d.Host)
	}
	t := &tracker{
		host:           d.Host,
		siteHost:       d.SiteHost,
		name:           name,
		bigbrainURL:    bigbrainURL,
		client:         client,
		streamClient:   &http.Client{Transport: newStreamTransport()},
		proxyTransport: newProxyTransport(),
		resolveCh:      make(chan struct{}, 1),
	}
	initCtx, cancel := context.WithTimeout(ctx, resolveTimeout)
	t.resolveOnce(initCtx)
	cancel()
	go t.resolveLoop(ctx)
	go t.streamBigbrain(ctx)
	log.Printf("usher: configured host=%s siteHost=%q name=%s bigbrain=%q", d.Host, d.SiteHost, name, bigbrainURL)
	return t
}

func (t *tracker) resolveLoop(ctx context.Context) {
	tick := time.NewTicker(resolveInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.resolveCh:
			rctx, rcancel := context.WithTimeout(ctx, resolveTimeout)
			t.resolveOnce(rctx)
			rcancel()
		case <-tick.C:
			rctx, rcancel := context.WithTimeout(ctx, resolveTimeout)
			t.resolveOnce(rctx)
			rcancel()
		}
	}
}

func (t *tracker) nudgeResolve() {
	t.resolveMu.Lock()
	if time.Now().Before(t.nextResolve) {
		t.resolveMu.Unlock()
		return
	}
	t.nextResolve = time.Now().Add(forcedResolveInterval)
	t.resolveMu.Unlock()
	select {
	case t.resolveCh <- struct{}{}:
	default:
	}
}

func newStreamTransport() *http.Transport {
	tr := clonedDefaultTransport()
	tr.Proxy = nil
	tr.ResponseHeaderTimeout = resolveTimeout
	tr.MaxIdleConns = 16
	tr.MaxIdleConnsPerHost = 4
	tr.IdleConnTimeout = idleTimeout
	return tr
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
	if err = validateConfig(cfg, *bigbrainURL); err != nil {
		log.Fatalf("invalid config: %v", err)
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
			http.Error(w, "unknown deployment host", http.StatusNotFound)
			return
		}
		rt.tracker.serveHTTP(w, r, rt.site)
	})
	log.Printf("usher listening on %s, %d deployment(s), bigbrain=%q", *addr, len(cfg.Deployments), *bigbrainURL)
	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}
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

func validateConfig(cfg config, bigbrainURL string) error {
	if len(cfg.Deployments) == 0 {
		return errors.New("at least one deployment must be configured")
	}
	u, err := url.ParseRequestURI(bigbrainURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return errors.New("USHER_BIGBRAIN_URL must be an absolute URL when deployments are configured")
	}
	hosts := make(map[string]struct{}, len(cfg.Deployments)*routesPerDeployment)
	for _, d := range cfg.Deployments {
		if d.Host == "" {
			return errors.New("deployment host must not be empty")
		}
		for _, host := range []string{d.Host, d.SiteHost} {
			if host == "" {
				continue
			}
			if _, exists := hosts[host]; exists {
				return errors.New("deployment hosts must be unique")
			}
			hosts[host] = struct{}{}
		}
	}
	return nil
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
