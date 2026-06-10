package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
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
	"sync/atomic"
	"syscall"
	"time"
)

const (
	bodyReadLimit         = 4096
	defaultMaxBodyBytes   = 128 * 1024 * 1024
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
	routesPerDeployment   = 2
	maxHeaderBytes        = 64 * 1024
	defaultConnIdle       = time.Hour
	connWatchFloor        = 10 * time.Millisecond
	connWatchCeil         = time.Minute
	connWatchDivisor      = 4
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
	Seq       uint64 `json:"seq"`
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
	maxBodyBytes   int64
	client         *http.Client
	streamClient   *http.Client
	proxyTransport *http.Transport
	resolveCh      chan struct{}
	resolveMu      sync.Mutex
	nextResolve    time.Time
	mu             sync.RWMutex
	lastAppliedSeq uint64
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
	tr.MaxIdleConns = 1024
	tr.MaxIdleConnsPerHost = 512
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
			if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
				http.Error(w, "request entity too large", http.StatusRequestEntityTooLarge)
				return
			}
			if r.Context().Err() != nil {
				http.Error(w, "client closed request", http.StatusBadGateway)
				return
			}
			nudge()
			w.Header().Set("Retry-After", "1")
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		},
		ModifyResponse: func(resp *http.Response) error {
			if resp.StatusCode != http.StatusMisdirectedRequest {
				return nil
			}
			_ = resp.Body.Close()
			body := []byte("service unavailable\n")
			resp.StatusCode = http.StatusServiceUnavailable
			resp.Status = "503 " + http.StatusText(http.StatusServiceUnavailable)
			resp.Header = make(http.Header)
			resp.Header.Set("Content-Type", "text/plain; charset=utf-8")
			resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
			resp.Header.Set("Retry-After", "1")
			resp.ContentLength = int64(len(body))
			resp.Body = io.NopCloser(bytes.NewReader(body))
			resp.Trailer = nil
			resp.TransferEncoding = nil
			nudge()
			return nil
		},
	}
	return rp
}

func (t *tracker) setLeader(leaderURL string) bool {
	return t.applyLeader(leaderURL, 0)
}

func (t *tracker) applyLeader(leaderURL string, seq uint64) bool {
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
	if seq != 0 {
		if seq <= t.lastAppliedSeq {
			return false
		}
		t.lastAppliedSeq = seq
	}
	if leaderURL == t.leaderURL {
		return false
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
		su.Path = "/http"
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
	lr, hasLeader, err := t.queryBigbrain(ctx)
	if err != nil {
		return
	}
	if !hasLeader {
		t.applyLeader("", lr.Seq)
		return
	}
	t.applyLeader(lr.LeaderURL, lr.Seq)
}

func (t *tracker) queryBigbrain(ctx context.Context) (leaderResponse, bool, error) {
	u := strings.TrimRight(t.bigbrainURL, "/") + "/registry/deployments/" + t.name + "/leader"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return leaderResponse{}, false, err
	}
	resp, err := t.client.Do(req) //nolint:bodyclose // drainClose drains and closes the body
	if err != nil {
		return leaderResponse{}, false, err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode == http.StatusServiceUnavailable {
		var lr leaderResponse
		_ = json.NewDecoder(io.LimitReader(resp.Body, bodyReadLimit)).Decode(&lr)
		lr.LeaderURL = ""
		return lr, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return leaderResponse{}, false, fmt.Errorf("bigbrain leader query: unexpected status %d", resp.StatusCode)
	}
	var lr leaderResponse
	if err = json.NewDecoder(io.LimitReader(resp.Body, bodyReadLimit)).Decode(&lr); err != nil {
		return leaderResponse{}, false, err
	}
	return lr, lr.LeaderURL != "", nil
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
		backoff *= 2
		if backoff > streamBackoffCap {
			backoff = streamBackoffCap
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
		t.applyLeader(ev.LeaderURL, ev.Seq)
	}
	if scanErr := sc.Err(); scanErr != nil {
		log.Printf("usher: %s leader-stream read error: %v", t.host, scanErr)
		return false
	}
	return time.Since(started) >= streamHealthyAfter
}

func isBlockedControlPath(p string) bool {
	c := strings.ToLower(path.Clean("/" + p))
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
	if t.maxBodyBytes > 0 {
		if r.ContentLength > t.maxBodyBytes {
			http.Error(w, "request entity too large", http.StatusRequestEntityTooLarge)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, t.maxBodyBytes)
	}
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

func startTracker(
	ctx context.Context,
	client *http.Client,
	bigbrainURL string,
	maxBodyBytes int64,
	d deploymentCfg,
) *tracker {
	name := d.Name
	if name == "" {
		name = firstLabel(d.Host)
	}
	t := &tracker{
		host:           d.Host,
		siteHost:       d.SiteHost,
		name:           name,
		bigbrainURL:    bigbrainURL,
		maxBodyBytes:   maxBodyBytes,
		client:         client,
		streamClient:   &http.Client{Transport: newStreamTransport()},
		proxyTransport: newProxyTransport(),
		resolveCh:      make(chan struct{}, 1),
	}
	go t.resolveLoop(ctx)
	go t.streamBigbrain(ctx)
	log.Printf("usher: configured host=%s siteHost=%q name=%s bigbrain=%q", d.Host, d.SiteHost, name, bigbrainURL)
	return t
}

func (t *tracker) resolveLoop(ctx context.Context) {
	tick := time.NewTicker(resolveInterval)
	defer tick.Stop()
	t.resolveWithTimeout(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.resolveCh:
			t.resolveWithTimeout(ctx)
		case <-tick.C:
			t.resolveWithTimeout(ctx)
		}
	}
}

func (t *tracker) resolveWithTimeout(ctx context.Context) {
	rctx, cancel := context.WithTimeout(ctx, resolveTimeout)
	defer cancel()
	t.resolveOnce(rctx)
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
	connIdle, err := parseConnIdle(os.Getenv("USHER_CONN_IDLE_TIMEOUT"))
	if err != nil {
		log.Fatalf("invalid USHER_CONN_IDLE_TIMEOUT: %v", err)
	}
	maxBody, err := parseMaxBodyBytes(os.Getenv("USHER_MAX_BODY_BYTES"))
	if err != nil {
		log.Fatalf("invalid USHER_MAX_BODY_BYTES: %v", err)
	}
	if maxBody == 0 {
		log.Print("usher: request-body cap disabled (USHER_MAX_BODY_BYTES=0)")
	}
	client := &http.Client{Timeout: httpClientTimeout}
	var lc net.ListenConfig
	rawLn, err := lc.Listen(context.Background(), "tcp", *addr)
	if err != nil {
		log.Fatalf("usher: listen %s: %v", *addr, err)
	}
	routes := make(map[string]route, len(cfg.Deployments)*routesPerDeployment)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	for _, d := range cfg.Deployments {
		t := startTracker(ctx, client, *bigbrainURL, maxBody, d)
		routes[strings.ToLower(d.Host)] = route{tracker: t}
		if d.SiteHost != "" {
			routes[strings.ToLower(d.SiteHost)] = route{tracker: t, site: true}
		}
	}
	mux := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/usher/healthz" {
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "ok")
			return
		}
		host := strings.ToLower(stripPort(r.Host))
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
	ln := rawLn
	if connIdle > 0 {
		//nolint:gosec // a parsed time.Duration, not attacker-controlled text
		log.Printf("usher: closing connections with no read/write progress for %s", connIdle.String())
		ln = &activityListener{Listener: ln, idle: connIdle, ctx: ctx}
	}
	go func() {
		if serr := srv.Serve(ln); serr != nil && !errors.Is(serr, http.ErrServerClosed) {
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

type activityListener struct {
	net.Listener

	idle time.Duration
	ctx  context.Context
}

func (l *activityListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	ac := newActivityConn(c)
	go ac.watch(l.ctx, l.idle)
	return ac, nil
}

type activityConn struct {
	net.Conn

	last      atomic.Int64
	closed    chan struct{}
	closeOnce sync.Once
}

func newActivityConn(c net.Conn) *activityConn {
	ac := &activityConn{Conn: c, closed: make(chan struct{})}
	ac.touch()
	return ac
}

func (c *activityConn) touch() {
	c.last.Store(time.Now().UnixNano())
}

func (c *activityConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 {
		c.touch()
	}
	return n, err
}

func (c *activityConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if n > 0 {
		c.touch()
	}
	return n, err
}

func (c *activityConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func (c *activityConn) watch(ctx context.Context, idle time.Duration) {
	tick := min(max(idle/connWatchDivisor, connWatchFloor), connWatchCeil)
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closed:
			return
		case <-t.C:
			if time.Since(time.Unix(0, c.last.Load())) > idle {
				log.Printf("usher: closing connection %s: no progress for %s", c.RemoteAddr(), idle)
				_ = c.Close()
				return
			}
		}
	}
}

func validateConfig(cfg config, bigbrainURL string) error {
	if len(cfg.Deployments) == 0 {
		return errors.New("at least one deployment must be configured")
	}
	u, err := url.ParseRequestURI(bigbrainURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return errors.New("USHER_BIGBRAIN_URL must be an absolute URL")
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
			host = strings.ToLower(host)
			if _, exists := hosts[host]; exists {
				return errors.New("deployment hosts must be unique")
			}
			hosts[host] = struct{}{}
		}
	}
	return nil
}

func parseConnIdle(v string) (time.Duration, error) {
	if v == "" {
		return defaultConnIdle, nil
	}
	return time.ParseDuration(v)
}

func parseMaxBodyBytes(v string) (int64, error) {
	if v == "" {
		return defaultMaxBodyBytes, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %q: %w", v, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("must be >= 0, got %d", n)
	}
	return n, nil
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
