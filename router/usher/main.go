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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	bodyReadLimit           = 4096
	defaultMaxBodyBytes     = 200_000_000
	followerGateHeader      = "X-Concave-Follower-Gate"
	upstreamDialTimeout     = 5 * time.Second
	upstreamKeepAlive       = 15 * time.Second
	upstreamKeepAliveProbes = 3
	streamBufferMax         = 1 << 16
	resolveTimeout          = 3 * time.Second
	resolveInterval         = 60 * time.Second
	forcedResolveInterval   = 1 * time.Second
	fastResolveInterval     = 5 * time.Second
	readHeaderTimeout       = 10 * time.Second
	idleTimeout             = 120 * time.Second
	shutdownTimeout         = 10 * time.Second
	streamBackoffInitial    = 1 * time.Second
	streamBackoffCap        = 15 * time.Second
	streamHealthyAfter      = 30 * time.Second
	streamIdleTimeout       = 75 * time.Second
	instancePrefix          = "/instance"
	healthzPath             = "/usher/healthz"
	readyzPath              = "/usher/readyz"
	routesPerDeployment     = 2
	maxHeaderBytes          = 64 * 1024
	defaultConnIdle         = time.Hour
	connWatchFloor          = 10 * time.Millisecond
	connWatchCeil           = time.Minute
	connWatchDivisor        = 4
)

var (
	labelValueRe = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._-]{0,61}[A-Za-z0-9])?$`)
	hostnameRe   = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$`)
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
	LeaderURL string `json:"leaderUrl"`
	Seq       uint64 `json:"seq"`
	Epoch     uint64 `json:"epoch"`
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
	streamIdle     time.Duration
	client         *http.Client
	streamClient   *http.Client
	proxyTransport *http.Transport
	resolveCh      chan struct{}
	resolveMu      sync.Mutex
	nextResolve    time.Time
	pollGate       stateGate
	streamGate     stateGate
	eventGate      stateGate
	resolved       atomic.Bool
	mu             sync.RWMutex
	lastAppliedSeq uint64
	lastEpoch      uint64
	leaderURL      string
	proxy          *httputil.ReverseProxy
	siteProxy      *httputil.ReverseProxy
}

type stateGate struct {
	down atomic.Bool
}

func (g *stateGate) fail() bool {
	return g.down.CompareAndSwap(false, true)
}

func (g *stateGate) ok() bool {
	return g.down.CompareAndSwap(true, false)
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
	tr.DialContext = (&net.Dialer{
		Timeout: upstreamDialTimeout,
		KeepAliveConfig: net.KeepAliveConfig{
			Enable:   true,
			Idle:     upstreamKeepAlive,
			Interval: upstreamKeepAlive,
			Count:    upstreamKeepAliveProbes,
		},
	}).DialContext
	return tr
}

func newReverseProxy(u *url.URL, nudge func(), tr *http.Transport) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(u)
			pr.Out.Header["X-Forwarded-For"] = pr.In.Header["X-Forwarded-For"]
			pr.SetXForwarded()
			if proto := pr.In.Header.Get("X-Forwarded-Proto"); proto != "" {
				pr.Out.Header.Set("X-Forwarded-Proto", proto)
			}
			pr.Out.Host = pr.In.Host
		},
		Transport:     tr,
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if r.Context().Err() != nil {
				return
			}
			if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
				http.Error(w, "request entity too large", http.StatusRequestEntityTooLarge)
				return
			}
			log.Printf("usher: upstream error: %v", err)
			nudge()
			w.Header().Set("Retry-After", "1")
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		},
		ModifyResponse: func(resp *http.Response) error {
			gated := resp.Header.Get(followerGateHeader) != ""
			resp.Header.Del(followerGateHeader)
			if resp.StatusCode != http.StatusMisdirectedRequest || !gated {
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
}

func (t *tracker) applyLeader(lr leaderResponse, fromPoll bool) bool {
	var u *url.URL
	var err error
	leaderURL := lr.LeaderURL
	if leaderURL != "" {
		u, err = url.ParseRequestURI(leaderURL)
		if err != nil || u.Scheme == "" || u.Host == "" {
			log.Printf("usher: %s bad leader url %q: %v", t.host, leaderURL, err)
			return false
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if lr.Epoch != t.lastEpoch {
		if !fromPoll {
			t.nudgeResolve()
			return false
		}
		t.lastEpoch = lr.Epoch
		t.lastAppliedSeq = 0
	}
	if lr.Seq <= t.lastAppliedSeq {
		return false
	}
	t.lastAppliedSeq = lr.Seq
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
	defer t.resolved.Store(true)
	lr, err := t.queryBigbrain(ctx)
	if err != nil {
		if !errors.Is(ctx.Err(), context.Canceled) && t.pollGate.fail() {
			log.Printf("usher: %s leader poll failed: %v", t.host, err)
		}
		return
	}
	if t.pollGate.ok() {
		log.Printf("usher: %s leader poll recovered", t.host)
	}
	t.applyLeader(lr, true)
}

func (t *tracker) queryBigbrain(ctx context.Context) (leaderResponse, error) {
	u := strings.TrimRight(t.bigbrainURL, "/") + "/registry/deployments/" + t.name + "/leader"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return leaderResponse{}, err
	}
	resp, err := t.client.Do(req) //nolint:bodyclose // drainClose drains and closes the body
	if err != nil {
		return leaderResponse{}, err
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusServiceUnavailable {
		return leaderResponse{}, fmt.Errorf("bigbrain leader query: unexpected status %d", resp.StatusCode)
	}
	var lr leaderResponse
	if err = json.NewDecoder(io.LimitReader(resp.Body, bodyReadLimit)).Decode(&lr); err != nil {
		return leaderResponse{}, err
	}
	if resp.StatusCode == http.StatusOK {
		return lr, nil
	}
	if lr.Epoch == 0 {
		return leaderResponse{}, errors.New("bigbrain leader query: 503 without epoch is not authoritative")
	}
	lr.LeaderURL = ""
	return lr, nil
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
	idle := t.streamIdle
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	resp, err := t.openStream(sctx, u)
	if err != nil {
		if ctx.Err() == nil && t.streamGate.fail() {
			log.Printf("usher: %s leader-stream connect failed: %v", t.host, err)
			t.nudgeResolve()
		}
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	if t.streamGate.ok() {
		log.Printf("usher: %s leader-stream connected", t.host)
	}
	watchdog := time.AfterFunc(idle, cancel)
	defer watchdog.Stop()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, bodyReadLimit), streamBufferMax)
	for sc.Scan() {
		watchdog.Reset(idle)
		t.handleStreamLine(sc.Text())
	}
	healthy := time.Since(started) >= streamHealthyAfter
	t.logStreamEnd(ctx, sctx, sc.Err(), idle)
	return healthy
}

func (t *tracker) openStream(ctx context.Context, u string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := t.streamClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return resp, nil
}

func (t *tracker) handleStreamLine(line string) {
	data, ok := strings.CutPrefix(line, "data: ")
	if !ok {
		return
	}
	var ev leaderResponse
	if err := json.Unmarshal([]byte(data), &ev); err != nil {
		if t.eventGate.fail() {
			log.Printf("usher: %s leader-stream bad event: %v", t.host, err)
		}
		return
	}
	t.eventGate.ok()
	t.applyLeader(ev, false)
}

func (t *tracker) logStreamEnd(ctx, sctx context.Context, scanErr error, idle time.Duration) {
	if scanErr == nil || ctx.Err() != nil {
		return
	}
	if sctx.Err() != nil {
		log.Printf("usher: %s leader-stream idle for %s, reconnecting", t.host, idle)
		return
	}
	log.Printf("usher: %s leader-stream read error: %v", t.host, scanErr)
}

func isBlockedAPIPath(p string) bool {
	for _, candidate := range []string{p, path.Clean("/" + p)} {
		c := strings.ToLower(candidate)
		if c == instancePrefix || strings.HasPrefix(c, instancePrefix+"/") || c == "/http" ||
			strings.HasPrefix(c, "/http/") {
			return true
		}
	}
	return false
}

func (t *tracker) serveHTTP(w http.ResponseWriter, r *http.Request, site bool) {
	if !site && isBlockedAPIPath(r.URL.Path) {
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
		w.Header().Set("Retry-After", "1")
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	_ = http.NewResponseController(w).EnableFullDuplex()
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
	t := &tracker{
		host:           d.Host,
		siteHost:       d.SiteHost,
		name:           d.Name,
		bigbrainURL:    bigbrainURL,
		maxBodyBytes:   maxBodyBytes,
		streamIdle:     streamIdleTimeout,
		client:         client,
		streamClient:   &http.Client{Transport: newStreamTransport()},
		proxyTransport: newProxyTransport(),
		resolveCh:      make(chan struct{}, 1),
	}
	go t.resolveLoop(ctx)
	go t.streamBigbrain(ctx)
	//nolint:gosec // operator-configured values, not request data
	log.Printf("usher: configured host=%s siteHost=%q name=%s bigbrain=%q", d.Host, d.SiteHost, d.Name, bigbrainURL)
	return t
}

func (t *tracker) resolveLoop(ctx context.Context) {
	tick := time.NewTicker(t.pollInterval())
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
		tick.Reset(t.pollInterval())
	}
}

func (t *tracker) pollInterval() time.Duration {
	if t.streamGate.down.Load() {
		return fastResolveInterval
	}
	return resolveInterval
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

func newMux(routes map[string]route, trackers []*tracker, monoHost string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := strings.ToLower(stripPort(r.Host))
		rt, ok := routes[host]
		if !ok && monoHost != "" && !isProbePath(r.URL.Path) {
			rt, ok = routes[monoHost]
		}
		if !ok {
			serveProbe(w, r, trackers)
			return
		}
		rt.tracker.serveHTTP(w, r, rt.site)
	})
}

func isProbePath(p string) bool {
	return p == healthzPath || p == readyzPath
}

func serveProbe(w http.ResponseWriter, r *http.Request, trackers []*tracker) {
	if !isProbePath(r.URL.Path) {
		http.Error(w, "unknown deployment host", http.StatusNotFound)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.URL.Path == readyzPath {
		for _, t := range trackers {
			if !t.resolved.Load() {
				http.Error(w, "resolving leader", http.StatusServiceUnavailable)
				return
			}
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok")
}

func main() {
	cfgPath := flag.String("config", "/etc/usher/config.json", "config file path")
	addr := flag.String("addr", ":8080", "listen address")
	bigbrainURL := flag.String(
		"bigbrain",
		os.Getenv("USHER_BIGBRAIN_URL"),
		"bigbrain base URL (e.g. http://bigbrain.convex-system.svc.cluster.local:8081)",
	)
	mono := flag.Bool(
		"mono",
		false,
		"route every request to the single configured deployment regardless of Host (requires exactly one deployment)",
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
	if err = validateConfig(cfg, *bigbrainURL, *mono); err != nil {
		log.Fatalf("invalid config: %v", err)
	}
	connIdle, err := parseConnIdle(os.Getenv("USHER_CONN_IDLE_TIMEOUT"))
	if err != nil {
		log.Fatalf("invalid USHER_CONN_IDLE_TIMEOUT: %v", err)
	}
	if connIdle == 0 {
		log.Print("usher: connection idle watchdog disabled (USHER_CONN_IDLE_TIMEOUT=0)")
	}
	maxBody, err := parseMaxBodyBytes(os.Getenv("USHER_MAX_BODY_BYTES"))
	if err != nil {
		log.Fatalf("invalid USHER_MAX_BODY_BYTES: %v", err)
	}
	if maxBody == 0 {
		log.Print("usher: request-body cap disabled (USHER_MAX_BODY_BYTES=0)")
	}
	var lc net.ListenConfig
	rawLn, err := lc.Listen(context.Background(), "tcp", *addr)
	if err != nil {
		log.Fatalf("usher: listen %s: %v", *addr, err)
	}
	os.Exit(run(cfg, *addr, *bigbrainURL, connIdle, maxBody, *mono, rawLn))
}

func run(
	cfg config,
	addr, bigbrainURL string,
	connIdle time.Duration,
	maxBody int64,
	mono bool,
	rawLn net.Listener,
) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client := &http.Client{Transport: newStreamTransport()}
	routes := make(map[string]route, len(cfg.Deployments)*routesPerDeployment)
	trackers := make([]*tracker, 0, len(cfg.Deployments))
	var monoHost string
	for _, d := range cfg.Deployments {
		t := startTracker(ctx, client, bigbrainURL, maxBody, d)
		trackers = append(trackers, t)
		routes[strings.ToLower(d.Host)] = route{tracker: t}
		if d.SiteHost != "" {
			routes[strings.ToLower(d.SiteHost)] = route{tracker: t, site: true}
		}
		if mono {
			monoHost = strings.ToLower(d.Host)
		}
	}
	//nolint:gosec // operator-configured values, not request data
	log.Printf("usher listening on %s, %d deployment(s), bigbrain=%q", addr, len(cfg.Deployments), bigbrainURL)
	srv := &http.Server{
		Addr:              addr,
		Handler:           newMux(routes, trackers, monoHost),
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
	serveErr := make(chan error, 1)
	go func() {
		serr := srv.Serve(ln)
		if serr != nil && !errors.Is(serr, http.ErrServerClosed) {
			log.Printf("usher: serve: %v", serr)
			serveErr <- serr
			stop()
			return
		}
		serveErr <- nil
	}()
	<-ctx.Done()
	log.Print("usher: shutting down")
	shutCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if serr := srv.Shutdown(shutCtx); serr != nil {
		log.Printf("usher: shutdown: %v", serr)
	}
	if <-serveErr != nil {
		return 1
	}
	return 0
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

func validateConfig(cfg config, bigbrainURL string, mono bool) error {
	if len(cfg.Deployments) == 0 {
		return errors.New("at least one deployment must be configured")
	}
	if mono && len(cfg.Deployments) != 1 {
		return errors.New("mono mode requires exactly one deployment")
	}
	u, err := url.ParseRequestURI(bigbrainURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New("USHER_BIGBRAIN_URL must be an absolute http(s) URL")
	}
	return validateDeployments(cfg.Deployments)
}

func validateDeployments(deployments []deploymentCfg) error {
	hosts := make(map[string]struct{}, len(deployments)*routesPerDeployment)
	names := make(map[string]string, len(deployments))
	for _, d := range deployments {
		if d.Host == "" {
			return errors.New("deployment host must not be empty")
		}
		name := d.Name
		if !labelValueRe.MatchString(name) {
			return fmt.Errorf("deployment %s: name %q must be a Kubernetes label value", d.Host, name)
		}
		if prev, dup := names[name]; dup {
			return fmt.Errorf("deployments %q and %q resolve to the same name %q", prev, d.Host, name)
		}
		names[name] = d.Host
		for _, host := range []string{d.Host, d.SiteHost} {
			if host == "" {
				continue
			}
			if err := validateHost(host); err != nil {
				return err
			}
			host = strings.ToLower(host)
			if _, exists := hosts[host]; exists {
				return fmt.Errorf("duplicate deployment host %q", host)
			}
			hosts[host] = struct{}{}
		}
	}
	return nil
}

func validateHost(host string) error {
	if !hostnameRe.MatchString(strings.ToLower(host)) {
		return fmt.Errorf("deployment host %q must be a bare hostname", host)
	}
	return nil
}

func parseConnIdle(v string) (time.Duration, error) {
	if v == "" {
		return defaultConnIdle, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("parse %q: %w", v, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("must be >= 0, got %s", d)
	}
	return d, nil
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

func stripPort(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}
	return host
}

func drainClose(rc io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(rc, bodyReadLimit))
	_ = rc.Close()
}
