package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestIsBlockedControlPath(t *testing.T) {
	t.Parallel()
	blocked := []string{
		"/instance",
		"/instance/",
		"/instance/promote",
		"/instance/demote",
		"/instance/promote/",
		"/instance/demote/",
		"/instance/leadership",
		"/instance/foo",
		"/instance/promotething",
		"/instance/./promote",
		"/instance/../instance/promote",
		"/INSTANCE/promote",
		"/InStAnCe/leadership",
	}
	for _, p := range blocked {
		if !isBlockedControlPath(p) {
			t.Errorf("path %q should be blocked", p)
		}
	}
	allowed := []string{
		"/api/query",
		"/sync",
		"/",
		"/instances",
		"/instancex",
		"/instance_name",
		"/instance_version",
	}
	for _, p := range allowed {
		if isBlockedControlPath(p) {
			t.Errorf("path %q should NOT be blocked", p)
		}
	}
}

func TestServeHTTP_RejectsActuationPaths(t *testing.T) {
	t.Parallel()
	var upstreamHits []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits = append(upstreamHits, r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "upstream-reached")
	}))
	defer upstream.Close()
	tr := &tracker{host: "convex-dev.localtest.me", client: &http.Client{}, proxyTransport: newProxyTransport()}
	if !tr.setLeader(upstream.URL) {
		t.Fatal("setLeader should install the proxy")
	}
	cases := []struct {
		method, path     string
		wantCode         int
		wantUpstreamHit  bool
		wantBodyContains string
	}{
		{http.MethodPost, "/instance/promote", http.StatusNotFound, false, ""},
		{http.MethodPost, "/instance/demote", http.StatusNotFound, false, ""},
		{http.MethodGet, "/instance/promote", http.StatusNotFound, false, ""},
		{http.MethodPost, "/instance/promote/", http.StatusNotFound, false, ""},
		{http.MethodGet, "/instance/leadership", http.StatusNotFound, false, ""},
		{http.MethodGet, "/instance/foo", http.StatusNotFound, false, ""},
		{http.MethodGet, "/INSTANCE/promote", http.StatusNotFound, false, ""},
		{http.MethodGet, "/instance_name", http.StatusOK, true, "upstream-reached"},
		{http.MethodGet, "/instance_version", http.StatusOK, true, "upstream-reached"},
		{http.MethodPost, "/api/mutation", http.StatusOK, true, "upstream-reached"},
	}
	for _, c := range cases {
		upstreamHits = nil
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(c.method, c.path, nil)
		tr.serveHTTP(rr, req, false)
		if rr.Code != c.wantCode {
			t.Errorf("%s %s: want status %d, got %d", c.method, c.path, c.wantCode, rr.Code)
		}
		hit := len(upstreamHits) > 0
		if hit != c.wantUpstreamHit {
			t.Errorf(
				"%s %s: upstream hit = %v, want %v (hits=%v)",
				c.method,
				c.path,
				hit,
				c.wantUpstreamHit,
				upstreamHits,
			)
		}
		if c.wantBodyContains != "" && !strings.Contains(rr.Body.String(), c.wantBodyContains) {
			t.Errorf("%s %s: body %q does not contain %q", c.method, c.path, rr.Body.String(), c.wantBodyContains)
		}
	}
}

func TestServeHTTP_SiteRoutePassesControlPaths(t *testing.T) {
	t.Parallel()
	var upstreamHits []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits = append(upstreamHits, r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "site-reached")
	}))
	defer upstream.Close()
	u, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream url: %v", err)
	}
	tr := &tracker{
		host:           "api.convex.localtest.me",
		siteHost:       "convex.localtest.me",
		client:         &http.Client{},
		proxyTransport: newProxyTransport(),
	}
	tr.siteProxy = newReverseProxy(u, func() {}, newProxyTransport())
	for _, p := range []string{"/instance/leadership", "/instance/promote", "/instance_version", "/arbitrary/action"} {
		upstreamHits = nil
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, p, nil)
		tr.serveHTTP(rr, req, true)
		if rr.Code != http.StatusOK {
			t.Errorf("site %s: want status %d, got %d", p, http.StatusOK, rr.Code)
		}
		if len(upstreamHits) == 0 {
			t.Errorf("site %s: upstream not reached", p)
		}
		if !strings.Contains(rr.Body.String(), "site-reached") {
			t.Errorf("site %s: body %q does not contain %q", p, rr.Body.String(), "site-reached")
		}
	}
}

func TestServeHTTP_MisdirectedResponseIsCleanAndNudgesResolve(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "14")
		w.Header().Set("X-Internal", "secret")
		w.WriteHeader(http.StatusMisdirectedRequest)
		_, _ = io.WriteString(w, "wrong backend\n")
	}))
	defer upstream.Close()
	tr := &tracker{host: "api.example", resolveCh: make(chan struct{}, 1), proxyTransport: newProxyTransport()}
	if !tr.setLeader(upstream.URL) {
		t.Fatal("setLeader should install proxy")
	}
	rr := httptest.NewRecorder()
	tr.serveHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/query", nil), false)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503", rr.Code)
	}
	if rr.Header().Get("X-Internal") != "" {
		t.Fatal("synthesized 503 leaked an upstream header")
	}
	if got, want := rr.Header().Get("Content-Length"), "20"; got != want {
		t.Fatalf("Content-Length=%q want %q", got, want)
	}
	select {
	case <-tr.resolveCh:
	case <-time.After(time.Second):
		t.Fatal("misdirected response did not nudge resolution")
	}
}

func TestServeHTTP_DeadLeaderReturns503AndNudges(t *testing.T) {
	t.Parallel()
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := dead.URL
	dead.Close()
	tr := &tracker{host: "api.example", resolveCh: make(chan struct{}, 1), proxyTransport: newProxyTransport()}
	if !tr.setLeader(deadURL) {
		t.Fatal("setLeader should install proxy")
	}
	rr := httptest.NewRecorder()
	tr.serveHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/query", nil), false)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503 for a dead leader (retriable failover signal)", rr.Code)
	}
	if rr.Header().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After=%q want 1", rr.Header().Get("Retry-After"))
	}
	select {
	case <-tr.resolveCh:
	case <-time.After(time.Second):
		t.Fatal("dead-leader transport error did not nudge resolution")
	}
}

func TestServeHTTP_ForwardsPublicRequestMetadata(t *testing.T) {
	t.Parallel()
	var gotProto, gotHost, gotXFF string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotProto = r.Header.Get("X-Forwarded-Proto")
		gotHost = r.Header.Get("X-Forwarded-Host")
		gotXFF = r.Header.Get("X-Forwarded-For")
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	tr := &tracker{host: "api.example", resolveCh: make(chan struct{}, 1), proxyTransport: newProxyTransport()}
	if !tr.setLeader(upstream.URL) {
		t.Fatal("setLeader should install proxy")
	}
	req := httptest.NewRequest(http.MethodGet, "https://api.example/api/query", nil)
	req.RemoteAddr = "192.0.2.10:4567"
	req.Host = "api.example"
	tr.serveHTTP(httptest.NewRecorder(), req, false)
	if gotProto != "https" || gotHost != "api.example" || gotXFF != "192.0.2.10" {
		t.Fatalf("forwarded proto/host/for = %q/%q/%q", gotProto, gotHost, gotXFF)
	}
}

func TestServeHTTP_UpgradeKeepsHijacker(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(r.Header.Get("Connection"), "upgrade") {
			http.Error(w, "missing upgrade", http.StatusBadRequest)
			return
		}
		h, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "upstream cannot hijack", http.StatusInternalServerError)
			return
		}
		conn, rw, err := h.Hijack()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n")
		_ = rw.Flush()
	}))
	defer upstream.Close()
	tr := &tracker{host: "api.example", resolveCh: make(chan struct{}, 1), proxyTransport: newProxyTransport()}
	if !tr.setLeader(upstream.URL) {
		t.Fatal("setLeader should install proxy")
	}
	edge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tr.serveHTTP(w, r, false)
	}))
	defer edge.Close()
	u, _ := url.Parse(edge.URL)
	conn, err := net.Dial("tcp", u.Host)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = io.WriteString(
		conn,
		"GET /sync HTTP/1.1\r\nHost: api.example\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n",
	)
	buf := make([]byte, 128)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(buf[:n]), "101 Switching Protocols") {
		t.Fatalf("upgrade response=%q", buf[:n])
	}
}

func TestNudgeResolveDeduplicates(t *testing.T) {
	t.Parallel()
	tr := &tracker{resolveCh: make(chan struct{}, 1), proxyTransport: newProxyTransport()}
	sent := 0
	for range 100 {
		tr.nudgeResolve()
	}
	for {
		select {
		case <-tr.resolveCh:
			sent++
		default:
			if sent != 1 {
				t.Fatalf("nudges queued=%d want 1", sent)
			}
			return
		}
	}
}

func TestServeHTTP_RejectsKnownOversizedBody(t *testing.T) {
	t.Parallel()
	var hits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	tr := &tracker{
		host:           "api.example",
		maxBodyBytes:   defaultMaxBodyBytes,
		resolveCh:      make(chan struct{}, 1),
		proxyTransport: newProxyTransport(),
	}
	if !tr.setLeader(upstream.URL) {
		t.Fatal("setLeader should install proxy")
	}
	req := httptest.NewRequest(http.MethodPost, "/api/mutation", strings.NewReader("x"))
	req.ContentLength = defaultMaxBodyBytes + 1
	rr := httptest.NewRecorder()
	tr.serveHTTP(rr, req, false)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d want 413", rr.Code)
	}
	if hits.Load() != 0 {
		t.Fatalf("upstream hits=%d want 0", hits.Load())
	}
}

func TestServeHTTP_ZeroMaxBodyBytesDisablesCap(t *testing.T) {
	t.Parallel()
	var hits atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	newTracker := func(maxBody int64) *tracker {
		tr := &tracker{
			host:           "api.example",
			maxBodyBytes:   maxBody,
			resolveCh:      make(chan struct{}, 1),
			proxyTransport: newProxyTransport(),
		}
		if !tr.setLeader(upstream.URL) {
			t.Fatal("setLeader should install proxy")
		}
		return tr
	}
	const body = "larger-than-four-bytes"
	capped := newTracker(4)
	rr := httptest.NewRecorder()
	capped.serveHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/storage/upload", strings.NewReader(body)), false)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("capped: status=%d want 413", rr.Code)
	}
	if hits.Load() != 0 {
		t.Fatalf("capped: upstream hits=%d want 0", hits.Load())
	}
	disabled := newTracker(0)
	rr = httptest.NewRecorder()
	disabled.serveHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/storage/upload", strings.NewReader(body)), false)
	if rr.Code != http.StatusOK {
		t.Fatalf("disabled: status=%d want 200 (no body cap)", rr.Code)
	}
	if hits.Load() != 1 {
		t.Fatalf("disabled: upstream hits=%d want 1", hits.Load())
	}
}

func TestParseMaxBodyBytes(t *testing.T) {
	t.Parallel()
	if got, err := parseMaxBodyBytes(""); err != nil || got != defaultMaxBodyBytes {
		t.Fatalf("empty: got %d err=%v, want default %d", got, err, int64(defaultMaxBodyBytes))
	}
	if got, err := parseMaxBodyBytes("1024"); err != nil || got != 1024 {
		t.Fatalf("1024: got %d err=%v", got, err)
	}
	if got, err := parseMaxBodyBytes("0"); err != nil || got != 0 {
		t.Fatalf("0 (disabled): got %d err=%v", got, err)
	}
	for _, bad := range []string{"garbage", "-1", "128MiB", "1.5"} {
		if _, err := parseMaxBodyBytes(bad); err == nil {
			t.Fatalf("%q: want parse error", bad)
		}
	}
}

func TestValidateConfig(t *testing.T) {
	t.Parallel()
	cfg := config{Deployments: []deploymentCfg{{Host: "api.example"}}}
	if err := validateConfig(cfg, ""); err == nil {
		t.Fatal("missing bigbrain URL must fail")
	}
	if err := validateConfig(cfg, "http://bigbrain:8081"); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

func TestSetLeader_SiteProxyRoutesToHttpPathOnCloudPort(t *testing.T) {
	t.Parallel()
	tr := &tracker{
		host:           "api.convex.localtest.me",
		siteHost:       "convex.localtest.me",
		client:         &http.Client{},
		proxyTransport: newProxyTransport(),
	}
	if !tr.setLeader("http://10.0.0.7:3210") {
		t.Fatal("setLeader should install proxies")
	}
	if ap := tr.currentLeader(false); ap == nil {
		t.Fatal("api proxy should be installed")
	}
	if host, path := siteUpstream(t, tr); host != "10.0.0.7:3210" || path != "/http/api/actions" {
		t.Errorf("site upstream = %q %q, want host %q path %q (cloud port + /http prefix, not the dev site proxy)",
			host, path, "10.0.0.7:3210", "/http/api/actions")
	}
	if !tr.setLeader("http://10.0.0.42:3210") {
		t.Fatal("setLeader should swap to the new leader")
	}
	if host, _ := siteUpstream(t, tr); host != "10.0.0.42:3210" {
		t.Errorf("after failover site upstream host = %q, want %q", host, "10.0.0.42:3210")
	}
}

func siteUpstream(t *testing.T, tr *tracker) (string, string) {
	t.Helper()
	sp := tr.currentLeader(true)
	if sp == nil {
		t.Fatal("site proxy should be installed when siteHost is set")
	}
	in := httptest.NewRequest(http.MethodGet, "/api/actions", nil)
	out := in.Clone(in.Context())
	sp.Rewrite(&httputil.ProxyRequest{In: in, Out: out})
	return out.URL.Host, out.URL.Path
}

func TestSetLeader_NoSiteProxyWithoutSiteHost(t *testing.T) {
	t.Parallel()
	tr := &tracker{host: "api.convex.localtest.me", client: &http.Client{}, proxyTransport: newProxyTransport()}
	if !tr.setLeader("http://10.0.0.7:3210") {
		t.Fatal("setLeader should install api proxy")
	}
	if sp := tr.currentLeader(true); sp != nil {
		t.Error("site proxy should be nil when siteHost is unset")
	}
}

func TestResolveOnce_ClearsLeaderOnAuthoritativeNoLeader(t *testing.T) {
	t.Parallel()
	t.Run("status_503_clears", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"no leader"}`))
		}))
		defer srv.Close()
		tr := &tracker{name: "test", bigbrainURL: srv.URL, client: &http.Client{}, proxyTransport: newProxyTransport()}
		tr.setLeader("http://10.0.0.5:3210")
		tr.resolveOnce(context.Background())
		if tr.currentLeader(false) != nil {
			t.Fatal("503 no-leader must clear the leader, matching SSE")
		}
	})
	t.Run("status_200_empty_url_clears", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"leaderUrl":""}`))
		}))
		defer srv.Close()
		tr := &tracker{name: "test", bigbrainURL: srv.URL, client: &http.Client{}, proxyTransport: newProxyTransport()}
		tr.setLeader("http://10.0.0.5:3210")
		tr.resolveOnce(context.Background())
		if tr.currentLeader(false) != nil {
			t.Fatal("200 with empty leaderUrl must clear the leader")
		}
	})
	t.Run("transport_error_preserves", func(t *testing.T) {
		t.Parallel()
		tr := &tracker{
			name:           "test",
			bigbrainURL:    "http://127.0.0.1:1",
			client:         &http.Client{Timeout: 250 * time.Millisecond},
			proxyTransport: newProxyTransport(),
		}
		tr.setLeader("http://10.0.0.5:3210")
		tr.resolveOnce(context.Background())
		if tr.currentLeader(false) == nil {
			t.Fatal("transport error must preserve last-known leader")
		}
	})
	t.Run("status_200_sets_leader", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"leaderUrl":"http://10.0.0.9:3210"}`))
		}))
		defer srv.Close()
		tr := &tracker{name: "test", bigbrainURL: srv.URL, client: &http.Client{}, proxyTransport: newProxyTransport()}
		tr.resolveOnce(context.Background())
		if tr.currentLeader(false) == nil {
			t.Fatal("200 with a leader URL must set the leader")
		}
	})
}

func sseServer(t *testing.T, events ...string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for _, ev := range events {
			_, _ = io.WriteString(w, "event: leader\ndata: "+ev+"\n\n")
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func leaderSnapshot(tr *tracker) string {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	return tr.leaderURL
}

func TestLeaderSeqOrdering_StaleStreamEventAfterNewerPoll(t *testing.T) {
	t.Parallel()
	poll := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"leaderUrl":"http://10.0.0.1:3210","seq":10}`)
	}))
	defer poll.Close()
	tr := &tracker{
		name:           "test",
		bigbrainURL:    poll.URL,
		client:         &http.Client{},
		streamClient:   &http.Client{},
		proxyTransport: newProxyTransport(),
	}
	tr.resolveOnce(context.Background())
	if got := leaderSnapshot(tr); got != "http://10.0.0.1:3210" {
		t.Fatalf("poll seq 10 must apply, got leader %q", got)
	}
	stale := sseServer(t, `{"leaderUrl":"http://10.0.0.2:3210","seq":5}`)
	tr.consumeStream(context.Background(), stale.URL)
	if got := leaderSnapshot(tr); got != "http://10.0.0.1:3210" {
		t.Fatalf("stale stream event (seq 5 <= 10) must be rejected, got leader %q", got)
	}
	fresh := sseServer(t, `{"leaderUrl":"http://10.0.0.3:3210","seq":11}`)
	tr.consumeStream(context.Background(), fresh.URL)
	if got := leaderSnapshot(tr); got != "http://10.0.0.3:3210" {
		t.Fatalf("newer stream event (seq 11 > 10) must apply, got leader %q", got)
	}
}

func TestLeaderSeqOrdering_StalePollAfterNewerStream(t *testing.T) {
	t.Parallel()
	poll := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"leaderUrl":"http://10.0.0.2:3210","seq":5}`)
	}))
	defer poll.Close()
	tr := &tracker{
		name:           "test",
		bigbrainURL:    poll.URL,
		client:         &http.Client{},
		streamClient:   &http.Client{},
		proxyTransport: newProxyTransport(),
	}
	stream := sseServer(t, `{"leaderUrl":"http://10.0.0.1:3210","seq":10}`)
	tr.consumeStream(context.Background(), stream.URL)
	if got := leaderSnapshot(tr); got != "http://10.0.0.1:3210" {
		t.Fatalf("stream seq 10 must apply, got leader %q", got)
	}
	tr.resolveOnce(context.Background())
	if got := leaderSnapshot(tr); got != "http://10.0.0.1:3210" {
		t.Fatalf("stale poll (seq 5 <= 10) must be rejected, got leader %q", got)
	}
}

func TestApplyLeader_SeqZeroAlwaysApplies(t *testing.T) {
	t.Parallel()
	tr := &tracker{host: "api.example", proxyTransport: newProxyTransport()}
	if !tr.applyLeader("http://10.0.0.1:3210", 10) {
		t.Fatal("seq 10 should install the leader")
	}
	if !tr.setLeader("http://10.0.0.2:3210") {
		t.Fatal("a seq-less (legacy bigbrain) event must apply unconditionally")
	}
	if got := leaderSnapshot(tr); got != "http://10.0.0.2:3210" {
		t.Fatalf("leader=%q want the seq-0 applied URL", got)
	}
}

func TestActivityConn_ClosesIdleAndSparesActive(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	al := &activityListener{Listener: ln, idle: 200 * time.Millisecond, ctx: t.Context()}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), ReadHeaderTimeout: time.Minute}
	go func() { _ = srv.Serve(al) }()
	defer func() { _ = srv.Close() }()
	idle, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = idle.Close() }()
	_ = idle.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, rerr := idle.Read(buf); rerr == nil || errors.Is(rerr, os.ErrDeadlineExceeded) {
		t.Fatalf("idle connection should have been closed by the watchdog, got %v", rerr)
	}
	active, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = active.Close() }()
	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, werr := active.Write([]byte("X")); werr != nil {
			t.Fatalf("active connection was closed despite progress: %v", werr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
