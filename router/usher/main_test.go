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

func (t *tracker) setLeader(leaderURL string) bool {
	return t.applyLeader(leaderURL, 0, 0)
}

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
	fromEdge := httptest.NewRequest(http.MethodGet, "http://api.example/api/query", nil)
	fromEdge.RemoteAddr = "192.0.2.10:4567"
	fromEdge.Host = "api.example"
	fromEdge.Header.Set("X-Forwarded-For", "203.0.113.7")
	fromEdge.Header.Set("X-Forwarded-Proto", "https")
	tr.serveHTTP(httptest.NewRecorder(), fromEdge, false)
	if gotXFF != "203.0.113.7, 192.0.2.10" {
		t.Fatalf("X-Forwarded-For=%q want %q (edge chain + usher-seen client)", gotXFF, "203.0.113.7, 192.0.2.10")
	}
	if gotProto != "https" || gotHost != "api.example" {
		t.Fatalf("forwarded proto/host = %q/%q want https/api.example", gotProto, gotHost)
	}
	bare := httptest.NewRequest(http.MethodGet, "http://api.example/api/query", nil)
	bare.RemoteAddr = "192.0.2.11:4567"
	bare.Host = "api.example"
	tr.serveHTTP(httptest.NewRecorder(), bare, false)
	if gotProto != "http" {
		t.Fatalf("X-Forwarded-Proto=%q want http when the edge sent none (plaintext hop)", gotProto)
	}
	if gotXFF != "192.0.2.11" {
		t.Fatalf("X-Forwarded-For=%q want %q when the edge sent none", gotXFF, "192.0.2.11")
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
	dup := config{Deployments: []deploymentCfg{
		{Host: "api.example", SiteHost: "site.example"},
		{Host: "API.EXAMPLE"},
	}}
	if err := validateConfig(dup, "http://bigbrain:8081"); err == nil || !strings.Contains(err.Error(), "api.example") {
		t.Fatalf("duplicate-host error must name the offending host, got %v", err)
	}
	for _, name := range []string{"bad/name", "bad name", "bad%name", "../escape"} {
		bad := config{Deployments: []deploymentCfg{{Host: "api.example", Name: name}}}
		if err := validateConfig(bad, "http://bigbrain:8081"); err == nil {
			t.Errorf("name %q must be rejected at boot", name)
		}
	}
	emptyDerived := config{Deployments: []deploymentCfg{{Host: ".example"}}}
	if err := validateConfig(emptyDerived, "http://bigbrain:8081"); err == nil {
		t.Fatal("host yielding an empty derived name must be rejected")
	}
	derived := config{Deployments: []deploymentCfg{{Host: "team-app.example.com"}}}
	if err := validateConfig(derived, "http://bigbrain:8081"); err != nil {
		t.Fatalf("derived name rejected: %v", err)
	}
}

func TestParseConnIdle(t *testing.T) {
	t.Parallel()
	if got, err := parseConnIdle(""); err != nil || got != defaultConnIdle {
		t.Fatalf("empty: got %s err=%v, want default %s", got, err, defaultConnIdle)
	}
	if got, err := parseConnIdle("90s"); err != nil || got != 90*time.Second {
		t.Fatalf("90s: got %s err=%v", got, err)
	}
	if got, err := parseConnIdle("0"); err != nil || got != 0 {
		t.Fatalf("0 (disabled): got %s err=%v", got, err)
	}
	for _, bad := range []string{"-5s", "-1h", "garbage", "10"} {
		if _, err := parseConnIdle(bad); err == nil {
			t.Errorf("%q: want parse error", bad)
		}
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
	t.Run("status_503_garbage_body_preserves", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "intermediary melted")
		}))
		defer srv.Close()
		tr := &tracker{name: "test", bigbrainURL: srv.URL, client: &http.Client{}, proxyTransport: newProxyTransport()}
		tr.setLeader("http://10.0.0.5:3210")
		tr.resolveOnce(context.Background())
		if tr.currentLeader(false) == nil {
			t.Fatal("503 with an undecodable body must preserve last-known leader")
		}
	})
	t.Run("status_425_preserves", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusTooEarly)
		}))
		defer srv.Close()
		tr := &tracker{name: "test", bigbrainURL: srv.URL, client: &http.Client{}, proxyTransport: newProxyTransport()}
		tr.setLeader("http://10.0.0.5:3210")
		tr.resolveOnce(context.Background())
		if tr.currentLeader(false) == nil {
			t.Fatal("425 must preserve last-known leader")
		}
	})
	t.Run("status_404_preserves", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer srv.Close()
		tr := &tracker{name: "test", bigbrainURL: srv.URL, client: &http.Client{}, proxyTransport: newProxyTransport()}
		tr.setLeader("http://10.0.0.5:3210")
		tr.resolveOnce(context.Background())
		if tr.currentLeader(false) == nil {
			t.Fatal("404 must preserve last-known leader")
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
	if !tr.applyLeader("http://10.0.0.1:3210", 10, 0) {
		t.Fatal("seq 10 should install the leader")
	}
	if !tr.setLeader("http://10.0.0.2:3210") {
		t.Fatal("a seq-less (legacy bigbrain) event must apply unconditionally")
	}
	if got := leaderSnapshot(tr); got != "http://10.0.0.2:3210" {
		t.Fatalf("leader=%q want the seq-0 applied URL", got)
	}
}

func TestApplyLeader_BadURLDoesNotConsumeSeq(t *testing.T) {
	t.Parallel()
	tr := &tracker{host: "api.example", proxyTransport: newProxyTransport()}
	if tr.applyLeader("http://[::1]:namedport", 7, 0) {
		t.Fatal("an unparseable leader URL must not apply")
	}
	if !tr.applyLeader("http://10.0.0.1:3210", 7, 0) {
		t.Fatal("the rejected event must not consume seq 7; a follow-up with the same seq must apply")
	}
	if got := leaderSnapshot(tr); got != "http://10.0.0.1:3210" {
		t.Fatalf("leader=%q want the seq-7 applied URL", got)
	}
}

func TestApplyLeader_EpochChangeResetsSeqHighWater(t *testing.T) {
	t.Parallel()
	tr := &tracker{host: "api.example", proxyTransport: newProxyTransport()}
	if !tr.applyLeader("http://10.0.0.1:3210", 5, 100) {
		t.Fatal("epoch 100 seq 5 should install the leader")
	}
	if tr.applyLeader("http://10.0.0.2:3210", 3, 100) {
		t.Fatal("a lower seq within the same epoch must be rejected")
	}
	if got := leaderSnapshot(tr); got != "http://10.0.0.1:3210" {
		t.Fatalf("leader=%q want the epoch-100 seq-5 URL after a rejected same-epoch event", got)
	}
	if !tr.applyLeader("http://10.0.0.2:3210", 3, 200) {
		t.Fatal("a new epoch (bigbrain restart) must reset the seq high-water and adopt even a lower seq")
	}
	if got := leaderSnapshot(tr); got != "http://10.0.0.2:3210" {
		t.Fatalf("leader=%q want the epoch-200 URL after the epoch change", got)
	}
}

func TestConsumeStream_IdleWatchdogCancelsSilentStream(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: leader\ndata: {\"leaderUrl\":\"http://10.0.0.8:3210\",\"seq\":3}\n\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()
	tr := &tracker{
		host:           "api.example",
		streamIdle:     150 * time.Millisecond,
		streamClient:   &http.Client{},
		proxyTransport: newProxyTransport(),
	}
	done := make(chan struct{})
	go func() {
		tr.consumeStream(context.Background(), srv.URL)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("idle watchdog did not cancel the silent stream")
	}
	if got := leaderSnapshot(tr); got != "http://10.0.0.8:3210" {
		t.Fatalf("leader=%q want the event applied before the stream went silent", got)
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

func TestNewMux_HealthzOnlyOnUnknownHosts(t *testing.T) {
	t.Parallel()
	var paths []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "upstream-reached")
	}))
	defer upstream.Close()
	tr := &tracker{host: "api.example", resolveCh: make(chan struct{}, 1), proxyTransport: newProxyTransport()}
	if !tr.setLeader(upstream.URL) {
		t.Fatal("setLeader should install proxy")
	}
	mux := newMux(map[string]route{"api.example": {tracker: tr}})
	edge := httptest.NewServer(mux)
	defer edge.Close()
	resp, err := http.Get(edge.URL + "/usher/healthz")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "ok" {
		t.Fatalf("unknown-host healthz: status=%d body=%q", resp.StatusCode, body)
	}
	req, err := http.NewRequest(http.MethodGet, edge.URL+"/usher/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "api.example"
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "upstream-reached") {
		t.Fatalf("configured-host healthz must proxy to the leader: status=%d body=%q", resp.StatusCode, body)
	}
	if len(paths) != 1 || paths[0] != "/usher/healthz" {
		t.Fatalf("upstream paths=%v want exactly [/usher/healthz]", paths)
	}
}

func TestNewMux_ReadyzGatesOnFirstResolve(t *testing.T) {
	t.Parallel()
	tr := &tracker{host: "api.example", resolveCh: make(chan struct{}, 1), proxyTransport: newProxyTransport()}
	mux := newMux(map[string]route{"api.example": {tracker: tr}})
	probe := func(path string) int {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Host = "probe.invalid"
		mux.ServeHTTP(rr, req)
		return rr.Code
	}
	if got := probe(readyzPath); got != http.StatusServiceUnavailable {
		t.Fatalf("readyz before first resolve: want 503, got %d", got)
	}
	if got := probe(healthzPath); got != http.StatusOK {
		t.Fatalf("healthz must stay 200 for liveness regardless of resolve state, got %d", got)
	}
	tr.resolved.Store(true)
	if got := probe(readyzPath); got != http.StatusOK {
		t.Fatalf("readyz after first resolve completes: want 200, got %d", got)
	}
}

func TestNewMux_MethodAndHostGates(t *testing.T) {
	t.Parallel()
	tr := &tracker{host: "api.example", resolveCh: make(chan struct{}, 1), proxyTransport: newProxyTransport()}
	mux := newMux(map[string]route{"api.example": {tracker: tr}})
	cases := []struct {
		method, host, path string
		want               int
	}{
		{http.MethodTrace, "probe.invalid", "/usher/healthz", http.StatusMethodNotAllowed},
		{http.MethodPost, "probe.invalid", "/usher/healthz", http.StatusMethodNotAllowed},
		{http.MethodHead, "probe.invalid", "/usher/healthz", http.StatusOK},
		{http.MethodGet, "127.0.0.1:8080", "/usher/healthz", http.StatusOK},
		{http.MethodTrace, "api.example", "/api/query", http.StatusMethodNotAllowed},
		{http.MethodConnect, "api.example", "/api/query", http.StatusMethodNotAllowed},
		{http.MethodTrace, "api.example", "/usher/healthz", http.StatusMethodNotAllowed},
		{http.MethodGet, "probe.invalid", "/api/query", http.StatusNotFound},
	}
	for _, c := range cases {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(c.method, c.path, nil)
		req.Host = c.host
		mux.ServeHTTP(rr, req)
		if rr.Code != c.want {
			t.Errorf("%s %s host=%s: status=%d want %d", c.method, c.path, c.host, rr.Code, c.want)
		}
	}
}

func TestNewMux_HostRoutingPreservesPublicHost(t *testing.T) {
	t.Parallel()
	var gotHost atomic.Value
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost.Store(r.Host)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	tr := &tracker{host: "api.example", resolveCh: make(chan struct{}, 1), proxyTransport: newProxyTransport()}
	if !tr.setLeader(upstream.URL) {
		t.Fatal("setLeader should install proxy")
	}
	mux := newMux(map[string]route{"api.example": {tracker: tr}})
	edge := httptest.NewServer(mux)
	defer edge.Close()
	req, err := http.NewRequest(http.MethodGet, edge.URL+"/api/query", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "API.Example:8443"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d want 200 (routing must lowercase and strip the port)", resp.StatusCode)
	}
	if got, _ := gotHost.Load().(string); got != "API.Example:8443" {
		t.Fatalf("upstream r.Host=%q want the original public Host %q", got, "API.Example:8443")
	}
}

func TestServeHTTP_RejectsChunkedOversizedBody(t *testing.T) {
	t.Parallel()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	tr := &tracker{
		host:           "api.example",
		maxBodyBytes:   8,
		resolveCh:      make(chan struct{}, 1),
		proxyTransport: newProxyTransport(),
	}
	if !tr.setLeader(upstream.URL) {
		t.Fatal("setLeader should install proxy")
	}
	edge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tr.serveHTTP(w, r, false)
	}))
	defer edge.Close()
	req, err := http.NewRequest(http.MethodPost, edge.URL+"/api/mutation", strings.NewReader(strings.Repeat("x", 64)))
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = -1
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d want 413 for a chunked oversize body", resp.StatusCode)
	}
	select {
	case <-tr.resolveCh:
		t.Fatal("an oversize body must not nudge leader resolution")
	default:
	}
}
