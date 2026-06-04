package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIsBlockedControlPath(t *testing.T) {
	t.Parallel()
	blocked := []string{
		"/instance/promote",
		"/instance/demote",
		"/instance/promote/",
		"/instance/demote/",
		"/instance/./promote",
		"/instance/../instance/promote",
	}
	for _, p := range blocked {
		if !isBlockedControlPath(p) {
			t.Errorf("path %q should be blocked", p)
		}
	}
	allowed := []string{
		"/instance/leadership",
		"/api/query",
		"/sync",
		"/",
		"/instance/promotething",
		"/instance",
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
	tr := &tracker{host: "convex-dev.localtest.me", client: &http.Client{}}
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
		{http.MethodGet, "/instance/leadership", http.StatusOK, true, "upstream-reached"},
		{http.MethodPost, "/api/mutation", http.StatusOK, true, "upstream-reached"},
	}
	for _, c := range cases {
		upstreamHits = nil
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(c.method, c.path, nil)
		tr.serveHTTP(rr, req)
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
