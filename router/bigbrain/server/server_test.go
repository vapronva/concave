package server_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"git.horse/vapronva/concave/router/bigbrain/insights"
	"git.horse/vapronva/concave/router/bigbrain/registry"
	"git.horse/vapronva/concave/router/bigbrain/server"
)

type leaderResponse struct {
	Name      string `json:"name"`
	LeaderPod string `json:"leaderPod"`
	LeaderURL string `json:"leaderUrl"`
	Seq       uint64 `json:"seq"`
}

func newTestServer(t *testing.T) (*registry.Registry, http.Handler) {
	t.Helper()
	reg := registry.New()
	return reg, server.New(reg, insights.New(10), map[string]string{"dev": "usage-secret"}, nil).Handler()
}

func TestServer_Leader(t *testing.T) {
	t.Parallel()
	reg, h := newTestServer(t)
	reg.EnsureDeployment("dev", "convex-dev")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/registry/deployments/nope/leader", nil))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("unknown leader: want 404, got %d", rr.Code)
	}
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/registry/deployments/dev/leader", nil))
	if rr.Code != http.StatusTooEarly {
		t.Fatalf("unreconciled: want 425, got %d", rr.Code)
	}
	reg.Update("dev", "", "")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/registry/deployments/dev/leader", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("known leaderless: want 503, got %d", rr.Code)
	}
	reg.Update("dev", "backend-0", "http://10.0.0.1:3210")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/registry/deployments/dev/leader", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("leader: want 200, got %d", rr.Code)
	}
	var lr leaderResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &lr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if lr.LeaderURL != "http://10.0.0.1:3210" || lr.LeaderPod != "backend-0" {
		t.Fatalf("unexpected leader response: %+v", lr)
	}
	if lr.Seq == 0 {
		t.Fatalf("leader response must carry a non-zero seq: %+v", lr)
	}
}

func TestServer_LeaderStreamInitialEvent(t *testing.T) {
	t.Parallel()
	reg, h := newTestServer(t)
	reg.EnsureDeployment("dev", "convex-dev")
	reg.Update("dev", "backend-0", "http://10.0.0.1:3210")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/registry/deployments/dev/leader-stream", nil).WithContext(ctx)
	rr := &flushRecorder{ResponseRecorder: httptest.NewRecorder(), onFlush: cancel}
	h.ServeHTTP(rr, req)
	body := rr.Body.String()
	if !strings.Contains(body, "event: leader") || !strings.Contains(body, "http://10.0.0.1:3210") {
		t.Fatalf("stream did not emit initial leader event, got: %q", body)
	}
	if !strings.Contains(body, `"seq":`) || strings.Contains(body, `"seq":0`) {
		t.Fatalf("stream event must carry a non-zero seq, got: %q", body)
	}
}

func TestServer_LeaderStreamSkipsUnpublishedSnapshot(t *testing.T) {
	t.Parallel()
	reg, h := newTestServer(t)
	reg.EnsureDeployment("dev", "convex-dev")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		time.Sleep(50 * time.Millisecond)
		reg.Update("dev", "backend-1", "http://10.0.0.2:3210")
	}()
	req := httptest.NewRequest(http.MethodGet, "/registry/deployments/dev/leader-stream", nil).WithContext(ctx)
	rr := &flushRecorder{ResponseRecorder: httptest.NewRecorder(), onFlush: cancel}
	h.ServeHTTP(rr, req)
	body := rr.Body.String()
	if strings.Contains(body, `"leaderUrl":""`) {
		t.Fatalf("stream emitted an unreconciled snapshot: %q", body)
	}
	if !strings.Contains(body, "http://10.0.0.2:3210") {
		t.Fatalf("stream did not emit the first reconciled event, got: %q", body)
	}
}

func TestServer_ShutdownClosesLeaderStream(t *testing.T) {
	t.Parallel()
	reg := registry.New()
	reg.EnsureDeployment("dev", "convex-dev")
	reg.Update("dev", "backend-0", "http://10.0.0.1:3210")
	srv := server.New(reg, insights.New(10), nil, slog.New(slog.DiscardHandler))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/registry/deployments/dev/leader-stream")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stream: want 200, got %d", resp.StatusCode)
	}
	buf := make([]byte, 1)
	if _, rerr := resp.Body.Read(buf); rerr != nil {
		t.Fatalf("read initial event: %v", rerr)
	}
	srv.Shutdown()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(io.Discard, resp.Body)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not close the connected leader-stream client")
	}
}

func TestServer_Readyz(t *testing.T) {
	t.Parallel()
	reg, h := newTestServer(t)
	reg.EnsureDeployment("dev", "convex-dev")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("unreconciled readyz: want 503, got %d", rr.Code)
	}
	reg.Update("dev", "", "")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("reconciled readyz: want 200, got %d", rr.Code)
	}
}

func TestServer_UsageEndpointsRequireTheDeploymentToken(t *testing.T) {
	t.Parallel()
	reg, h := newTestServer(t)
	reg.EnsureDeployment("dev", "convex-dev")
	post := func(deployment, auth string) int {
		rr := httptest.NewRecorder()
		body := `{"deployment":"` + deployment + `","events":[]}`
		req := httptest.NewRequest(http.MethodPost, "/internal/usage", strings.NewReader(body))
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		h.ServeHTTP(rr, req)
		return rr.Code
	}
	if code := post("dev", "Bearer usage-secret"); code != http.StatusOK {
		t.Fatalf("valid token: want 200, got %d", code)
	}
	for _, tc := range []struct{ deployment, auth string }{
		{"dev", ""},
		{"dev", "bearerusage-secret"},
		{"dev", "Bearer usage-secret extra"},
		{"ghost", "Bearer usage-secret"},
	} {
		if code := post(tc.deployment, tc.auth); code != http.StatusUnauthorized {
			t.Fatalf("deployment=%q auth=%q: want 401, got %d", tc.deployment, tc.auth, code)
		}
	}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/dashboard/teams/0/usage/query?deploymentName=ghost", nil)
	req.Header.Set("Authorization", "Bearer usage-secret")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("query for an unknown deployment must be 401 (no existence leak), got %d", rr.Code)
	}
}

func TestServer_UsageIngestThenQuery(t *testing.T) {
	t.Parallel()
	reg, h := newTestServer(t)
	reg.EnsureDeployment("dev", "convex-dev")
	ingest := `{"deployment":"dev","read_limits":{"documents":64000,"bytes":16777216},"events":[` +
		`{"FunctionCall":{"is_occ":true,"udf_id":"mod:occFn","id":"o1","request_id":"q1",` +
		`"component_path":"-root-component-","occ_table_name":"docs","status":"retried"}},` +
		`{"InsightReadLimit":{"udf_id":"mod:readFn","id":"r1","request_id":"q2",` +
		`"component_path":"-root-component-","calls":[` +
		`{"table_name":"big","bytes_read":17000000,"documents_read":52000}]}}]}`
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/internal/usage", strings.NewReader(ingest))
	req.Header.Set("Authorization", "Bearer usage-secret")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("ingest: want 200, got %d (%s)", rr.Code, rr.Body.String())
	}
	var ing struct {
		Ingested int `json:"ingested"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &ing); err != nil {
		t.Fatalf("decode ingest resp: %v", err)
	}
	if ing.Ingested != 2 {
		t.Fatalf("ingested=%d want 2", ing.Ingested)
	}
	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet,
		"/api/dashboard/teams/0/usage/query?deploymentName=dev", nil)
	req.Header.Set("Authorization", "Bearer usage-secret")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("query: want 200, got %d (%s)", rr.Code, rr.Body.String())
	}
	var rows [][]any
	if err := json.Unmarshal(rr.Body.Bytes(), &rows); err != nil {
		t.Fatalf("decode query rows: %v", err)
	}
	kinds := make(map[string]bool)
	for _, row := range rows {
		if len(row) != 4 {
			t.Fatalf("row must have 4 cells, got %d: %v", len(row), row)
		}
		k, _ := row[0].(string)
		if _, ok := row[3].(string); !ok {
			t.Fatalf("cell 4 must be a JSON string, got %T", row[3])
		}
		kinds[k] = true
	}
	if !kinds["occRetried"] || !kinds["bytesReadLimit"] || !kinds["documentsReadThreshold"] {
		t.Errorf("want occRetried, bytesReadLimit and (against the 64000 limit) documentsReadThreshold; got %v", rows)
	}
	if kinds["documentsReadLimit"] {
		t.Errorf("52,000 documents must be classified against the deployment's own limit, got %v", rows)
	}
}

type flushRecorder struct {
	*httptest.ResponseRecorder

	onFlush func()
	flushed bool
}

func (f *flushRecorder) Flush() {
	f.ResponseRecorder.Flush()
	if !f.flushed && f.Body.Len() > 0 {
		f.flushed = true
		if f.onFlush != nil {
			f.onFlush()
		}
	}
}
