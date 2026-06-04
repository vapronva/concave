package server_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"git.horse/vapronva/concave/router/bigbrain/registry"
	"git.horse/vapronva/concave/router/bigbrain/server"
)

type leaderResponse struct {
	Name      string `json:"name"`
	LeaderPod string `json:"leaderPod"`
	LeaderURL string `json:"leaderUrl"`
}

func newTestServer(t *testing.T) (*registry.Registry, http.Handler) {
	t.Helper()
	reg := registry.New()
	return reg, server.New(reg, nil).Handler()
}

func TestServer_Healthz(t *testing.T) {
	t.Parallel()
	_, h := newTestServer(t)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("healthz: want 200, got %d", rr.Code)
	}
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
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("leaderless: want 503, got %d", rr.Code)
	}
	reg.Update("dev", nil, "backend-0", "http://10.0.0.1:3210")
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
}

func TestServer_ListAndGet(t *testing.T) {
	t.Parallel()
	reg, h := newTestServer(t)
	reg.EnsureDeployment("dev", "convex-dev")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/registry/deployments", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("list: want 200, got %d", rr.Code)
	}
	var all []registry.Deployment
	if err := json.Unmarshal(rr.Body.Bytes(), &all); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(all) != 1 || all[0].Name != "dev" {
		t.Fatalf("unexpected list: %+v", all)
	}
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/registry/deployments/dev", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("get: want 200, got %d", rr.Code)
	}
}

func TestServer_NoProvisioningRoutes(t *testing.T) {
	t.Parallel()
	_, h := newTestServer(t)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/provision", strings.NewReader(`{"name":"x"}`)))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("POST /provision should be unrouted: want 404, got %d", rr.Code)
	}
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodDelete, "/deployments/x", nil))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("DELETE /deployments/{name} should be unrouted: want 404, got %d", rr.Code)
	}
}

func TestServer_LeaderStreamInitialEvent(t *testing.T) {
	t.Parallel()
	reg, h := newTestServer(t)
	reg.EnsureDeployment("dev", "convex-dev")
	reg.Update("dev", nil, "backend-0", "http://10.0.0.1:3210")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/registry/deployments/dev/leader-stream", nil).WithContext(ctx)
	rr := &flushRecorder{ResponseRecorder: httptest.NewRecorder(), onFlush: cancel}
	h.ServeHTTP(rr, req)
	body := rr.Body.String()
	if !strings.Contains(body, "event: leader") || !strings.Contains(body, "http://10.0.0.1:3210") {
		t.Fatalf("stream did not emit initial leader event, got: %q", body)
	}
}

type flushRecorder struct {
	*httptest.ResponseRecorder

	onFlush func()
	flushed bool
}

func (f *flushRecorder) Flush() {
	if !f.flushed {
		f.flushed = true
		if f.onFlush != nil {
			f.onFlush()
		}
	}
	f.ResponseRecorder.Flush()
}
