//nolint:testpackage // white-box
package election

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"git.horse/vapronva/concave/router/bigbrain/backend"
	"git.horse/vapronva/concave/router/bigbrain/registry"
)

func TestRunActions_StalledPodDoesNotStarveLaterDemotes(t *testing.T) {
	t.Parallel()
	actuation := 200 * time.Millisecond
	release := make(chan struct{})
	defer close(release)
	stalled := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer stalled.Close()
	var demoted atomic.Int64
	responsive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		demoted.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer responsive.Close()
	reg := registry.New()
	reg.EnsureDeployment("dev", "convex-dev")
	c := New(
		Config{ActuationTimeout: &actuation},
		nil,
		backend.New(map[string]string{}),
		reg,
		quietLogger(),
	)
	st := c.deploymentState("dev")
	c.runActions(context.Background(), "dev", st, []action{
		{pod: "backend-0", url: stalled.URL},
		{pod: "backend-1", url: responsive.URL},
	})
	c.actWG.Wait()
	if got := demoted.Load(); got != 1 {
		t.Fatalf("demote of the reachable pod must still happen after a stalled peer; got %d calls, want 1", got)
	}
}
