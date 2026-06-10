//nolint:testpackage // white-box
package election

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"git.horse/vapronva/concave/router/bigbrain/backend"
	"git.horse/vapronva/concave/router/bigbrain/k8sclient"
	"git.horse/vapronva/concave/router/bigbrain/registry"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func racePod(name, ip string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "race",
			Labels: map[string]string{
				"convex/instance":        "race",
				"convex/role":            "follower",
				"convex/component":       "backend",
				"convex/leader-priority": "10",
			},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			PodIP:      ip,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

func TestController_ReconcileConcurrentWithActuation(t *testing.T) {
	t.Parallel()
	var promoteCalls atomic.Int64
	ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(k8sclient.BackendPort)))
	if err != nil {
		t.Skipf("cannot bind 127.0.0.1:%d (in use): %v", k8sclient.BackendPort, err)
	}
	srv := &httptest.Server{
		Listener: ln,
		Config: &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && r.URL.Path == "/instance/promote" {
				promoteCalls.Add(1)
				w.WriteHeader(http.StatusAccepted)
				return
			}
			_, _ = io.WriteString(w, `{"role":"follower","is_leader":false,"latest_ts":1,"lease_ts":null}`)
		})},
	}
	srv.Start()
	defer srv.Close()
	cs := fake.NewClientset(
		racePod("backend-0", "127.0.0.1"),
		racePod("backend-1", "127.0.0.1"),
		racePod("backend-2", "127.0.0.1"),
	)
	k8s := k8sclient.NewFromInterface(cs, "convex")
	reg := registry.New()
	reg.EnsureDeployment("race", "race")
	c := New(
		Config{Interval: new(time.Millisecond), PromoteDebounce: new(1)},
		k8s, backend.New(nil), reg, quietLogger(),
	)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() {
			for ctx.Err() == nil {
				c.reconcile(ctx, "race")
			}
		})
	}
	time.Sleep(150 * time.Millisecond)
	cancel()
	wg.Wait()
	c.actWG.Wait()
	if promoteCalls.Load() == 0 {
		t.Fatal("test did not exercise concurrent actuation")
	}
}

func TestController_SnapshotCommitConcurrent(t *testing.T) {
	t.Parallel()
	c := New(Config{}, nil, nil, registry.New(), quietLogger())
	st := c.deploymentState("d")
	in := []observation{
		obs("backend-0", false, 90, -1),
		obs("backend-1", false, 120, -1),
		obs("backend-2", false, 110, -1),
	}
	var wg sync.WaitGroup
	const iterations = 2000
	wg.Go(func() {
		for range iterations {
			_ = c.snapshotState(st)
			dec := decide(in, decideParams{incumbent: "backend-0"})
			c.commitState(st, dec, false, time.Now())
		}
	})
	wg.Go(func() {
		for range iterations {
			_ = c.snapshotState(st)
		}
	})
	wg.Wait()
}
