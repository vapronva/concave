package registry_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"git.horse/vapronva/concave/router/bigbrain/registry"
)

func TestRegistry_LeaderAndSnapshot(t *testing.T) {
	t.Parallel()
	r := registry.New()
	r.EnsureDeployment("dev", "convex-dev")
	if _, _, ok := r.Leader("dev"); ok {
		t.Fatal("fresh deployment should have no leader")
	}
	r.Update("dev", []registry.BackendState{{Pod: "backend-0", URL: "http://x:3210"}}, "backend-0", "http://x:3210")
	pod, url, ok := r.Leader("dev")
	if !ok || pod != "backend-0" || url != "http://x:3210" {
		t.Fatalf("want backend-0/http://x:3210, got %q/%q ok=%v", pod, url, ok)
	}
	snap, ok := r.Snapshot("dev")
	if !ok || snap.Phase != "Ready" || len(snap.Backends) != 1 {
		t.Fatalf("unexpected snapshot: %+v ok=%v", snap, ok)
	}
	snap.Backends[0].Pod = "mutated"
	again, _ := r.Snapshot("dev")
	if again.Backends[0].Pod != "backend-0" {
		t.Fatal("snapshot should be a copy")
	}
}

func TestRegistry_LeaderEventOnChange(t *testing.T) {
	t.Parallel()
	r := registry.New()
	r.EnsureDeployment("dev", "convex-dev")
	ch, cancel, ok := r.Subscribe("dev")
	if !ok {
		t.Fatal("subscribe failed")
	}
	defer cancel()
	r.Update("dev", nil, "backend-0", "http://a:3210")
	select {
	case ev := <-ch:
		if ev.LeaderPod != "backend-0" {
			t.Fatalf("want backend-0, got %q", ev.LeaderPod)
		}
	case <-time.After(time.Second):
		t.Fatal("expected a leader event")
	}
	r.Update("dev", nil, "backend-0", "http://a:3210")
	select {
	case ev := <-ch:
		t.Fatalf("unexpected event for unchanged leader: %+v", ev)
	case <-time.After(100 * time.Millisecond):
	}
	r.Update("dev", nil, "backend-1", "http://b:3210")
	select {
	case ev := <-ch:
		if ev.LeaderPod != "backend-1" {
			t.Fatalf("want backend-1, got %q", ev.LeaderPod)
		}
	case <-time.After(time.Second):
		t.Fatal("expected a leader-change event")
	}
}

func TestRegistry_SubscribeCancelClosesChannel(t *testing.T) {
	t.Parallel()
	r := registry.New()
	r.EnsureDeployment("dev", "convex-dev")
	ch, cancel, ok := r.Subscribe("dev")
	if !ok {
		t.Fatal("subscribe failed")
	}
	cancel()
	select {
	case _, open := <-ch:
		if open {
			t.Fatal("channel should be closed after cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber channel was not closed by cancel")
	}
}

func TestRegistry_ConcurrentUpdateAndSubscribe(t *testing.T) {
	t.Parallel()
	const deployments = 16
	const iterations = 200
	var wg sync.WaitGroup
	for i := range deployments {
		name := fmt.Sprintf("dep-%d", i)
		wg.Go(func() {
			r := registry.New()
			for range iterations {
				r.EnsureDeployment(name, name)
				ch, cancel, ok := r.Subscribe(name)
				if !ok {
					t.Errorf("subscribe failed for %s", name)
					return
				}
				go func() {
					for range ch { //nolint:revive // drain until the subscription closes
					}
				}()
				var inner sync.WaitGroup
				inner.Add(2)
				go func() {
					defer inner.Done()
					for k := range 8 {
						pod := fmt.Sprintf("backend-%d", k%2)
						r.Update(name, nil, pod, "http://"+pod+":3210")
					}
				}()
				go func() {
					defer inner.Done()
					cancel()
				}()
				inner.Wait()
			}
		})
	}
	wg.Wait()
}
