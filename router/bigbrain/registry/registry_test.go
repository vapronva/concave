package registry_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"git.horse/vapronva/concave/router/bigbrain/registry"
)

func TestRegistry_Leader(t *testing.T) {
	t.Parallel()
	r := registry.New()
	r.EnsureDeployment("dev", "convex-dev")
	if _, _, ok := r.Leader("dev"); ok {
		t.Fatal("fresh deployment should have no leader")
	}
	r.Update("dev", "backend-0", "http://x:3210")
	pod, url, ok := r.Leader("dev")
	if !ok || pod != "backend-0" || url != "http://x:3210" {
		t.Fatalf("want backend-0/http://x:3210, got %q/%q ok=%v", pod, url, ok)
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
	r.Update("dev", "backend-0", "http://a:3210")
	select {
	case ev := <-ch:
		if ev.LeaderPod != "backend-0" {
			t.Fatalf("want backend-0, got %q", ev.LeaderPod)
		}
	case <-time.After(time.Second):
		t.Fatal("expected a leader event")
	}
	r.Update("dev", "backend-0", "http://a:3210")
	select {
	case ev := <-ch:
		t.Fatalf("unexpected event for unchanged leader: %+v", ev)
	case <-time.After(100 * time.Millisecond):
	}
	r.Update("dev", "backend-1", "http://b:3210")
	select {
	case ev := <-ch:
		if ev.LeaderPod != "backend-1" {
			t.Fatalf("want backend-1, got %q", ev.LeaderPod)
		}
	case <-time.After(time.Second):
		t.Fatal("expected a leader-change event")
	}
}

func TestRegistry_LeaderEventOnURLChangeAndSlowSubscriberGetsLatest(t *testing.T) {
	t.Parallel()
	r := registry.New()
	r.EnsureDeployment("dev", "convex-dev")
	ch, cancel, ok := r.Subscribe("dev")
	if !ok {
		t.Fatal("subscribe failed")
	}
	defer cancel()
	r.Update("dev", "backend-0", "http://a:3210")
	r.Update("dev", "backend-0", "http://b:3210")
	select {
	case ev := <-ch:
		if ev.LeaderURL != "http://b:3210" {
			t.Fatalf("slow subscriber got stale event: %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("expected latest leader event")
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
				_ = ch
				var inner sync.WaitGroup
				inner.Add(2)
				go func() {
					defer inner.Done()
					for k := range 8 {
						pod := fmt.Sprintf("backend-%d", k%2)
						r.Update(name, pod, "http://"+pod+":3210")
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
