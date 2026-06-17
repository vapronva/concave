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
	if _, _, _, ok := r.Leader("dev"); ok {
		t.Fatal("fresh deployment should have no leader")
	}
	r.Update("dev", "backend-0", "http://x:3210")
	pod, url, seq, ok := r.Leader("dev")
	if !ok || pod != "backend-0" || url != "http://x:3210" {
		t.Fatalf("want backend-0/http://x:3210, got %q/%q ok=%v", pod, url, ok)
	}
	if seq == 0 {
		t.Fatal("a published leader must carry a non-zero seq")
	}
}

func TestRegistry_SeqMonotonicPerDistinctLeaderState(t *testing.T) {
	t.Parallel()
	r := registry.New()
	r.EnsureDeployment("dev", "convex-dev")
	r.Update("dev", "backend-0", "http://a:3210")
	_, _, seq1, _ := r.Leader("dev")
	if seq1 == 0 {
		t.Fatalf("a published leader must carry a non-zero seq, got %d", seq1)
	}
	r.Update("dev", "backend-0", "http://a:3210")
	if _, _, seq2, _ := r.Leader("dev"); seq2 != seq1 {
		t.Fatalf("an unchanged Update must not advance seq: %d -> %d", seq1, seq2)
	}
	r.Update("dev", "backend-1", "http://b:3210")
	_, _, seq3, _ := r.Leader("dev")
	if seq3 <= seq1 {
		t.Fatalf("a leader change must strictly advance seq: %d -> %d", seq1, seq3)
	}
	r.Update("dev", "", "")
	if _, _, seq4, ok := r.Leader("dev"); ok || seq4 <= seq3 {
		t.Fatalf("a leaderless publish must strictly advance seq: %d -> %d (ok=%v)", seq3, seq4, ok)
	}
}

func TestRegistry_EventsCarrySeq(t *testing.T) {
	t.Parallel()
	r := registry.New()
	r.EnsureDeployment("dev", "convex-dev")
	ch, cancel, ok := r.Subscribe("dev")
	if !ok {
		t.Fatal("subscribe failed")
	}
	defer cancel()
	r.Update("dev", "backend-0", "http://a:3210")
	var first uint64
	select {
	case ev := <-ch:
		_, _, seq, _ := r.Leader("dev")
		if ev.Seq == 0 || ev.Seq != seq {
			t.Fatalf("event seq %d must match Leader() seq %d and be non-zero", ev.Seq, seq)
		}
		first = ev.Seq
	case <-time.After(time.Second):
		t.Fatal("expected a leader event")
	}
	r.Update("dev", "backend-1", "http://b:3210")
	select {
	case ev := <-ch:
		if ev.Seq <= first {
			t.Fatalf("event seq must strictly increase across changes: %d -> %d", first, ev.Seq)
		}
	case <-time.After(time.Second):
		t.Fatal("expected a leader-change event")
	}
}

func TestRegistry_EventsCarryEpoch(t *testing.T) {
	t.Parallel()
	r := registry.New()
	r.EnsureDeployment("dev", "convex-dev")
	if r.Epoch() == 0 {
		t.Fatal("registry epoch must be a non-zero per-process nonce")
	}
	ch, cancel, ok := r.Subscribe("dev")
	if !ok {
		t.Fatal("subscribe failed")
	}
	defer cancel()
	r.Update("dev", "backend-0", "http://a:3210")
	select {
	case ev := <-ch:
		if ev.Epoch != r.Epoch() {
			t.Fatalf("event epoch %d must match registry epoch %d", ev.Epoch, r.Epoch())
		}
	case <-time.After(time.Second):
		t.Fatal("expected a leader event")
	}
	if registry.New().Epoch() == r.Epoch() {
		t.Fatal("a fresh registry (a bigbrain restart) must carry a distinct epoch")
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

func TestRegistry_PublishedLifecycle(t *testing.T) {
	t.Parallel()
	r := registry.New()
	r.EnsureDeployment("dev", "convex-dev")
	if r.Published("dev") || r.AllPublished() {
		t.Fatal("nothing published before the first Update")
	}
	r.Update("dev", "", "")
	if !r.Published("dev") || !r.AllPublished() {
		t.Fatal("an explicit leaderless Update must publish")
	}
	if _, _, _, ok := r.Leader("dev"); ok {
		t.Fatal("published leaderless deployment must still report no leader")
	}
	r.EnsureDeployment("prod", "convex-prod")
	if r.AllPublished() {
		t.Fatal("AllPublished must be false while prod is unreconciled")
	}
	if r.Published("ghost") {
		t.Fatal("unknown deployment must not report published")
	}
}
