package registry

import (
	"crypto/rand"
	"encoding/binary"
	"sync"
)

type deployment struct {
	namespace string
	leaderPod string
	leaderURL string
	seq       uint64
	published bool
}

type Registry struct {
	mu    sync.RWMutex
	epoch uint64
	deps  map[string]*deployment
	subs  map[string]map[chan LeaderEvent]struct{}
}

type LeaderEvent struct {
	Name      string `json:"name"`
	LeaderPod string `json:"leaderPod"`
	LeaderURL string `json:"leaderUrl"`
	Seq       uint64 `json:"seq"`
	Epoch     uint64 `json:"epoch"`
}

const (
	subscriberBuffer            = 1
	maxSubscribersPerDeployment = 128
)

func New() *Registry {
	return &Registry{
		epoch: newEpoch(),
		deps:  make(map[string]*deployment),
		subs:  make(map[string]map[chan LeaderEvent]struct{}),
	}
}

func newEpoch() uint64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	if e := binary.LittleEndian.Uint64(b[:]); e != 0 {
		return e
	}
	return 1
}

func (r *Registry) Epoch() uint64 {
	return r.epoch
}

func (r *Registry) EnsureDeployment(name, ns string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.deps[name]; !ok {
		r.deps[name] = &deployment{namespace: ns}
	}
}

func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.deps))
	for n := range r.deps {
		out = append(out, n)
	}
	return out
}

func (r *Registry) Namespace(name string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.deps[name]
	if !ok {
		return "", false
	}
	return d.namespace, true
}

func (r *Registry) Update(name, leaderPod, leaderURL string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.deps[name]
	if !ok {
		return
	}
	changed := d.leaderPod != leaderPod || d.leaderURL != leaderURL || !d.published
	d.leaderPod = leaderPod
	d.leaderURL = leaderURL
	d.published = true
	if !changed {
		return
	}
	d.seq++
	ev := LeaderEvent{Name: name, LeaderPod: leaderPod, LeaderURL: leaderURL, Seq: d.seq, Epoch: r.epoch}
	for ch := range r.subs[name] {
		select {
		case ch <- ev:
		default:
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- ev:
			default:
			}
		}
	}
}

func (r *Registry) Leader(name string) (string, string, uint64, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, present := r.deps[name]
	if !present {
		return "", "", 0, false
	}
	return d.leaderPod, d.leaderURL, d.seq, d.leaderURL != ""
}

func (r *Registry) Published(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.deps[name]
	return ok && d.published
}

func (r *Registry) AllPublished() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, d := range r.deps {
		if !d.published {
			return false
		}
	}
	return len(r.deps) > 0
}

func (r *Registry) Subscribe(name string) (<-chan LeaderEvent, func(), bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.deps[name]; !ok {
		return nil, nil, false
	}
	if len(r.subs[name]) >= maxSubscribersPerDeployment {
		return nil, nil, false
	}
	ch := make(chan LeaderEvent, subscriberBuffer)
	if r.subs[name] == nil {
		r.subs[name] = make(map[chan LeaderEvent]struct{})
	}
	r.subs[name][ch] = struct{}{}
	cancel := func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if set, ok := r.subs[name]; ok {
			if _, present := set[ch]; present {
				delete(set, ch)
				close(ch)
			}
		}
	}
	return ch, cancel, true
}
