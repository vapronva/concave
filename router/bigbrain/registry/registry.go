package registry

import (
	"sync"
)

type Deployment struct {
	Namespace string
	LeaderPod string
	LeaderURL string
}

type Registry struct {
	mu   sync.RWMutex
	deps map[string]*Deployment
	subs map[string]map[chan LeaderEvent]struct{}
}

type LeaderEvent struct {
	Name      string `json:"name"`
	LeaderPod string `json:"leaderPod"`
	LeaderURL string `json:"leaderUrl"`
}

const (
	subscriberBuffer            = 1
	maxSubscribersPerDeployment = 128
)

func New() *Registry {
	return &Registry{
		deps: make(map[string]*Deployment),
		subs: make(map[string]map[chan LeaderEvent]struct{}),
	}
}

func (r *Registry) EnsureDeployment(name, ns string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.deps[name]; !ok {
		r.deps[name] = &Deployment{Namespace: ns}
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
	return d.Namespace, true
}

func (r *Registry) Update(name, leaderPod, leaderURL string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.deps[name]
	if !ok {
		return
	}
	changed := d.LeaderPod != leaderPod || d.LeaderURL != leaderURL
	d.LeaderPod = leaderPod
	d.LeaderURL = leaderURL
	if !changed {
		return
	}
	ev := LeaderEvent{Name: name, LeaderPod: leaderPod, LeaderURL: leaderURL}
	for ch := range r.subs[name] {
		select {
		case ch <- ev:
		default:
			select {
			case <-ch:
			default:
			}
			ch <- ev
		}
	}
}

func (r *Registry) Leader(name string) (string, string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, present := r.deps[name]
	if !present {
		return "", "", false
	}
	return d.LeaderPod, d.LeaderURL, d.LeaderURL != ""
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
