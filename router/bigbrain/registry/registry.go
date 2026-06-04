package registry

import (
	"sync"
	"time"

	"git.horse/vapronva/concave/router/bigbrain/backend"
	"git.horse/vapronva/concave/router/bigbrain/k8sclient"
)

type BackendState struct {
	Pod      string             `json:"pod"`
	URL      string             `json:"url"`
	Ready    bool               `json:"ready"`
	Phase    string             `json:"phase"`
	Role     string             `json:"role,omitempty"`
	Reach    bool               `json:"reachable"`
	Status   backend.Leadership `json:"status"`
	LastSeen time.Time          `json:"lastSeen"`
}

type Deployment struct {
	Name      string         `json:"name"`
	Namespace string         `json:"namespace"`
	LeaderPod string         `json:"leaderPod,omitempty"`
	LeaderURL string         `json:"leaderUrl,omitempty"`
	Backends  []BackendState `json:"backends"`
	Updated   time.Time      `json:"updated"`
	Phase     string         `json:"phase"`
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

const subscriberBuffer = 4

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
		r.deps[name] = &Deployment{Name: name, Namespace: ns, Phase: "Unknown", Backends: []BackendState{}}
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

func (r *Registry) Update(name string, backends []BackendState, leaderPod, leaderURL string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.deps[name]
	if !ok {
		return
	}
	changed := d.LeaderPod != leaderPod
	d.Backends = backends
	d.LeaderPod = leaderPod
	d.LeaderURL = leaderURL
	d.Updated = time.Now()
	if leaderPod == "" {
		d.Phase = "Leaderless"
	} else {
		d.Phase = "Ready"
	}
	if !changed {
		return
	}
	ev := LeaderEvent{Name: name, LeaderPod: leaderPod, LeaderURL: leaderURL}
	for ch := range r.subs[name] {
		select {
		case ch <- ev:
		default:
		}
	}
}

func (r *Registry) Snapshot(name string) (Deployment, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.deps[name]
	if !ok {
		return Deployment{}, false
	}
	return cloneDeployment(d), true
}

func (r *Registry) SnapshotAll() []Deployment {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Deployment, 0, len(r.deps))
	for _, d := range r.deps {
		out = append(out, cloneDeployment(d))
	}
	return out
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

func cloneDeployment(d *Deployment) Deployment {
	cp := *d
	cp.Backends = append([]BackendState(nil), d.Backends...)
	return cp
}

func BackendStateFrom(b k8sclient.Backend, status backend.Leadership, reachable bool, seen time.Time) BackendState {
	bs := BackendState{
		Pod:   b.Pod,
		URL:   b.URL,
		Ready: b.Ready,
		Phase: b.Phase,
		Role:  b.Role,
		Reach: reachable,
	}
	if reachable {
		bs.Status = status
		bs.LastSeen = seen
	}
	return bs
}
