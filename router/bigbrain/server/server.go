package server

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"git.horse/vapronva/concave/router/bigbrain/insights"
	"git.horse/vapronva/concave/router/bigbrain/registry"
)

const (
	keepaliveInterval = 25 * time.Second
	usageBodyLimit    = 4 * 1024 * 1024
	writeTimeout      = 10 * time.Second
)

type Server struct {
	reg         *registry.Registry
	ins         *insights.Insights
	usageTokens map[string]string
	log         *slog.Logger
	done        chan struct{}
	stopOnce    sync.Once
}

func New(reg *registry.Registry, ins *insights.Insights, usageTokens map[string]string, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{reg: reg, ins: ins, usageTokens: usageTokens, log: log, done: make(chan struct{})}
}

func (s *Server) Shutdown() {
	s.stopOnce.Do(func() { close(s.done) })
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.reg.AllPublished() {
			writeErr(w, http.StatusServiceUnavailable, "leadership not yet reconciled")
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /registry/deployments/{name}/leader", s.handleGetLeader)
	mux.HandleFunc("GET /registry/deployments/{name}/leader-stream", s.handleLeaderStream)
	mux.HandleFunc("POST /internal/usage", s.handleUsageIngest)
	mux.HandleFunc("GET /api/dashboard/teams/0/usage/query", s.handleUsageQuery)
	return logging(s.log, mux)
}

func (s *Server) bearerOK(r *http.Request, deployment string) bool {
	expected := s.usageTokens[deployment]
	if expected == "" {
		return false
	}
	provided := bearerToken(r.Header.Get("Authorization"))
	a := sha256.Sum256([]byte(provided))
	b := sha256.Sum256([]byte(expected))
	return provided != "" && subtle.ConstantTimeCompare(a[:], b[:]) == 1
}

func (s *Server) handleUsageIngest(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, usageBodyLimit)
	var body struct {
		Deployment string              `json:"deployment"`
		Events     []insights.AnyEvent `json:"events"`
	}
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(&body); err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			writeErr(w, http.StatusRequestEntityTooLarge, "usage body too large")
			return
		}
		writeErr(w, http.StatusBadRequest, "invalid usage body")
		return
	}
	if body.Deployment == "" {
		writeErr(w, http.StatusBadRequest, "missing deployment")
		return
	}
	if !s.bearerOK(r, body.Deployment) {
		writeErr(w, http.StatusUnauthorized, "invalid usage token")
		return
	}
	if _, ok := s.reg.Namespace(body.Deployment); !ok {
		writeErr(w, http.StatusNotFound, "unknown deployment "+body.Deployment)
		return
	}
	if s.ins == nil {
		writeErr(w, http.StatusServiceUnavailable, "insights disabled")
		return
	}
	kept := s.ins.Ingest(body.Deployment, body.Events)
	writeJSON(w, http.StatusOK, map[string]int{"ingested": kept})
}

func (s *Server) handleUsageQuery(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	dep := q.Get("deploymentName")
	if dep == "" {
		writeErr(w, http.StatusBadRequest, "deploymentName is required")
		return
	}
	if !s.bearerOK(r, dep) {
		writeErr(w, http.StatusUnauthorized, "invalid usage token")
		return
	}
	if _, ok := s.reg.Namespace(dep); !ok {
		writeErr(w, http.StatusNotFound, "unknown deployment "+dep)
		return
	}
	if s.ins == nil {
		writeErr(w, http.StatusServiceUnavailable, "insights disabled")
		return
	}
	now := time.Now().UTC()
	from := q.Get("from")
	to := q.Get("to")
	if to == "" {
		to = now.Format("2006-01-02")
	}
	if from == "" {
		from = now.AddDate(0, 0, -3).Format("2006-01-02")
	}
	out, err := s.ins.Query(dep, from, to)
	if err != nil {
		if errors.Is(err, insights.ErrBadDateRange) {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		s.log.ErrorContext(r.Context(), "usage query failed", "deployment", dep, "err", err)
		writeErr(w, http.StatusInternalServerError, "query failed")
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func bearerToken(authHeader string) string {
	scheme, token, ok := strings.Cut(strings.TrimSpace(authHeader), " ")
	if !ok || !strings.EqualFold(scheme, "bearer") {
		return ""
	}
	token = strings.TrimSpace(token)
	if token == "" || strings.ContainsAny(token, " \t") {
		return ""
	}
	return token
}

func (s *Server) handleGetLeader(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if _, ok := s.reg.Namespace(name); !ok {
		writeErr(w, http.StatusNotFound, "unknown deployment "+name)
		return
	}
	if !s.reg.Published(name) {
		writeErr(w, http.StatusTooEarly, "leadership not yet reconciled for "+name)
		return
	}
	pod, url, seq, ok := s.reg.Leader(name)
	if !ok {
		writeJSON(w, http.StatusServiceUnavailable, registry.LeaderEvent{Name: name, Seq: seq})
		return
	}
	writeJSON(w, http.StatusOK, registry.LeaderEvent{Name: name, LeaderPod: pod, LeaderURL: url, Seq: seq})
}

func (s *Server) handleLeaderStream(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if _, ok := s.reg.Namespace(name); !ok {
		writeErr(w, http.StatusNotFound, "unknown deployment "+name)
		return
	}
	ch, cancel, ok := s.reg.Subscribe(name)
	if !ok {
		writeErr(w, http.StatusServiceUnavailable, "too many leader-stream subscribers")
		return
	}
	defer cancel()
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	rc := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if s.reg.Published(name) {
		pod, url, seq, _ := s.reg.Leader(name)
		if !emitSSE(w, rc, flusher, registry.LeaderEvent{Name: name, LeaderPod: pod, LeaderURL: url, Seq: seq}) {
			return
		}
	}
	keepalive := time.NewTicker(keepaliveInterval)
	defer keepalive.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-r.Context().Done():
			return
		case ev, open := <-ch:
			if !open {
				return
			}
			if !emitSSE(w, rc, flusher, ev) {
				return
			}
		case <-keepalive.C:
			if !emitKeepalive(w, rc, flusher) {
				return
			}
		}
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func emitSSE(w http.ResponseWriter, rc *http.ResponseController, flusher http.Flusher, ev registry.LeaderEvent) bool {
	_ = rc.SetWriteDeadline(time.Now().Add(writeTimeout))
	b, _ := json.Marshal(ev)
	if _, err := fmt.Fprintf(w, "event: leader\ndata: %s\n\n", b); err != nil {
		return false
	}
	flusher.Flush()
	return true
}

func emitKeepalive(w http.ResponseWriter, rc *http.ResponseController, flusher http.Flusher) bool {
	_ = rc.SetWriteDeadline(time.Now().Add(writeTimeout))
	if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
		return false
	}
	flusher.Flush()
	return true
}

func logging(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		level := slog.LevelInfo
		if strings.HasSuffix(r.URL.Path, "/leader") || strings.HasSuffix(r.URL.Path, "/leader-stream") {
			level = slog.LevelDebug
		}
		log.Log(
			r.Context(),
			level,
			"http",
			"method",
			r.Method,
			"path",
			r.URL.Path,
			"status",
			sw.status,
			"dur",
			time.Since(start).String(),
		)
	})
}

type statusWriter struct {
	http.ResponseWriter

	status int
	wrote  bool
}

func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *statusWriter) WriteHeader(code int) {
	if w.wrote {
		return
	}
	w.status = code
	w.wrote = true
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	w.wrote = true
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
