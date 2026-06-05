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
	"sync/atomic"
	"time"

	"git.horse/vapronva/concave/router/bigbrain/insights"
	"git.horse/vapronva/concave/router/bigbrain/registry"
)

const (
	keepaliveInterval = 25 * time.Second
	usageBodyLimit    = 4 * 1024 * 1024
	bearerPrefixLen   = len("bearer ")
	maxLeaderStreams  = 256
)

type Server struct {
	reg           *registry.Registry
	ins           *insights.Insights
	usageToken    string
	log           *slog.Logger
	leaderStreams atomic.Int64
}

func New(reg *registry.Registry, ins *insights.Insights, usageToken string, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{reg: reg, ins: ins, usageToken: usageToken, log: log}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /registry/deployments/{name}/leader", s.handleGetLeader)
	mux.HandleFunc("GET /registry/deployments/{name}/leader-stream", s.handleLeaderStream)
	mux.HandleFunc("POST /internal/usage", s.handleUsageIngest)
	mux.HandleFunc("GET /api/dashboard/teams/{teamId}/usage/query", s.handleUsageQuery)
	return logging(s.log, mux)
}

func (s *Server) bearerOK(r *http.Request) bool {
	if s.usageToken == "" {
		return false
	}
	provided := bearerToken(r.Header.Get("Authorization"))
	a := sha256.Sum256([]byte(provided))
	b := sha256.Sum256([]byte(s.usageToken))
	return provided != "" && subtle.ConstantTimeCompare(a[:], b[:]) == 1
}

func (s *Server) handleUsageIngest(w http.ResponseWriter, r *http.Request) {
	if !s.bearerOK(r) {
		writeErr(w, http.StatusUnauthorized, "invalid usage token")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, usageBodyLimit)
	var body struct {
		Deployment string              `json:"deployment"`
		Events     []insights.AnyEvent `json:"events"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid usage body")
		return
	}
	if body.Deployment == "" {
		writeErr(w, http.StatusBadRequest, "missing deployment")
		return
	}
	if s.ins == nil {
		writeErr(w, http.StatusServiceUnavailable, "insights disabled")
		return
	}
	kept, err := s.ins.Ingest(r.Context(), body.Deployment, body.Events)
	if err != nil {
		s.log.ErrorContext(r.Context(), "usage ingest failed", "deployment", body.Deployment, "err", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"ingested": kept})
}

func (s *Server) handleUsageQuery(w http.ResponseWriter, r *http.Request) {
	if !s.bearerOK(r) {
		writeErr(w, http.StatusUnauthorized, "invalid usage token")
		return
	}
	if s.ins == nil {
		writeErr(w, http.StatusServiceUnavailable, "insights disabled")
		return
	}
	q := r.URL.Query()
	dep := q.Get("deploymentName")
	if dep == "" {
		writeErr(w, http.StatusBadRequest, "deploymentName is required")
		return
	}
	if _, ok := s.reg.Namespace(dep); !ok {
		writeErr(w, http.StatusNotFound, "unknown deployment "+dep)
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
	out, err := s.ins.Query(r.Context(), dep, from, to)
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
	h := strings.TrimSpace(authHeader)
	if len(h) < bearerPrefixLen {
		return ""
	}
	if !strings.EqualFold(h[:bearerPrefixLen-1], "bearer") {
		return ""
	}
	rest := strings.TrimLeft(h[bearerPrefixLen-1:], " \t")
	return rest
}

type leaderResponse struct {
	Name      string `json:"name"`
	LeaderPod string `json:"leaderPod"`
	LeaderURL string `json:"leaderUrl"`
}

func (s *Server) handleGetLeader(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if _, ok := s.reg.Namespace(name); !ok {
		writeErr(w, http.StatusNotFound, "unknown deployment "+name)
		return
	}
	pod, url, ok := s.reg.Leader(name)
	if !ok {
		writeErr(w, http.StatusServiceUnavailable, "no leader for "+name)
		return
	}
	writeJSON(w, http.StatusOK, leaderResponse{Name: name, LeaderPod: pod, LeaderURL: url})
}

func (s *Server) handleLeaderStream(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if s.leaderStreams.Add(1) > maxLeaderStreams {
		s.leaderStreams.Add(-1)
		writeErr(w, http.StatusServiceUnavailable, "too many leader streams")
		return
	}
	defer s.leaderStreams.Add(-1)
	ch, cancel, ok := s.reg.Subscribe(name)
	if !ok {
		writeErr(w, http.StatusNotFound, "unknown deployment "+name)
		return
	}
	defer cancel()
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	pod, url, _ := s.reg.Leader(name)
	writeSSE(w, registry.LeaderEvent{Name: name, LeaderPod: pod, LeaderURL: url})
	flusher.Flush()
	keepalive := time.NewTicker(keepaliveInterval)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case ev, open := <-ch:
			if !open {
				return
			}
			writeSSE(w, ev)
			flusher.Flush()
		case <-keepalive.C:
			_, _ = fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	if err := enc.Encode(v); err != nil && !errors.Is(err, http.ErrHandlerTimeout) {
		_ = err
	}
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeSSE(w http.ResponseWriter, ev registry.LeaderEvent) {
	b, _ := json.Marshal(ev)
	_, _ = fmt.Fprintf(w, "event: leader\ndata: %s\n\n", b)
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

func (w *statusWriter) WriteHeader(code int) {
	if !w.wrote {
		w.status = code
		w.wrote = true
	}
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
