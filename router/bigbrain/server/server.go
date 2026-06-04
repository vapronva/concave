package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"git.horse/vapronva/concave/router/bigbrain/registry"
)

const keepaliveInterval = 25 * time.Second

type Server struct {
	reg *registry.Registry
	log *slog.Logger
}

func New(reg *registry.Registry, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{reg: reg, log: log}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /registry/deployments", s.handleListDeployments)
	mux.HandleFunc("GET /registry/deployments/{name}", s.handleGetDeployment)
	mux.HandleFunc("GET /registry/deployments/{name}/leader", s.handleGetLeader)
	mux.HandleFunc("GET /registry/deployments/{name}/leader-stream", s.handleLeaderStream)
	return logging(s.log, mux)
}

func (s *Server) handleListDeployments(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.reg.SnapshotAll())
}

func (s *Server) handleGetDeployment(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	d, ok := s.reg.Snapshot(name)
	if !ok {
		writeErr(w, http.StatusNotFound, "unknown deployment "+name)
		return
	}
	writeJSON(w, http.StatusOK, d)
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
		if strings.HasSuffix(r.URL.Path, "/leader") || strings.HasSuffix(r.URL.Path, "/leader-stream") {
			return
		}
		log.InfoContext(
			r.Context(),
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
