// Package api exposes the REST + SSE API and serves the embedded Angular app.
package api

import (
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/stolato/kafpeek/internal/kafka"
	"github.com/stolato/kafpeek/internal/stream"
)

type Server struct {
	kc  *kafka.Client
	hub *stream.Hub
	log *slog.Logger
}

// Options configures the HTTP layer.
type Options struct {
	// Username/Password enable Basic auth on every route when both are set.
	Username string
	Password string
	// AllowedHosts are the Host header values served (DNS-rebinding guard).
	AllowedHosts []string
	// TrustProxy honors X-Forwarded-For / X-Real-IP. Off by default: any
	// client could otherwise forge the IP recorded in audit logs.
	TrustProxy bool
}

func NewRouter(kc *kafka.Client, hub *stream.Hub, log *slog.Logger, ui fs.FS, opts Options) http.Handler {
	s := &Server{kc: kc, hub: hub, log: log}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	if opts.TrustProxy {
		r.Use(middleware.RealIP)
	}
	r.Use(
		middleware.Recoverer,
		SecurityHeaders,
		HostAllowlist(opts.AllowedHosts, log), // before auth: rebinding attempts never reach the login
		CSRFProtection(log),
	)
	if opts.Username != "" && opts.Password != "" {
		r.Use(BasicAuth(opts.Username, opts.Password, log))
	}

	r.Route("/api", func(r chi.Router) {
		// Regular REST endpoints: bounded time, compressed responses.
		r.Group(func(r chi.Router) {
			r.Use(requestLogger(log), middleware.Timeout(30*time.Second), middleware.Compress(5))
			r.Get("/health", s.health)
			r.Get("/cluster", s.cluster)
			r.Get("/topics", s.topics)
			r.Post("/topics", s.createTopic)
			r.Get("/topics/{name}", s.topic)
			r.Delete("/topics/{name}", s.deleteTopic)
			r.Post("/topics/{name}/partitions", s.setPartitions)
			r.Patch("/topics/{name}/configs", s.alterTopicConfigs)
			r.Post("/topics/{name}/purge", s.purgeTopic)
			r.Get("/topics/{name}/messages", s.messages)
			r.Post("/topics/{name}/messages", s.produce)
			r.Get("/consumer-groups", s.groups)
			r.Get("/consumer-groups/{name}", s.group)
			r.Post("/consumer-groups/{name}/reset-offsets", s.resetOffsets)
			r.Get("/streams", s.streams)
			r.Get("/filter/validate", s.validateFilter)
		})
		// Long-lived SSE stream: no timeout, no compression (needs flushing).
		r.Get("/topics/{name}/live", s.live)
		r.Get("/topics/{name}/search", s.search)
	})

	r.Handle("/*", spaHandler(ui))
	return r
}

// ---- helpers ----

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) writeErr(w http.ResponseWriter, r *http.Request, err error) {
	status := http.StatusBadGateway // most failures come from the Kafka cluster
	switch {
	case errors.Is(err, kafka.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, kafka.ErrInvalid):
		status = http.StatusBadRequest
	case errors.Is(err, kafka.ErrConflict):
		status = http.StatusConflict
	case errors.Is(err, kafka.ErrForbidden), errors.Is(err, kafka.ErrReadOnly):
		status = http.StatusForbidden
	}
	s.log.Warn("request failed", "path", r.URL.Path, "status", status, "err", err)
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func badRequest(w http.ResponseWriter, msg string) {
	writeJSON(w, http.StatusBadRequest, map[string]string{"error": msg})
}

func requestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			start := time.Now()
			next.ServeHTTP(ww, r)
			log.Debug("http", "method", r.Method, "path", r.URL.Path, "status", ww.Status(), "dur", time.Since(start))
		})
	}
}

// assetExts are the file types the Angular build emits. Other dotted paths
// (e.g. /topics/orders.created.v1) are client-side routes.
var assetExts = map[string]bool{
	".js": true, ".mjs": true, ".css": true, ".map": true, ".html": true, ".json": true, ".txt": true,
	".ico": true, ".png": true, ".svg": true, ".jpg": true, ".webp": true, ".webmanifest": true,
	".woff": true, ".woff2": true, ".ttf": true,
}

func isAsset(p string) bool { return assetExts[strings.ToLower(path.Ext(p))] }

// spaHandler serves the Angular build and falls back to index.html so
// client-side routes (e.g. /topics/orders) work on reload.
func spaHandler(ui fs.FS) http.Handler {
	if ui == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = w.Write([]byte("kafpeek API is running. UI not embedded: run `make web` then rebuild,\n" +
				"or use the Angular dev server (`make web-dev`) at http://localhost:4200\n"))
		})
	}
	files := http.FileServerFS(ui)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if p == "" {
			p = "index.html"
		}
		if _, err := fs.Stat(ui, p); err != nil {
			if isAsset(p) { // missing asset: real 404 (topic names like orders.v1 are routes)
				http.NotFound(w, r)
				return
			}
			r2 := r.Clone(r.Context())
			r2.URL.Path = "/"
			files.ServeHTTP(w, r2)
			return
		}
		files.ServeHTTP(w, r)
	})
}
