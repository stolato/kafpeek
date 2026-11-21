package api

import (
	"log/slog"
	"mime"
	"net"
	"net/http"
	"strings"
	"sync"
)

// contentSecurityPolicy: scripts only from our own origin (no inline JS),
// inline styles allowed (Angular injects component styles), no framing.
const contentSecurityPolicy = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; " +
	"img-src 'self' data:; font-src 'self'; connect-src 'self'; object-src 'none'; " +
	"base-uri 'self'; form-action 'self'; frame-ancestors 'none'"

// SecurityHeaders sets browser hardening headers on every response.
func SecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", contentSecurityPolicy)
		h.Set("X-Frame-Options", "DENY") // legacy browsers; CSP frame-ancestors covers modern ones
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cross-Origin-Opener-Policy", "same-origin")
		if strings.HasPrefix(r.URL.Path, "/api/") {
			// Kafka messages may be sensitive: never keep them in browser/proxy caches.
			h.Set("Cache-Control", "no-store")
		}
		next.ServeHTTP(w, r)
	})
}

// HostAllowlist blocks DNS-rebinding attacks: a malicious domain that resolves
// to 127.0.0.1 makes the browser send requests here with its own Host header,
// which would let that page read the API as "same origin". Only known hosts
// are served. A "*" entry disables the check (e.g. behind a trusted proxy that
// already validates Host).
func HostAllowlist(allowed []string, log *slog.Logger) func(http.Handler) http.Handler {
	set := map[string]bool{}
	allowAll := false
	for _, h := range allowed {
		h = strings.ToLower(strings.TrimSpace(h))
		if h == "*" {
			allowAll = true
		}
		set[strings.Trim(h, "[]")] = true
	}
	var warned sync.Map
	return func(next http.Handler) http.Handler {
		if allowAll {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			host := strings.ToLower(r.Host)
			if h, _, err := net.SplitHostPort(host); err == nil {
				host = h
			}
			host = strings.Trim(host, "[]")
			if set[host] {
				next.ServeHTTP(w, r)
				return
			}
			if _, seen := warned.LoadOrStore(host, true); !seen {
				log.Warn("request blocked: Host not allowed (add it to ALLOWED_HOSTS if legitimate)", "host", host, "remote", r.RemoteAddr)
			}
			writeJSON(w, http.StatusForbidden, map[string]string{
				"error": "host " + host + " is not allowed; add it to ALLOWED_HOSTS",
			})
		})
	}
}

// CSRFProtection rejects state-changing requests that come from another site
// (via Sec-Fetch-Site / Origin, Go's http.CrossOriginProtection) and requires
// a JSON content type: HTML forms cannot send application/json, and a
// cross-origin fetch with it needs a CORS preflight, which we never grant.
func CSRFProtection(log *slog.Logger) func(http.Handler) http.Handler {
	cop := http.NewCrossOriginProtection()
	cop.SetDenyHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Warn("request blocked: cross-site write attempt", "method", r.Method, "path", r.URL.Path,
			"origin", r.Header.Get("Origin"), "remote", r.RemoteAddr)
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "cross-site request blocked"})
	}))
	return func(next http.Handler) http.Handler {
		requireJSON := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if isUnsafe(r.Method) {
				mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
				if mt != "application/json" {
					writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
					return
				}
			}
			next.ServeHTTP(w, r)
		})
		return cop.Handler(requireJSON)
	}
}

func isUnsafe(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	}
	return true
}

// DefaultAllowedHosts returns the loopback names plus the host the server is
// explicitly bound to (e.g. HTTP_ADDR=10.0.0.5:8090 allows 10.0.0.5).
func DefaultAllowedHosts(listenAddr string) []string {
	hosts := []string{"localhost", "127.0.0.1", "::1"}
	if h, _, err := net.SplitHostPort(listenAddr); err == nil && h != "" && h != "0.0.0.0" && h != "::" {
		hosts = append(hosts, h)
	}
	return hosts
}

// IsLoopbackAddr reports whether a listen address only accepts local connections.
func IsLoopbackAddr(listenAddr string) bool {
	h, _, err := net.SplitHostPort(listenAddr)
	if err != nil || h == "" {
		return false
	}
	if h == "localhost" {
		return true
	}
	ip := net.ParseIP(h)
	return ip != nil && ip.IsLoopback()
}
