package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
}

func TestHostAllowlist(t *testing.T) {
	h := HostAllowlist(append(DefaultAllowedHosts(":8090"), "kafpeek.internal"), discard)(okHandler())
	cases := map[string]int{
		"localhost:8090":         200,
		"127.0.0.1:8090":         200,
		"[::1]:8090":             200,
		"KAFPEEK.internal":       200,
		"attacker.example":       403, // DNS rebinding
		"attacker.example:8090":  403,
		"localhost.attacker.com": 403,
		"192.168.0.10:8090":      403, // LAN IP not configured
	}
	for host, want := range cases {
		req := httptest.NewRequest(http.MethodGet, "/api/topics", nil)
		req.Host = host
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != want {
			t.Errorf("Host %q: got %d, want %d", host, rec.Code, want)
		}
	}

	allowAll := HostAllowlist([]string{"*"}, discard)(okHandler())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Host = "whatever.example"
	rec := httptest.NewRecorder()
	allowAll.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Errorf(`"*" should allow any host, got %d`, rec.Code)
	}
}

func TestCSRFProtection(t *testing.T) {
	h := CSRFProtection(discard)(okHandler())
	cases := []struct {
		name    string
		method  string
		ctype   string
		headers map[string]string
		want    int
	}{
		{"GET is always allowed", "GET", "", map[string]string{"Sec-Fetch-Site": "cross-site"}, 200},
		{"same-origin JSON POST", "POST", "application/json", map[string]string{"Sec-Fetch-Site": "same-origin"}, 200},
		{"non-browser JSON POST (curl)", "POST", "application/json; charset=utf-8", nil, 200},
		{"cross-site POST (form CSRF)", "POST", "text/plain", map[string]string{"Sec-Fetch-Site": "cross-site", "Origin": "https://evil.example"}, 403},
		{"cross-site JSON POST", "POST", "application/json", map[string]string{"Sec-Fetch-Site": "cross-site"}, 403},
		{"old browser, foreign Origin", "POST", "application/json", map[string]string{"Origin": "https://evil.example"}, 403},
		{"text/plain POST without browser headers", "POST", "text/plain", nil, 415},
		{"form-urlencoded POST", "POST", "application/x-www-form-urlencoded", nil, 415},
	}
	for _, c := range cases {
		req := httptest.NewRequest(c.method, "http://localhost:8090/api/topics/t/messages", strings.NewReader(`{}`))
		if c.ctype != "" {
			req.Header.Set("Content-Type", c.ctype)
		}
		for k, v := range c.headers {
			req.Header.Set(k, v)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != c.want {
			t.Errorf("%s: got %d, want %d", c.name, rec.Code, c.want)
		}
	}
}

func TestSecurityHeaders(t *testing.T) {
	rec := httptest.NewRecorder()
	SecurityHeaders(okHandler()).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/topics", nil))
	for _, h := range []string{"Content-Security-Policy", "X-Frame-Options", "X-Content-Type-Options", "Referrer-Policy"} {
		if rec.Header().Get(h) == "" {
			t.Errorf("missing %s", h)
		}
	}
	if !strings.Contains(rec.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'") {
		t.Error("CSP must forbid framing")
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Error("API responses must not be cached")
	}
}

func TestLoopbackDetection(t *testing.T) {
	cases := map[string]bool{"127.0.0.1:8090": true, "localhost:8090": true, "[::1]:8090": true, ":8090": false, "0.0.0.0:8090": false, "10.0.0.5:8090": false}
	for addr, want := range cases {
		if got := IsLoopbackAddr(addr); got != want {
			t.Errorf("IsLoopbackAddr(%q) = %v, want %v", addr, got, want)
		}
	}
}
