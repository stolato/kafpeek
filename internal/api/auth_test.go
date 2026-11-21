package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBasicAuth(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	h := BasicAuth("admin", "s3cr3t", slog.New(slog.NewTextHandler(io.Discard, nil)))(ok)

	cases := []struct {
		name       string
		user, pass string
		set        bool
		want       int
	}{
		{"no credentials", "", "", false, http.StatusUnauthorized},
		{"right credentials", "admin", "s3cr3t", true, http.StatusOK},
		{"wrong password", "admin", "nope", true, http.StatusUnauthorized},
		{"wrong user", "root", "s3cr3t", true, http.StatusUnauthorized},
		{"password prefix", "admin", "s3cr", true, http.StatusUnauthorized},
		{"empty password", "admin", "", true, http.StatusUnauthorized},
	}
	for _, c := range cases {
		req := httptest.NewRequest(http.MethodGet, "/api/topics", nil)
		if c.set {
			req.SetBasicAuth(c.user, c.pass)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != c.want {
			t.Errorf("%s: got %d, want %d", c.name, rec.Code, c.want)
		}
		if c.want == http.StatusUnauthorized && rec.Header().Get("WWW-Authenticate") == "" {
			t.Errorf("%s: missing WWW-Authenticate header (browser would not prompt)", c.name)
		}
	}
}
