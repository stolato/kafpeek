package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Both streams share this framing; the browser's EventSource depends on it.
func TestSSEFraming(t *testing.T) {
	rec := httptest.NewRecorder()
	send, ping := sseStart(rec)
	if err := send("batch", map[string]int{"dropped": 2}); err != nil {
		t.Fatal(err)
	}
	if err := ping(); err != nil {
		t.Fatal(err)
	}
	if got, want := rec.Body.String(), "event: batch\ndata: {\"dropped\":2}\n\n: ping\n\n"; got != want {
		t.Fatalf("frames = %q, want %q", got, want)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q", ct)
	}
}

func TestParsePartition(t *testing.T) {
	rec := httptest.NewRecorder()
	if p, ok := parsePartition(rec, ""); !ok || p != nil {
		t.Fatalf("empty: p=%v ok=%v, want nil/true", p, ok)
	}
	if p, ok := parsePartition(rec, "3"); !ok || *p != 3 {
		t.Fatalf("3: ok=%v", ok)
	}
	for _, bad := range []string{"-1", "x", "99999999999"} {
		rec := httptest.NewRecorder()
		if _, ok := parsePartition(rec, bad); ok || rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "invalid partition") {
			t.Fatalf("%q: ok=%v code=%d", bad, ok, rec.Code)
		}
	}
}
