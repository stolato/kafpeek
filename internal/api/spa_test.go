package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"
)

func TestSPAHandlerServesDottedRoutes(t *testing.T) {
	ui := fstest.MapFS{
		"index.html":  {Data: []byte("<app-root>")},
		"main-abc.js": {Data: []byte("js")},
	}
	h := spaHandler(ui)
	cases := map[string]int{
		"/":                         http.StatusOK,
		"/topics/orders":            http.StatusOK,
		"/topics/orders.created.v1": http.StatusOK, // route, not a file
		"/groups/billing.v2":        http.StatusOK,
		"/main-abc.js":              http.StatusOK,
		"/chunk-missing.js":         http.StatusNotFound,
		"/favicon.png":              http.StatusNotFound,
	}
	for p, want := range cases {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))
		if rec.Code != want {
			t.Errorf("GET %s = %d, want %d", p, rec.Code, want)
		}
	}
}
