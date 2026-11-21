package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"log/slog"
	"net/http"
	"time"
)

// failedAuthDelay slows down password guessing without any per-IP state.
const failedAuthDelay = 400 * time.Millisecond

// BasicAuth protects every route (UI, REST and SSE) with HTTP Basic auth.
// Browsers show their native login prompt and then resend the credentials
// automatically on every request to the same origin, including EventSource.
//
// Credentials are compared in constant time over SHA-256 digests, so neither
// the content nor the length of the secret leaks through response timing.
func BasicAuth(user, pass string, log *slog.Logger) func(http.Handler) http.Handler {
	wantUser := sha256.Sum256([]byte(user))
	wantPass := sha256.Sum256([]byte(pass))

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			u, p, ok := r.BasicAuth()
			if ok {
				gotUser := sha256.Sum256([]byte(u))
				gotPass := sha256.Sum256([]byte(p))
				// Evaluate both comparisons (no short-circuit) before deciding.
				userOK := subtle.ConstantTimeCompare(gotUser[:], wantUser[:])
				passOK := subtle.ConstantTimeCompare(gotPass[:], wantPass[:])
				if userOK&passOK == 1 {
					next.ServeHTTP(w, r)
					return
				}
				// The typed username is not logged: people sometimes paste the password there.
				log.Warn("ui login failed", "remote", r.RemoteAddr, "path", r.URL.Path)
				time.Sleep(failedAuthDelay)
			}
			w.Header().Set("WWW-Authenticate", `Basic realm="kafpeek", charset="UTF-8"`)
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
		})
	}
}
