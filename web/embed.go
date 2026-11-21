// Package web embeds the compiled Angular app (web/dist/app/browser).
// Run `make web` (or `npm run build` inside web/) before `go build` to
// include the UI; otherwise the server falls back to an API-only mode.
package web

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var dist embed.FS

// FS returns the built Angular app, or nil if it has not been built.
func FS() fs.FS {
	sub, err := fs.Sub(dist, "dist/app/browser")
	if err != nil {
		return nil
	}
	if _, err := fs.Stat(sub, "index.html"); err != nil {
		return nil
	}
	return sub
}
