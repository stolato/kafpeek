// Package version exposes build metadata. Values are injected at build time:
//
//	go build -ldflags "-X github.com/stolato/kafpeek/internal/version.Version=0.1.0 \
//	  -X github.com/stolato/kafpeek/internal/version.Commit=$(git rev-parse --short HEAD) \
//	  -X github.com/stolato/kafpeek/internal/version.BuildDate=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
//
// (`make build` does this from the VERSION file). Without ldflags, the commit
// and date fall back to the VCS info Go embeds automatically in builds.
package version

import (
	"runtime/debug"
	"sync"
)

var (
	Version   = "dev"
	Commit    = ""
	BuildDate = ""
)

type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit,omitempty"`
	BuildDate string `json:"buildDate,omitempty"`
	Dirty     bool   `json:"dirty,omitempty"` // built with uncommitted changes
}

var (
	once sync.Once
	info Info
)

func Get() Info {
	once.Do(func() {
		info = Info{Version: Version, Commit: Commit, BuildDate: BuildDate}
		bi, ok := debug.ReadBuildInfo()
		if !ok {
			return
		}
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				if info.Commit == "" && len(s.Value) >= 7 {
					info.Commit = s.Value[:7]
				}
			case "vcs.time":
				if info.BuildDate == "" {
					info.BuildDate = s.Value
				}
			case "vcs.modified":
				info.Dirty = s.Value == "true"
			}
		}
	})
	return info
}

// String renders e.g. "0.1.0 (a1b2c3d)" or "0.1.0 (a1b2c3d, modified)".
func (i Info) String() string {
	s := i.Version
	switch {
	case i.Commit != "" && i.Dirty:
		s += " (" + i.Commit + ", modified)"
	case i.Commit != "":
		s += " (" + i.Commit + ")"
	case i.Dirty:
		s += " (modified)"
	}
	return s
}
