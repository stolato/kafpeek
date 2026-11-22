package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/stolato/kafpeek/internal/filter"
	"github.com/stolato/kafpeek/internal/kafka"
)

// maxConcurrentSearches protects the cluster (whole-topic scans pull a lot of
// data) and this process.
const maxConcurrentSearches = 4

var searchSlots = make(chan struct{}, maxConcurrentSearches)

// GET /api/topics/{name}/search  (text/event-stream)
//
// Query: filter=<CEL> partition=N limit=N
//
//	mode=newest&scan=N | mode=range&from=RFC3339&to=RFC3339 | mode=all
//
// Events:
//
//	start   {"total": N}
//	batch   {"messages": [...], "progress": {...}}   (every ~250ms)
//	done    {"reason": "finished|limit|timeout|incomplete", "progress": {...}, "elapsedMs": N}
//	failure {"error": "..."}
//
// Closing the connection cancels the scan immediately.
func (s *Server) search(w http.ResponseWriter, r *http.Request) {
	topic := chi.URLParam(r, "name")
	q := r.URL.Query()

	opts, ok := parseSearchOptions(w, q)
	if !ok {
		return
	}
	prog, ok := compileFilter(w, q.Get("filter"))
	if !ok {
		return
	}
	if prog != nil {
		opts.Match = func(m *kafka.Message) bool { return prog.Match(filter.NewInput(m)) }
	}

	select {
	case searchSlots <- struct{}{}:
		defer func() { <-searchSlots }()
	default:
		writeJSON(w, http.StatusTooManyRequests, map[string]string{
			"error": fmt.Sprintf("too many searches running (max %d); try again in a moment", maxConcurrentSearches),
		})
		return
	}

	// The scan goroutine and this goroutine both send; sseStart serializes them.
	send, _ := sseStart(w)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	matches := make(chan kafka.Message, 512) // backpressure: a slow browser slows the scan, nothing is dropped
	var (
		progMu   sync.Mutex
		progress kafka.SearchProgress
	)
	type outcome struct {
		res kafka.SearchResult
		err error
	}
	doneCh := make(chan outcome, 1)
	started := time.Now()

	s.log.Info("search started", "topic", topic, "filter", prog.String(), "remote", r.RemoteAddr)
	go func() {
		res, err := s.kc.Search(ctx, topic, opts, kafka.SearchCallbacks{
			OnStart: func(total int64) { _ = send("start", map[string]int64{"total": total}) },
			OnMatch: func(m kafka.Message) error {
				select {
				case matches <- m:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
			OnProgress: func(p kafka.SearchProgress) {
				progMu.Lock()
				progress = p
				progMu.Unlock()
			},
		})
		close(matches)
		if errors.Is(err, context.Canceled) {
			s.log.Info("search cancelled by client", "topic", topic, "scanned", res.Scanned, "position", res.Position, "total", res.Total)
		}
		doneCh <- outcome{res, err}
	}()

	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	batch := make([]kafka.Message, 0, 256)
	flush := func() error {
		progMu.Lock()
		p := progress
		progMu.Unlock()
		err := send("batch", map[string]any{"messages": batch, "progress": p})
		batch = batch[:0]
		return err
	}

	for {
		select {
		case m, ok := <-matches:
			if !ok {
				out := <-doneCh
				if len(batch) > 0 {
					_ = flush()
				}
				elapsed := time.Since(started).Milliseconds()
				if out.err != nil {
					if !errors.Is(out.err, context.Canceled) {
						s.log.Warn("search failed", "topic", topic, "err", out.err)
						_ = send("failure", map[string]string{"error": out.err.Error()})
					}
					return
				}
				s.log.Info("search done", "topic", topic, "reason", out.res.Reason, "scanned", out.res.Scanned,
					"matched", out.res.Matched, "ms", elapsed)
				_ = send("done", map[string]any{"reason": out.res.Reason, "progress": out.res.SearchProgress, "elapsedMs": elapsed})
				return
			}
			batch = append(batch, m)
			if len(batch) >= 256 {
				if flush() != nil {
					return // client gone: deferred cancel stops the scan
				}
			}
		case <-tick.C:
			if flush() != nil {
				return
			}
		case <-r.Context().Done():
			return
		}
	}
}

func parseSearchOptions(w http.ResponseWriter, q map[string][]string) (kafka.SearchOptions, bool) {
	get := func(k string) string {
		if v := q[k]; len(v) > 0 {
			return v[0]
		}
		return ""
	}
	opts := kafka.SearchOptions{Limit: 1000}
	if v := get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > kafka.MaxSearchMatches {
			badRequest(w, fmt.Sprintf("limit must be between 1 and %d", kafka.MaxSearchMatches))
			return opts, false
		}
		opts.Limit = n
	}
	part, ok := parsePartition(w, get("partition"))
	if !ok {
		return opts, false
	}
	opts.Partition = part
	switch mode := get("mode"); mode {
	case "", "newest":
		if v := get("scan"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 || n > kafka.MaxScan {
				badRequest(w, fmt.Sprintf("scan must be between 1 and %d", kafka.MaxScan))
				return opts, false
			}
			opts.Scan = n
		}
	case "all":
		opts.All = true
	case "range":
		for _, f := range []struct {
			key string
			dst *time.Time
		}{{"from", &opts.From}, {"to", &opts.To}} {
			if v := get(f.key); v != "" {
				t, err := time.Parse(time.RFC3339, v)
				if err != nil {
					badRequest(w, f.key+" must be an RFC 3339 timestamp, e.g. 2026-11-19T14:00:00-03:00")
					return opts, false
				}
				*f.dst = t
			}
		}
		if opts.From.IsZero() && opts.To.IsZero() {
			badRequest(w, "mode=range needs from and/or to")
			return opts, false
		}
		if !opts.From.IsZero() && !opts.To.IsZero() && !opts.To.After(opts.From) {
			badRequest(w, "to must be after from")
			return opts, false
		}
	default:
		badRequest(w, "mode must be newest, range or all")
		return opts, false
	}
	return opts, true
}
