package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/stolato/kafpeek/internal/kafka"
	"github.com/stolato/kafpeek/internal/stream"
)

const (
	sseFlushEvery    = 250 * time.Millisecond // batch messages to keep the browser responsive
	sseMaxBatch      = 500
	sseMaxBatchBytes = 1 << 20
	sseHeartbeat     = 15 * time.Second // keeps proxies/load balancers from closing idle streams
	// sseWriteTimeout disconnects clients that stop reading: a stalled socket
	// would otherwise pin its handler, batch and subscriber buffer forever.
	sseWriteTimeout = 10 * time.Second
	// maxLiveClients caps concurrent live-tail streams; with the per-subscriber
	// byte budget this bounds the memory live tail can use.
	maxLiveClients = 64
)

var liveSlots = make(chan struct{}, maxLiveClients)

// sseStart writes the event-stream headers and returns the two writers shared
// by the live and search streams. Both are safe for concurrent use.
func sseStart(w http.ResponseWriter) (send func(event string, v any) error, ping func() error) {
	rc := http.NewResponseController(w)
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no") // disable nginx buffering
	w.WriteHeader(http.StatusOK)

	var mu sync.Mutex
	write := func(frame string) error {
		mu.Lock()
		defer mu.Unlock()
		// The deadline covers only this write; it is cleared afterwards so it
		// cannot leak into a later request on the same keep-alive connection.
		// Writers without deadline support (tests) just skip it.
		_ = rc.SetWriteDeadline(time.Now().Add(sseWriteTimeout))
		defer func() { _ = rc.SetWriteDeadline(time.Time{}) }()
		if _, err := fmt.Fprint(w, frame); err != nil {
			return err
		}
		return rc.Flush()
	}
	send = func(event string, v any) error {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		return write(fmt.Sprintf("event: %s\ndata: %s\n\n", event, b))
	}
	ping = func() error { return write(": ping\n\n") }
	return send, ping
}

type liveBatch struct {
	Messages []kafka.Message `json:"messages"`
	Dropped  int64           `json:"dropped"` // messages skipped because this client was too slow
	Scanned  int64           `json:"scanned"` // messages evaluated by the filter since the last batch
}

// GET /api/topics/{name}/live?filter=<CEL>  (text/event-stream)
//
// Events:
//
//	event: ready   data: {"topic": "..."}
//	event: batch   data: {"messages":[...], "dropped": N}
//	event: failure data: {"error": "..."}
func (s *Server) live(w http.ResponseWriter, r *http.Request) {
	topic := chi.URLParam(r, "name")
	prog, ok := compileFilter(w, r.URL.Query().Get("filter"))
	if !ok {
		return
	}
	if err := s.kc.TopicExists(r.Context(), topic); err != nil {
		s.writeErr(w, r, err)
		return
	}
	select {
	case liveSlots <- struct{}{}:
		defer func() { <-liveSlots }()
	default:
		s.log.Warn("live client rejected: too many streams", "topic", topic, "max", maxLiveClients, "remote", r.RemoteAddr)
		writeJSON(w, http.StatusTooManyRequests, map[string]string{
			"error": fmt.Sprintf("too many live streams open (max %d); close some tabs and try again", maxLiveClients),
		})
		return
	}

	w.Header().Set("Connection", "keep-alive")
	send, ping := sseStart(w)

	sub, unsubscribe := s.hub.Subscribe(topic, prog)
	defer unsubscribe()
	s.log.Info("live client connected", "topic", topic, "remote", r.RemoteAddr, "filter", prog.String())
	defer s.log.Info("live client disconnected", "topic", topic, "remote", r.RemoteAddr)

	if err := send("ready", map[string]string{"topic": topic}); err != nil {
		return
	}

	flush := time.NewTicker(sseFlushEvery)
	defer flush.Stop()
	heartbeat := time.NewTicker(sseHeartbeat)
	defer heartbeat.Stop()

	batch := make([]kafka.Message, 0, sseMaxBatch)
	var batchBytes int64
	sendBatch := func() error {
		dropped, scanned := sub.Dropped(), sub.Scanned()
		if len(batch) == 0 && dropped == 0 && scanned == 0 {
			return nil
		}
		err := send("batch", liveBatch{Messages: batch, Dropped: dropped, Scanned: scanned})
		clear(batch) // drop references so payloads can be collected
		batch, batchBytes = batch[:0], 0
		return err
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case m, ok := <-sub.C:
			if !ok {
				_ = sendBatch()
				_ = send("failure", map[string]string{"error": fmt.Sprint(sub.Err())})
				return
			}
			sub.Release(&m)
			batch = append(batch, m)
			batchBytes += stream.MessageBytes(&m)
			if len(batch) >= sseMaxBatch || batchBytes >= sseMaxBatchBytes {
				if sendBatch() != nil {
					return
				}
			}
		case <-flush.C:
			if sendBatch() != nil {
				return
			}
		case <-heartbeat.C:
			if ping() != nil {
				return
			}
		}
	}
}
