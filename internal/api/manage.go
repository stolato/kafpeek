package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/stolato/kafpeek/internal/kafka"
)

// decodeJSON reads a small, strict JSON body. It writes a 400 and returns
// false on failure.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 256<<10)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		badRequest(w, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

// confirmed requires destructive requests to repeat the target name, so a
// stray or scripted request cannot wipe the wrong topic.
func confirmed(w http.ResponseWriter, got, want string) bool {
	if got != want {
		badRequest(w, `"confirm" must be the exact topic name`)
		return false
	}
	return true
}

// DELETE /api/topics/{name}  {confirm}
func (s *Server) deleteTopic(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Confirm string `json:"confirm"`
	}
	name := chi.URLParam(r, "name")
	if !decodeJSON(w, r, &req) || !confirmed(w, req.Confirm, name) {
		return
	}
	if err := s.kc.DeleteTopic(r.Context(), name); err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.log.Info("topic deleted", "topic", name, "remote", r.RemoteAddr)
	w.WriteHeader(http.StatusNoContent)
}

// POST /api/topics/{name}/partitions  {count}
func (s *Server) setPartitions(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Count int `json:"count"`
	}
	if !decodeJSON(w, r, &req) {
		return
	}
	name := chi.URLParam(r, "name")
	if err := s.kc.SetPartitions(r.Context(), name, req.Count); err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.log.Info("partitions added", "topic", name, "count", req.Count, "remote", r.RemoteAddr)
	writeJSON(w, http.StatusOK, map[string]int{"partitions": req.Count})
}

// PATCH /api/topics/{name}/configs  {set: {k: v}, reset: [k]}
func (s *Server) alterTopicConfigs(w http.ResponseWriter, r *http.Request) {
	var req kafka.TopicConfigChange
	if !decodeJSON(w, r, &req) {
		return
	}
	name := chi.URLParam(r, "name")
	if err := s.kc.AlterTopicConfigs(r.Context(), name, req); err != nil {
		s.writeErr(w, r, err)
		return
	}
	set := make([]string, 0, len(req.Set))
	for k := range req.Set {
		set = append(set, k)
	}
	s.log.Info("topic configs altered", "topic", name, "set", set, "reset", req.Reset, "remote", r.RemoteAddr)
	w.WriteHeader(http.StatusNoContent)
}

// POST /api/topics/{name}/purge  {confirm, partitions?}
func (s *Server) purgeTopic(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Confirm    string  `json:"confirm"`
		Partitions []int32 `json:"partitions"`
	}
	name := chi.URLParam(r, "name")
	if !decodeJSON(w, r, &req) || !confirmed(w, req.Confirm, name) {
		return
	}
	res, err := s.kc.PurgeTopic(r.Context(), name, req.Partitions)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.log.Info("topic purged", "topic", name, "partitions", res.Partitions, "deleted", res.Deleted, "remote", r.RemoteAddr)
	writeJSON(w, http.StatusOK, res)
}

// POST /api/consumer-groups/{name}/reset-offsets
// {topic, partitions?, to, timestamp?, offset?, dryRun}
func (s *Server) resetOffsets(w http.ResponseWriter, r *http.Request) {
	var req kafka.ResetOffsetsRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	group := chi.URLParam(r, "name")
	res, err := s.kc.ResetOffsets(r.Context(), group, req)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	if res.Applied {
		s.log.Info("offsets reset", "group", group, "topic", req.Topic, "to", req.To,
			"partitions", len(res.Changes), "remote", r.RemoteAddr)
	}
	writeJSON(w, http.StatusOK, res)
}
