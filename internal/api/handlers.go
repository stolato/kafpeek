package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/stolato/kafpeek/internal/filter"
	"github.com/stolato/kafpeek/internal/kafka"
	"github.com/stolato/kafpeek/internal/version"
)

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "cluster": s.kc.Name(), "readOnly": s.kc.ReadOnly(), "build": version.Get(),
		"schemaRegistry": s.kc.HasSchemaRegistry(),
	})
}

func (s *Server) cluster(w http.ResponseWriter, r *http.Request) {
	info, err := s.kc.Cluster(r.Context())
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, info)
}

func (s *Server) topics(w http.ResponseWriter, r *http.Request) {
	topics, err := s.kc.Topics(r.Context())
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, topics)
}

func (s *Server) topic(w http.ResponseWriter, r *http.Request) {
	t, err := s.kc.Topic(r.Context(), chi.URLParam(r, "name"))
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

// POST /api/topics  {name, partitions, replicationFactor, configs}
func (s *Server) createTopic(w http.ResponseWriter, r *http.Request) {
	var req kafka.CreateTopicRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	res, err := s.kc.CreateTopic(r.Context(), req)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.log.Info("topic created", "topic", res.Name, "partitions", res.Partitions,
		"replicationFactor", res.ReplicationFactor, "configs", len(req.Configs), "remote", r.RemoteAddr)
	writeJSON(w, http.StatusCreated, res)
}

// GET /api/topics/{name}/messages?limit=100&partition=0&filter=<CEL>&scan=10000
func (s *Server) messages(w http.ResponseWriter, r *http.Request) {
	opts := kafka.BrowseOptions{Limit: 100}
	q := r.URL.Query()
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			badRequest(w, "limit must be between 1 and 1000")
			return
		}
		opts.Limit = n
	}
	part, ok := parsePartition(w, q.Get("partition"))
	if !ok {
		return
	}
	opts.Partition = part
	prog, ok := compileFilter(w, q.Get("filter"))
	if !ok {
		return
	}
	if prog != nil {
		opts.Match = func(m *kafka.Message) bool { return prog.Match(filter.NewInput(m)) }
		if v := q.Get("scan"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 || n > kafka.MaxScan {
				badRequest(w, "scan must be between 1 and "+strconv.Itoa(kafka.MaxScan))
				return
			}
			opts.Scan = n
		}
	}
	res, err := s.kc.Browse(r.Context(), chi.URLParam(r, "name"), opts)
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// POST /api/topics/{name}/messages  {key, value, headers, partition}
func (s *Server) produce(w http.ResponseWriter, r *http.Request) {
	if s.kc.ReadOnly() {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": kafka.ErrReadOnly.Error()})
		return
	}
	var req kafka.ProduceRequest
	r.Body = http.MaxBytesReader(w, r.Body, 2*kafka.MaxProduceBytes) // JSON escaping overhead
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		badRequest(w, "invalid JSON body: "+err.Error())
		return
	}
	topic := chi.URLParam(r, "name")
	res, err := s.kc.Produce(r.Context(), topic, req)
	if errors.Is(err, kafka.ErrInvalid) {
		badRequest(w, err.Error())
		return
	}
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	s.log.Info("message produced", "topic", topic, "partition", res.Partition, "offset", res.Offset, "remote", r.RemoteAddr)
	writeJSON(w, http.StatusCreated, res)
}

func (s *Server) groups(w http.ResponseWriter, r *http.Request) {
	gs, err := s.kc.ConsumerGroups(r.Context())
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, gs)
}

func (s *Server) group(w http.ResponseWriter, r *http.Request) {
	g, err := s.kc.ConsumerGroup(r.Context(), chi.URLParam(r, "name"))
	if err != nil {
		s.writeErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, g)
}

// GET /api/filter/validate?expr=<CEL>  -> 200 {"ok":true} | 400 {"error","column"}
func (s *Server) validateFilter(w http.ResponseWriter, r *http.Request) {
	if _, ok := compileFilter(w, r.URL.Query().Get("expr")); ok {
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	}
}

// parsePartition returns nil for an empty value (all partitions). It writes a
// 400 and returns false when the value is not a valid partition number.
func parsePartition(w http.ResponseWriter, v string) (*int32, bool) {
	if v == "" {
		return nil, true
	}
	p, err := strconv.ParseInt(v, 10, 32)
	if err != nil || p < 0 {
		badRequest(w, "invalid partition")
		return nil, false
	}
	p32 := int32(p)
	return &p32, true
}

// compileFilter writes a 400 with the error position when expr is invalid.
func compileFilter(w http.ResponseWriter, expr string) (*filter.Program, bool) {
	prog, err := filter.Compile(expr)
	if err != nil {
		if ce, ok := filter.AsCompileError(err); ok {
			writeJSON(w, http.StatusBadRequest, ce)
		} else {
			badRequest(w, err.Error())
		}
		return nil, false
	}
	return prog, true
}

func (s *Server) streams(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.hub.Stats())
}
