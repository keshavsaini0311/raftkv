package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/keshavsaini0311/raftkv/kv"
	"github.com/keshavsaini0311/raftkv/raft"
)

// registerAPI wires the client-facing HTTP surface.
//
//	GET    /kv/{key}   linearizable read (goes through ReadIndex)
//	PUT    /kv/{key}   write, body is the value
//	DELETE /kv/{key}   delete
//	GET    /status     role, term, leader, and dropped-message counters
//	GET    /keys       every key, sorted (debugging)
func (s *Server) registerAPI(mux *http.ServeMux) {
	mux.HandleFunc("/kv/", s.kvHandler)
	mux.HandleFunc("/status", s.statusHandler)
	mux.HandleFunc("/keys", s.keysHandler)
}

type apiError struct {
	Error  string `json:"error"`
	Leader string `json:"leader,omitempty"`
}

// writeNotLeader returns 421 Misdirected Request with the leader's address.
//
// 421 rather than a 307 redirect: the client must re-issue against a different
// host, and an automatic redirect would replay the request body without the
// client's session bookkeeping noticing — which is exactly how a retry turns
// into a duplicate write.
func (s *Server) writeNotLeader(w http.ResponseWriter) {
	addr, ok := s.LeaderAddr()
	w.Header().Set("Content-Type", "application/json")
	if ok {
		w.Header().Set("X-Raft-Leader", addr)
	}
	w.WriteHeader(http.StatusMisdirectedRequest)
	json.NewEncoder(w).Encode(apiError{Error: "not the leader", Leader: addr})
}

func (s *Server) writeErr(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(apiError{Error: err.Error()})
}

func (s *Server) kvHandler(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		s.writeErr(w, http.StatusBadRequest, errors.New("empty key"))
		return
	}

	// Client session headers give exactly-once writes. A client that retries
	// through a leader change without them applies its write twice, and the
	// store stops being linearizable.
	clientID, _ := strconv.ParseUint(r.Header.Get("X-Client-ID"), 10, 64)
	seq, _ := strconv.ParseUint(r.Header.Get("X-Client-Seq"), 10, 64)

	switch r.Method {
	case http.MethodGet:
		res, err := s.Read(r.Context(), key)
		s.respond(w, res, err)

	case http.MethodPut, http.MethodPost:
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			s.writeErr(w, http.StatusBadRequest, err)
			return
		}
		res, err := s.Propose(r.Context(), kv.Command{
			Op: kv.OpPut, Key: key, Value: body,
			ClientID: kv.ClientID(clientID), Seq: seq,
		})
		s.respond(w, res, err)

	case http.MethodDelete:
		res, err := s.Propose(r.Context(), kv.Command{
			Op: kv.OpDelete, Key: key,
			ClientID: kv.ClientID(clientID), Seq: seq,
		})
		s.respond(w, res, err)

	default:
		s.writeErr(w, http.StatusMethodNotAllowed, errors.New("unsupported method"))
	}
}

func (s *Server) respond(w http.ResponseWriter, res kv.Result, err error) {
	switch {
	case err == nil:
	case errors.Is(err, ErrNotLeader), errors.Is(err, raft.ErrNotLeader):
		s.writeNotLeader(w)
		return
	case errors.Is(err, raft.ErrReadIndexUnavailable):
		// Transient: the leader has not yet committed its no-op. Retrying in a
		// moment succeeds, so say so rather than failing the request outright.
		w.Header().Set("Retry-After", "1")
		s.writeErr(w, http.StatusServiceUnavailable, err)
		return
	case errors.Is(err, ErrTimeout):
		s.writeErr(w, http.StatusGatewayTimeout, err)
		return
	case errors.Is(err, ErrShuttingDwn):
		s.writeErr(w, http.StatusServiceUnavailable, err)
		return
	default:
		s.writeErr(w, http.StatusInternalServerError, err)
		return
	}

	if res.Err != "" {
		s.writeErr(w, http.StatusBadRequest, errors.New(res.Err))
		return
	}
	if !res.Found && res.Value == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(res.Value)
}

type statusResponse struct {
	ID      uint64         `json:"id"`
	Role    string         `json:"role"`
	Term    uint64         `json:"term"`
	Leader  uint64         `json:"leader"`
	Applied int            `json:"keys"`
	Dropped map[string]int `json:"dropped_messages,omitempty"`
}

func (s *Server) statusHandler(w http.ResponseWriter, r *http.Request) {
	role, term, lead := s.Status()
	dropped := make(map[string]int)
	for id, n := range s.tr.Dropped() {
		if n > 0 {
			dropped[strconv.FormatUint(uint64(id), 10)] = n
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(statusResponse{
		ID:      uint64(s.cfg.ID),
		Role:    role.String(),
		Term:    uint64(term),
		Leader:  uint64(lead),
		Applied: s.kv.Len(),
		Dropped: dropped,
	})
}

// keysHandler is a debugging aid. It reads local state WITHOUT ReadIndex, so
// the answer may be stale — which is fine for inspecting a node's view during
// a partition, and exactly why it is not the read path clients use.
func (s *Server) keysHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.kv.Keys())
}
