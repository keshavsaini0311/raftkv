package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/keshavsaini0311/raftkv/raft"
)

// Transport carries raft messages between peers over HTTP.
//
// HTTP+JSON rather than gRPC, per the design doc's open question: it keeps the
// focus on the algorithm, and a message can be reproduced with curl during
// debugging. Nothing about Raft depends on the wire format.
type Transport struct {
	self  raft.NodeID
	peers map[raft.NodeID]string
	log   *slog.Logger

	client *http.Client

	// One send queue per peer. A dead or slow peer must not block the raft
	// core, and Raft already treats message loss as normal — so a full queue
	// drops rather than waits. That is a deliberate trade: the alternative is
	// unbounded memory growth pointed at a machine that may never return.
	mu     sync.Mutex
	queues map[raft.NodeID]chan raft.Message
	wg     sync.WaitGroup
	closed bool

	// dropped counts messages shed under back-pressure, so the choice above is
	// observable rather than silent.
	droppedMu sync.Mutex
	dropped   map[raft.NodeID]int
}

const sendQueueDepth = 256

func NewTransport(self raft.NodeID, peers map[raft.NodeID]string, log *slog.Logger) *Transport {
	t := &Transport{
		self:  self,
		peers: peers,
		log:   log,
		client: &http.Client{
			// Shorter than an election timeout on purpose: a request that
			// outlives the election it belongs to is worse than useless, since
			// its reply would arrive carrying a stale term.
			Timeout: 500 * time.Millisecond,
		},
		queues:  make(map[raft.NodeID]chan raft.Message, len(peers)),
		dropped: make(map[raft.NodeID]int, len(peers)),
	}
	for id := range peers {
		q := make(chan raft.Message, sendQueueDepth)
		t.queues[id] = q
		t.wg.Add(1)
		go t.sender(id, q)
	}
	return t
}

// Send hands messages to the per-peer queues. Never blocks.
func (t *Transport) Send(msgs []raft.Message) {
	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return
	}

	for _, m := range msgs {
		q, ok := t.queues[m.To]
		if !ok {
			continue // unknown peer; nothing sensible to do
		}
		select {
		case q <- m:
		default:
			t.droppedMu.Lock()
			t.dropped[m.To]++
			n := t.dropped[m.To]
			t.droppedMu.Unlock()
			// Log sparsely: under a partition this fires continuously, and a
			// log line per dropped heartbeat is its own outage.
			if n%sendQueueDepth == 1 {
				t.log.Warn("send queue full, dropping message",
					"peer", uint64(m.To), "type", m.Type.String(), "dropped_total", n)
			}
		}
	}
}

// sender is one goroutine per peer, draining that peer's queue.
func (t *Transport) sender(id raft.NodeID, q chan raft.Message) {
	defer t.wg.Done()
	url := t.peers[id] + "/raft"

	for m := range q {
		body, err := json.Marshal(m)
		if err != nil {
			t.log.Error("encoding raft message", "err", err)
			continue
		}
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := t.client.Do(req)
		if err != nil {
			// Expected and normal: the peer is down, partitioned, or slow.
			// Raft recovers by retrying on the next heartbeat, so this is not
			// an error condition to escalate.
			continue
		}
		resp.Body.Close()
	}
}

// Close stops all senders.
func (t *Transport) Close() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	for _, q := range t.queues {
		close(q)
	}
	t.mu.Unlock()
	t.wg.Wait()
}

// Dropped reports messages shed per peer, for the status endpoint.
func (t *Transport) Dropped() map[raft.NodeID]int {
	t.droppedMu.Lock()
	defer t.droppedMu.Unlock()
	out := make(map[raft.NodeID]int, len(t.dropped))
	for k, v := range t.dropped {
		out[k] = v
	}
	return out
}

// raftHandler receives peer traffic and hands it to the driver loop.
//
// The HTTP goroutine does NOT touch the raft core. It puts the message on a
// channel and returns; run() steps it. That indirection is the entire reason
// the core needs no locks.
func (s *Server) raftHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var m raft.Message
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		http.Error(w, fmt.Sprintf("decoding message: %v", err), http.StatusBadRequest)
		return
	}

	select {
	case s.recvC <- m:
		w.WriteHeader(http.StatusOK)
	case <-s.stop:
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
	case <-r.Context().Done():
		http.Error(w, "client gone", http.StatusRequestTimeout)
	default:
		// The core is behind. Shedding here is better than queueing without
		// bound; the sender will retry on its next heartbeat.
		http.Error(w, "busy", http.StatusTooManyRequests)
	}
}

// startHTTP starts the raft and API listeners.
func (s *Server) startHTTP(ctx context.Context) error {
	raftMux := http.NewServeMux()
	raftMux.HandleFunc("/raft", s.raftHandler)

	apiMux := http.NewServeMux()
	s.registerAPI(apiMux)

	raftSrv := &http.Server{Addr: s.cfg.RaftAddr, Handler: raftMux}
	apiSrv := &http.Server{Addr: s.cfg.APIAddr, Handler: apiMux}

	go func() {
		if err := raftSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Error("raft listener", "err", err)
		}
	}()
	go func() {
		if err := apiSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Error("api listener", "err", err)
		}
	}()

	go func() {
		select {
		case <-ctx.Done():
		case <-s.stop:
		}
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		raftSrv.Shutdown(shutCtx)
		apiSrv.Shutdown(shutCtx)
		s.tr.Close()
	}()

	s.log.Info("listening", "raft", s.cfg.RaftAddr, "api", s.cfg.APIAddr)
	return nil
}
