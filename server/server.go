// Package server is the real-world driver: goroutines, wall-clock timers, HTTP
// transport, and an fsync-ing file.
//
// Everything the raft core refuses to do happens here. The core is single
// threaded by construction, so exactly ONE goroutine — run() — ever touches it.
// Every other goroutine (HTTP handlers, the ticker, inbound peer traffic)
// communicates with it over channels. That is what lets the core hold no locks
// and still be safe under a concurrent server.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/keshavsaini0311/raftkv/kv"
	"github.com/keshavsaini0311/raftkv/raft"
	"github.com/keshavsaini0311/raftkv/storage"
)

var (
	ErrTimeout     = errors.New("server: request timed out")
	ErrNotLeader   = errors.New("server: not the leader")
	ErrShuttingDwn = errors.New("server: shutting down")
)

// Config describes one node.
type Config struct {
	ID    raft.NodeID
	Peers map[raft.NodeID]string // peer id -> base URL for raft traffic

	DataDir  string
	RaftAddr string
	APIAddr  string

	// TickInterval is what one logical tick means in wall time. The core
	// counts ticks; only this line decides how fast they pass.
	TickInterval time.Duration

	// Seed makes this node's election-timeout jitter reproducible. The core
	// takes randomness as an injected function precisely so this is the only
	// place a seed exists.
	Seed int64

	// RequestTimeout bounds how long a client waits for a proposal to commit.
	RequestTimeout time.Duration

	Logger *slog.Logger
}

func (c *Config) setDefaults() {
	if c.TickInterval == 0 {
		c.TickInterval = 50 * time.Millisecond
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 5 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.DataDir == "" {
		c.DataDir = "."
	}
}

// waiter is a client write bound to a specific (term, index).
type waiter struct {
	prop proposal
	term raft.Term
}

// proposal is a client write waiting for its entry to commit and apply.
type proposal struct {
	data   []byte
	result chan kv.Result
	err    chan error
}

// readReq is a client read waiting for ReadIndex to confirm leadership.
type readReq struct {
	key    string
	result chan kv.Result
	err    chan error
}

// Server wires a raft.Node to storage, a transport, and a state machine.
type Server struct {
	cfg   Config
	node  *raft.Node
	kv    *kv.Store
	store storage.Storage
	tr    *Transport
	log   *slog.Logger

	propC chan proposal
	readC chan readReq
	recvC chan raft.Message

	// waiters maps a log index to the client blocked on it. Touched only by
	// run(), so it needs no lock.
	//
	// The TERM is stored alongside, and it is load-bearing. An index alone does
	// not identify a proposal: if this node is deposed before the entry
	// commits, a new leader can place a DIFFERENT entry at the same index. On
	// index alone we would hand the client another write's result and report
	// success for a write that was truncated away — an acknowledged write that
	// never happened.
	//
	// Found by the deterministic simulator, which hit exactly this case under
	// combined partitions and crashes at seed 2. It is invisible without
	// faults, which is why no amount of manual testing surfaced it.
	waiters map[raft.Index]waiter

	// pendingReads are reads whose ReadIndex has been registered, keyed by the
	// context token echoed back through ReadState.
	pendingReads map[uint64]readReq
	readToken    uint64

	// status is a snapshot of observable state for the API, updated by run()
	// and read by HTTP handlers, so it does need a lock.
	statusMu sync.RWMutex
	role     raft.Role
	term     raft.Term
	lead     raft.NodeID

	stop  chan struct{}
	done  chan struct{}
	once  sync.Once
	ready chan struct{}
}

// New builds a server and recovers any persisted state.
func New(cfg Config) (*Server, error) {
	cfg.setDefaults()

	path := fmt.Sprintf("%s/raft-%d.log", cfg.DataDir, cfg.ID)
	st, err := storage.Open(path)
	if err != nil {
		return nil, err
	}

	peers := make([]raft.NodeID, 0, len(cfg.Peers))
	for id := range cfg.Peers {
		peers = append(peers, id)
	}
	// Sort so every node builds an identical peer list. Ranging a map here
	// would give a different order per process, and while raft does not depend
	// on peer order today, seeding nondeterminism into cluster construction is
	// how a reproducible bug stops reproducing.
	sortNodeIDs(peers)

	// The ONLY randomness in the system, seeded and therefore replayable.
	rng := rand.New(rand.NewSource(cfg.Seed))

	s := &Server{
		cfg:          cfg,
		kv:           kv.New(),
		store:        st,
		log:          cfg.Logger.With("node", uint64(cfg.ID)),
		propC:        make(chan proposal),
		readC:        make(chan readReq),
		recvC:        make(chan raft.Message, 1024),
		waiters:      make(map[raft.Index]waiter),
		pendingReads: make(map[uint64]readReq),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
		ready:        make(chan struct{}),
	}
	s.node = raft.NewNode(cfg.ID, peers, rng.Intn)
	s.tr = NewTransport(cfg.ID, cfg.Peers, s.log)

	if err := s.recover(); err != nil {
		return nil, err
	}
	return s, nil
}

// recover replays persisted state into the core before it runs.
func (s *Server) recover() error {
	hs, ents, snap, err := s.store.Load()
	if err != nil {
		return fmt.Errorf("loading persisted state: %w", err)
	}
	if snap != nil {
		if err := s.kv.Restore(snap.Data); err != nil {
			return err
		}
	}
	s.node.Restore(hs, ents, snap)
	if !hs.IsEmpty() || len(ents) > 0 {
		s.log.Info("recovered from disk",
			"term", uint64(hs.Term), "vote", uint64(hs.VotedFor),
			"commit", uint64(hs.Commit), "entries", len(ents))
	}
	return nil
}

// Start runs the driver loop and both HTTP listeners.
func (s *Server) Start(ctx context.Context) error {
	if err := s.startHTTP(ctx); err != nil {
		return err
	}
	go s.run(ctx)
	close(s.ready)
	return nil
}

// Ready blocks until the driver loop is running.
func (s *Server) Ready() <-chan struct{} { return s.ready }

// Stop shuts the node down.
func (s *Server) Stop() {
	s.once.Do(func() {
		close(s.stop)
		<-s.done
		s.store.Close()
	})
}

// run is the single goroutine that owns the raft core.
//
// Every path into the core funnels through this select. Nothing else calls
// Tick, Step, Ready, or Advance — which is the whole reason a lock-free core is
// safe here.
func (s *Server) run(ctx context.Context) {
	defer close(s.done)

	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.failAllWaiters(ErrShuttingDwn)
			return
		case <-s.stop:
			s.failAllWaiters(ErrShuttingDwn)
			return

		case <-ticker.C:
			s.node.Tick()

		case m := <-s.recvC:
			if err := s.node.Step(m); err != nil && !errors.Is(err, raft.ErrStepStaleTerm) {
				s.log.Warn("step failed", "err", err, "type", m.Type.String())
			}

		case p := <-s.propC:
			idx, err := s.node.Propose(p.data)
			if err != nil {
				p.err <- err
				continue
			}
			// s.node.Term(), not the cached status: updateStatus runs later
			// in the loop, so the cache can still hold the previous term.
			s.waiters[idx] = waiter{prop: p, term: s.node.Term()}

		case r := <-s.readC:
			s.readToken++
			token := s.readToken
			if err := s.node.ReadIndex(encodeToken(token)); err != nil {
				r.err <- err
				continue
			}
			s.pendingReads[token] = r
		}

		s.handleReady()
	}
}

// handleReady performs the intentions, in the order the contract demands.
func (s *Server) handleReady() {
	rd := s.node.Ready()
	if rd.IsEmpty() {
		return
	}

	// --- 1. PERSIST FIRST -------------------------------------------------
	// Before any message goes out. A node that replies to a RequestVote before
	// the vote is durable can crash, forget, and vote twice in one term.
	if rd.HardState != nil {
		if err := s.store.SaveHardState(*rd.HardState); err != nil {
			s.log.Error("fsync hard state failed; refusing to send", "err", err)
			return // do NOT Advance: the same work is retried next loop
		}
	}
	if len(rd.Entries) > 0 {
		if err := s.store.Append(rd.Entries); err != nil {
			s.log.Error("fsync entries failed; refusing to send", "err", err)
			return
		}
	}
	if !rd.Snapshot.IsEmpty() {
		if err := s.store.SaveSnapshot(rd.Snapshot); err != nil {
			s.log.Error("fsync snapshot failed", "err", err)
			return
		}
		if err := s.kv.Restore(rd.Snapshot.Data); err != nil {
			s.log.Error("restoring snapshot into the state machine", "err", err)
			return
		}
	}

	// --- 2. SEND ----------------------------------------------------------
	// Asynchronously: a slow or dead peer must not stall the core. Raft is
	// designed for messages to be lost, so dropping one under back-pressure is
	// a normal event, not an error.
	s.tr.Send(rd.Messages)

	// --- 3. APPLY ---------------------------------------------------------
	for _, e := range rd.CommittedEntries {
		if len(e.Data) == 0 {
			continue // the no-op appended on election
		}
		res := s.kv.Apply(e.Data)
		if w, ok := s.waiters[e.Index]; ok {
			delete(s.waiters, e.Index)
			if w.term == e.Term {
				w.prop.result <- res
			} else {
				// A different leader's entry landed here, so our proposal was
				// truncated. Report failure: the client must retry rather than
				// believe a write that never happened.
				w.prop.err <- ErrNotLeader
			}
		}
	}

	// --- 4. RELEASE CONFIRMED READS ---------------------------------------
	for _, rs := range rd.ReadStates {
		token := decodeToken(rs.Ctx)
		r, ok := s.pendingReads[token]
		if !ok {
			continue
		}
		delete(s.pendingReads, token)
		v, found := s.kv.Get(r.key)
		r.result <- kv.Result{Value: v, Found: found}
	}

	s.updateStatus(rd)
	s.node.Advance(rd)
}

// updateStatus publishes observable state for the HTTP handlers.
//
// If we are no longer the leader, every waiter is failed rather than left to
// time out: their entries may never commit, and a fast error the client can
// retry against the real leader beats a five-second hang.
func (s *Server) updateStatus(rd raft.Ready) {
	s.statusMu.Lock()
	wasLeader := s.role == raft.Leader
	s.role, s.lead = rd.Role, rd.Lead
	s.term = s.node.Term()
	s.statusMu.Unlock()

	if wasLeader && rd.Role != raft.Leader {
		s.log.Info("lost leadership", "term", uint64(s.node.Term()))
		s.failAllWaiters(ErrNotLeader)
	}
}

func (s *Server) failAllWaiters(err error) {
	for idx, w := range s.waiters {
		w.prop.err <- err
		delete(s.waiters, idx)
	}
	for token, r := range s.pendingReads {
		r.err <- err
		delete(s.pendingReads, token)
	}
}

// Status reports observable state.
func (s *Server) Status() (role raft.Role, term raft.Term, lead raft.NodeID) {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()
	return s.role, s.term, s.lead
}

// LeaderAddr returns the API address of the current leader, if known.
func (s *Server) LeaderAddr() (string, bool) {
	_, _, lead := s.Status()
	if lead == raft.None {
		return "", false
	}
	addr, ok := s.cfg.Peers[lead]
	if !ok && lead == s.cfg.ID {
		return s.cfg.APIAddr, true
	}
	return addr, ok
}

// Propose submits a write and waits for it to commit and apply.
func (s *Server) Propose(ctx context.Context, cmd kv.Command) (kv.Result, error) {
	data, err := cmd.Encode()
	if err != nil {
		return kv.Result{}, err
	}
	p := proposal{
		data:   data,
		result: make(chan kv.Result, 1),
		err:    make(chan error, 1),
	}

	timeout := time.NewTimer(s.cfg.RequestTimeout)
	defer timeout.Stop()

	select {
	case s.propC <- p:
	case <-ctx.Done():
		return kv.Result{}, ctx.Err()
	case <-timeout.C:
		return kv.Result{}, ErrTimeout
	case <-s.stop:
		return kv.Result{}, ErrShuttingDwn
	}

	select {
	case res := <-p.result:
		return res, nil
	case err := <-p.err:
		return kv.Result{}, err
	case <-ctx.Done():
		return kv.Result{}, ctx.Err()
	case <-timeout.C:
		return kv.Result{}, ErrTimeout
	}
}

// Read performs a linearizable read.
//
// It goes through ReadIndex rather than reading the local map directly. A
// partitioned leader does not know it has been deposed and would otherwise
// serve data a newer leader has already overwritten.
func (s *Server) Read(ctx context.Context, key string) (kv.Result, error) {
	r := readReq{
		key:    key,
		result: make(chan kv.Result, 1),
		err:    make(chan error, 1),
	}

	timeout := time.NewTimer(s.cfg.RequestTimeout)
	defer timeout.Stop()

	select {
	case s.readC <- r:
	case <-ctx.Done():
		return kv.Result{}, ctx.Err()
	case <-timeout.C:
		return kv.Result{}, ErrTimeout
	case <-s.stop:
		return kv.Result{}, ErrShuttingDwn
	}

	select {
	case res := <-r.result:
		return res, nil
	case err := <-r.err:
		return kv.Result{}, err
	case <-ctx.Done():
		return kv.Result{}, ctx.Err()
	case <-timeout.C:
		return kv.Result{}, ErrTimeout
	}
}

func encodeToken(t uint64) []byte {
	b := make([]byte, 8)
	for i := 0; i < 8; i++ {
		b[i] = byte(t >> (8 * i))
	}
	return b
}

func decodeToken(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	var t uint64
	for i := 0; i < 8; i++ {
		t |= uint64(b[i]) << (8 * i)
	}
	return t
}

func sortNodeIDs(ids []raft.NodeID) {
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j] < ids[j-1]; j-- {
			ids[j], ids[j-1] = ids[j-1], ids[j]
		}
	}
}
