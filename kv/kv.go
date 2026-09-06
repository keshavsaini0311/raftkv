// Package kv is the replicated state machine: a key-value map plus the client
// session bookkeeping that makes retries safe.
//
// It deliberately does not import raft. The state machine consumes opaque
// command bytes and knows nothing about terms, logs, or quorums; raft consumes
// opaque entry bytes and knows nothing about keys. The two meet only in the
// driver, which is what lets each be tested without the other.
package kv

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Op is the operation a Command carries.
type Op uint8

const (
	OpGet Op = iota
	OpPut
	OpDelete
)

func (o Op) String() string {
	switch o {
	case OpGet:
		return "GET"
	case OpPut:
		return "PUT"
	case OpDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// ClientID identifies a client session. Zero means "no session": the command is
// not deduplicated, which is correct only for reads.
type ClientID uint64

// Command is one client operation, encoded into a raft log entry.
//
// JSON rather than protobuf, following the design doc: the log file is
// inspectable with cat during debugging, and that is worth more here than
// bytes on the wire.
type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`

	// ClientID and Seq give exactly-once semantics. A client retrying through
	// a leader change would otherwise apply its write twice, and the store
	// would no longer be linearizable.
	ClientID ClientID `json:"client_id,omitempty"`
	Seq      uint64   `json:"seq,omitempty"`
}

// Result is what applying a Command produced.
type Result struct {
	Value []byte `json:"value,omitempty"`
	Found bool   `json:"found"`
	Err   string `json:"err,omitempty"`
}

func (c Command) Encode() ([]byte, error) { return json.Marshal(c) }

func Decode(b []byte) (Command, error) {
	var c Command
	err := json.Unmarshal(b, &c)
	return c, err
}

// session records the last sequence number applied for a client and the result
// it produced, so a retry returns the cached answer instead of reapplying.
type session struct {
	Seq    uint64 `json:"seq"`
	Result Result `json:"result"`
}

// Store is the replicated map.
//
// Not safe for concurrent use. The driver applies entries from a single
// goroutine, matching the raft core's own single-threaded contract.
type Store struct {
	data     map[string][]byte
	sessions map[ClientID]session
}

func New() *Store {
	return &Store{
		data:     make(map[string][]byte),
		sessions: make(map[ClientID]session),
	}
}

// Apply executes one committed command and returns its result.
//
// Applying must be DETERMINISTIC: every replica runs this on the same entries
// in the same order and must reach byte-identical state. No clock, no
// randomness, no map-iteration order that reaches the output.
func (s *Store) Apply(data []byte) Result {
	cmd, err := Decode(data)
	if err != nil {
		// A malformed entry is already committed and every replica will see
		// the same bytes, so failing identically everywhere keeps the
		// replicas consistent. Skipping it on some nodes would not.
		return Result{Err: fmt.Sprintf("decode: %v", err)}
	}
	return s.ApplyCommand(cmd)
}

// ApplyCommand is Apply on an already-decoded command.
func (s *Store) ApplyCommand(cmd Command) Result {
	// Exactly-once: a duplicate of the last request from this client returns
	// the cached response rather than reapplying.
	//
	// Only the LAST sequence number is retained, not every one ever seen.
	// Clients are strictly sequential — one outstanding request at a time — so
	// a retry can only ever be of the most recent command. Keeping full
	// history would grow without bound.
	if cmd.ClientID != 0 {
		if sess, ok := s.sessions[cmd.ClientID]; ok {
			if cmd.Seq == sess.Seq {
				return sess.Result // duplicate: replay the cached answer
			}
			if cmd.Seq < sess.Seq {
				// Older than what we have already applied. The client has
				// moved on; this is a straggler retransmission.
				return Result{Err: "stale sequence number"}
			}
		}
	}

	res := s.execute(cmd)

	if cmd.ClientID != 0 && cmd.Op != OpGet {
		s.sessions[cmd.ClientID] = session{Seq: cmd.Seq, Result: res}
	}
	return res
}

func (s *Store) execute(cmd Command) Result {
	switch cmd.Op {
	case OpGet:
		v, ok := s.data[cmd.Key]
		if !ok {
			return Result{Found: false}
		}
		// Return a copy: the caller must not be able to mutate stored bytes
		// through the slice header it receives.
		out := make([]byte, len(v))
		copy(out, v)
		return Result{Value: out, Found: true}

	case OpPut:
		v := make([]byte, len(cmd.Value))
		copy(v, cmd.Value)
		s.data[cmd.Key] = v
		return Result{Found: true}

	case OpDelete:
		_, existed := s.data[cmd.Key]
		delete(s.data, cmd.Key)
		return Result{Found: existed}

	default:
		return Result{Err: "unknown op"}
	}
}

// Get reads directly without going through the log.
//
// Only safe when the caller has already established that this node is the
// leader AND its commit index is current — see raft.ReadIndex. Calling it on a
// stale leader returns stale data and breaks linearizability.
func (s *Store) Get(key string) ([]byte, bool) {
	v, ok := s.data[key]
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

func (s *Store) Len() int { return len(s.data) }

// Keys returns every key, SORTED.
//
// Sorted because Go randomises map iteration order, and any nondeterminism
// that reaches output would break both snapshot equality across replicas and
// the replayability the whole project depends on.
func (s *Store) Keys() []string {
	out := make([]string, 0, len(s.data))
	for k := range s.data {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// snapshotState is the serialisable form of a Store.
type snapshotState struct {
	Data     map[string][]byte    `json:"data"`
	Sessions map[ClientID]session `json:"sessions"`
}

// Snapshot serialises the entire state machine.
//
// Sessions are included, and that is not optional: restoring data without
// sessions would let every client's last write be applied a second time after
// a restore, which is exactly the duplicate the sessions exist to prevent.
func (s *Store) Snapshot() ([]byte, error) {
	return json.Marshal(snapshotState{Data: s.data, Sessions: s.sessions})
}

// Restore replaces the entire state machine from a snapshot.
func (s *Store) Restore(b []byte) error {
	var st snapshotState
	if err := json.Unmarshal(b, &st); err != nil {
		return fmt.Errorf("restore snapshot: %w", err)
	}
	if st.Data == nil {
		st.Data = make(map[string][]byte)
	}
	if st.Sessions == nil {
		st.Sessions = make(map[ClientID]session)
	}
	s.data = st.Data
	s.sessions = st.Sessions
	return nil
}
