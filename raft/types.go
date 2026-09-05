// Package raft implements the Raft consensus algorithm as a PURE state machine.
//
// The contract, from the design doc:
//
//	no goroutines, no timers, no sockets, no time.Now(), no rand, no disk.
//
// A Node is a function of (state, input) -> (state', outputs). It consumes
// Tick() and Step(), and emits intentions via Ready(). A driver — server/ in
// production, sim/ in tests — performs those intentions. The core never learns
// that networks or disks exist.
//
// import_test.go enforces this mechanically. If it fails, the design has been
// violated, and no amount of "I'll fix it later" recovers determinism.
package raft

// Scalar types. All three are uint64 underneath, and all three are DISTINCT —
// module 2's nominal typing. Raft is full of uint64s, and passing a Term where
// an Index belongs is a classic bug. Here it does not compile.
type (
	NodeID uint64
	Term   uint64
	Index  uint64
)

// None is the zero NodeID, meaning "no vote cast" / "no leader known".
// The zero value doing real work again: a fresh Node has already voted for
// nobody, with no initialization.
const None NodeID = 0

// Role is the node's current state in the protocol.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "unknown"
	}
}

// Entry is one command in the replicated log.
//
// Note: Entry contains a slice, so Entry is NOT comparable — `a == b` will not
// compile (module 1). Compare Term and Index explicitly, or use a helper.
type Entry struct {
	Term  Term
	Index Index
	Data  []byte // opaque to raft/ — kv/ interprets it
}

// HardState is the subset that MUST be persisted before responding to any RPC.
//
// Every field is a numeric type, so HardState IS comparable — `a == b` works.
// That is deliberate: detecting "did the hard state change this tick?" is a
// single ==, which is what makes Ready.HardState's nil-means-unchanged work.
type HardState struct {
	Term     Term
	VotedFor NodeID // None == no vote cast this term
	Commit   Index
}
