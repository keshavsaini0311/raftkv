// Package raft implements the Raft consensus algorithm as a pure state machine.
//
// The core performs no I/O and observes no clock. A Node consumes Tick() and
// Step(), and emits intentions through Ready(); a driver — server/ in
// production, sim/ under test — performs them. Because the core cannot observe
// wall time, randomness, or the network, its behaviour is a pure function of
// its inputs, and a seeded simulation replays identically forever.
//
// import_test.go enforces this mechanically.
package raft

// NodeID, Term and Index are all uint64 underneath and all distinct types.
// Raft is dense with uint64s and passing a Term where an Index belongs is a
// classic bug; nominal typing makes it a compile error.
type (
	NodeID uint64
	Term   uint64
	Index  uint64
)

// None is the zero NodeID: no vote cast, or no leader known.
const None NodeID = 0

// Role is a node's current state in the protocol.
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

// EntryType distinguishes ordinary commands from configuration changes.
// Milestone 5 adds EntryConfChange; the log machinery carries the field from
// the start so adding it later does not change the on-disk format.
type EntryType uint8

const (
	EntryNormal EntryType = iota
	EntryConfChange
)

// Entry is one command in the replicated log.
//
// Entry contains a slice and is therefore not comparable: a == b does not
// compile. Compare Term and Index explicitly, which is all identity requires —
// an index paired with a term uniquely identifies an entry in Raft.
type Entry struct {
	Type  EntryType
	Term  Term
	Index Index
	Data  []byte // opaque to raft/; kv/ interprets it
}

// HardState is the subset of a node's state that must reach stable storage
// before it responds to any RPC.
//
// Every field is numeric, so HardState is comparable and "did this change?" is
// a single ==. That is deliberate, and it is why Entry carries the []byte and
// HardState does not.
type HardState struct {
	Term     Term
	VotedFor NodeID
	Commit   Index
}

// IsEmpty reports whether hs carries no information.
func (hs HardState) IsEmpty() bool { return hs == HardState{} }

// SnapshotMetadata describes the state a Snapshot replaces.
// Used from milestone 5; declared here so storage interfaces are stable.
type SnapshotMetadata struct {
	Index Index
	Term  Term
	// Voters is the cluster membership as of this snapshot. A restoring node
	// must adopt it, or it would rejoin with a stale view of the cluster.
	Voters []NodeID
}

// Snapshot is a compacted prefix of the log plus the state machine bytes that
// prefix produced.
type Snapshot struct {
	Metadata SnapshotMetadata
	Data     []byte
}

func (s *Snapshot) IsEmpty() bool { return s == nil || s.Metadata.Index == 0 }
