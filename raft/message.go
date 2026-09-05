package raft

// MessageType identifies which RPC a Message carries.
//
// One Message struct with a type tag, rather than four separate types, because
// the core's entrypoint is a single func (n *Node) Step(m Message) error. This
// is the etcd/raft shape.
type MessageType uint8

const (
	MsgRequestVote MessageType = iota
	MsgRequestVoteResp
	MsgAppendEntries
	MsgAppendEntriesResp
)

func (t MessageType) String() string {
	switch t {
	case MsgRequestVote:
		return "RequestVote"
	case MsgRequestVoteResp:
		return "RequestVoteResp"
	case MsgAppendEntries:
		return "AppendEntries"
	case MsgAppendEntriesResp:
		return "AppendEntriesResp"
	default:
		return "unknown"
	}
}

// Message is one RPC, request or response.
//
// Fields are shared across types — Figure 2's CandidateID and LeaderID are both
// just From. Unused fields for a given Type stay at their zero values.
//
// Milestone 1 scope only. Milestone 2 adds Entries, LeaderCommit,
// ConflictIndex, and ConflictTerm for log replication.
type Message struct {
	Type MessageType
	From NodeID
	To   NodeID
	Term Term

	// MsgRequestVote — the up-to-date check from §5.4.1.
	// Trivial while the log is empty, but implemented in M1 so M2 inherits it.
	LastLogIndex Index
	LastLogTerm  Term

	// MsgRequestVoteResp
	VoteGranted bool

	// MsgAppendEntries — M1 sends these empty, as heartbeats.
	PrevLogIndex Index
	PrevLogTerm  Term

	// MsgAppendEntriesResp
	Success bool
}
