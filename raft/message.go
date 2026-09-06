package raft

// MessageType identifies which RPC a Message carries.
//
// One Message struct with a type tag rather than four types, because the core's
// entrypoint is a single Step(m Message) error.
type MessageType uint8

const (
	MsgRequestVote MessageType = iota
	MsgRequestVoteResp
	MsgAppendEntries
	MsgAppendEntriesResp
	MsgInstallSnapshot
	MsgInstallSnapshotResp
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
	case MsgInstallSnapshot:
		return "InstallSnapshot"
	case MsgInstallSnapshotResp:
		return "InstallSnapshotResp"
	default:
		return "unknown"
	}
}

// IsResponse reports whether t is a reply rather than a request. Used to decide
// whether a stale message deserves an answer: Raft never replies to a reply.
func (t MessageType) IsResponse() bool {
	switch t {
	case MsgRequestVoteResp, MsgAppendEntriesResp, MsgInstallSnapshotResp:
		return true
	default:
		return false
	}
}

// Message is one RPC, request or response.
//
// Fields are shared across types; Figure 2's CandidateID and LeaderID are both
// From. Fields irrelevant to a given Type stay at their zero values.
type Message struct {
	Type MessageType
	From NodeID
	To   NodeID
	Term Term

	// MsgRequestVote: the §5.4.1 election restriction.
	LastLogIndex Index
	LastLogTerm  Term

	// MsgRequestVoteResp.
	VoteGranted bool

	// MsgAppendEntries: the consistency check, the payload, and the leader's
	// commit index. Entries is empty for a heartbeat.
	PrevLogIndex Index
	PrevLogTerm  Term
	Entries      []Entry
	LeaderCommit Index

	// MsgAppendEntriesResp.
	//
	// ConflictIndex and ConflictTerm are the §5.3 optimisation: on rejection a
	// follower reports where its log actually diverges, so the leader can skip
	// an entire conflicting term in one round trip instead of decrementing
	// nextIndex one entry at a time. Without it, a follower that is 10,000
	// entries behind costs 10,000 round trips to repair.
	Success       bool
	ConflictIndex Index
	ConflictTerm  Term

	// MsgAppendEntriesResp: the highest index the follower now has. Lets the
	// leader advance matchIndex without inferring it from what it sent.
	MatchIndex Index

	// ReadSeq supports linearizable reads. The leader stamps every
	// AppendEntries with its current read sequence; a follower echoes it back
	// untouched. A read registered at sequence R is safe to serve once a
	// quorum has acknowledged a message stamped R or higher, which proves the
	// leader was still the leader after the read was registered.
	ReadSeq uint64

	// MsgInstallSnapshot (milestone 5).
	Snapshot *Snapshot
}
