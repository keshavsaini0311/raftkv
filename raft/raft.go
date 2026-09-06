package raft

import "errors"

// Timing is counted in TICKS, never durations. One Tick() is one unit of
// logical time; what that means in wall time is the driver's business.
//
// The paper's constraint is broadcastTime << electionTimeout << MTBF.
const (
	electionTimeoutMin = 10 // inclusive
	electionTimeoutMax = 20 // exclusive
	heartbeatTimeout   = 1
)

var (
	// ErrStepStaleTerm reports that a message from an older term was dropped.
	// Informational; a driver need not act on it.
	ErrStepStaleTerm = errors.New("raft: message from a stale term ignored")

	// ErrNotLeader is returned by Propose on a node that is not the leader.
	ErrNotLeader = errors.New("raft: not the leader")
)

// Node is one Raft replica: a pure state machine.
//
// No goroutine touches a Node. The driver serialises every call onto a single
// goroutine, which is what lets the core hold no locks.
type Node struct {
	id    NodeID
	peers []NodeID // other nodes; does not include id
	role  Role

	// Persistent state (Figure 2). Must be durable before any RPC reply.
	currentTerm Term
	votedFor    NodeID

	// Volatile state.
	lead NodeID
	log  *raftLog

	// votes records RequestVote responses while a candidate. A false entry is a
	// recorded refusal, not a missing answer — count grants, not entries.
	votes map[NodeID]bool

	// Timing, in ticks.
	electionElapsed  int
	heartbeatElapsed int
	electionTimeout  int

	// randIntn returns a value in [0,n). Injected rather than imported: naming
	// *rand.Rand here would mean importing math/rand, which import_test.go
	// bans. Determinism is therefore the caller's guarantee, seeded and
	// replayable.
	randIntn func(n int) int

	msgs          []Message
	prevHardState HardState
}

// NewNode creates a follower at term 0 that has voted for nobody.
//
// peers must not include id. randIntn must not be nil: the core cannot supply
// its own randomness without breaking determinism, so it refuses to guess.
func NewNode(id NodeID, peers []NodeID, randIntn func(n int) int) *Node {
	if id == None {
		panic("raft: node id must not be zero; 0 means None")
	}
	if randIntn == nil {
		panic("raft: NewNode requires a randIntn source")
	}

	// Copy the peer slice: an append on the caller's side must not silently
	// reshape this node's view of the cluster.
	peersCopy := make([]NodeID, len(peers))
	copy(peersCopy, peers)

	n := &Node{
		id:       id,
		peers:    peersCopy,
		log:      newLog(),
		votes:    make(map[NodeID]bool),
		randIntn: randIntn,
	}

	// Routed through becomeFollower so construction and stepdown produce
	// identical state from one code path.
	n.becomeFollower(0, None)
	return n
}

// ID returns this node's identifier.
func (n *Node) ID() NodeID { return n.id }

// Role returns the current role. Observational; drivers use it for logging and
// to decide whether to accept client writes.
func (n *Node) Role() Role { return n.role }

// Term returns the current term.
func (n *Node) Term() Term { return n.currentTerm }

// Lead returns the last known leader, or None.
func (n *Node) Lead() NodeID { return n.lead }

// =============================================================================
// Public surface
// =============================================================================

// Tick advances logical time by one unit.
//
// The branches are mutually exclusive because n.role can change inside this
// call: a follower may become candidate and, in a one-node cluster, leader
// before Tick returns. Re-testing the role afterwards would fire the heartbeat
// timer on the tick the node was elected, duplicating becomeLeader's own
// initial broadcast.
func (n *Node) Tick() {
	switch n.role {
	case Follower, Candidate:
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout {
			n.becomeCandidate()
		}
	case Leader:
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= heartbeatTimeout {
			n.heartbeatElapsed = 0
			n.bcastHeartbeat()
		}
	}
}

// Step delivers one inbound message.
//
// The term check runs first and unconditionally, so every handler below may
// assume m.Term == n.currentTerm. It applies to responses as well as requests:
// a partitioned leader learns it has been deposed from a reply to its own
// heartbeat, and checking only inbound requests leaves it serving reads forever.
func (n *Node) Step(m Message) error {
	if m.Term > n.currentTerm {
		// Only an AppendEntries or InstallSnapshot proves the sender is the
		// leader. A RequestVote comes from a candidate that has won nothing.
		lead := None
		if m.Type == MsgAppendEntries || m.Type == MsgInstallSnapshot {
			lead = m.From
		}
		n.becomeFollower(m.Term, lead)
	}

	if m.Term < n.currentTerm {
		// Answer requests so the sender learns it is behind; send stamps our
		// higher term, which is the entire payload of the reply. Never reply
		// to a reply.
		if !m.Type.IsResponse() {
			n.send(Message{Type: respTypeFor(m.Type), To: m.From})
		}
		return ErrStepStaleTerm
	}

	switch m.Type {
	case MsgRequestVote:
		n.handleRequestVote(m)
	case MsgRequestVoteResp:
		n.handleRequestVoteResp(m)
	case MsgAppendEntries:
		n.handleAppendEntries(m)
	case MsgAppendEntriesResp:
		n.handleAppendEntriesResp(m)
	case MsgInstallSnapshot:
		n.handleInstallSnapshot(m)
	case MsgInstallSnapshotResp:
		n.handleInstallSnapshotResp(m)
	}
	return nil
}

func respTypeFor(t MessageType) MessageType {
	switch t {
	case MsgRequestVote:
		return MsgRequestVoteResp
	case MsgAppendEntries:
		return MsgAppendEntriesResp
	case MsgInstallSnapshot:
		return MsgInstallSnapshotResp
	default:
		return t
	}
}

// Ready returns the intentions accumulated since the last Advance.
//
// Read-only: a driver may call Ready, decide it is busy, and call again next
// loop; both calls must return the same thing. Draining here would lose
// messages that were never sent.
func (n *Node) Ready() Ready {
	rd := Ready{
		Messages:         n.msgs,
		CommittedEntries: n.log.nextApplicable(),
		Lead:             n.lead,
		Role:             n.role,
	}

	// &hs takes the address of a local, which is safe: escape analysis
	// heap-allocates hs because the pointer outlives this call.
	if hs := n.hardState(); hs != n.prevHardState {
		rd.HardState = &hs
	}
	return rd
}

// Advance acknowledges that the driver handled everything in r.
//
// It is the commit point of the driver loop: a crash before Advance means the
// same work is handed back next time and retried, rather than lost.
func (n *Node) Advance(r Ready) {
	if r.HardState != nil {
		n.prevHardState = *r.HardState
	}
	if len(r.CommittedEntries) > 0 {
		last := r.CommittedEntries[len(r.CommittedEntries)-1]
		n.log.appliedTo(last.Index)
	}

	// Drop by count rather than truncating to nil: a driver that appended
	// between Ready and Advance loses nothing. Silently dropping unsent
	// messages would present as a network fault and be near-impossible to trace.
	if len(r.Messages) >= len(n.msgs) {
		n.msgs = nil
	} else {
		n.msgs = n.msgs[len(r.Messages):]
	}
}

// =============================================================================
// State transitions
// =============================================================================

// becomeFollower moves to the follower state at the given term.
//
// The vote resets when the TERM advances, not when the role changes. Clearing
// it on every stepdown would let a candidate that voted for itself in term 5
// vote again for someone else in term 5; never clearing it would leave a node
// refusing to vote in a term it just joined, stalling elections silently.
func (n *Node) becomeFollower(term Term, lead NodeID) {
	if term > n.currentTerm {
		n.votedFor = None
	}
	n.currentTerm = term
	n.role = Follower
	n.lead = lead
	n.resetElectionTimer()
}

// becomeCandidate starts a new election.
func (n *Node) becomeCandidate() {
	n.currentTerm++
	n.role = Candidate
	n.lead = None

	// A fresh map, not a cleared reuse. A grant left over from the previous
	// election would count toward this one: fail at term 3 with one grant,
	// campaign at term 4, and a single new grant looks like two.
	n.votes = make(map[NodeID]bool)
	n.votedFor = n.id
	n.votes[n.id] = true

	// A new random timeout per election. Without the redraw, nodes that split a
	// vote wake together and split it identically, forever.
	n.resetElectionTimer()

	// The self-vote may already be a majority. This is not an N==1 special
	// case: a candidate evaluates quorum whenever its vote count changes, and
	// its own vote is the first change. Without it a one-node cluster waits
	// for responses that no peer will ever send.
	if n.grantedVotes() >= n.quorum() {
		n.becomeLeader()
		return
	}
	n.campaign()
}

// becomeLeader is called on winning an election.
func (n *Node) becomeLeader() {
	// The term is untouched: the leader serves the term it just won.
	n.role = Leader
	n.lead = n.id
	n.heartbeatElapsed = 0

	// Immediately, not on the next heartbeat tick. Every peer's election clock
	// is already running, and the first to time out deposes us for no reason.
	n.bcastHeartbeat()
}

func (n *Node) campaign() {
	for _, peer := range n.peers {
		n.send(Message{
			Type:         MsgRequestVote,
			To:           peer,
			LastLogIndex: n.log.lastIndex(),
			LastLogTerm:  n.log.lastTerm(),
		})
	}
}

// bcastHeartbeat sends an empty AppendEntries to every peer. Two callers: the
// heartbeat timer and becomeLeader. One function cannot drift from itself.
func (n *Node) bcastHeartbeat() {
	for _, peer := range n.peers {
		n.send(Message{
			Type:         MsgAppendEntries,
			To:           peer,
			PrevLogIndex: n.log.lastIndex(),
			PrevLogTerm:  n.log.lastTerm(),
			LeaderCommit: n.log.committed,
		})
	}
}

// =============================================================================
// Handlers
// =============================================================================

func (n *Node) handleRequestVote(m Message) {
	// "or already m.From" is idempotency: if our reply was dropped and the
	// candidate retries, it must get the same answer. Without it one lost
	// packet costs an election.
	canVote := n.votedFor == None || n.votedFor == m.From
	granted := canVote && n.log.isUpToDate(m.LastLogIndex, m.LastLogTerm)

	if granted {
		n.votedFor = m.From
		// Figure 2: the clock restarts on granting a vote. Having just endorsed
		// someone, running against them would split the vote we just cast.
		n.resetElectionTimer()
	}

	// Reply either way. Silence is indistinguishable from a partition, and the
	// reply carries our term, which is how a stale candidate learns to stop.
	n.send(Message{Type: MsgRequestVoteResp, To: m.From, VoteGranted: granted})
}

func (n *Node) handleRequestVoteResp(m Message) {
	// Responses outlive their elections. Acting on a reply to a finished one
	// could promote a node that is now following a legitimate leader.
	if n.role != Candidate {
		return
	}
	n.votes[m.From] = m.VoteGranted
	if n.grantedVotes() >= n.quorum() {
		n.becomeLeader()
	}
}

func (n *Node) handleAppendEntries(m Message) {
	// Step guarantees m.Term == n.currentTerm, so this is a legitimate leader
	// of our own term. A candidate steps down at an EQUAL term: someone else
	// collected a majority for it while we waited, and there is exactly one
	// leader per term. becomeFollower also resets the election timer, which is
	// the only thing keeping followers quiet.
	n.becomeFollower(m.Term, m.From)

	// Milestone 1 keeps no log, so the consistency check trivially succeeds;
	// milestone 2 replaces this with maybeAppend.
	n.send(Message{
		Type:       MsgAppendEntriesResp,
		To:         m.From,
		Success:    true,
		MatchIndex: n.log.lastIndex(),
	})
}

func (n *Node) handleAppendEntriesResp(m Message) {
	// Milestone 2 uses this to advance matchIndex and drive the commit rule.
	// A heartbeat ack carries no information while there is no log.
}

func (n *Node) handleInstallSnapshot(m Message)     {} // milestone 5
func (n *Node) handleInstallSnapshotResp(m Message) {} // milestone 5

// =============================================================================
// Helpers
// =============================================================================

// quorum is a strict majority of the cluster, which is the peers plus this node.
//
//	peers 0 -> cluster 1 -> 1     peers 3 -> cluster 4 -> 3
//	peers 1 -> cluster 2 -> 2     peers 4 -> cluster 5 -> 3
//	peers 2 -> cluster 3 -> 2
//
// Majority rather than unanimity because any two majorities share a member, and
// that shared member is what makes two leaders in one term impossible.
func (n *Node) quorum() int { return (len(n.peers)+1)/2 + 1 }

// grantedVotes counts grants, not replies. n.votes stores refusals as false so
// a retried response stays idempotent, so len(n.votes) is a different number
// entirely — counting it would elect a node that everybody rejected.
func (n *Node) grantedVotes() int {
	count := 0
	for _, granted := range n.votes {
		if granted {
			count++
		}
	}
	return count
}

// resetElectionTimer clears the elapsed counter and draws a new random timeout.
func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	n.electionTimeout = electionTimeoutMin +
		n.randIntn(electionTimeoutMax-electionTimeoutMin)
}

func (n *Node) hardState() HardState {
	return HardState{
		Term:     n.currentTerm,
		VotedFor: n.votedFor,
		Commit:   n.log.committed,
	}
}

// send queues a message. From and Term are always stamped from this node:
// every Raft message, request or reply, carries the sender's current term.
func (n *Node) send(m Message) {
	m.From = n.id
	m.Term = n.currentTerm
	n.msgs = append(n.msgs, m)
}
