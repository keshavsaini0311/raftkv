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

	// maxEntriesPerMsg bounds one AppendEntries.
	//
	// Without a cap, a follower that is 100,000 entries behind is sent all of
	// them in a single message — which the transport must buffer whole, and
	// which blocks every other peer behind it. Repair still converges at this
	// size, just over several round trips, which is the correct trade.
	maxEntriesPerMsg = 256
)

var (
	// ErrStepStaleTerm reports that a message from an older term was dropped.
	// Informational; a driver need not act on it.
	ErrStepStaleTerm = errors.New("raft: message from a stale term ignored")

	// ErrNotLeader is returned by Propose on a node that is not the leader.
	ErrNotLeader = errors.New("raft: not the leader")

	// ErrConfChangeInFlight means a membership change is already mid-transition.
	ErrConfChangeInFlight = errors.New("raft: a configuration change is already in flight")

	// ErrNoSuchNode means the node to remove is not a member.
	ErrNoSuchNode = errors.New("raft: node is not a member")

	// ErrNodeExists means the node to add is already a member.
	ErrNodeExists = errors.New("raft: node is already a member")

	// ErrSnapshotAhead means the requested snapshot index is past what the
	// state machine has applied.
	ErrSnapshotAhead = errors.New("raft: snapshot index is ahead of applied")

	// ErrSnapshotCompacted means the requested index is already compacted.
	ErrSnapshotCompacted = errors.New("raft: snapshot index already compacted")

	// ErrReadIndexUnavailable means the leader has not yet committed an entry
	// in its own term, so it cannot trust its commit index. Transient: the
	// no-op appended on election resolves it within a round trip.
	ErrReadIndexUnavailable = errors.New("raft: read index not yet available")
)

// pendingRead is a read waiting for a quorum to confirm our leadership.
type pendingRead struct {
	index Index
	ctx   []byte
	seq   uint64
}

// Node is one Raft replica: a pure state machine.
//
// No goroutine touches a Node. The driver serialises every call onto a single
// goroutine, which is what lets the core hold no locks.
type Node struct {
	id   NodeID
	role Role

	// cfg is cluster membership. While joint (mid-transition) it holds both
	// the old and new voter sets, and every quorum decision needs a majority
	// of BOTH — that overlap is what makes a membership change safe.
	cfg config

	// bootstrapCfg is the membership to fall back to when the log contains no
	// configuration entry, either at startup or after truncation removed one.
	bootstrapCfg config

	// confIndex is the index of the configuration entry currently applied, or
	// 0 for bootstrapCfg. Kept so the common path — an AppendEntries carrying
	// no configuration change — costs nothing. Rescanning the log on every
	// message would make replication O(log length) per message and O(n^2)
	// overall, which a benchmark caught before it reached anything real.
	confIndex Index

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

	// Leader only, reinitialised on every election. nextIndex is optimistic
	// (assume the follower matches us) and walks back on rejection;
	// matchIndex is pessimistic (assume nothing) and only ever moves forward
	// on a confirmed success. The asymmetry is deliberate: guessing high
	// costs a round trip, guessing high on matchIndex would commit an entry
	// a follower never received.
	nextIndex  map[NodeID]Index
	matchIndex map[NodeID]Index

	// Linearizable reads (ReadIndex). readSeq increments per read request;
	// ackedReadSeq is the highest sequence each peer has echoed back.
	readSeq      uint64
	ackedReadSeq map[NodeID]uint64
	pendingReads []pendingRead
	readStates   []ReadState

	// pendingSnapshot is a snapshot received from the leader, waiting to be
	// surfaced through Ready so the driver can persist it and hand it to the
	// state machine.
	pendingSnapshot *Snapshot

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

	// The configuration is built from a copy: an append on the caller's side
	// must not silently reshape this node's view of the cluster.
	voters := make([]NodeID, 0, len(peers)+1)
	voters = append(voters, id)
	voters = append(voters, peers...)
	cfg := newConfig(voters)

	n := &Node{
		id:           id,
		cfg:          cfg,
		bootstrapCfg: cfg,
		log:          newLog(),
		votes:        make(map[NodeID]bool),
		randIntn:     randIntn,
	}

	// Routed through becomeFollower so construction and stepdown produce
	// identical state from one code path.
	n.becomeFollower(0, None)
	return n
}

// Restore reinstates persisted state after a crash, before the node runs.
//
// Order matters: the snapshot establishes the log's base, entries extend it,
// and only then does the hard state set the term, vote, and commit index —
// commitTo clamps to lastIndex, so applying it before the entries exist would
// silently lose the commit index.
//
// prevHardState is set to what was loaded, so the first Ready does not ask the
// driver to re-persist state it just read off disk.
func (n *Node) Restore(hs HardState, ents []Entry, snap *Snapshot) {
	if !snap.IsEmpty() {
		n.log.restore(snap)
	}
	if len(ents) > 0 {
		n.log.appendAt(ents)
		n.log.stable = n.log.lastIndex()
	}
	if !hs.IsEmpty() {
		n.currentTerm = hs.Term
		n.votedFor = hs.VotedFor
		n.log.commitTo(hs.Commit)
		n.prevHardState = hs
	}
	n.applyConfFromLog()

	// Always a follower on restart. A node that was leader before the crash
	// has no idea whether the cluster elected someone else meanwhile, and
	// resuming leadership on its own authority would be a split brain.
	n.role = Follower
	n.lead = None
	n.resetElectionTimer()
}

// peers returns every other voting node, in a fixed order.
func (n *Node) peers() []NodeID {
	all := n.cfg.all()
	out := make([]NodeID, 0, len(all))
	for _, v := range all {
		if v != n.id {
			out = append(out, v)
		}
	}
	return out
}

// Voters returns the current membership.
func (n *Node) Voters() []NodeID { return append([]NodeID(nil), n.cfg.voters...) }

// IsJoint reports whether a membership change is mid-transition.
func (n *Node) IsJoint() bool { return n.cfg.isJoint() }

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
			n.bcastAppend()
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
		Snapshot:         n.pendingSnapshot,
		Entries:          n.log.unstable(),
		Messages:         n.msgs,
		CommittedEntries: n.log.nextApplicable(),
		ReadStates:       n.readStates,
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
	if !r.Snapshot.IsEmpty() && n.pendingSnapshot != nil &&
		r.Snapshot.Metadata.Index == n.pendingSnapshot.Metadata.Index {
		n.pendingSnapshot = nil
	}
	if len(r.Entries) > 0 {
		// The driver has fsynced these. Only now may the leader count them
		// toward a quorum: an entry acknowledged before it is durable can
		// vanish in a crash while the leader believes it committed.
		last := r.Entries[len(r.Entries)-1]
		n.log.stableTo(last.Index)
		if n.role == Leader {
			n.matchIndex[n.id] = n.log.stable
			n.maybeCommit()
		}
	}
	if len(r.ReadStates) >= len(n.readStates) {
		n.readStates = nil
	} else {
		n.readStates = n.readStates[len(r.ReadStates):]
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

	// Reads registered while we were leader can no longer be served: we can no
	// longer prove our commit index is current. Dropping them makes the driver
	// time the request out and the client retry against the real leader, which
	// is correct. Serving them would be a stale read.
	n.pendingReads = nil
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
	if n.hasVoteQuorum() {
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

	peers := n.peers()
	n.nextIndex = make(map[NodeID]Index, len(peers))
	n.matchIndex = make(map[NodeID]Index, len(peers))
	n.ackedReadSeq = make(map[NodeID]uint64, len(peers))
	n.readSeq = 0
	n.pendingReads = nil
	for _, p := range peers {
		n.nextIndex[p] = n.log.lastIndex() + 1
		n.matchIndex[p] = 0
	}

	// Append an empty entry for the new term.
	//
	// Figure 8 forbids committing a previous-term entry by replica count
	// alone, so entries inherited from a deposed leader can only commit
	// indirectly — carried along once a CURRENT-term entry commits. Without
	// this no-op, that cannot happen until a client happens to write, and a
	// quiet cluster can hold entries that are replicated everywhere and
	// committed nowhere. Three lines to close a liveness gap.
	n.log.append(n.currentTerm, Entry{Type: EntryNormal})
	n.matchIndex[n.id] = n.log.lastIndex()

	// Immediately, not on the next heartbeat tick. Every peer's election clock
	// is already running, and the first to time out deposes us for no reason.
	n.bcastAppend()
}

// Propose appends a command to the log and starts replicating it. Only the
// leader may propose; everyone else redirects the client to n.lead.
//
// Returning the index lets a driver wait for exactly this entry to commit.
func (n *Node) Propose(data []byte) (Index, error) {
	if n.role != Leader {
		return 0, ErrNotLeader
	}
	added := n.log.append(n.currentTerm, Entry{Type: EntryNormal, Data: data})
	n.matchIndex[n.id] = n.log.lastIndex()
	n.bcastAppend()
	return added[0].Index, nil
}

func (n *Node) campaign() {
	for _, peer := range n.peers() {
		n.send(Message{
			Type:         MsgRequestVote,
			To:           peer,
			LastLogIndex: n.log.lastIndex(),
			LastLogTerm:  n.log.lastTerm(),
		})
	}
}

// ReadIndex registers a linearizable read.
//
// Two conditions must hold before a leader may serve a read from local state,
// and neither is about the data itself:
//
//  1. The leader must have committed an entry in its OWN term. A freshly
//     elected leader inherits a commit index it cannot verify; the no-op
//     appended on election supplies the missing proof within one round trip.
//
//  2. The leader must still be the leader NOW. A partitioned leader does not
//     know it has been deposed and would happily serve data that a new leader
//     has already overwritten. Confirming with a heartbeat round proves
//     leadership at the moment of the read.
//
// This is ReadIndex rather than a leader lease, following the design doc:
// a lease is faster but assumes bounded clock drift, and the core has no
// clock to bound.
func (n *Node) ReadIndex(ctx []byte) error {
	if n.role != Leader {
		return ErrNotLeader
	}
	if t, ok := n.log.term(n.log.committed); !ok || t != n.currentTerm {
		return ErrReadIndexUnavailable
	}

	n.readSeq++
	n.pendingReads = append(n.pendingReads, pendingRead{
		index: n.log.committed,
		ctx:   ctx,
		seq:   n.readSeq,
	})

	// A single-node cluster is its own quorum: leadership is not in question.
	if len(n.peers()) == 0 {
		n.maybeReleaseReads()
		return nil
	}
	n.bcastAppend()
	return nil
}

// jointEntryCommitted reports whether the most recent configuration entry —
// the joint one — has committed.
func (n *Node) jointEntryCommitted() bool {
	for i := n.log.lastIndex(); i >= n.log.firstIndex(); i-- {
		e, ok := n.log.at(i)
		if !ok {
			return false
		}
		if e.Type == EntryConfChange {
			return e.Index <= n.log.committed
		}
	}
	return false
}

// maybeReleaseReads promotes pending reads whose leadership has been confirmed.
func (n *Node) maybeReleaseReads() {
	if len(n.pendingReads) == 0 {
		return
	}
	var kept []pendingRead
	for _, pr := range n.pendingReads {
		confirmed := n.cfg.hasQuorum(func(id NodeID) bool {
			return id == n.id || n.ackedReadSeq[id] >= pr.seq
		})
		if confirmed {
			n.readStates = append(n.readStates, ReadState{Index: pr.index, Ctx: pr.ctx})
		} else {
			kept = append(kept, pr)
		}
	}
	n.pendingReads = kept
}

// sendSnapshot ships the current snapshot to a follower that has fallen behind
// the compaction point.
func (n *Node) sendSnapshot(to NodeID) {
	if n.log.snapshot == nil {
		return // nothing to send; the follower retries and we catch up later
	}
	n.send(Message{Type: MsgInstallSnapshot, To: to, Snapshot: n.log.snapshot})
	// Optimistically assume it lands. A loss or a stale response simply walks
	// nextIndex back again on the next round.
	n.nextIndex[to] = n.log.snapshot.Metadata.Index + 1
}

// ProposeConfChange starts a membership change.
//
// It appends the JOINT configuration C_old,new. Once that entry commits, the
// leader automatically appends C_new and the transition completes. Two entries,
// not one, and the intermediate state is the entire safety argument: while
// joint, every decision needs a majority of BOTH configurations, so C_old and
// C_new can never independently elect different leaders.
//
// Only one change may be in flight. A second would produce a configuration
// nobody can reason about, and the paper forbids it.
func (n *Node) ProposeConfChange(cc ConfChange) (Index, error) {
	if n.role != Leader {
		return 0, ErrNotLeader
	}
	if n.cfg.isJoint() {
		return 0, ErrConfChangeInFlight
	}
	if cc.Type == ConfChangeRemoveNode && !n.cfg.contains(cc.NodeID) {
		return 0, ErrNoSuchNode
	}
	if cc.Type == ConfChangeAddNode && n.cfg.contains(cc.NodeID) {
		return 0, ErrNodeExists
	}

	joint := n.cfg.enter(cc)
	data, err := confState{
		Voters:   joint.voters,
		Outgoing: joint.outgoing,
		Change:   &cc,
	}.encode()
	if err != nil {
		return 0, err
	}

	added := n.log.append(n.currentTerm, Entry{Type: EntryConfChange, Data: data})
	n.applyConfFromLog()
	n.matchIndex[n.id] = n.log.lastIndex()
	n.bcastAppend()
	return added[0].Index, nil
}

// leaveJoint appends C_new, completing a transition whose joint entry has
// committed.
func (n *Node) leaveJoint() {
	final := n.cfg.leave()
	data, err := confState{Voters: final.voters}.encode()
	if err != nil {
		return
	}
	n.log.append(n.currentTerm, Entry{Type: EntryConfChange, Data: data})
	n.applyConfFromLog()
	n.matchIndex[n.id] = n.log.lastIndex()
	n.bcastAppend()
}

// applyConfFromLog adopts the most recent configuration entry in the log.
//
// Applied when the entry is APPENDED, not when it commits. Figure 2's §6 rule:
// "a server always uses the latest configuration in its log, regardless of
// whether it is committed." Waiting for commitment would deadlock — the entry
// cannot commit without a quorum of the configuration it defines.
//
// A rescan is also correct after truncation: if a new leader removes the
// configuration entry, membership must revert to whatever remains, and
// scanning backwards from the tail finds exactly that.
func (n *Node) applyConfFromLog() {
	for i := n.log.lastIndex(); i >= n.log.firstIndex(); i-- {
		e, ok := n.log.at(i)
		if !ok {
			break
		}
		if e.Type != EntryConfChange {
			continue
		}
		cs, err := decodeConfState(e.Data)
		if err != nil {
			break
		}
		n.cfg = config{
			voters:   append([]NodeID(nil), cs.Voters...),
			outgoing: append([]NodeID(nil), cs.Outgoing...),
		}
		sortNodeIDs(n.cfg.voters)
		sortNodeIDs(n.cfg.outgoing)
		n.confIndex = i
		n.ensureProgress()
		return
	}
	// No configuration entry survives in the log.
	n.cfg = n.bootstrapCfg
	n.confIndex = 0
	n.ensureProgress()
}

// ensureProgress gives newly added peers progress entries, so the leader starts
// replicating to them immediately rather than on the next election.
func (n *Node) ensureProgress() {
	if n.role != Leader || n.nextIndex == nil {
		return
	}
	for _, p := range n.peers() {
		if _, ok := n.nextIndex[p]; !ok {
			n.nextIndex[p] = n.log.lastIndex() + 1
			n.matchIndex[p] = 0
		}
	}
}

// bcastAppend sends each peer whatever it is missing. A caught-up peer gets
// zero entries, which is exactly a heartbeat — so replication and heartbeating
// are one code path rather than two that can disagree.
func (n *Node) bcastAppend() {
	for _, peer := range n.peers() {
		n.sendAppend(peer)
	}
}

// sendAppend sends one peer the entries after its nextIndex, plus the
// consistency check for the entry immediately before them.
func (n *Node) sendAppend(to NodeID) {
	next := n.nextIndex[to]
	if next < 1 {
		next = 1
	}

	prevIndex := next - 1
	prevTerm, ok := n.log.term(prevIndex)
	if !ok {
		// The entries this follower needs have been compacted away, so no
		// incremental repair is possible. Send the whole snapshot instead.
		n.sendSnapshot(to)
		return
	}

	ents := n.log.slice(next)
	if len(ents) > maxEntriesPerMsg {
		ents = ents[:maxEntriesPerMsg]
	}

	n.send(Message{
		Type:         MsgAppendEntries,
		To:           to,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      ents,
		LeaderCommit: n.log.committed,
	})
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
	if n.hasVoteQuorum() {
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

	last, conflictIndex, conflictTerm, ok := n.log.maybeAppend(m.PrevLogIndex, m.PrevLogTerm, m.Entries)
	if !ok {
		// Reject with a hint, so the leader can skip a whole conflicting term
		// per round trip instead of one entry at a time.
		n.send(Message{
			Type:          MsgAppendEntriesResp,
			To:            m.From,
			Success:       false,
			ConflictIndex: conflictIndex,
			ConflictTerm:  conflictTerm,
		})
		return
	}

	// Only rescan when this message could have changed the configuration:
	// either it carried one, or a truncation removed the one in force.
	if conflictIndex == 0 {
		rescan := n.confIndex > 0 && n.confIndex > last
		if !rescan {
			for _, e := range m.Entries {
				if e.Type == EntryConfChange {
					rescan = true
					break
				}
			}
		}
		if rescan {
			n.applyConfFromLog()
		}
	}

	// Rule 5. min(LeaderCommit, last new entry), NOT LeaderCommit alone: the
	// leader may have committed entries this follower has not received yet,
	// and committing past our own log would mean applying entries we do not
	// have.
	if m.LeaderCommit > n.log.committed {
		n.log.commitTo(min(m.LeaderCommit, last))
	}

	n.send(Message{
		Type:       MsgAppendEntriesResp,
		To:         m.From,
		Success:    true,
		MatchIndex: last,
		ReadSeq:    m.ReadSeq, // echoed untouched; the leader counts these
	})
}

func (n *Node) handleAppendEntriesResp(m Message) {
	if n.role != Leader {
		return
	}

	// Echoed read sequence: proof this peer still considered us leader after
	// the read was registered.
	if m.ReadSeq > n.ackedReadSeq[m.From] {
		n.ackedReadSeq[m.From] = m.ReadSeq
		n.maybeReleaseReads()
	}

	if m.Success {
		// Only ever forward. A delayed response from an earlier, shorter
		// AppendEntries must not drag matchIndex backwards — that would
		// un-commit an entry, and commitment is permanent.
		if m.MatchIndex > n.matchIndex[m.From] {
			n.matchIndex[m.From] = m.MatchIndex
			n.nextIndex[m.From] = m.MatchIndex + 1
		}
		n.maybeCommit()
		return
	}

	// Rejected: back nextIndex up using the follower's conflict hint.
	next := m.ConflictIndex
	if m.ConflictTerm != 0 {
		// If we also hold that term, jump to just past OUR last entry of it:
		// everything before is already known to agree.
		if idx, found := n.log.lastIndexOfTerm(m.ConflictTerm); found {
			next = idx + 1
		}
	}
	if next < 1 {
		next = 1
	}
	if next >= n.nextIndex[m.From] {
		// Never move forward on a rejection. A stale reject arriving after a
		// success would otherwise undo the repair and loop forever.
		return
	}
	n.nextIndex[m.From] = next
	n.sendAppend(m.From)
}

// maybeCommit advances the commit index to the largest N such that a majority
// has matchIndex >= N, and log[N].Term == currentTerm.
//
// That last clause is the Figure 8 condition and it is not optional. Without
// it, a leader can commit an entry from a PREVIOUS term that a later leader
// then overwrites — the one safety violation the paper devotes a full figure
// to. Entries from earlier terms commit indirectly, carried along once a
// current-term entry commits.
func (n *Node) maybeCommit() bool {
	// Collect over n.peers (a slice) rather than ranging the matchIndex map:
	// map iteration order is randomised, and while sorting would erase the
	// difference here, ranging maps for anything order-sensitive is the habit
	// that breaks determinism elsewhere.
	candidate := n.cfg.committedIndex(func(id NodeID) Index {
		if id == n.id {
			return n.matchIndex[n.id]
		}
		return n.matchIndex[id]
	})
	if candidate <= n.log.committed {
		return false
	}
	if term, ok := n.log.term(candidate); !ok || term != n.currentTerm {
		return false
	}

	n.log.commitTo(candidate)

	// The joint configuration is committed, so C_new is now safe to adopt.
	// Doing this automatically means a caller makes one ProposeConfChange call
	// and the two-phase transition completes on its own.
	if n.cfg.isJoint() && n.jointEntryCommitted() {
		n.leaveJoint()
		return true
	}

	// Followers learn of the new commit index on the next AppendEntries.
	n.bcastAppend()
	return true
}

// handleInstallSnapshot accepts a snapshot from the leader.
//
// A snapshot arrives when the leader has already compacted away the entries
// this follower needs, so incremental repair is impossible. The snapshot is
// authoritative: it came from a leader whose log by definition contains every
// committed entry, so the follower discards its own log entirely rather than
// trying to reconcile. Local entries past the snapshot were uncommitted.
func (n *Node) handleInstallSnapshot(m Message) {
	// Step guarantees m.Term == n.currentTerm, so this is a real leader.
	n.becomeFollower(m.Term, m.From)

	if m.Snapshot == nil {
		n.send(Message{Type: MsgInstallSnapshotResp, To: m.From, MatchIndex: n.log.lastIndex()})
		return
	}

	// Already covered. A stale or duplicated InstallSnapshot must not roll the
	// log backwards — that would un-commit entries, and commitment is final.
	if m.Snapshot.Metadata.Index <= n.log.committed {
		n.send(Message{Type: MsgInstallSnapshotResp, To: m.From, MatchIndex: n.log.lastIndex()})
		return
	}

	// If we happen to hold a matching entry at the snapshot's boundary, our log
	// agrees with the leader up to that point and only needs compacting — no
	// need to throw away entries we can keep.
	if n.log.matches(m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term) {
		n.log.compact(m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term)
		n.log.commitTo(m.Snapshot.Metadata.Index)
	} else {
		n.log.restore(m.Snapshot)
	}

	n.pendingSnapshot = m.Snapshot
	if len(m.Snapshot.Metadata.Voters) > 0 {
		n.applyVoters(m.Snapshot.Metadata.Voters)
	}

	n.send(Message{Type: MsgInstallSnapshotResp, To: m.From, MatchIndex: n.log.lastIndex()})
}

func (n *Node) handleInstallSnapshotResp(m Message) {
	if n.role != Leader {
		return
	}
	if m.MatchIndex > n.matchIndex[m.From] {
		n.matchIndex[m.From] = m.MatchIndex
		n.nextIndex[m.From] = m.MatchIndex + 1
	}
	n.maybeCommit()
}

// CreateSnapshot compacts the log up to index, which the driver has just
// snapshotted from the state machine.
//
// index must be <= applied: snapshotting past what the state machine has
// consumed would produce a snapshot of a state that never existed.
func (n *Node) CreateSnapshot(index Index, data []byte) (*Snapshot, error) {
	if index > n.log.applied {
		return nil, ErrSnapshotAhead
	}
	term, ok := n.log.term(index)
	if !ok {
		return nil, ErrSnapshotCompacted
	}

	snap := &Snapshot{
		Metadata: SnapshotMetadata{Index: index, Term: term, Voters: n.cfg.voters},
		Data:     data,
	}
	n.log.compact(index, term)
	n.log.snapshot = snap
	return snap, nil
}

// AppliedIndex is the highest index the state machine has consumed, which is
// the highest index a snapshot may cover.
func (n *Node) AppliedIndex() Index { return n.log.applied }

// FirstIndex is the oldest entry still in the log; everything below it lives in
// a snapshot.
func (n *Node) FirstIndex() Index { return n.log.firstIndex() }

// LastIndex is the newest entry in the log.
func (n *Node) LastIndex() Index { return n.log.lastIndex() }

// applyVoters replaces membership from a snapshot.
func (n *Node) applyVoters(voters []NodeID) {
	n.cfg = newConfig(voters)
	n.bootstrapCfg = n.cfg
}

// sortNodeIDs keeps peer order deterministic. Insertion sort: the slice is
// tiny, and avoiding the sort package keeps one fewer import in the core.
func sortNodeIDs(ids []NodeID) {
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j] < ids[j-1]; j-- {
			ids[j], ids[j-1] = ids[j-1], ids[j]
		}
	}
}

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
func (n *Node) quorum() int { return len(n.cfg.voters)/2 + 1 }

// hasVoteQuorum reports whether the granted votes form a majority of every
// active configuration.
func (n *Node) hasVoteQuorum() bool {
	return n.cfg.hasQuorum(func(id NodeID) bool { return n.votes[id] })
}

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
	if m.Type == MsgAppendEntries {
		m.ReadSeq = n.readSeq
	}
	n.msgs = append(n.msgs, m)
}
