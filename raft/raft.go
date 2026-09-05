package raft

import (
	"errors"
	"fmt"
)

// Timing, in TICKS — never durations. The core has no clock; one Tick() is one
// unit of logical time, and the driver decides what a tick means in wall time.
//
// The constraint from the paper is broadcastTime << electionTimeout << MTBF.
const (
	electionTimeoutMin = 10 // inclusive
	electionTimeoutMax = 20 // exclusive
	heartbeatTimeout   = 1
)

// ErrStepStaleTerm is returned by Step when a message from an older term is
// dropped. It is informational — the caller is not required to act on it.
var ErrStepStaleTerm = errors.New("raft: message from a stale term ignored")

// Node is one Raft replica: a pure state machine.
//
// Everything is unexported. The only way in is Tick and Step; the only way out
// is Ready. No goroutine touches a Node — the driver serializes all access on
// a single goroutine, which is what lets this be lock-free.
type Node struct {
	id    NodeID
	peers []NodeID // OTHER nodes. Does NOT include id.
	role  Role

	// --- Persistent state (Figure 2) ---------------------------------------
	// Must be durable before responding to any RPC. Surfaced via Ready.
	currentTerm Term
	votedFor    NodeID // None when no vote has been cast this term

	// --- Volatile state ----------------------------------------------------
	lead NodeID // last known leader this term; None if unknown

	// votes records RequestVote responses while a candidate. A false entry is
	// a recorded refusal, not a missing answer — do not count it as a grant.
	votes map[NodeID]bool

	// --- Timing, counted in ticks ------------------------------------------
	electionElapsed  int // ticks since the last useful leader contact
	heartbeatElapsed int // leader only: ticks since the last heartbeat
	electionTimeout  int // redrawn randomly at the start of each election

	// --- Injected dependencies ---------------------------------------------
	// randIntn returns a value in [0,n). Injected rather than imported: naming
	// *rand.Rand here would mean `import "math/rand"`, which import_test.go
	// bans. The driver closes over its own seeded source, so determinism is
	// the caller's guarantee, replayable from a seed.
	randIntn func(n int) int

	// --- Output ------------------------------------------------------------
	msgs []Message // outbound queue, drained by Ready()

	// prevHardState is the last state the driver was told to persist. Ready()
	// compares against it to decide whether HardState changed. This works as a
	// single == because HardState is comparable.
	prevHardState HardState
}

// NewNode creates a follower at term 0 that has voted for nobody.
//
// peers must NOT include id. randIntn must not be nil — the core cannot supply
// its own randomness without breaking determinism, so it refuses to guess.
func NewNode(id NodeID, peers []NodeID, randIntn func(n int) int) *Node {
	if id == None {
		panic("raft: node id must not be zero; 0 means None")
	}
	if randIntn == nil {
		panic("raft: NewNode requires a randIntn source")
	}

	// Copy the peer slice. The caller keeps their own; if we aliased it, a
	// later append on their side could silently reshape this node's cluster —
	// module 5's slice-header lesson, applied where it matters.
	peersCopy := make([]NodeID, len(peers))
	copy(peersCopy, peers)

	n := &Node{
		id:       id,
		peers:    peersCopy,
		randIntn: randIntn,
		votes:    make(map[NodeID]bool),
	}

	// Deliberately routed through becomeFollower rather than setting fields
	// here: construction and stepdown must produce the SAME state, and the
	// only way to guarantee that is to have one code path.
	n.becomeFollower(0, None)
	return n
}

// =============================================================================
// Public surface — the entire contract with the outside world.
// =============================================================================

// Tick advances logical time by one unit. It is the only thing that makes a
// quiet cluster do anything.
//
// Followers and candidates count toward their election timeout; on reaching it,
// they campaign. Leaders count toward their heartbeat timeout; on reaching it,
// they reassert themselves to every peer. A leader never runs an election
// clock — it does not need to detect a leader, it is one.
//
// The branches are mutually exclusive on purpose. n.role can CHANGE inside this
// call (a follower can become candidate and, in a one-node cluster, leader
// before Tick returns). Re-testing the role after that would fire the heartbeat
// timer on the very tick the node was elected — the initial heartbeat on
// election is becomeLeader's job, not this timer's.
func (n *Node) Tick() {
	if n.role == Follower || n.role == Candidate {
		n.electionElapsed++

		if n.electionElapsed >= n.electionTimeout {
			n.becomeCandidate()
		}
	} else if n.role == Leader {
		n.heartbeatElapsed++

		if n.heartbeatElapsed >= heartbeatTimeout {
			n.heartbeatElapsed = 0
			n.bcastHeartbeat()
		}
	}
	fmt.Println("Tick", n.role, n.electionElapsed, n.heartbeatElapsed)

}

// Step delivers one inbound message.
//
// WHAT IT DOES, in this exact order:
//
//  1. m.Term > n.currentTerm — someone is ahead of us. Adopt their term and
//     become a follower. Which leader we record depends on the message:
//     an AppendEntries means the sender IS the leader, so lead = m.From;
//     a RequestVote means the sender is only a candidate, so lead = None.
//
//  2. m.Term < n.currentTerm — the message is from the past. Reply where
//     Figure 2 requires one (so the sender learns it is behind), otherwise
//     drop it. Return ErrStepStaleTerm either way.
//
// 3. Otherwise m.Term == n.currentTerm. Dispatch on m.Type to a handler.
//
// WHY THE ORDER IS FIXED:
//
// Term is Raft's logical clock and the highest term always wins. Resolving it
// first means every handler below can assume m.Term == n.currentTerm and never
// has to think about staleness again. Push the check into the handlers and you
// will get it right in three of them and wrong in the fourth.
//
// WHY STEP 1 APPLIES TO RESPONSES, NOT JUST REQUESTS:
//
// A leader gets partitioned and keeps heartbeating into the void. The partition
// heals. A follower — now on a higher term under a new leader — replies to that
// heartbeat with the higher term. The deposed leader learns it is deposed FROM
// A RESPONSE TO ITS OWN MESSAGE. Check terms only on inbound requests and that
// leader never steps down: two leaders, both serving reads, both wrong.
//
// WHY votedFor CLEARS IN STEP 1:
//
// New term, fresh vote. Carry a stale vote forward and the node refuses to vote
// in the term it just joined, so elections stall for no visible reason.
// becomeFollower already owns this rule — call it rather than reimplementing.
//
// FAILURE MODE IF WRONG: two leaders in one term, which is the single
// invariant Raft exists to prevent.
func (n *Node) Step(m Message) error {
	if m.Term > n.currentTerm {
		lead := None
		if m.Type == MsgAppendEntries {
			lead = m.From
		}
		n.becomeFollower(m.Term, lead)
	}
	if m.Term < n.currentTerm {
		switch m.Type {
		case MsgRequestVote:
			n.send(Message{Type: MsgRequestVoteResp, To: m.From, VoteGranted: false})
		case MsgAppendEntries:
			n.send(Message{Type: MsgAppendEntriesResp, To: m.From, Success: false})
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
	}
	return nil
}

// Ready returns the intentions accumulated since the last Advance.
//
// WHAT IT DOES:
//
//  1. Put n.msgs into Ready.Messages.
//  2. Compare hardState() against n.prevHardState. If they differ, point
//     Ready.HardState at the current one; if identical, leave it nil.
//  3. Entries and CommittedEntries stay empty — milestone 1 has no log.
//
// WHY IT RETURNS INTENTIONS INSTEAD OF DOING THE WORK:
//
// This is the determinism boundary. If the core called send() and save()
// itself, it would need a socket and a file handle, and a test would need a
// network and a disk. By handing back a description of what should happen, the
// same core runs under server/ (real I/O) and sim/ (virtual clock, injected
// faults) without knowing which. That is what makes seed 42891 replayable.
//
// It also makes the durability ordering auditable: the driver can be REQUIRED
// to persist HardState before sending Messages, because they arrive together
// in one struct with a documented contract. A core that sent its own messages
// could not enforce that from the outside.
//
// WHY nil MEANS UNCHANGED:
//
// Persisting is an fsync. Doing it on every tick when nothing changed would
// dominate the cost of the whole system. nil tells the driver "skip the disk
// this round". The comparison is a single == because HardState is comparable —
// see the note on the type.
//
// WHY IT MUST NOT MUTATE:
//
// A driver may call Ready, decide it is too busy, and call again next loop.
// Two calls with no Advance between them must return the same thing. Draining
// the queue here would silently lose messages the driver never sent.
func (n *Node) Ready() Ready {
	// Entries and CommittedEntries are deliberately absent: Node has no log in
	// milestone 1, so there is nothing to populate them from. Their zero value
	// is nil, which is exactly right.
	rd := Ready{Messages: n.msgs}

	// Only ask the driver to fsync when something durable actually changed.
	// A plain == works because every HardStat
	if hs := n.hardState(); hs != n.prevHardState {
		rd.HardState = &hs
	}

	return rd
}

// Advance acknowledges that the driver handled everything in r.
//
// WHAT IT DOES:
//
//  1. Remove from n.msgs the messages that were handed out in r.
//  2. Record the hard state the driver just persisted into n.prevHardState,
//     so the next Ready can tell whether anything changed since.
//
// WHY IT EXISTS AT ALL:
//
// Ready is deliberately read-only, so something has to say "done, you can
// forget it." Without Advance the core would return the same messages forever
// and every peer would be flooded with duplicate heartbeats.
//
// Splitting it in two is also what makes the ordering enforceable: the driver
// persists, sends, applies, and only THEN calls Advance. If it crashes halfway
// through, it never advanced, so the next Ready hands back the same work and
// the operation is retried rather than lost. Advance is the commit point of
// the driver loop.
//
// THE SUBTLETY:
//
// Only update prevHardState if r.HardState was non-nil — a nil one means the
// driver persisted nothing, so overwriting the record would make the next
// change invisible.
//
// And think about the queue. A correct driver does not call Tick or Step
// between Ready and Advance, so n.msgs should still be exactly what was handed
// out — truncating to nil is fine under that assumption. Dropping exactly
// len(r.Messages) survives a driver that breaks it. Decide which invariant you
// want to rely on, and if you choose truncation, say so in a comment so the
// assumption is written down rather than implied.
func (n *Node) Advance(r Ready) {
	// Only record what was actually persisted. A nil HardState means the
	// driver wrote nothing, so overwriting prevHardState here would make the
	// next genuine change invisible and skip an fsync we needed.
	if r.HardState != nil {
		n.prevHardState = *r.HardState
	}

	// Drop exactly what was handed out. A correct driver calls neither Tick
	// nor Step between Ready and Advance, so this is normally all of n.msgs.
	// Dropping by COUNT rather than truncating to nil means a driver that
	// breaks that rule loses nothing — silently dropping unsent messages
	// would look like a network fault and be nearly impossible to trace.
	if len(r.Messages) >= len(n.msgs) {
		n.msgs = nil // release the backing array rather than growing forever
	} else {
		n.msgs = n.msgs[len(r.Messages):]
	}
}

// =============================================================================
// State transitions
// =============================================================================

// becomeFollower moves to the follower state at the given term.
//
// Called from three places: NewNode (term 0), Step discovering a higher term,
// and a candidate or leader stepping down.
//
// WHAT IT DOES:
//
//  1. If term > n.currentTerm, clear votedFor — do this BEFORE overwriting
//     currentTerm, or the comparison you need is already gone.
//  2. Set currentTerm = term, role = Follower, lead = lead.
//  3. resetElectionTimer().
//
// WHY votedFor RESETS ON TERM ADVANCE AND NOT ON ROLE CHANGE:
//
// The invariant is one vote per TERM, and that is what makes one-leader-per-term
// possible. Two failure directions, both real:
//
//   - Clear it too eagerly (on every stepdown): a candidate that voted for
//     itself in term 5, steps down within term 5, then grants a vote to
//     someone else in term 5. Two nodes each collect a majority. Split brain.
//
//   - Clear it too rarely (never): a node carries a term-5 vote into term 6
//     and refuses to vote for anyone. If enough nodes do this, no candidate
//     reaches a quorum and the cluster has no leader, forever, with nothing
//     in the logs to explain it.
//
// WHY IT RESETS THE ELECTION TIMER UNCONDITIONALLY:
//
// Every path into follower state means something legitimate happened — a real
// leader appeared, or a higher term arrived. Restarting our patience is
// correct, and unconditional is one fewer branch to get wrong. Know that this
// is a choice: a stream of stale messages that all reach this function could
// keep a node from ever campaigning ("livelock by resets"). etcd/raft resets
// unconditionally too, and instead controls which messages get here.
func (n *Node) becomeFollower(term Term, lead NodeID) {
	// Order matters: once currentTerm is overwritten, the comparison that
	// tells us whether the term advanced is gone. One vote per TERM, so the
	// vote resets on a term change and survives a mere role change.
	if term > n.currentTerm {
		n.votedFor = None
	}

	n.currentTerm = term
	n.role = Follower
	n.lead = lead
	n.resetElectionTimer()
}

// becomeCandidate starts a new election. Figure 2, "Candidates".
//
// WHAT IT DOES:
//
//  1. currentTerm++ — a new election is always a new term.
//  2. role = Candidate, lead = None (we do not know of a leader; that is why
//     we are here).
//  3. votedFor = n.id, and record that self-vote in the votes map.
//  4. RESET the votes map first, so nothing from the previous election survives.
//  5. resetElectionTimer() — a NEW random timeout, not the old one.
//
// WHY THE TERM INCREMENTS:
//
// Terms are how every other node decides whose claim is newer. A candidate
// that reused its term would be indistinguishable from the election that just
// failed, and every node that already voted in that term would refuse it.
//
// WHY STEP 4 IS NOT OPTIONAL — this is the bug worth remembering:
//
// Say we campaign at term 3 and collect one grant out of three nodes. Not a
// quorum, so the election times out and we campaign again at term 4. If the
// votes map still holds the term-3 grant, then ONE new grant at term 4 makes
// the count look like two — a quorum. The node declares itself leader of term 4
// on a minority of term-4 votes, while another node legitimately wins the same
// term elsewhere. Two leaders, one term, from a map that was never cleared.
//
// WHY THE TIMEOUT IS REDRAWN:
//
// If three nodes time out simultaneously they all campaign, all split the vote,
// and all fail. With a fixed timeout they wake up together again and repeat
// identically, forever. A fresh random draw per election is the only thing that
// breaks the symmetry, and it is the entire reason randIntn exists.
func (n *Node) becomeCandidate() {
	n.currentTerm++
	n.role = Candidate
	n.lead = None // we are here precisely because we know of no leader

	// A FRESH map, not a cleared reuse. Any grant left over from the previous
	// election would count toward this one: campaign at term 3, collect one
	// grant, fail, campaign at term 4, and a single new grant now looks like
	// two. Leader of term 4 on a minority of term-4 votes.
	n.votes = make(map[NodeID]bool)

	n.votedFor = n.id
	n.votes[n.id] = true

	// New random timeout for this election. Without the redraw, nodes that
	// split a vote wake together and split it identically, forever.
	n.resetElectionTimer()

	// The self-vote may already BE the majority — a one-node cluster has
	// quorum 1. Check before campaigning, because campaign() would send zero
	// messages and we would then wait for responses that never arrive.
	//
	// This is not an N==1 special case: it is the general rule that a
	// candidate evaluates its quorum whenever its vote count changes, and its
	// own vote is the first change.
	if n.grantedVotes() >= n.quorum() {
		n.becomeLeader()
		return
	}

	n.campaign()
}

// becomeLeader is called on winning an election.
//
// WHAT IT DOES:
//
// 1. role = Leader, lead = n.id.
// 2. heartbeatElapsed = 0.
// 3. Immediately broadcast an empty AppendEntries to every peer.
//
// WHY IT MUST NOT TOUCH THE TERM:
//
// The leader serves the term it just won. Incrementing here would abandon the
// term whose votes it holds and claim one nobody elected it to.
//
// WHY THE IMMEDIATE HEARTBEAT (Figure 2: "upon election, send initial empty
// AppendEntries RPCs to each server"):
//
// Every other node is running its own election timer right now. Until they
// hear from a leader, one of them will time out and start a competing election
// at a higher term — which deposes the leader that just won, for no reason.
// The first heartbeat is what stops all those clocks. Waiting even one
// heartbeatTimeout tick makes this measurably more likely under load.
//
// Note this is the same broadcast Tick does on the heartbeat timer. Two
// call sites for identical message construction is an argument for a small
// bcastHeartbeat helper — duplicating a message literal is how the earlier
// duplicated-block bug happened.
func (n *Node) becomeLeader() {
	// currentTerm is deliberately untouched. The leader serves the term it
	// just won; incrementing here would abandon the votes it holds.
	n.role = Leader
	n.lead = n.id
	n.heartbeatElapsed = 0

	// Immediately, not on the next heartbeat tick. Every peer is running its
	// own election clock right now, and the first one to time out starts a
	// competing election at a higher term that deposes us for no reason.
	n.bcastHeartbeat()
}

// campaign broadcasts RequestVote to every peer.
//
// WHAT IT DOES:
//
// Send one MsgRequestVote per peer, carrying LastLogIndex = n.lastIndex() and
// LastLogTerm = n.lastTerm(). Both are 0 in milestone 1. Do not set Term or
// From — send fills those in.
//
// WHY IT SENDS THE LOG POSITION:
//
// The voter uses it for the §5.4.1 election restriction: a candidate whose log
// is missing committed entries must never win, or the new leader would truncate
// data that a majority had already acknowledged. Always-zero in milestone 1,
// but see isUpToDate for why it is wired now rather than later.
//
// THE SINGLE-NODE PROBLEM — think about this before writing it:
//
// A one-node cluster has no peers. This function sends nothing, so no
// RequestVoteResp will ever arrive, so handleRequestVoteResp never runs. If
// winning is only ever detected while counting a response, that node campaigns
// at term 1, times out, campaigns at term 2, forever — a cluster that never
// elects a leader and never errors.
//
// The fix is not an N==1 special case. A candidate must evaluate its quorum at
// the moment it records its own vote, because the self-vote IS a vote. With
// quorum() == 1 that is already a majority, and the same code is what makes
// N=3 correct for the same reason rather than a different one. Decide whether
// that check lives here or in becomeCandidate, and be able to say why.
func (n *Node) campaign() {
	for _, peer := range n.peers {
		// Term and From are left unset on purpose — send() stamps both.
		n.send(Message{
			Type:         MsgRequestVote,
			To:           peer,
			LastLogIndex: n.lastIndex(),
			LastLogTerm:  n.lastTerm(),
		})
	}
}

// bcastHeartbeat sends an empty AppendEntries to every peer.
//
// Extracted because it has two callers — the heartbeat timer in Tick and the
// immediate broadcast in becomeLeader. Two copies of a message literal is how
// the duplicated-leader-block bug happened earlier; one function cannot drift
// from itself.
func (n *Node) bcastHeartbeat() {
	for _, peer := range n.peers {
		n.send(Message{
			Type:         MsgAppendEntries,
			To:           peer,
			PrevLogIndex: n.lastIndex(),
			PrevLogTerm:  n.lastTerm(),
		})
	}
}

// grantedVotes counts how many peers actually GRANTED, not how many replied.
//
// n.votes stores refusals as false so a retried response stays idempotent, so
// len(n.votes) is the number of answers received — a completely different
// number. Counting answers instead of grants elects a node that everybody
// rejected. Two callers: becomeCandidate (the self-vote) and
// handleRequestVoteResp (every subsequent one).
func (n *Node) grantedVotes() int {
	count := 0
	for _, granted := range n.votes {
		if granted {
			count++
		}
	}
	return count
}

// =============================================================================
// Message handlers
// =============================================================================

// handleRequestVote decides whether to grant a vote. Figure 2, "RequestVote
// RPC / Receiver implementation".
//
// Step has already guaranteed m.Term == n.currentTerm by the time we get here.
//
// WHAT IT DOES:
//
// Grant the vote if BOTH hold:
//   - votedFor is None, OR votedFor is already m.From
//   - isUpToDate(m.LastLogIndex, m.LastLogTerm)
//
// On granting: set votedFor = m.From and resetElectionTimer().
// Either way, reply with MsgRequestVoteResp carrying VoteGranted.
//
// WHY "OR ALREADY m.From" — this is not redundancy, it is idempotency:
//
// A candidate sends RequestVote, we grant it, and our reply is dropped by the
// network. The candidate retries the identical request. Without this clause we
// would now refuse, because votedFor is set — and refuse a candidate we already
// voted for. A retry must produce the same answer as the original, or a single
// dropped packet can cost an election.
//
// WHY WE REPLY EVEN WHEN REFUSING:
//
// Silence is indistinguishable from a partition. The candidate would wait for
// an answer that is never coming until its election times out. A refusal also
// carries our term, which is how a candidate on a stale term learns to step
// down instead of retrying forever.
//
// WHY GRANTING RESETS THE ELECTION TIMER:
//
// Figure 2, Followers: the timer restarts on AppendEntries from the leader OR
// on granting a vote. You just endorsed someone else's candidacy; immediately
// timing out and campaigning against them would split the vote you just cast.
func (n *Node) handleRequestVote(m Message) {
	// "or already m.From" is idempotency, not redundancy: if our reply was
	// dropped and the candidate retries, it must get the same answer. Without
	// it we would refuse a candidate we already voted for, and one lost packet
	// costs an election.
	canVote := n.votedFor == None || n.votedFor == m.From

	// §5.4.1. Always true in milestone 1 (both logs empty), and wired now so
	// milestone 2 inherits it instead of bolting it on after replication works.
	granted := canVote && n.isUpToDate(m.LastLogIndex, m.LastLogTerm)

	if granted {
		n.votedFor = m.From
		// Figure 2, Followers: the clock restarts on granting a vote. Having
		// just endorsed someone, timing out and running against them would
		// split the vote we just cast.
		n.resetElectionTimer()
	}

	// Reply either way. Silence is indistinguishable from a partition, and the
	// reply carries our term (stamped by send), which is how a candidate on a
	// stale term learns to step down instead of retrying forever.
	n.send(Message{
		Type:        MsgRequestVoteResp,
		To:          m.From,
		VoteGranted: granted,
	})
}

// handleRequestVoteResp counts a vote.
//
// WHAT IT DOES:
//
// 1. If n.role != Candidate, ignore it and return.
// 2. Record n.votes[m.From] = m.VoteGranted.
// 3. Count the GRANTS — entries whose value is true.
// 4. If that count >= quorum(), becomeLeader().
//
// WHY STEP 1:
//
// Responses outlive the elections that caused them. We may have already won,
// already stepped down, or moved to a newer term. Acting on a reply to an
// election that is over could promote a node that has since become a follower
// of a legitimate leader.
//
// WHY STEP 3 COUNTS GRANTS AND NOT len(n.votes):
//
// A false entry is a RECORDED REFUSAL, not a missing answer — we store it so a
// retried response is idempotent, and so we could detect a lost election later.
// Counting entries instead of grants means three refusals look like three
// votes. In a 5-node cluster, a node that everybody refused declares itself
// leader. This is the same class of bug as the uncleared votes map in
// becomeCandidate, and it is why the field is map[NodeID]bool rather than a
// counter.
//
// NOT REQUIRED IN MILESTONE 1: reacting to a majority of REFUSALS by giving up
// early. Figure 2 does not do this — the election timeout handles it. Adding it
// is an optimization, not a correctness fix.
func (n *Node) handleRequestVoteResp(m Message) {
	// Responses outlive the elections that caused them. We may have already
	// won, already stepped down, or moved on. Acting on a reply to a finished
	// election could promote a node that is now following a real leader.
	if n.role != Candidate {
		return
	}

	// Record refusals too — see grantedVotes for why this is a bool map and
	// not a counter.
	n.votes[m.From] = m.VoteGranted

	if n.grantedVotes() >= n.quorum() {
		n.becomeLeader()
	}
}

// handleAppendEntries processes a heartbeat. Milestone 1 carries no entries, so
// this is purely "a leader is alive at this term".
//
// Step has already guaranteed m.Term == n.currentTerm.
//
// WHAT IT DOES:
//
// 1. becomeFollower(m.Term, m.From) — record who the leader is.
// 2. Reply with MsgAppendEntriesResp, Success = true.
//
// WHY THIS IS THE FUNCTION THAT KEEPS THE CLUSTER STABLE:
//
// It is the only thing that stops followers from campaigning. becomeFollower
// resets the election timer, so as long as heartbeats keep arriving every
// heartbeatTimeout tick, no follower's electionTimeout is ever reached. Lose
// the leader and those timers run out — which is exactly how a re-election
// starts, with no failure detector and no extra machinery.
//
// WHY A CANDIDATE MUST STEP DOWN HERE EVEN AT AN EQUAL TERM:
//
// We are a candidate at term 5 and an AppendEntries arrives at term 5. It is
// not stale and it is not ahead — it means someone else already collected a
// majority for term 5 while we were waiting. There is exactly one leader per
// term, so we lost. Only accepting a HIGHER term here leaves a candidate
// campaigning against a leader it has already heard from, splitting votes in a
// term that is already decided.
//
// Routing through becomeFollower gives you this for free, which is the point
// of having one transition function rather than assigning fields inline.
func (n *Node) handleAppendEntries(m Message) {
	// Step guarantees m.Term == n.currentTerm here, so this is a legitimate
	// leader of our own term. becomeFollower does three jobs at once:
	//
	//   - a CANDIDATE steps down. Equal term, not higher — someone else
	//     already collected a majority for this term while we waited, and
	//     there is exactly one leader per term, so we lost.
	//   - lead is recorded, which milestone 3 needs for client redirects.
	//   - the election timer resets. THIS is the only thing keeping followers
	//     quiet; lose the leader and these timers run out, which is the whole
	//     failure detector.
	//
	// votedFor survives, because the term did not change.
	n.becomeFollower(m.Term, m.From)

	n.send(Message{
		Type:    MsgAppendEntriesResp,
		To:      m.From,
		Success: true,
	})
}

// =============================================================================
// Helpers
// =============================================================================

// quorum returns the number of nodes needed for a majority of the cluster.
//
// WHAT IT DOES:
//
// The cluster is n.peers PLUS this node, so the size is len(n.peers)+1 and a
// majority is strictly more than half of that.
//
//	cluster 1 -> 1     cluster 3 -> 2     cluster 5 -> 3
//	cluster 2 -> 2     cluster 4 -> 3
//
// WHY MAJORITY AND NOT ALL:
//
// Requiring every node means one crashed machine halts the cluster — no better
// than a single server. Requiring a majority means any two majorities must
// share at least one member, and that shared member is what makes it impossible
// for two different leaders to both be elected in the same term. Overlap is the
// entire mechanism; "more than half" is just how you guarantee it.
//
// WHY EVEN CLUSTER SIZES ARE POINTLESS:
//
// 4 needs 3, same as 5 — so it tolerates one failure, same as 3, while costing
// an extra machine and an extra round trip. That is why Raft clusters are odd.
//
// FAILURE MODE IF OFF BY ONE:
//
// Return len(peers)/2 + 1 (forgetting yourself) and in a 3-node cluster you get
// 2 from 2 peers... which happens to be right. In a 5-node cluster you get 3
// from 4 peers — also right. Now try it with peers counted correctly but the
// wrong rounding: a quorum of 2 in a 5-node cluster lets TWO nodes each claim a
// majority from disjoint vote sets. Split brain, and it only shows up under a
// partition, which is to say only in production or in milestone 4's simulator.
// Write the worked examples above as a table test if you are unsure.
func (n *Node) quorum() int {
	// +1 because the cluster is the peers PLUS this node. Integer division
	// then +1 gives "strictly more than half":
	//
	//	peers 0 -> cluster 1 -> 1     peers 3 -> cluster 4 -> 3
	//	peers 1 -> cluster 2 -> 2     peers 4 -> cluster 5 -> 3
	//	peers 2 -> cluster 3 -> 2
	return (len(n.peers)+1)/2 + 1
}

// isUpToDate implements the §5.4.1 election restriction: is a candidate's log
// at least as complete as ours?
//
// WHAT IT DOES:
//
// True if the candidate's last term is HIGHER than ours, or the terms are EQUAL
// and its log is at least as long:
//
//	lastTerm > n.lastTerm() || (lastTerm == n.lastTerm() && lastIndex >= n.lastIndex())
//
// Note >= on the index, not >. An identical log is perfectly electable; there
// is nothing about being exactly as up to date that should disqualify anyone.
// Using > here would make a 3-node cluster with identical logs unable to elect
// anybody at all.
//
// WHY TERM BEATS LENGTH:
//
// A longer log is not a better log. A node can accumulate uncommitted entries
// from a leader that was partitioned and never reached a majority — those
// entries are garbage that will be truncated. An entry from a HIGHER term was
// accepted more recently by a cluster that had moved on. So compare terms
// first, and only use length to break a tie within the same term.
//
// WHY THIS EXISTS AT ALL — the safety property:
//
// A leader never overwrites or deletes entries in its own log; it only makes
// followers match it. So if a node missing committed entries were elected, it
// would force every follower to truncate down to its shorter log, ERASING data
// a majority had already acknowledged and a client was told was durable. This
// check is what makes that impossible: any node missing a committed entry is
// necessarily behind a majority, so it can never collect a majority of votes.
//
// WHY WRITE IT NOW WHEN IT IS ALWAYS TRUE:
//
// Both arguments are 0 in milestone 1, so this looks like dead code. But if you
// skip it, milestone 2 arrives with replication working, elections working, and
// a rare partition-only failure where a lagging node wins and truncates
// committed entries off a majority. It reproduces once in a thousand runs and
// costs a day. Four lines now, and the callers never change.
func (n *Node) isUpToDate(lastIndex Index, lastTerm Term) bool {
	myTerm, myIndex := n.lastTerm(), n.lastIndex()

	// Term first: a longer log is not a better log. Extra entries can be
	// uncommitted garbage from a partitioned leader that never reached a
	// majority. A higher last term was accepted more recently by a cluster
	// that had moved on.
	//
	// >= on the index, not >. An identical log is perfectly electable — using
	// > would make a cluster whose nodes all agree unable to elect anyone.
	return lastTerm > myTerm || (lastTerm == myTerm && lastIndex >= myIndex)
}

// lastIndex and lastTerm describe the tail of the log.
//
// Milestone 1 has no log, so both are 0. They exist as functions rather than
// literals so that campaign and isUpToDate are written against the real shape
// now — milestone 2 fills these in without touching a single caller.
func (n *Node) lastIndex() Index { return 0 }
func (n *Node) lastTerm() Term   { return 0 }

// resetElectionTimer clears the elapsed counter and draws a NEW random timeout.
//
// The redraw is the whole point. A fixed timeout means nodes that split a vote
// will split it again identically, and again, forever — randomization is the
// only thing that breaks the symmetry. Called on every election start, on
// every stepdown, and whenever a vote is granted.
func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	n.electionTimeout = electionTimeoutMin +
		n.randIntn(electionTimeoutMax-electionTimeoutMin)
}

// hardState snapshots the durable subset of this node's state.
//
// Commit is always 0 in milestone 1 — there is no log to commit from yet.
func (n *Node) hardState() HardState {
	return HardState{
		Term:     n.currentTerm,
		VotedFor: n.votedFor,
	}
}

// send queues a message for the driver to deliver. The core never delivers
// anything itself.
//
// From and Term are always overwritten with this node's values, because every
// message Raft sends — request or reply — carries the sender's currentTerm.
// Callers set Type, To, and the type-specific fields; identity and term are
// not their business.
func (n *Node) send(m Message) {
	m.From = n.id
	m.Term = n.currentTerm
	n.msgs = append(n.msgs, m)
}
