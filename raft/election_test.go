package raft

import "testing"

// Test 1 — a single node elects itself immediately on timeout.
//
// The degenerate case, and a good first test: it exercises the whole election
// path with no network. There are no peers, so no RequestVoteResp will ever
// arrive; a candidate that only detects victory while counting a response would
// campaign forever. The self-vote must itself be evaluated against quorum.
func TestSingleNodeElectsItself(t *testing.T) {
	nw := newNetwork(t, 1)
	n := nw.node(1)

	if n.role != Follower || n.currentTerm != 0 || n.votedFor != None {
		t.Fatalf("fresh node: role=%v term=%d votedFor=%d, want Follower/0/None",
			n.role, n.currentTerm, n.votedFor)
	}

	nw.tick(electionTimeoutMin - 1)
	if n.role != Follower {
		t.Fatalf("role after %d ticks = %v, want Follower — election fired early",
			electionTimeoutMin-1, n.role)
	}

	nw.tick(1)
	if n.role != Leader {
		t.Fatalf("role = %v, want Leader — a single node is its own majority", n.role)
	}
	if n.currentTerm != 1 {
		t.Errorf("term = %d, want 1", n.currentTerm)
	}
	if n.votedFor != 1 || n.lead != 1 {
		t.Errorf("votedFor=%d lead=%d, want 1/1", n.votedFor, n.lead)
	}
	if len(n.msgs) != 0 {
		t.Errorf("queued %d messages with no peers", len(n.msgs))
	}
}

// Test 2 — three nodes elect exactly one leader; the rest follow in that term.
func TestThreeNodesElectOneLeader(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	nw.tick(electionTimeoutMin)

	leader := nw.assertSingleLeader()
	term := nw.node(leader).currentTerm

	for _, id := range nw.ids {
		n := nw.node(id)
		if id == leader {
			continue
		}
		if n.role != Follower {
			t.Errorf("node %d role = %v, want Follower", id, n.role)
		}
		if n.currentTerm != term {
			t.Errorf("node %d term = %d, want %d — followers adopt the leader's term",
				id, n.currentTerm, term)
		}
		if n.lead != leader {
			t.Errorf("node %d lead = %d, want %d", id, n.lead, leader)
		}
	}
}

// Test 3 — a leader that sees a higher term steps down.
//
// Checked for a REQUEST and for a RESPONSE. The response case is the one that
// matters: a partitioned leader learns it has been deposed from a reply to its
// own heartbeat, and an implementation that only inspects inbound requests
// leaves that leader serving reads forever.
func TestLeaderStepsDownOnHigherTerm(t *testing.T) {
	t.Run("via a request", func(t *testing.T) {
		nw := newNetwork(t, 1, 2, 3)
		nw.tick(electionTimeoutMin)
		leader := nw.assertSingleLeader()
		n := nw.node(leader)
		higher := n.currentTerm + 5

		_ = n.Step(Message{Type: MsgAppendEntries, From: 2, To: leader, Term: higher})

		if n.role != Follower {
			t.Errorf("role = %v, want Follower", n.role)
		}
		if n.currentTerm != higher {
			t.Errorf("term = %d, want %d", n.currentTerm, higher)
		}
		if n.lead != 2 {
			t.Errorf("lead = %d, want 2 — an AppendEntries proves the sender is leader", n.lead)
		}
		if n.votedFor != None {
			t.Errorf("votedFor = %d, want None — a new term means a fresh vote", n.votedFor)
		}
	})

	t.Run("via a response", func(t *testing.T) {
		nw := newNetwork(t, 1, 2, 3)
		nw.tick(electionTimeoutMin)
		leader := nw.assertSingleLeader()
		n := nw.node(leader)
		higher := n.currentTerm + 5

		_ = n.Step(Message{Type: MsgAppendEntriesResp, From: 3, To: leader, Term: higher})

		if n.role != Follower {
			t.Errorf("role = %v, want Follower — the term rule applies to responses", n.role)
		}
		if n.currentTerm != higher {
			t.Errorf("term = %d, want %d", n.currentTerm, higher)
		}
		if n.lead != None {
			t.Errorf("lead = %d, want None — a response does not identify a leader", n.lead)
		}
	})
}

// Test 4 — a split vote produces no leader in that term; a later term resolves.
func TestSplitVoteResolvesInALaterTerm(t *testing.T) {
	nw := newSyncedNetwork(t, 1, 2, 3)

	// Identical timeouts: all three campaign on the same tick, each votes for
	// itself, and nobody reaches 2 of 3.
	nw.tick(electionTimeoutMin)

	if ls := nw.leaders(); len(ls) != 0 {
		t.Fatalf("leaders after a split vote = %v, want none (%s)", ls, nw.describe())
	}
	for _, id := range nw.ids {
		n := nw.node(id)
		if n.role != Candidate {
			t.Errorf("node %d role = %v, want Candidate", id, n.role)
		}
		if n.currentTerm != 1 {
			t.Errorf("node %d term = %d, want 1", id, n.currentTerm)
		}
		if n.votedFor != id {
			t.Errorf("node %d votedFor = %d, want itself", id, n.votedFor)
		}
	}

	// A later term resolves it: one node times out first and campaigns alone.
	// Its higher term makes the other two step down, clearing their votes, so
	// they are free to grant. This is what randomised timeouts buy — without
	// the redraw the cluster would split identically, forever.
	nw.tickOne(1, electionTimeoutMin)
	nw.deliver()

	leader := nw.assertSingleLeader()
	if leader != 1 {
		t.Errorf("leader = %d, want 1", leader)
	}
	if got := nw.node(1).currentTerm; got != 2 {
		t.Errorf("winning term = %d, want 2", got)
	}
}

// Test 5 — heartbeats keep followers from starting elections.
//
// Run far past any election timeout. If heartbeats did not reset follower
// clocks, a follower would campaign and the term would climb.
func TestHeartbeatsPreventElections(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	nw.tick(electionTimeoutMin)

	leader := nw.assertSingleLeader()
	term := nw.node(leader).currentTerm

	nw.tick(20 * electionTimeoutMax)

	if got := nw.assertSingleLeader(); got != leader {
		t.Errorf("leader changed from %d to %d under steady heartbeats", leader, got)
	}
	for _, id := range nw.ids {
		if got := nw.node(id).currentTerm; got != term {
			t.Errorf("node %d term = %d, want %d — the term must not advance while "+
				"a leader is alive", id, got, term)
		}
	}
}

// Test 6 — a candidate steps down on AppendEntries at an equal term.
//
// Equal, not merely higher. An AppendEntries at our own term means someone else
// already collected a majority for it while we were waiting: there is exactly
// one leader per term, so we lost. Accepting only a higher term leaves a
// candidate campaigning against a leader it has already heard from.
func TestCandidateStepsDownOnAppendEntries(t *testing.T) {
	for _, tc := range []struct {
		name  string
		bump  Term
		wantT Term
	}{
		{"equal term", 0, 1},
		{"higher term", 3, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nw := newNetwork(t, 1, 2, 3)
			nw.isolate(2)
			nw.isolate(3)

			// Node 1 campaigns alone; its peers are cut off, so it stays a
			// candidate with only its own vote.
			nw.tickOne(1, electionTimeoutMin)
			n := nw.node(1)
			if n.role != Candidate {
				t.Fatalf("role = %v, want Candidate", n.role)
			}

			_ = n.Step(Message{
				Type: MsgAppendEntries,
				From: 2,
				To:   1,
				Term: n.currentTerm + tc.bump,
			})

			if n.role != Follower {
				t.Errorf("role = %v, want Follower", n.role)
			}
			if n.currentTerm != tc.wantT {
				t.Errorf("term = %d, want %d", n.currentTerm, tc.wantT)
			}
			if n.lead != 2 {
				t.Errorf("lead = %d, want 2", n.lead)
			}
		})
	}
}

// Test 7 — a node grants at most one vote per term.
//
// Also covers the idempotency clause: a repeat request from the SAME candidate
// must be granted again, because our reply may simply have been dropped and one
// lost packet must not cost an election.
func TestOneVotePerTerm(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	n := nw.node(1)

	vote := func(from NodeID, term Term) bool {
		t.Helper()
		before := len(n.msgs)
		_ = n.Step(Message{Type: MsgRequestVote, From: from, To: 1, Term: term})
		if len(n.msgs) != before+1 {
			t.Fatalf("no reply sent to a RequestVote from %d", from)
		}
		reply := n.msgs[len(n.msgs)-1]
		if reply.Type != MsgRequestVoteResp {
			t.Fatalf("reply type = %v, want RequestVoteResp", reply.Type)
		}
		return reply.VoteGranted
	}

	if !vote(2, 1) {
		t.Fatal("first vote in a term was refused")
	}
	if n.votedFor != 2 {
		t.Fatalf("votedFor = %d, want 2", n.votedFor)
	}

	if vote(3, 1) {
		t.Error("granted a second vote in the same term — this is how two " +
			"leaders appear in one term")
	}

	if !vote(2, 1) {
		t.Error("refused a repeat request from the candidate we already voted " +
			"for; a dropped reply would then cost the election")
	}

	// A new term clears the vote, so a different candidate can win it.
	if !vote(3, 2) {
		t.Error("refused the first vote of a new term")
	}
	if n.votedFor != 3 || n.currentTerm != 2 {
		t.Errorf("votedFor=%d term=%d, want 3/2", n.votedFor, n.currentTerm)
	}
}

// A candidate whose log is behind must not win: §5.4.1. Trivially satisfied in
// milestone 1 with empty logs, wired now so milestone 2 inherits it.
func TestVoteDeniedToStaleLog(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	n := nw.node(1)

	// Give node 1 a log the candidate cannot match.
	n.log.append(5, Entry{}, Entry{})
	n.currentTerm = 5

	_ = n.Step(Message{
		Type: MsgRequestVote, From: 2, To: 1, Term: 6,
		LastLogIndex: 1, LastLogTerm: 1, // shorter and older
	})

	reply := n.msgs[len(n.msgs)-1]
	if reply.VoteGranted {
		t.Error("granted a vote to a candidate with a stale log — a leader " +
			"missing committed entries would truncate them off the cluster")
	}
}

// The votes map must be REPLACED at each election, not accumulated.
//
// Found by mutation testing: deleting the reset in becomeCandidate broke no
// test, despite being a direct path to two leaders in one term.
//
// Five nodes, so quorum is 3. One grant per election is never enough — unless
// the grant from the previous election is still sitting in the map.
func TestVotesResetBetweenElections(t *testing.T) {
	n := NewNode(1, []NodeID{2, 3, 4, 5}, func(int) int { return 0 })

	for i := 0; i < electionTimeoutMin; i++ {
		n.Tick()
	}
	if n.role != Candidate || n.currentTerm != 1 {
		t.Fatalf("role=%v term=%d, want Candidate/1", n.role, n.currentTerm)
	}
	_ = n.Step(Message{Type: MsgRequestVoteResp, From: 2, To: 1, Term: 1, VoteGranted: true})
	if n.role == Leader {
		t.Fatal("2 votes of 5 is not a majority")
	}

	// Time out and campaign again. A DIFFERENT peer grants this time.
	for i := 0; i < electionTimeoutMin; i++ {
		n.Tick()
	}
	if n.currentTerm != 2 {
		t.Fatalf("term = %d, want 2", n.currentTerm)
	}
	_ = n.Step(Message{Type: MsgRequestVoteResp, From: 3, To: 1, Term: 2, VoteGranted: true})

	if n.role == Leader {
		t.Errorf("became leader with 2 grants of 5 — a grant from term 1 was "+
			"counted toward term 2 (votes=%v)", n.votes)
	}
}

// votedFor must survive a stepdown WITHIN a term and reset only when the term
// advances. Found by mutation testing: clearing it unconditionally broke no test.
func TestVotedForSurvivesSameTermStepdown(t *testing.T) {
	n := NewNode(1, []NodeID{2, 3}, func(int) int { return 0 })

	for i := 0; i < electionTimeoutMin; i++ {
		n.Tick()
	}
	if n.role != Candidate || n.votedFor != 1 {
		t.Fatalf("role=%v votedFor=%d, want Candidate/1", n.role, n.votedFor)
	}

	// A leader for this same term appears; we lost and step down.
	_ = n.Step(Message{Type: MsgAppendEntries, From: 2, To: 1, Term: 1})
	if n.role != Follower || n.currentTerm != 1 {
		t.Fatalf("role=%v term=%d, want Follower/1", n.role, n.currentTerm)
	}
	if n.votedFor != 1 {
		t.Fatalf("votedFor = %d, want 1 — the term did not change, so the vote "+
			"must stand", n.votedFor)
	}

	// Node 3 now asks for a vote in that same term. We already voted.
	_ = n.Step(Message{Type: MsgRequestVote, From: 3, To: 1, Term: 1})
	if reply := n.msgs[len(n.msgs)-1]; reply.VoteGranted {
		t.Error("granted a second vote in term 1 after stepping down — this is " +
			"exactly how two candidates each reach a majority")
	}
}

// A new leader must heartbeat immediately, not wait for the heartbeat timer.
// Found by mutation testing: removing the broadcast broke no test.
func TestLeaderHeartbeatsImmediatelyOnElection(t *testing.T) {
	n := NewNode(1, []NodeID{2, 3}, func(int) int { return 0 })

	for i := 0; i < electionTimeoutMin; i++ {
		n.Tick()
	}
	n.msgs = nil // discard the RequestVotes so we see only what election emits

	_ = n.Step(Message{Type: MsgRequestVoteResp, From: 2, To: 1, Term: 1, VoteGranted: true})
	if n.role != Leader {
		t.Fatalf("role = %v, want Leader", n.role)
	}

	heartbeats := 0
	for _, m := range n.msgs {
		if m.Type == MsgAppendEntries {
			heartbeats++
		}
	}
	if heartbeats != len(n.peers()) {
		t.Errorf("sent %d heartbeats on election, want %d (one per peer). Every "+
			"peer's election clock is already running; waiting a tick lets one "+
			"time out and depose the leader that just won", heartbeats, len(n.peers()))
	}
}
