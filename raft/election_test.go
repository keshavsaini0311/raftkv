// Milestone 1 election tests, delivered one at a time.
//
// These are INTERNAL tests (package raft, not raft_test) so they can assert on
// unexported state directly. For a state machine that is the right trade: the
// alternative is exporting a dozen getters that exist only for tests, which
// widens the public API for no caller's benefit. import_test.go stays external
// because it genuinely only needs the file system.
package raft

import "testing"

// newTestNode builds a node whose randomness is fixed, so electionTimeout is
// always exactly electionTimeoutMin. Real determinism comes from a seeded
// source in the driver; here we want no variance at all.
func newTestNode(t *testing.T, id NodeID, peers ...NodeID) *Node {
	t.Helper()
	return NewNode(id, peers, func(int) int { return 0 })
}

// Test 1 — "Single node elects itself immediately on timeout."
//
// A one-node cluster is the degenerate case, and it is a good first test
// because it exercises the whole election path with no network at all:
// timeout fires -> become candidate -> vote for self -> that is already a
// majority of one -> leader.
//
// The subtlety worth slowing down for: there are no peers, so no RequestVote
// response will ever arrive. If winning is only ever detected when counting a
// response, this node waits forever. A candidate must check whether its own
// vote already constitutes a quorum.
func TestSingleNodeElectsItselfOnTimeout(t *testing.T) {
	n := newTestNode(t, 1) // no peers — a cluster of one

	// A fresh node is a follower at term 0 that has voted for nobody.
	// The zero value is the correct starting state.
	if n.role != Follower {
		t.Errorf("initial role = %v, want %v", n.role, Follower)
	}
	if n.currentTerm != 0 {
		t.Errorf("initial term = %d, want 0", n.currentTerm)
	}
	if n.votedFor != None {
		t.Errorf("initial votedFor = %d, want None", n.votedFor)
	}

	// randIntn is fixed at 0, so electionTimeout == electionTimeoutMin.
	if n.electionTimeout != electionTimeoutMin {
		t.Fatalf("electionTimeout = %d, want %d (with randIntn returning 0)",
			n.electionTimeout, electionTimeoutMin)
	}

	// One tick short of the timeout: nothing has happened yet.
	for i := 0; i < electionTimeoutMin-1; i++ {
		n.Tick()
	}
	if n.role != Follower {
		t.Fatalf("role after %d ticks = %v, want %v — the election fired early",
			electionTimeoutMin-1, n.role, Follower)
	}
	if n.currentTerm != 0 {
		t.Fatalf("term after %d ticks = %d, want 0", electionTimeoutMin-1, n.currentTerm)
	}

	// The tick that reaches the timeout. Figure 2 fires on
	// electionElapsed >= electionTimeout, so this is the one.
	n.Tick()

	if n.role != Leader {
		t.Fatalf("role after %d ticks = %v, want %v — a single node is its own "+
			"majority and should not wait for responses that will never come",
			electionTimeoutMin, n.role, Leader)
	}
	if n.currentTerm != 1 {
		t.Errorf("term = %d, want 1 — becoming a candidate increments the term",
			n.currentTerm)
	}
	if n.votedFor != 1 {
		t.Errorf("votedFor = %d, want 1 — a candidate votes for itself", n.votedFor)
	}
	if n.lead != 1 {
		t.Errorf("lead = %d, want 1 — a leader knows it is the leader", n.lead)
	}

	// No peers means nothing to send. Heartbeats to an empty peer set are
	// zero messages, not one addressed to nobody.
	if len(n.msgs) != 0 {
		t.Errorf("queued %d message(s) with no peers: %+v", len(n.msgs), n.msgs)
	}
}
