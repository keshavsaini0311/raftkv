package raft

import "testing"

// electLeader ticks until a leader emerges and returns it.
func electLeader(t *testing.T, nw *network) *Node {
	t.Helper()
	nw.tick(electionTimeoutMax)
	return nw.node(nw.assertSingleLeader())
}

// dataEntries filters out the empty no-op a leader appends on election, so
// tests can assert on the commands a client actually proposed.
func dataEntries(ents []Entry) []Entry {
	var out []Entry
	for _, e := range ents {
		if len(e.Data) > 0 {
			out = append(out, e)
		}
	}
	return out
}

// Test 8 — a single-node cluster commits and applies immediately.
//
// There is nobody to replicate to, so the leader is its own majority and the
// entry must commit on the tick it is durable. A cluster of one is where
// "wait for a quorum" degenerates, and an implementation that only advances
// commitIndex when a response arrives hangs here forever.
func TestSingleNodeCommitsImmediately(t *testing.T) {
	nw := newNetwork(t, 1)
	leader := electLeader(t, nw)

	idx, err := leader.Propose([]byte("x=1"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	nw.deliver()

	if leader.log.committed < idx {
		t.Errorf("committed = %d, want >= %d", leader.log.committed, idx)
	}
	got := dataEntries(nw.applied[1])
	if len(got) != 1 || string(got[0].Data) != "x=1" {
		t.Errorf("applied = %v, want one entry x=1", got)
	}
}

// Test 9 — an entry proposed to the leader reaches a majority and commits.
func TestEntryReplicatesAndCommits(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := electLeader(t, nw)

	idx, err := leader.Propose([]byte("hello"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	nw.deliver()

	if leader.log.committed < idx {
		t.Fatalf("leader committed = %d, want >= %d", leader.log.committed, idx)
	}
	for _, id := range nw.ids {
		n := nw.node(id)
		if got, ok := n.log.term(idx); !ok || got != leader.currentTerm {
			t.Errorf("node %d missing entry %d at the leader's term", id, idx)
		}
	}
}

// Test 10 — a follower with a divergent tail is truncated and repaired.
//
// The follower holds entries 4 and 5 from a term that never committed, left
// behind by a leader that was partitioned before reaching a majority. The new
// leader's entry 4 conflicts, so rule 3 deletes it and everything after.
//
// Note what must NOT happen: entries 1..3 agree, and a correct implementation
// leaves them alone. Truncating from PrevLogIndex unconditionally would discard
// entries the follower had legitimately accepted.
func TestDivergentTailIsTruncated(t *testing.T) {
	f := NewNode(2, []NodeID{1, 3}, func(int) int { return 0 })
	f.log.append(1, Entry{}, Entry{}, Entry{}) // 1,2,3 @ term 1
	f.log.append(2, Entry{}, Entry{})          // 4,5 @ term 2 — never committed
	f.currentTerm = 3

	_ = f.Step(Message{
		Type: MsgAppendEntries, From: 1, To: 2, Term: 3,
		PrevLogIndex: 3, PrevLogTerm: 1,
		Entries: []Entry{{Index: 4, Term: 3, Data: []byte("real")}},
	})

	reply := f.msgs[len(f.msgs)-1]
	if !reply.Success {
		t.Fatalf("rejected a matching PrevLogIndex/PrevLogTerm: %+v", reply)
	}
	if f.log.lastIndex() != 4 {
		t.Errorf("lastIndex = %d, want 4 — entry 5 should have been truncated", f.log.lastIndex())
	}
	if term, _ := f.log.term(4); term != 3 {
		t.Errorf("term at 4 = %d, want 3 — the conflicting entry was not replaced", term)
	}
	for i := Index(1); i <= 3; i++ {
		if term, _ := f.log.term(i); term != 1 {
			t.Errorf("term at %d = %d, want 1 — agreeing entries must survive", i, term)
		}
	}
}

// A stale or duplicated AppendEntries must not truncate anything.
//
// Rule 3 applies only at a GENUINE conflict. The naive reading — "truncate at
// PrevLogIndex, then append" — silently discards correct entries whenever a
// retransmitted message arrives out of order, which under a lossy network is
// constantly.
func TestDuplicateAppendDoesNotTruncate(t *testing.T) {
	f := NewNode(2, []NodeID{1, 3}, func(int) int { return 0 })
	f.log.append(1, Entry{}, Entry{}, Entry{})
	f.currentTerm = 1

	// Re-deliver entries the follower already has, as a duplicate would.
	_ = f.Step(Message{
		Type: MsgAppendEntries, From: 1, To: 2, Term: 1,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []Entry{{Index: 2, Term: 1}},
	})

	if f.log.lastIndex() != 3 {
		t.Errorf("lastIndex = %d, want 3 — a duplicate append truncated entry 3",
			f.log.lastIndex())
	}
}

// Test 11 — nextIndex backtracking converges on a follower far behind.
//
// The conflict-term hint is what makes this cheap: the follower reports where
// its log actually diverges, so the leader skips an entire term per round trip.
// Decrementing nextIndex one entry at a time would take 200 round trips here.
func TestNextIndexBacktrackingConverges(t *testing.T) {
	leader := NewNode(1, []NodeID{2}, func(int) int { return 0 })
	follower := NewNode(2, []NodeID{1}, func(int) int { return 0 })

	// Follower holds 200 entries across four terms, all divergent from index 1.
	for term := Term(1); term <= 4; term++ {
		for i := 0; i < 50; i++ {
			follower.log.append(term, Entry{})
		}
	}
	follower.currentTerm = 9

	// Leader holds 200 entries of term 9 and believes the follower matches.
	for i := 0; i < 200; i++ {
		leader.log.append(9, Entry{})
	}
	leader.currentTerm = 9
	leader.role = Leader
	leader.lead = 1
	leader.log.stable = leader.log.lastIndex()
	leader.nextIndex = map[NodeID]Index{2: leader.log.lastIndex() + 1}
	leader.matchIndex = map[NodeID]Index{1: leader.log.lastIndex(), 2: 0}

	leader.sendAppend(2)

	roundTrips := 0
	for roundTrips = 0; roundTrips < 50; roundTrips++ {
		msgs := leader.msgs
		leader.msgs = nil
		if len(msgs) == 0 {
			break
		}
		for _, m := range msgs {
			_ = follower.Step(m)
		}
		replies := follower.msgs
		follower.msgs = nil
		for _, m := range replies {
			_ = leader.Step(m)
		}
	}

	if follower.log.lastIndex() != leader.log.lastIndex() {
		t.Fatalf("follower lastIndex = %d, leader = %d — repair did not converge",
			follower.log.lastIndex(), leader.log.lastIndex())
	}
	for i := Index(1); i <= leader.log.lastIndex(); i++ {
		lt, _ := leader.log.term(i)
		ft, _ := follower.log.term(i)
		if lt != ft {
			t.Fatalf("logs disagree at index %d: leader term %d, follower term %d", i, lt, ft)
		}
	}
	// Four divergent terms should cost a handful of round trips, not 200.
	if roundTrips > 10 {
		t.Errorf("took %d round trips to repair 200 entries across 4 terms; the "+
			"conflict-term hint should need roughly one per term", roundTrips)
	}
	t.Logf("repaired 200 divergent entries in %d round trips", roundTrips)
}

// Test 12 — an entry replicated to a minority does not commit.
func TestMinorityReplicationDoesNotCommit(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3, 4, 5)
	leader := electLeader(t, nw)

	// Cut off three of the four followers: the leader plus one is 2 of 5.
	for _, id := range nw.ids {
		if id != leader.id && len(nw.isolated) < 3 {
			nw.isolate(id)
		}
	}

	idx, err := leader.Propose([]byte("minority"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	nw.deliver()

	if leader.log.committed >= idx {
		t.Errorf("committed = %d with only a minority replicated; want < %d",
			leader.log.committed, idx)
	}
	if got := dataEntries(nw.applied[leader.id]); len(got) != 0 {
		t.Errorf("applied %v without a quorum — an uncommitted entry reached the "+
			"state machine", got)
	}
}

// Test 13 — a previous-term entry does not commit on replica count alone.
//
// This is Figure 8, the safety violation the paper devotes a full figure to.
// Without the log[N].Term == currentTerm clause, a leader commits an inherited
// entry that a later leader can still overwrite, so a client is told its write
// is durable and then loses it.
func TestPreviousTermEntryNeedsCurrentTermToCommit(t *testing.T) {
	n := NewNode(1, []NodeID{2, 3}, func(int) int { return 0 })

	// One entry inherited from term 1; we are leader of term 2.
	n.log.append(1, Entry{Data: []byte("old")})
	n.currentTerm = 2
	n.role = Leader
	n.lead = 1
	n.log.stable = 1
	n.nextIndex = map[NodeID]Index{2: 2, 3: 2}
	n.matchIndex = map[NodeID]Index{1: 1, 2: 0, 3: 0}

	// Both followers acknowledge it: a clear majority holds the entry.
	n.matchIndex[2] = 1
	n.matchIndex[3] = 1
	n.maybeCommit()

	if n.log.committed != 0 {
		t.Errorf("committed = %d, want 0 — replica count alone must not commit a "+
			"previous-term entry (Figure 8)", n.log.committed)
	}

	// A current-term entry replicated to a majority commits, and carries the
	// earlier one with it.
	n.log.append(2, Entry{Data: []byte("new")})
	n.log.stable = 2
	n.matchIndex[1] = 2
	n.matchIndex[2] = 2
	n.maybeCommit()

	if n.log.committed != 2 {
		t.Errorf("committed = %d, want 2 — a current-term entry commits and the "+
			"prior-term entry commits indirectly with it", n.log.committed)
	}
}

// Test 14 — every node applies the same entries in the same order.
//
// State Machine Safety: if any node has applied an entry at a given index, no
// other node ever applies a different entry at that index.
func TestAllNodesApplyTheSameOrder(t *testing.T) {
	nw := newNetwork(t, 1, 2, 3)
	leader := electLeader(t, nw)

	for _, cmd := range []string{"a=1", "b=2", "c=3", "d=4", "e=5"} {
		if _, err := leader.Propose([]byte(cmd)); err != nil {
			t.Fatalf("Propose(%s): %v", cmd, err)
		}
		nw.deliver()
	}
	// A few more ticks so followers learn the final commit index.
	nw.tick(3)

	want := dataEntries(nw.applied[leader.id])
	if len(want) != 5 {
		t.Fatalf("leader applied %d entries, want 5", len(want))
	}

	for _, id := range nw.ids {
		got := dataEntries(nw.applied[id])
		if len(got) != len(want) {
			t.Errorf("node %d applied %d entries, want %d", id, len(got), len(want))
			continue
		}
		for i := range want {
			if got[i].Index != want[i].Index || string(got[i].Data) != string(want[i].Data) {
				t.Errorf("node %d applied[%d] = (idx %d, %q), want (idx %d, %q)",
					id, i, got[i].Index, got[i].Data, want[i].Index, want[i].Data)
			}
		}
	}
}

// A follower must never commit past its own log, even if the leader has.
//
// Rule 5 is min(LeaderCommit, index of last new entry). Using LeaderCommit
// alone would have a lagging follower mark entries committed that it does not
// have, and then apply whatever happens to land at those indices later.
func TestFollowerCommitIsClampedToItsOwnLog(t *testing.T) {
	f := NewNode(2, []NodeID{1, 3}, func(int) int { return 0 })
	f.currentTerm = 1

	_ = f.Step(Message{
		Type: MsgAppendEntries, From: 1, To: 2, Term: 1,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      []Entry{{Index: 1, Term: 1}},
		LeaderCommit: 99, // the leader is far ahead
	})

	if f.log.committed != 1 {
		t.Errorf("committed = %d, want 1 — a follower cannot commit entries it "+
			"does not have", f.log.committed)
	}
}

// commitTo already clamps to lastIndex, so the earlier clamp test passed even
// with min() removed. This is the case that actually needs it: the follower
// holds entries BEYOND what this AppendEntries carried, and those extra
// entries are not confirmed by this leader.
//
// Rule 5 is min(LeaderCommit, index of last NEW entry) — not min(LeaderCommit,
// lastIndex). Committing to lastIndex would mark uncommitted, possibly
// divergent, tail entries as durable and hand them to the state machine.
func TestFollowerDoesNotCommitUnconfirmedTail(t *testing.T) {
	f := NewNode(2, []NodeID{1, 3}, func(int) int { return 0 })
	f.log.append(1, Entry{}, Entry{}, Entry{}) // holds 1,2,3
	f.currentTerm = 1

	// The leader re-sends only entry 1, but reports a high commit index.
	_ = f.Step(Message{
		Type: MsgAppendEntries, From: 1, To: 2, Term: 1,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      []Entry{{Index: 1, Term: 1}},
		LeaderCommit: 3,
	})

	if f.log.committed != 1 {
		t.Errorf("committed = %d, want 1 — entries 2 and 3 were not part of this "+
			"AppendEntries and are not confirmed by this leader", f.log.committed)
	}
}

// matchIndex must be monotonic per peer.
//
// A delayed response from an earlier, shorter AppendEntries must not drag it
// backwards. This is a liveness and efficiency property rather than a safety
// one — commitTo never lowers the commit index, so a regression delays commits
// and causes redundant re-sends rather than corrupting anything. Asserted
// anyway, because "it happens to be safe today" is how invariants get deleted.
func TestMatchIndexIsMonotonic(t *testing.T) {
	n := NewNode(1, []NodeID{2, 3}, func(int) int { return 0 })
	n.currentTerm = 1
	n.role = Leader
	n.lead = 1
	n.log.append(1, Entry{}, Entry{}, Entry{}, Entry{}, Entry{})
	n.log.stable = 5
	n.nextIndex = map[NodeID]Index{2: 6, 3: 6}
	n.matchIndex = map[NodeID]Index{1: 5, 2: 5, 3: 0}

	// A stale success from an earlier, shorter append arrives late.
	_ = n.Step(Message{
		Type: MsgAppendEntriesResp, From: 2, To: 1, Term: 1,
		Success: true, MatchIndex: 2,
	})

	if n.matchIndex[2] != 5 {
		t.Errorf("matchIndex[2] = %d, want 5 — a delayed response from a shorter "+
			"append moved progress backwards", n.matchIndex[2])
	}
}
