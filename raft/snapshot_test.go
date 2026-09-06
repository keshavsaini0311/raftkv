package raft

import "testing"

// leaderWithLog builds a leader holding n entries, all applied.
func leaderWithLog(t *testing.T, entries int, peers ...NodeID) *Node {
	t.Helper()
	nd := NewNode(1, peers, func(int) int { return 0 })
	nd.currentTerm = 5
	nd.role = Leader
	nd.lead = 1
	for i := 0; i < entries; i++ {
		nd.log.append(5, Entry{Data: []byte{byte(i)}})
	}
	nd.log.stable = nd.log.lastIndex()
	nd.log.committed = nd.log.lastIndex()
	nd.log.applied = nd.log.lastIndex()
	nd.nextIndex = make(map[NodeID]Index, len(peers))
	nd.matchIndex = map[NodeID]Index{1: nd.log.lastIndex()}
	nd.ackedReadSeq = make(map[NodeID]uint64, len(peers))
	for _, p := range peers {
		nd.nextIndex[p] = nd.log.lastIndex() + 1
	}
	return nd
}

func TestCreateSnapshotCompactsTheLog(t *testing.T) {
	n := leaderWithLog(t, 10, 2, 3)

	snap, err := n.CreateSnapshot(7, []byte(`{"state":"at 7"}`))
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if snap.Metadata.Index != 7 {
		t.Errorf("snapshot index = %d, want 7", snap.Metadata.Index)
	}
	if got := n.FirstIndex(); got != 8 {
		t.Errorf("firstIndex = %d, want 8 — entries 1..7 should be compacted", got)
	}
	if got := n.LastIndex(); got != 10 {
		t.Errorf("lastIndex = %d, want 10 — entries past the snapshot must survive", got)
	}

	// The boundary must remain queryable: the consistency check for entry 8
	// asks for the term at 7, which now lives only in the sentinel.
	if term, ok := n.log.term(7); !ok || term != 5 {
		t.Errorf("term(7) = %d, %v; the compaction boundary must stay addressable", term, ok)
	}
	if _, ok := n.log.term(6); ok {
		t.Error("term(6) is still available; it should have been compacted away")
	}

	// Membership travels with the snapshot: a restoring node that adopted only
	// the data would rejoin with a stale view of the cluster.
	if len(snap.Metadata.Voters) != 3 {
		t.Errorf("snapshot voters = %v, want all 3 nodes", snap.Metadata.Voters)
	}
}

// A snapshot may never cover entries the state machine has not consumed: it
// would describe a state that never existed.
func TestCreateSnapshotRejectsUnappliedIndex(t *testing.T) {
	n := leaderWithLog(t, 10, 2, 3)
	n.log.applied = 5

	if _, err := n.CreateSnapshot(8, nil); err != ErrSnapshotAhead {
		t.Errorf("err = %v, want ErrSnapshotAhead", err)
	}
	if _, err := n.CreateSnapshot(5, nil); err != nil {
		t.Errorf("snapshotting exactly the applied index failed: %v", err)
	}
}

// Once entries are compacted away, incremental repair is impossible and the
// leader must fall back to shipping the whole snapshot.
func TestLeaderSendsSnapshotWhenFollowerIsTooFarBehind(t *testing.T) {
	n := leaderWithLog(t, 20, 2, 3)
	if _, err := n.CreateSnapshot(15, []byte("state")); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	// Node 2 needs entry 3, which no longer exists.
	n.nextIndex[2] = 3
	n.msgs = nil
	n.sendAppend(2)

	if len(n.msgs) != 1 {
		t.Fatalf("sent %d messages, want 1", len(n.msgs))
	}
	if got := n.msgs[0].Type; got != MsgInstallSnapshot {
		t.Errorf("message type = %v, want InstallSnapshot — the entries it needs "+
			"are gone, so AppendEntries can never succeed", got)
	}
	if n.msgs[0].Snapshot == nil || n.msgs[0].Snapshot.Metadata.Index != 15 {
		t.Errorf("snapshot = %+v, want index 15", n.msgs[0].Snapshot)
	}
}

func TestFollowerInstallsSnapshot(t *testing.T) {
	f := NewNode(2, []NodeID{1, 3}, func(int) int { return 0 })
	f.currentTerm = 5
	f.log.append(1, Entry{}, Entry{}) // two stale entries from an old term

	snap := &Snapshot{
		Metadata: SnapshotMetadata{Index: 12, Term: 5, Voters: []NodeID{1, 2, 3}},
		Data:     []byte(`{"state":"snapshot"}`),
	}
	_ = f.Step(Message{Type: MsgInstallSnapshot, From: 1, To: 2, Term: 5, Snapshot: snap})

	if got := f.LastIndex(); got != 12 {
		t.Errorf("lastIndex = %d, want 12", got)
	}
	if got := f.log.committed; got != 12 {
		t.Errorf("committed = %d, want 12 — a snapshot is committed by construction", got)
	}
	if f.role != Follower || f.lead != 1 {
		t.Errorf("role=%v lead=%d, want Follower/1", f.role, f.lead)
	}

	// The driver must be told to persist it and hand it to the state machine.
	rd := f.Ready()
	if rd.Snapshot == nil || rd.Snapshot.Metadata.Index != 12 {
		t.Fatalf("Ready.Snapshot = %+v, want the snapshot surfaced to the driver", rd.Snapshot)
	}
	// Read the reply from the Ready, before Advance drains the queue.
	var reply Message
	for _, m := range rd.Messages {
		if m.Type == MsgInstallSnapshotResp {
			reply = m
		}
	}
	if reply.Type != MsgInstallSnapshotResp || reply.MatchIndex != 12 {
		t.Errorf("reply = %+v, want InstallSnapshotResp with MatchIndex 12", reply)
	}

	f.Advance(rd)
	if rd2 := f.Ready(); rd2.Snapshot != nil {
		t.Error("the snapshot was surfaced twice; Advance should have cleared it")
	}
}

// A stale or duplicated InstallSnapshot must not roll the log backwards.
// Commitment is permanent; un-committing is the one thing that cannot happen.
func TestStaleSnapshotIsIgnored(t *testing.T) {
	f := NewNode(2, []NodeID{1, 3}, func(int) int { return 0 })
	f.currentTerm = 5
	for i := 0; i < 20; i++ {
		f.log.append(5, Entry{})
	}
	f.log.commitTo(20)

	old := &Snapshot{Metadata: SnapshotMetadata{Index: 8, Term: 5}, Data: []byte("old")}
	_ = f.Step(Message{Type: MsgInstallSnapshot, From: 1, To: 2, Term: 5, Snapshot: old})

	if got := f.log.committed; got != 20 {
		t.Errorf("committed = %d, want 20 — a stale snapshot un-committed entries", got)
	}
	if got := f.LastIndex(); got != 20 {
		t.Errorf("lastIndex = %d, want 20", got)
	}
	if rd := f.Ready(); rd.Snapshot != nil {
		t.Error("a stale snapshot was surfaced to the driver")
	}
}

// If the follower already holds a matching entry at the snapshot boundary, its
// log agrees with the leader up to there and only needs compacting — throwing
// away entries it could keep would force a needless re-transfer.
func TestSnapshotAtAMatchingBoundaryOnlyCompacts(t *testing.T) {
	f := NewNode(2, []NodeID{1, 3}, func(int) int { return 0 })
	f.currentTerm = 5
	for i := 0; i < 20; i++ {
		f.log.append(5, Entry{})
	}

	snap := &Snapshot{Metadata: SnapshotMetadata{Index: 10, Term: 5}, Data: []byte("s")}
	_ = f.Step(Message{Type: MsgInstallSnapshot, From: 1, To: 2, Term: 5, Snapshot: snap})

	if got := f.LastIndex(); got != 20 {
		t.Errorf("lastIndex = %d, want 20 — entries 11..20 agreed with the leader "+
			"and should have been kept", got)
	}
	if got := f.FirstIndex(); got != 11 {
		t.Errorf("firstIndex = %d, want 11", got)
	}
}

// A snapshot carries cluster membership, and a restoring node must adopt it.
func TestSnapshotRestoresMembership(t *testing.T) {
	f := NewNode(2, []NodeID{1, 3}, func(int) int { return 0 })
	f.currentTerm = 5

	snap := &Snapshot{
		Metadata: SnapshotMetadata{Index: 5, Term: 5, Voters: []NodeID{1, 2, 3, 4, 5}},
		Data:     []byte("s"),
	}
	_ = f.Step(Message{Type: MsgInstallSnapshot, From: 1, To: 2, Term: 5, Snapshot: snap})

	if len(f.peers()) != 4 {
		t.Fatalf("peers = %v, want 4 (the cluster grew to 5 while we were away)", f.peers())
	}
	for _, p := range f.peers() {
		if p == 2 {
			t.Error("the node listed itself as its own peer")
		}
	}
	if got := f.quorum(); got != 3 {
		t.Errorf("quorum = %d, want 3 for a 5-node cluster — a node using its old "+
			"membership would compute the wrong majority", got)
	}
}

// Replication must keep working after compaction: the entry immediately after
// the boundary needs a consistency check against the sentinel.
func TestReplicationContinuesAcrossACompactionBoundary(t *testing.T) {
	leader := leaderWithLog(t, 10, 2)
	if _, err := leader.CreateSnapshot(10, []byte("s")); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	follower := NewNode(2, []NodeID{1}, func(int) int { return 0 })
	follower.currentTerm = 5

	// The follower is empty, so it gets the snapshot first.
	leader.nextIndex[2] = 1
	leader.msgs = nil
	leader.sendAppend(2)
	for _, m := range leader.msgs {
		_ = follower.Step(m)
	}
	follower.Advance(follower.Ready())

	// New entries after the snapshot must replicate normally.
	leader.msgs = nil
	if _, err := leader.Propose([]byte("after")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	for i := 0; i < 10; i++ {
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

	if follower.LastIndex() != leader.LastIndex() {
		t.Errorf("follower lastIndex = %d, leader = %d — replication did not resume "+
			"after the snapshot", follower.LastIndex(), leader.LastIndex())
	}
}
