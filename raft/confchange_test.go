package raft

import "testing"

// cluster is a tiny hand-driven mesh for membership tests, where the harness in
// harness_test.go cannot help: it fixes the peer set at construction, and the
// whole point here is that the peer set changes.
type mesh struct {
	t     *testing.T
	nodes map[NodeID]*Node
}

func newMesh(t *testing.T, ids ...NodeID) *mesh {
	t.Helper()
	m := &mesh{t: t, nodes: make(map[NodeID]*Node, len(ids))}
	for i, id := range ids {
		var peers []NodeID
		for _, o := range ids {
			if o != id {
				peers = append(peers, o)
			}
		}
		// Staggered timeouts, so the lowest id campaigns first. Identical
		// timeouts would split the vote every round, forever.
		offset := i
		m.nodes[id] = NewNode(id, peers, func(int) int { return offset })
	}
	return m
}

// add introduces a node that starts knowing only itself; it learns the real
// membership from the configuration entry the leader replicates to it.
func (m *mesh) add(id NodeID) {
	m.nodes[id] = NewNode(id, nil, func(int) int { return 0 })
}

// pump delivers messages until the mesh falls silent.
func (m *mesh) pump() {
	m.t.Helper()
	for round := 0; round < 200; round++ {
		var inFlight []Message
		for _, id := range sortedIDs(m.nodes) {
			n := m.nodes[id]
			rd := n.Ready()
			inFlight = append(inFlight, rd.Messages...)
			n.Advance(rd)
		}
		if len(inFlight) == 0 {
			return
		}
		for _, msg := range inFlight {
			if dst, ok := m.nodes[msg.To]; ok {
				_ = dst.Step(msg)
			}
		}
	}
	m.t.Fatal("mesh did not settle")
}

func (m *mesh) tick(n int) {
	m.t.Helper()
	for i := 0; i < n; i++ {
		for _, id := range sortedIDs(m.nodes) {
			m.nodes[id].Tick()
		}
		m.pump()
	}
}

func (m *mesh) leader() *Node {
	for _, id := range sortedIDs(m.nodes) {
		if m.nodes[id].role == Leader {
			return m.nodes[id]
		}
	}
	return nil
}

func sortedIDs(nodes map[NodeID]*Node) []NodeID {
	out := make([]NodeID, 0, len(nodes))
	for id := range nodes {
		out = append(out, id)
	}
	sortNodeIDs(out)
	return out
}

// Adding a node goes through a joint configuration and completes on its own.
func TestAddNodeCompletesThroughJointConsensus(t *testing.T) {
	m := newMesh(t, 1, 2, 3)
	m.tick(electionTimeoutMax)

	leader := m.leader()
	if leader == nil {
		t.Fatal("no leader")
	}
	if leader.IsJoint() {
		t.Fatal("a fresh cluster must not be joint")
	}

	m.add(4)
	if _, err := leader.ProposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 4}); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}

	// The joint configuration is adopted on APPEND, not on commit: the entry
	// cannot commit without a quorum of the configuration it defines.
	if !leader.IsJoint() {
		t.Error("the leader did not enter the joint configuration on append")
	}

	m.pump()
	m.tick(5)

	if leader.IsJoint() {
		t.Error("still joint after the change committed; C_new was never appended")
	}
	if got := leader.Voters(); len(got) != 4 || !containsID(got, 4) {
		t.Errorf("voters = %v, want all four nodes", got)
	}

	// Every node must converge on the same membership. If they disagreed about
	// who votes, two of them could compute different majorities.
	for _, id := range sortedIDs(m.nodes) {
		n := m.nodes[id]
		if got := n.Voters(); len(got) != 4 {
			t.Errorf("node %d voters = %v, want 4", id, got)
		}
		if n.IsJoint() {
			t.Errorf("node %d is still joint", id)
		}
	}
}

func TestRemoveNode(t *testing.T) {
	m := newMesh(t, 1, 2, 3, 4, 5)
	m.tick(electionTimeoutMax)

	leader := m.leader()
	if leader == nil {
		t.Fatal("no leader")
	}
	var victim NodeID = 5
	if leader.id == victim {
		victim = 4
	}

	if _, err := leader.ProposeConfChange(ConfChange{Type: ConfChangeRemoveNode, NodeID: victim}); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}
	m.pump()
	m.tick(5)

	if leader.IsJoint() {
		t.Error("still joint after the removal committed")
	}
	if got := leader.Voters(); len(got) != 4 || containsID(got, victim) {
		t.Errorf("voters = %v, want 4 without node %d", got, victim)
	}
	if got := leader.quorum(); got != 3 {
		t.Errorf("quorum = %d, want 3 for the new 4-node cluster", got)
	}
}

// While joint, a decision needs a majority of BOTH configurations. Satisfying
// only one is exactly the hole that makes naive membership changes unsafe:
// C_old and C_new could independently elect different leaders.
func TestJointQuorumRequiresBothConfigurations(t *testing.T) {
	// C_old = {1,2,3}, C_new = {1,2,3,4,5}.
	c := config{voters: []NodeID{1, 2, 3, 4, 5}, outgoing: []NodeID{1, 2, 3}}

	// 3 of 5 in C_new, but only 1 of 3 in C_old.
	granted := map[NodeID]bool{1: true, 4: true, 5: true}
	if c.hasQuorum(func(id NodeID) bool { return granted[id] }) {
		t.Error("granted a quorum with a majority of C_new but not C_old — C_old " +
			"could elect a different leader from {2,3}+one more")
	}

	// 2 of 3 in C_old, 3 of 5 in C_new: both satisfied.
	granted = map[NodeID]bool{1: true, 2: true, 4: true}
	if !c.hasQuorum(func(id NodeID) bool { return granted[id] }) {
		t.Error("refused a quorum that satisfies both configurations")
	}

	// Unanimous C_old is still only 3 of 5 in C_new — which IS a majority.
	granted = map[NodeID]bool{1: true, 2: true, 3: true}
	if !c.hasQuorum(func(id NodeID) bool { return granted[id] }) {
		t.Error("refused a quorum that is 3 of 3 in C_old and 3 of 5 in C_new")
	}
}

// The commit index during a joint transition is bounded by the SLOWER of the
// two configurations.
func TestJointCommitIndexTakesTheMinimum(t *testing.T) {
	c := config{voters: []NodeID{1, 2, 3, 4, 5}, outgoing: []NodeID{1, 2, 3}}
	match := map[NodeID]Index{1: 10, 2: 10, 3: 4, 4: 10, 5: 10}

	got := c.committedIndex(func(id NodeID) Index { return match[id] })
	if got != 10 {
		// C_new majority (3 of 5) reaches 10; C_old majority (2 of 3) also
		// reaches 10 via nodes 1 and 2.
		t.Errorf("committedIndex = %d, want 10", got)
	}

	match[2] = 4 // now C_old's majority only reaches 4
	got = c.committedIndex(func(id NodeID) Index { return match[id] })
	if got != 4 {
		t.Errorf("committedIndex = %d, want 4 — C_old lags and bounds the commit", got)
	}
}

func TestOnlyOneChangeInFlight(t *testing.T) {
	m := newMesh(t, 1, 2, 3)
	m.tick(electionTimeoutMax)
	leader := m.leader()
	if leader == nil {
		t.Fatal("no leader")
	}

	m.add(4)
	if _, err := leader.ProposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 4}); err != nil {
		t.Fatalf("first change: %v", err)
	}
	if _, err := leader.ProposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 5}); err != ErrConfChangeInFlight {
		t.Errorf("second change err = %v, want ErrConfChangeInFlight — overlapping "+
			"transitions produce a configuration nobody can reason about", err)
	}
}

func TestConfChangeValidation(t *testing.T) {
	m := newMesh(t, 1, 2, 3)
	m.tick(electionTimeoutMax)
	leader := m.leader()
	if leader == nil {
		t.Fatal("no leader")
	}

	if _, err := leader.ProposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 2}); err != ErrNodeExists {
		t.Errorf("adding an existing node: err = %v, want ErrNodeExists", err)
	}
	if _, err := leader.ProposeConfChange(ConfChange{Type: ConfChangeRemoveNode, NodeID: 9}); err != ErrNoSuchNode {
		t.Errorf("removing a non-member: err = %v, want ErrNoSuchNode", err)
	}

	follower := m.nodes[1]
	if follower.role == Leader {
		follower = m.nodes[2]
	}
	if _, err := follower.ProposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 4}); err != ErrNotLeader {
		t.Errorf("follower proposing: err = %v, want ErrNotLeader", err)
	}
}

// If truncation removes a configuration entry, membership must revert to
// whatever configuration remains in the log. A node that kept applying a
// configuration whose entry was thrown away would compute a majority of a
// cluster that no longer exists.
func TestTruncationRevertsTheConfiguration(t *testing.T) {
	n := NewNode(1, []NodeID{2, 3}, func(int) int { return 0 })
	n.currentTerm = 5
	n.role = Leader
	n.lead = 1
	n.nextIndex = map[NodeID]Index{2: 1, 3: 1}
	n.matchIndex = map[NodeID]Index{1: 0, 2: 0, 3: 0}

	if _, err := n.ProposeConfChange(ConfChange{Type: ConfChangeAddNode, NodeID: 4}); err != nil {
		t.Fatalf("ProposeConfChange: %v", err)
	}
	if !n.IsJoint() {
		t.Fatal("expected the joint configuration after append")
	}

	// A new leader at a higher term overwrites the configuration entry.
	confIdx := n.log.lastIndex()
	n.log.truncateFrom(confIdx)
	n.applyConfFromLog()

	if n.IsJoint() {
		t.Error("still joint after the configuration entry was truncated")
	}
	if got := n.Voters(); len(got) != 3 || containsID(got, 4) {
		t.Errorf("voters = %v, want the original three without node 4", got)
	}
}
