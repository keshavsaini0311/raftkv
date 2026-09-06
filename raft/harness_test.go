// Hand-driven multi-node harness for milestone 1 and 2 tests.
//
// No goroutines, no timers, no sleeps. The test IS the scheduler: it decides
// when time advances and when messages are delivered, so every run is
// identical. Milestone 4 replaces this with a simulator that adds latency,
// reordering, drops, and crash/restart on a seeded PRNG — but the principle is
// already here.
//
// Internal tests (package raft) so they can assert on unexported state. For a
// state machine that is the right trade: the alternative is exporting a dozen
// getters that exist only for tests.
package raft

import "testing"

// network is a set of nodes and a delivery policy.
type network struct {
	t     *testing.T
	ids   []NodeID
	nodes map[NodeID]*Node

	// isolated nodes neither send nor receive: a total partition of one.
	isolated map[NodeID]bool

	// dropVotes silently discards RequestVote traffic, used to force a term
	// to pass without a leader being elected.
	dropVotes bool
}

// newNetwork builds n nodes with STAGGERED election timeouts: node i draws
// electionTimeoutMin+i-1. Deterministic and distinct, so the lowest id always
// campaigns first and tests do not depend on tie-breaking.
func newNetwork(t *testing.T, ids ...NodeID) *network {
	t.Helper()
	nw := &network{
		t:        t,
		ids:      ids,
		nodes:    make(map[NodeID]*Node, len(ids)),
		isolated: make(map[NodeID]bool),
	}
	for i, id := range ids {
		peers := make([]NodeID, 0, len(ids)-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		offset := i
		nw.nodes[id] = NewNode(id, peers, func(int) int { return offset })
	}
	return nw
}

// newSyncedNetwork gives every node the SAME election timeout, so they all
// campaign on the same tick. Used to force a split vote.
func newSyncedNetwork(t *testing.T, ids ...NodeID) *network {
	t.Helper()
	nw := newNetwork(t, ids...)
	for _, id := range ids {
		peers := make([]NodeID, 0, len(ids)-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		nw.nodes[id] = NewNode(id, peers, func(int) int { return 0 })
	}
	return nw
}

func (nw *network) node(id NodeID) *Node { return nw.nodes[id] }

// tick advances every non-isolated node by n ticks, delivering messages after
// each one so the cluster converges the way a real one would.
func (nw *network) tick(n int) {
	nw.t.Helper()
	for i := 0; i < n; i++ {
		for _, id := range nw.ids {
			if !nw.isolated[id] {
				nw.nodes[id].Tick()
			}
		}
		nw.deliver()
	}
}

// tickOne advances a single node without delivering anything, for tests that
// need to observe an intermediate state.
func (nw *network) tickOne(id NodeID, n int) {
	nw.t.Helper()
	for i := 0; i < n; i++ {
		nw.nodes[id].Tick()
	}
}

// deliver drains every node's Ready and Steps the messages into their targets,
// repeating until the cluster falls silent.
func (nw *network) deliver() {
	nw.t.Helper()
	const maxRounds = 100
	for round := 0; round < maxRounds; round++ {
		var inFlight []Message
		for _, id := range nw.ids {
			n := nw.nodes[id]
			rd := n.Ready()
			if !nw.isolated[id] {
				inFlight = append(inFlight, rd.Messages...)
			}
			n.Advance(rd)
		}
		if len(inFlight) == 0 {
			return
		}
		for _, m := range inFlight {
			if nw.isolated[m.To] {
				continue
			}
			if nw.dropVotes && (m.Type == MsgRequestVote || m.Type == MsgRequestVoteResp) {
				continue
			}
			if dst, ok := nw.nodes[m.To]; ok {
				_ = dst.Step(m)
			}
		}
	}
	nw.t.Fatalf("cluster did not fall silent within %d delivery rounds", maxRounds)
}

// isolate cuts a node off entirely, in both directions.
func (nw *network) isolate(id NodeID) { nw.isolated[id] = true }

// leaders returns every node currently claiming leadership. More than one at
// the same term is a safety violation; more than one across different terms is
// normal during a transition.
func (nw *network) leaders() []NodeID {
	var out []NodeID
	for _, id := range nw.ids {
		if nw.nodes[id].role == Leader {
			out = append(out, id)
		}
	}
	return out
}

// assertSingleLeader checks the core safety property: at most one leader per
// term, and here exactly one.
func (nw *network) assertSingleLeader() NodeID {
	nw.t.Helper()
	ls := nw.leaders()
	if len(ls) != 1 {
		nw.t.Fatalf("want exactly 1 leader, got %d: %v (%s)", len(ls), ls, nw.describe())
	}
	return ls[0]
}

func (nw *network) describe() string {
	s := ""
	for _, id := range nw.ids {
		n := nw.nodes[id]
		if s != "" {
			s += ", "
		}
		s += string(rune('0'+id)) + "=" + n.role.String() + "@t" + itoa(int(n.currentTerm))
	}
	return s
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var buf []byte
	for v > 0 {
		buf = append([]byte{byte('0' + v%10)}, buf...)
		v /= 10
	}
	return string(buf)
}
