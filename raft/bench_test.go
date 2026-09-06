package raft

import "testing"

// These measure the CORE, which does no I/O — so they measure the algorithm
// itself rather than a disk or a network. That is the useful thing to know: it
// establishes the floor, and anything slower in production is the driver's
// doing, not Raft's.

func BenchmarkTick(b *testing.B) {
	n := NewNode(1, []NodeID{2, 3, 4, 5}, func(int) int { return 0 })
	for b.Loop() {
		n.Tick()
	}
}

// Note what this measures: no responses ever arrive, so nextIndex never
// advances and every Propose re-sends the full maxEntriesPerMsg window to
// every peer. The per-op cost is therefore dominated by that re-send, and is a
// measurement of the WORST case (a peer that never acknowledges), not of
// steady-state replication where matchIndex tracks the leader.
func BenchmarkPropose(b *testing.B) {
	n := leaderForBench(5)
	data := []byte(`{"op":1,"key":"k","value":"dmFsdWU="}`)
	b.ReportAllocs()
	for b.Loop() {
		_, _ = n.Propose(data)
		n.msgs = n.msgs[:0] // the driver would drain these
	}
}

func BenchmarkStepAppendEntries(b *testing.B) {
	f := NewNode(2, []NodeID{1, 3}, func(int) int { return 0 })
	f.currentTerm = 1
	ents := []Entry{{Index: 1, Term: 1, Data: []byte("payload")}}

	b.ReportAllocs()
	i := Index(0)
	for b.Loop() {
		i++
		ents[0].Index = i
		_ = f.Step(Message{
			Type: MsgAppendEntries, From: 1, To: 2, Term: 1,
			PrevLogIndex: i - 1, PrevLogTerm: termOrZero(f, i-1),
			Entries: ents,
		})
		f.msgs = f.msgs[:0]
	}
}

func termOrZero(n *Node, i Index) Term {
	t, _ := n.log.term(i)
	return t
}

// Ready is called on every driver loop iteration, including the overwhelming
// majority where nothing happened. Its cost on an idle node is therefore paid
// constantly, and is worth knowing.
func BenchmarkReadyIdle(b *testing.B) {
	n := NewNode(1, []NodeID{2, 3}, func(int) int { return 0 })
	n.Advance(n.Ready())
	b.ReportAllocs()
	for b.Loop() {
		rd := n.Ready()
		if !rd.IsEmpty() {
			b.Fatal("expected an idle node to produce nothing")
		}
	}
}

// The commit rule runs on every AppendEntriesResp, so it is on the hot path of
// every write. It sorts match indices across the configuration.
func BenchmarkMaybeCommit(b *testing.B) {
	n := leaderForBench(5)
	for i := 0; i < 1000; i++ {
		n.log.append(n.currentTerm, Entry{Data: []byte("x")})
	}
	n.log.stable = n.log.lastIndex()
	for _, p := range n.peers() {
		n.matchIndex[p] = n.log.lastIndex()
	}
	n.matchIndex[n.id] = n.log.lastIndex()

	b.ReportAllocs()
	for b.Loop() {
		n.log.committed = 0
		n.maybeCommit()
		n.msgs = n.msgs[:0]
	}
}

// Log truncation copies rather than reslicing, which is a deliberate
// correctness cost. This is what that cost actually is.
func BenchmarkLogTruncate(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		l := newLog()
		for i := 0; i < 1000; i++ {
			l.append(1, Entry{Data: []byte("x")})
		}
		b.StartTimer()
		l.truncateFrom(500)
	}
}

func leaderForBench(nodes int) *Node {
	peers := make([]NodeID, 0, nodes-1)
	for i := 2; i <= nodes; i++ {
		peers = append(peers, NodeID(i))
	}
	n := NewNode(1, peers, func(int) int { return 0 })
	n.currentTerm = 1
	n.becomeLeader()
	n.msgs = nil
	return n
}
