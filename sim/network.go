// Package sim is a deterministic simulator for the raft core.
//
// It owns time. Nothing here reads a clock, spawns a goroutine, or touches a
// socket; a run is a pure function of (seed, config). The same seed produces
// byte-identical behaviour today and next year, so a failure found once is a
// test case forever rather than a ghost.
//
// Three rules keep that true, and breaking any one of them silently destroys
// the guarantee:
//
//  1. All randomness comes from one seeded *rand.Rand, drawn in a fixed order.
//  2. No map is ever iterated where the order can reach an outcome. Node and
//     link collections are kept as sorted slices.
//  3. Ties are broken by an explicit monotonic sequence number, never by
//     whatever order a container happens to produce.
package sim

import (
	"container/heap"
	"math/rand"

	"github.com/keshavsaini0311/raftkv/raft"
)

// inFlight is one message travelling between nodes.
type inFlight struct {
	msg       raft.Message
	deliverAt uint64 // virtual tick
	seq       uint64 // tie-break, so equal delivery times have a fixed order
}

// msgQueue is a priority queue ordered by delivery time then sequence.
type msgQueue []*inFlight

func (q msgQueue) Len() int { return len(q) }
func (q msgQueue) Less(i, j int) bool {
	if q[i].deliverAt != q[j].deliverAt {
		return q[i].deliverAt < q[j].deliverAt
	}
	// Without this the heap's internal ordering would decide, and heap order
	// for equal keys is an implementation detail — exactly the kind of hidden
	// nondeterminism that makes a seed stop replaying.
	return q[i].seq < q[j].seq
}
func (q msgQueue) Swap(i, j int)       { q[i], q[j] = q[j], q[i] }
func (q *msgQueue) Push(x interface{}) { *q = append(*q, x.(*inFlight)) }
func (q *msgQueue) Pop() interface{} {
	old := *q
	n := len(old)
	it := old[n-1]
	*q = old[:n-1]
	return it
}

// NetworkConfig describes how unpleasant the network is.
type NetworkConfig struct {
	// MinLatency and MaxLatency bound delivery delay, in ticks. A range wider
	// than one tick is what produces reordering: two messages sent in order can
	// arrive out of order, which Raft must tolerate.
	MinLatency int
	MaxLatency int

	// DropRate is the probability in [0,1] that a message is discarded.
	DropRate float64

	// DuplicateRate is the probability a message is delivered twice. Raft must
	// be idempotent under this: a duplicated AppendEntries must not truncate,
	// and a duplicated RequestVote must get the same answer.
	DuplicateRate float64
}

func DefaultNetwork() NetworkConfig {
	return NetworkConfig{MinLatency: 1, MaxLatency: 3, DropRate: 0, DuplicateRate: 0}
}

// network delivers messages with latency, loss, reordering, and partitions.
type network struct {
	cfg NetworkConfig
	rng *rand.Rand

	queue msgQueue
	seq   uint64

	// partition assigns each node a group. Nodes in different groups cannot
	// exchange messages. A single group means a healthy network.
	partition map[raft.NodeID]int

	// Counters, so a test can assert the nemesis actually did something rather
	// than passing because nothing ever went wrong.
	sent, delivered, dropped, partitionDropped, duplicated int
}

func newNetwork(cfg NetworkConfig, rng *rand.Rand) *network {
	n := &network{cfg: cfg, rng: rng, partition: make(map[raft.NodeID]int)}
	heap.Init(&n.queue)
	return n
}

// send enqueues a message for future delivery, or drops it.
func (n *network) send(now uint64, m raft.Message) {
	n.sent++

	if n.partition[m.From] != n.partition[m.To] {
		n.partitionDropped++
		return
	}
	if n.cfg.DropRate > 0 && n.rng.Float64() < n.cfg.DropRate {
		n.dropped++
		return
	}

	n.enqueue(now, m)

	if n.cfg.DuplicateRate > 0 && n.rng.Float64() < n.cfg.DuplicateRate {
		n.duplicated++
		n.enqueue(now, m) // an independent draw, so the copy may arrive first
	}
}

func (n *network) enqueue(now uint64, m raft.Message) {
	spread := n.cfg.MaxLatency - n.cfg.MinLatency + 1
	if spread < 1 {
		spread = 1
	}
	delay := n.cfg.MinLatency + n.rng.Intn(spread)
	if delay < 1 {
		delay = 1
	}
	n.seq++
	heap.Push(&n.queue, &inFlight{
		msg:       m,
		deliverAt: now + uint64(delay),
		seq:       n.seq,
	})
}

// due pops every message whose delivery time has arrived.
//
// A message crossing a partition that was healed while it was in flight is
// still delivered: it was already on the wire. One that was sent across a live
// link and finds a partition on arrival is dropped, because the packet would
// have been in a queue somewhere that is now unreachable. Either choice is
// defensible; what matters is that it is deterministic.
func (n *network) due(now uint64) []raft.Message {
	var out []raft.Message
	for n.queue.Len() > 0 && n.queue[0].deliverAt <= now {
		it := heap.Pop(&n.queue).(*inFlight)
		if n.partition[it.msg.From] != n.partition[it.msg.To] {
			n.partitionDropped++
			continue
		}
		n.delivered++
		out = append(out, it.msg)
	}
	return out
}

// partitionInto assigns nodes to groups. groups[i] is the set of node ids that
// can still talk to each other.
func (n *network) partitionInto(groups [][]raft.NodeID) {
	n.partition = make(map[raft.NodeID]int)
	for g, ids := range groups {
		for _, id := range ids {
			n.partition[id] = g
		}
	}
}

// heal puts every node back in one group.
func (n *network) heal(ids []raft.NodeID) {
	n.partition = make(map[raft.NodeID]int)
	for _, id := range ids {
		n.partition[id] = 0
	}
}

// inFlightCount reports undelivered messages, so a test can drive the cluster
// until it is genuinely quiescent rather than guessing at a tick count.
func (n *network) inFlightCount() int { return n.queue.Len() }
