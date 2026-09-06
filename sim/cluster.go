package sim

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/keshavsaini0311/raftkv/kv"
	"github.com/keshavsaini0311/raftkv/raft"
	"github.com/keshavsaini0311/raftkv/storage"
)

// Config is everything a run needs. Two runs with equal Configs and equal
// seeds are identical, byte for byte.
type Config struct {
	Seed    int64
	Nodes   int
	Network NetworkConfig

	// SnapshotEvery compacts the log once this many entries have accumulated
	// past the last snapshot. Zero disables compaction.
	//
	// Small values are deliberate here. Real deployments snapshot every few
	// thousand entries, which in a test means never — and untested compaction
	// is where "the follower fell behind and could not catch up" bugs live.
	SnapshotEvery int
}

func DefaultConfig(seed int64, nodes int) Config {
	return Config{Seed: seed, Nodes: nodes, Network: DefaultNetwork()}
}

// pendingOp is a client write waiting on a specific (term, index).
type pendingOp struct {
	op   *Op
	term raft.Term
}

// simNode is one replica plus the state a driver would own.
type simNode struct {
	id    raft.NodeID
	node  *raft.Node
	kv    *kv.Store
	store *storage.Memory

	// crashed nodes neither tick nor step. Their storage survives, which is
	// the entire point: a crash must lose volatile state and keep durable
	// state, or the simulation is testing something easier than reality.
	crashed bool

	// applied is every entry this node handed to its state machine, in order.
	// State Machine Safety says these must agree across nodes at every index.
	applied []raft.Entry

	// appliedIdx is the highest index actually reflected in kv right now.
	// Tracked separately from raftLog.applied because that field is updated in
	// Advance, after the apply loop has already run.
	appliedIdx raft.Index
}

// Cluster is a deterministic in-memory Raft cluster.
type Cluster struct {
	cfg Config
	rng *rand.Rand
	net *network

	now uint64 // virtual time, in ticks

	// ids is sorted and is the ONLY thing iterated when order matters. The
	// nodes map exists for lookup and is never ranged.
	ids   []raft.NodeID
	nodes map[raft.NodeID]*simNode

	// pending client operations, keyed by the log index they are waiting on.
	//
	// The TERM is recorded alongside, and that is not bookkeeping. An index
	// alone does not identify a proposal: if the leader is deposed before the
	// entry commits, a new leader can place a DIFFERENT entry at the same
	// index. Resolving on index alone would then report the client's write as
	// successful using another write's result — an acknowledged write that
	// never happened, which is the worst failure a store can have.
	pending map[raft.NodeID]map[raft.Index]pendingOp

	// pendingReads keyed by read token.
	pendingReads map[raft.NodeID]map[uint64]*Op
	readToken    uint64

	history *History

	// ReadTooEarly counts reads released before the state machine caught up to
	// their read index. Must be zero in a correct driver.
	ReadTooEarly int

	// Snapshots and SnapshotsInstalled count compaction activity, so a run that
	// never compacted cannot pass as one that did.
	Snapshots          int
	SnapshotsInstalled int

	// ProposalsLost counts writes whose entry was truncated by a new leader.
	// Expected to be non-zero under chaos: it is a normal Raft outcome, and
	// the client is correctly told nothing rather than told it succeeded.
	ProposalsLost int
}

// New builds a cluster. Nodes are numbered 1..cfg.Nodes.
func New(cfg Config) *Cluster {
	rng := rand.New(rand.NewSource(cfg.Seed))
	c := &Cluster{
		cfg:          cfg,
		rng:          rng,
		net:          newNetwork(cfg.Network, rng),
		nodes:        make(map[raft.NodeID]*simNode, cfg.Nodes),
		pending:      make(map[raft.NodeID]map[raft.Index]pendingOp),
		pendingReads: make(map[raft.NodeID]map[uint64]*Op),
		history:      NewHistory(),
	}

	for i := 1; i <= cfg.Nodes; i++ {
		c.ids = append(c.ids, raft.NodeID(i))
	}
	sort.Slice(c.ids, func(a, b int) bool { return c.ids[a] < c.ids[b] })

	for _, id := range c.ids {
		peers := make([]raft.NodeID, 0, cfg.Nodes-1)
		for _, other := range c.ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		// Each node draws from the SAME shared rng. That is intentional: the
		// interleaving of draws is part of what the seed determines, so the
		// whole cluster replays together rather than each node independently.
		c.nodes[id] = &simNode{
			id:    id,
			node:  raft.NewNode(id, peers, rng.Intn),
			kv:    kv.New(),
			store: storage.NewMemory(),
		}
		c.pending[id] = make(map[raft.Index]pendingOp)
		c.pendingReads[id] = make(map[uint64]*Op)
	}
	c.net.heal(c.ids)
	return c
}

func (c *Cluster) Now() uint64        { return c.now }
func (c *Cluster) History() *History  { return c.history }
func (c *Cluster) IDs() []raft.NodeID { return c.ids }

// Tick advances virtual time by one and runs the whole cluster for that tick.
func (c *Cluster) Tick() {
	c.now++

	// Fixed order: sorted ids, every tick. Ranging c.nodes here would make the
	// order of Ready processing vary per run and the seed stop replaying.
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.crashed {
			continue
		}
		n.node.Tick()
	}

	for _, m := range c.net.due(c.now) {
		n := c.nodes[m.To]
		if n == nil || n.crashed {
			continue
		}
		_ = n.node.Step(m)
	}

	for _, id := range c.ids {
		if !c.nodes[id].crashed {
			c.handleReady(c.nodes[id])
		}
	}
}

// RunTicks advances the cluster.
func (c *Cluster) RunTicks(n int) {
	for i := 0; i < n; i++ {
		c.Tick()
	}
}

// handleReady is the simulator's driver: the same contract as server/, minus
// the goroutines and the disk.
func (c *Cluster) handleReady(n *simNode) {
	rd := n.node.Ready()
	if rd.IsEmpty() {
		return
	}

	// Persist before sending. The in-memory storage makes this cheap, but the
	// ORDER is what is being modelled: a crash between the send and the
	// persist is exactly the window Raft's durability rule closes.
	if rd.HardState != nil {
		_ = n.store.SaveHardState(*rd.HardState)
	}
	if len(rd.Entries) > 0 {
		_ = n.store.Append(rd.Entries)
	}
	if !rd.Snapshot.IsEmpty() {
		_ = n.store.SaveSnapshot(rd.Snapshot)
		_ = n.kv.Restore(rd.Snapshot.Data)
	}

	for _, m := range rd.Messages {
		c.net.send(c.now, m)
	}

	for _, e := range rd.CommittedEntries {
		n.applied = append(n.applied, e)
		n.appliedIdx = e.Index
		if e.Type != raft.EntryNormal || len(e.Data) == 0 {
			// Election no-ops and configuration entries are raft's own
			// bookkeeping. Feeding them to the state machine would fail to
			// decode, and on every replica identically — a consistent
			// corruption is still a corruption.
			continue
		}
		res := n.kv.Apply(e.Data)
		if p, ok := c.pending[n.id][e.Index]; ok {
			delete(c.pending[n.id], e.Index)
			if p.term == e.Term {
				c.history.Return(p.op, res, c.now)
			} else {
				// A different leader's entry landed at this index, so our
				// proposal was truncated. The client never learns the outcome:
				// indeterminate, not successful.
				c.ProposalsLost++
				c.history.Abandon(p.op)
			}
		}
	}

	for _, rs := range rd.ReadStates {
		token := decodeToken(rs.Ctx)
		op, ok := c.pendingReads[n.id][token]
		if !ok {
			continue
		}
		delete(c.pendingReads[n.id], token)

		// ReadState's contract: serve only once the state machine has applied
		// through rs.Index. Serving earlier returns a value from before the
		// read's own read-index and is a stale read by construction.
		if n.appliedIdx < rs.Index {
			c.ReadTooEarly++
			c.history.Abandon(op)
			continue
		}

		v, found := n.kv.Get(op.Key)
		c.history.Return(op, kv.Result{Value: v, Found: found}, c.now)
	}

	if !rd.Snapshot.IsEmpty() {
		c.SnapshotsInstalled++
	}

	n.node.Advance(rd)
	c.maybeSnapshot(n)
}

// maybeSnapshot compacts the log once it has grown past the threshold.
//
// The snapshot is taken from the STATE MACHINE, not from the log: it is the
// result of applying entries, which is exactly what makes the entries
// discardable. Snapshotting at applied (never past it) means the snapshot
// always describes a state that actually existed.
func (c *Cluster) maybeSnapshot(n *simNode) {
	if c.cfg.SnapshotEvery <= 0 {
		return
	}
	applied := n.node.AppliedIndex()
	if applied < n.node.FirstIndex() {
		return
	}
	if int(applied-n.node.FirstIndex()) < c.cfg.SnapshotEvery {
		return
	}

	data, err := n.kv.Snapshot()
	if err != nil {
		return
	}
	snap, err := n.node.CreateSnapshot(applied, data)
	if err != nil {
		return
	}
	_ = n.store.SaveSnapshot(snap)
	c.Snapshots++
}

// Leader returns the current leader, or 0 if there is none.
//
// More than one leader in the SAME term is a safety violation; more than one
// across different terms is normal during a transition, so this returns the
// one with the highest term.
func (c *Cluster) Leader() raft.NodeID {
	var best raft.NodeID
	var bestTerm raft.Term
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.crashed || n.node.Role() != raft.Leader {
			continue
		}
		if best == 0 || n.node.Term() > bestTerm {
			best, bestTerm = id, n.node.Term()
		}
	}
	return best
}

// LeadersByTerm reports how many nodes claim leadership in each term. Any term
// with more than one is a safety violation.
func (c *Cluster) LeadersByTerm() map[raft.Term][]raft.NodeID {
	out := make(map[raft.Term][]raft.NodeID)
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.crashed || n.node.Role() != raft.Leader {
			continue
		}
		out[n.node.Term()] = append(out[n.node.Term()], id)
	}
	return out
}

// Propose submits a write to the given node and records the invocation.
// Returns false if that node is not the leader.
func (c *Cluster) Propose(id raft.NodeID, cmd kv.Command) (*Op, bool) {
	n := c.nodes[id]
	if n == nil || n.crashed {
		return nil, false
	}
	data, err := cmd.Encode()
	if err != nil {
		return nil, false
	}
	idx, err := n.node.Propose(data)
	if err != nil {
		return nil, false
	}
	op := c.history.Invoke(id, cmd, c.now)
	c.pending[id][idx] = pendingOp{op: op, term: n.node.Term()}
	return op, true
}

// Read submits a linearizable read and records the invocation.
func (c *Cluster) Read(id raft.NodeID, key string) (*Op, bool) {
	n := c.nodes[id]
	if n == nil || n.crashed {
		return nil, false
	}
	c.readToken++
	token := c.readToken
	if err := n.node.ReadIndex(encodeToken(token)); err != nil {
		return nil, false
	}
	op := c.history.Invoke(id, kv.Command{Op: kv.OpGet, Key: key}, c.now)
	c.pendingReads[id][token] = op
	return op, true
}

// Crash stops a node. Volatile state is lost; storage survives.
func (c *Cluster) Crash(id raft.NodeID) {
	n := c.nodes[id]
	if n == nil || n.crashed {
		return
	}
	n.crashed = true

	// Every in-flight client operation on this node is abandoned. A real
	// client would time out and retry, and leaving them pending would let a
	// later restart resolve them at the wrong virtual time.
	for idx, p := range c.pending[id] {
		c.history.Abandon(p.op)
		delete(c.pending[id], idx)
	}
	for tok, op := range c.pendingReads[id] {
		c.history.Abandon(op)
		delete(c.pendingReads[id], tok)
	}
}

// Restart brings a crashed node back, recovering ONLY what was persisted.
//
// This is where a durability bug surfaces: the node is rebuilt from storage
// alone, so anything the driver acknowledged without fsyncing is simply gone.
func (c *Cluster) Restart(id raft.NodeID) {
	n := c.nodes[id]
	if n == nil || !n.crashed {
		return
	}

	peers := make([]raft.NodeID, 0, len(c.ids)-1)
	for _, other := range c.ids {
		if other != id {
			peers = append(peers, other)
		}
	}

	hs, ents, snap, _ := n.store.Load()

	n.node = raft.NewNode(id, peers, c.rng.Intn)
	n.kv = kv.New()
	if snap != nil {
		_ = n.kv.Restore(snap.Data)
	}
	n.node.Restore(hs, ents, snap)

	// Replay committed entries into the fresh state machine. A real driver
	// would do this from its own applied index; here the whole log is replayed
	// because the state machine is rebuilt from nothing.
	n.applied = nil
	n.crashed = false
}

// Crashed reports whether a node is down.
func (c *Cluster) Crashed(id raft.NodeID) bool { return c.nodes[id].crashed }

// Applied returns the entries a node handed to its state machine.
func (c *Cluster) Applied(id raft.NodeID) []raft.Entry { return c.nodes[id].applied }

// Stats reports network counters.
func (c *Cluster) Stats() string {
	return fmt.Sprintf("sent=%d delivered=%d dropped=%d partitioned=%d duplicated=%d inflight=%d snapshots=%d installed=%d",
		c.net.sent, c.net.delivered, c.net.dropped,
		c.net.partitionDropped, c.net.duplicated, c.net.inFlightCount(),
		c.Snapshots, c.SnapshotsInstalled)
}

// Partition splits the cluster into groups that cannot talk to each other.
func (c *Cluster) Partition(groups [][]raft.NodeID) { c.net.partitionInto(groups) }

// Heal reconnects everything.
func (c *Cluster) Heal() { c.net.heal(c.ids) }

// IsolateLeader is the pathological case worth naming: the leader alone on one
// side of a partition. It keeps believing it leads, the majority elects
// someone else, and any read the old leader serves is stale. This is the
// scenario ReadIndex exists to defeat.
func (c *Cluster) IsolateLeader() (raft.NodeID, bool) {
	leader := c.Leader()
	if leader == 0 {
		return 0, false
	}
	var rest []raft.NodeID
	for _, id := range c.ids {
		if id != leader {
			rest = append(rest, id)
		}
	}
	c.net.partitionInto([][]raft.NodeID{{leader}, rest})
	return leader, true
}

func encodeToken(t uint64) []byte {
	b := make([]byte, 8)
	for i := 0; i < 8; i++ {
		b[i] = byte(t >> (8 * i))
	}
	return b
}

func decodeToken(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	var t uint64
	for i := 0; i < 8; i++ {
		t |= uint64(b[i]) << (8 * i)
	}
	return t
}
