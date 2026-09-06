package sim

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/keshavsaini0311/raftkv/raft"
)

// Trace records a run frame by frame so it can be replayed visually.
//
// It records what an OBSERVER could see — roles, terms, log lengths, messages
// on the wire, which links are cut — not the internals. That constraint is the
// point: if the visualisation needed to reach inside a Node, it would be
// showing you the implementation rather than the algorithm.
//
// Recording is off by default and adds no cost to a normal run.
type Trace struct {
	Seed   int64         `json:"seed"`
	Nodes  []raft.NodeID `json:"nodes"`
	Frames []Frame       `json:"frames"`

	pending []MsgFrame // messages sent during the tick being recorded
}

// Frame is one tick.
type Frame struct {
	T      uint64          `json:"t"`
	Nodes  []NodeFrame     `json:"n"`
	Msgs   []MsgFrame      `json:"m,omitempty"`
	Groups [][]raft.NodeID `json:"g,omitempty"` // partition groups, omitted when healthy
	Event  string          `json:"e,omitempty"`
}

// NodeFrame is one node's observable state.
type NodeFrame struct {
	ID      raft.NodeID `json:"i"`
	Role    string      `json:"r"`
	Term    uint64      `json:"t"`
	Log     int         `json:"l"`
	Commit  uint64      `json:"c"`
	Crashed bool        `json:"x,omitempty"`
	Voter   bool        `json:"v"`
}

// MsgFrame is one message put on the wire this tick.
type MsgFrame struct {
	From raft.NodeID `json:"f"`
	To   raft.NodeID `json:"o"`
	Type string      `json:"t"`
}

// EnableTrace starts recording. Call before the first Tick.
func (c *Cluster) EnableTrace() {
	c.trace = &Trace{Seed: c.cfg.Seed, Nodes: c.ids}
}

// Trace returns the recording, or nil.
func (c *Cluster) Trace() *Trace { return c.trace }

// RunTraced drives a recorded run and returns the trace along with the cluster.
//
// It repeats Run's tick order exactly — faults, tick, workload — because that
// order is what the seed replays. A recording made under a different order
// would be a real run of a different simulator.
func RunTraced(cfg Config, nemCfg NemesisConfig, wlCfg WorkloadConfig, ticks int) (*Cluster, *Nemesis, *Workload) {
	c := New(cfg)
	c.EnableTrace()
	rng := rand.New(rand.NewSource(cfg.Seed ^ 0x5eed))
	nem := NewNemesis(nemCfg, rng)
	wl := NewWorkload(wlCfg, rng)

	for i := 0; i < ticks; i++ {
		nem.Step(c)
		c.Tick()
		wl.Step(c)
	}
	return c, nem, wl
}

// Note records a human-readable event on the next frame — an election, a
// crash, a partition. These are what make a replay legible: without them a
// viewer sees colours change and has to infer why.
func (c *Cluster) Note(s string) {
	if c.trace == nil {
		return
	}
	if c.pendingNote != "" {
		c.pendingNote += " · " + s
		return
	}
	c.pendingNote = s
}

// recordFrame captures the cluster at the end of a tick.
func (c *Cluster) recordFrame() {
	if c.trace == nil {
		return
	}

	nodes := make([]NodeFrame, 0, len(c.ids))
	for _, id := range c.ids { // sorted: frame order is stable
		n := c.nodes[id]
		nodes = append(nodes, NodeFrame{
			ID:      id,
			Role:    n.node.Role().String(),
			Term:    uint64(n.node.Term()),
			Log:     int(n.node.LastIndex()),
			Commit:  uint64(n.node.CommitIndex()),
			Crashed: n.crashed,
			Voter:   containsNodeID(c.leaderVoters(), id),
		})
	}

	f := Frame{T: c.now, Nodes: nodes, Msgs: c.trace.pending}
	if g := c.partitionGroups(); len(g) > 1 {
		f.Groups = g
	}
	f.Event = joinNotes(c.pendingNote, diffEvent(c.lastFrame(), f))

	c.trace.Frames = append(c.trace.Frames, f)
	c.trace.pending = nil
	c.pendingNote = ""
}

func (c *Cluster) lastFrame() *Frame {
	if n := len(c.trace.Frames); n > 0 {
		return &c.trace.Frames[n-1]
	}
	return nil
}

func joinNotes(a, b string) string {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	}
	return a + " · " + b
}

// diffEvent derives what changed between two frames.
//
// Deriving beats instrumenting: a Note call in Crash, another in the election
// path, another in the nemesis, and the annotations drift out of step with the
// state they describe. A diff of two frames cannot disagree with the frames.
func diffEvent(prev *Frame, cur Frame) string {
	if prev == nil {
		return "cluster starts"
	}

	var notes []string
	for i, n := range cur.Nodes {
		p := prev.Nodes[i] // both are built from c.ids, so indices line up

		switch {
		case n.Crashed && !p.Crashed:
			notes = append(notes, fmt.Sprintf("n%d crashed", n.ID))
		case !n.Crashed && p.Crashed:
			notes = append(notes, fmt.Sprintf("n%d restarted", n.ID))
		}
		if n.Crashed {
			continue
		}
		if n.Role != p.Role && n.Role == "Leader" {
			notes = append(notes, fmt.Sprintf("n%d elected leader, term %d", n.ID, n.Term))
		}
		if n.Voter != p.Voter {
			verb := "removed from"
			if n.Voter {
				verb = "added to"
			}
			notes = append(notes, fmt.Sprintf("n%d %s the configuration", n.ID, verb))
		}
	}

	switch {
	case len(cur.Groups) > 1 && !sameGroups(prev.Groups, cur.Groups):
		notes = append(notes, "network partitioned: "+describeGroups(cur.Groups))
	case len(cur.Groups) <= 1 && len(prev.Groups) > 1:
		notes = append(notes, "network healed")
	}

	return strings.Join(notes, " · ")
}

func sameGroups(a, b [][]raft.NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}

func describeGroups(groups [][]raft.NodeID) string {
	parts := make([]string, 0, len(groups))
	for _, g := range groups {
		ids := make([]string, 0, len(g))
		for _, id := range g {
			ids = append(ids, fmt.Sprintf("n%d", id))
		}
		parts = append(parts, strings.Join(ids, "+"))
	}
	return strings.Join(parts, " | ")
}

// leaderVoters is the membership as the current leader sees it, so the
// visualisation can show a node that has been removed from the cluster but is
// still running.
func (c *Cluster) leaderVoters() []raft.NodeID {
	if l := c.Leader(); l != 0 {
		return c.nodes[l].node.Voters()
	}
	return c.ids
}

// partitionGroups reconstructs the partition as a list of groups.
func (c *Cluster) partitionGroups() [][]raft.NodeID {
	byGroup := make(map[int][]raft.NodeID)
	var order []int
	for _, id := range c.ids {
		g := c.net.partition[id]
		if _, seen := byGroup[g]; !seen {
			order = append(order, g)
		}
		byGroup[g] = append(byGroup[g], id)
	}
	// Sort group keys so the output is stable run to run.
	for i := 1; i < len(order); i++ {
		for j := i; j > 0 && order[j] < order[j-1]; j-- {
			order[j], order[j-1] = order[j-1], order[j]
		}
	}
	out := make([][]raft.NodeID, 0, len(order))
	for _, g := range order {
		out = append(out, byGroup[g])
	}
	return out
}
