package sim

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/keshavsaini0311/raftkv/kv"
	"github.com/keshavsaini0311/raftkv/raft"
)

// This file exists for the interactive lab: a person driving the cluster by
// hand needs to SEE more than a test does. A test asserts on one number; a
// person needs the log, the messages in flight, and the state machine.
//
// Everything here is read-only inspection or an explicit operator action.
// Nothing reaches inside raft.Node — the views are assembled from its exported
// accessors and from the storage and state machine the driver already owns.

// EntryView is one log entry, described rather than decoded raw.
type EntryView struct {
	Index uint64 `json:"i"`
	Term  uint64 `json:"t"`
	Kind  string `json:"k"` // normal | conf | noop
	Desc  string `json:"d"` // "put a=1", "add n4", "no-op"
}

// NodeView is one replica as the lab draws it.
type NodeView struct {
	ID      uint64 `json:"id"`
	Role    string `json:"role"`
	Term    uint64 `json:"term"`
	Lead    uint64 `json:"lead"`
	First   uint64 `json:"first"`
	Last    uint64 `json:"last"`
	Commit  uint64 `json:"commit"`
	Applied uint64 `json:"applied"`
	Crashed bool   `json:"crashed"`
	Voter   bool   `json:"voter"`
	Joint   bool   `json:"joint"`

	Entries []EntryView       `json:"entries"`
	Keys    map[string]string `json:"keys"`
}

// WireView is one message still travelling.
type WireView struct {
	From    uint64 `json:"f"`
	To      uint64 `json:"o"`
	Type    string `json:"t"`
	Term    uint64 `json:"tm"`
	Index   uint64 `json:"ix"`
	Count   int    `json:"n"`             // entries carried
	Ok      bool   `json:"ok,omitempty"`  // responses only
	Granted bool   `json:"g,omitempty"`   // vote responses only
	Arrives uint64 `json:"at"`            // virtual tick
	Cut     bool   `json:"cut,omitempty"` // will be dropped: partitioned
}

// View is one complete snapshot of the cluster for the UI.
type View struct {
	T      uint64          `json:"t"`
	Leader uint64          `json:"leader"`
	Nodes  []NodeView      `json:"nodes"`
	Wire   []WireView      `json:"wire"`
	Groups [][]raft.NodeID `json:"groups,omitempty"`
	Log    []string        `json:"log"`

	Sent       int `json:"sent"`
	Delivered  int `json:"delivered"`
	DroppedNet int `json:"droppedNet"`
	Snapshots  int `json:"snapshots"`
}

// confEntry mirrors the JSON a configuration entry carries.
//
// The raft type behind it is unexported, and it stays that way — this decodes
// the entry's WIRE FORMAT, which is a published thing that any peer must be
// able to read, rather than reaching into the package.
type confEntry struct {
	Voters   []raft.NodeID    `json:"voters"`
	Outgoing []raft.NodeID    `json:"outgoing"`
	Change   *raft.ConfChange `json:"Change"`
}

func (cs confEntry) describe() string {
	what := "config"
	if cs.Change != nil {
		verb := "add"
		if cs.Change.Type == raft.ConfChangeRemoveNode {
			verb = "remove"
		}
		what = fmt.Sprintf("%s n%d", verb, cs.Change.NodeID)
	}
	// Two entries are written per change: one entering the joint configuration
	// and one leaving it. Saying which is which is the difference between
	// membership changes looking arbitrary and looking like a protocol.
	if len(cs.Outgoing) > 0 {
		return what + " · enter joint"
	}
	if cs.Change == nil {
		// The exit transition carries only the resulting configuration.
		return "leave joint · now " + idList(cs.Voters)
	}
	return what + " · now " + idList(cs.Voters)
}

func idList(ids []raft.NodeID) string {
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("n%d", id))
	}
	return strings.Join(parts, "+")
}

// describe turns an entry's bytes into something a person can read.
//
// A log of opaque byte counts teaches nothing. The whole reason to watch a
// replicated log is to see the SAME commands land in the SAME order on every
// replica, which requires the commands to be legible.
func describe(e raft.Entry) EntryView {
	v := EntryView{Index: uint64(e.Index), Term: uint64(e.Term)}
	switch {
	case e.Type == raft.EntryConfChange:
		v.Kind = "conf"
		v.Desc = "config change"
		var cs confEntry
		if err := json.Unmarshal(e.Data, &cs); err == nil {
			v.Desc = cs.describe()
		}
	case len(e.Data) == 0:
		// The entry a new leader commits to establish its term. It carries no
		// command on purpose: committing it proves the leader's log is current
		// without changing the state machine.
		v.Kind = "noop"
		v.Desc = "no-op (new term)"
	default:
		v.Kind = "normal"
		v.Desc = "?"
		if cmd, err := kv.Decode(e.Data); err == nil {
			switch cmd.Op {
			case kv.OpPut:
				v.Desc = fmt.Sprintf("put %s=%s", cmd.Key, cmd.Value)
			case kv.OpDelete:
				v.Desc = fmt.Sprintf("del %s", cmd.Key)
			default:
				v.Desc = cmd.Op.String() + " " + cmd.Key
			}
		}
	}
	return v
}

// tailOf returns at most n entries from the end of a node's persisted log.
//
// The PERSISTED log, not the in-memory one: that is what would survive a crash,
// and showing anything else would let the lab display an entry that a restart
// makes vanish.
func (c *Cluster) tailOf(n *simNode, max int) []EntryView {
	_, ents, _, err := n.store.Load()
	if err != nil {
		return []EntryView{}
	}
	if len(ents) > max {
		ents = ents[len(ents)-max:]
	}
	out := make([]EntryView, 0, len(ents))
	for _, e := range ents {
		out = append(out, describe(e))
	}
	return out
}

// View assembles the whole cluster for the UI.
//
// Every slice is allocated rather than left nil. A nil slice marshals to JSON
// `null`, not `[]` — so an empty message queue would hand the page a null it
// then tried to iterate. Fixing it here means no consumer needs a null guard.
func (c *Cluster) View(logLines []string, entryTail int) View {
	if logLines == nil {
		logLines = []string{}
	}
	v := View{
		T:          c.now,
		Leader:     uint64(c.Leader()),
		Log:        logLines,
		Nodes:      []NodeView{},
		Wire:       []WireView{},
		Sent:       c.net.sent,
		Delivered:  c.net.delivered,
		DroppedNet: c.net.dropped + c.net.partitionDropped,
		Snapshots:  c.Snapshots,
	}

	voters := c.leaderVoters()
	for _, id := range c.ids { // sorted, always
		n := c.nodes[id]
		nv := NodeView{
			ID:      uint64(id),
			Role:    n.node.Role().String(),
			Term:    uint64(n.node.Term()),
			Lead:    uint64(n.node.Lead()),
			First:   uint64(n.node.FirstIndex()),
			Last:    uint64(n.node.LastIndex()),
			Commit:  uint64(n.node.CommitIndex()),
			Applied: uint64(n.node.AppliedIndex()),
			Crashed: n.crashed,
			Voter:   containsNodeID(voters, id),
			Joint:   n.node.IsJoint(),
			Entries: c.tailOf(n, entryTail),
			Keys:    map[string]string{},
		}
		for _, k := range n.kv.Keys() {
			if val, ok := n.kv.Get(k); ok {
				nv.Keys[k] = string(val)
			}
		}
		v.Nodes = append(v.Nodes, nv)
	}

	if g := c.partitionGroups(); len(g) > 1 {
		v.Groups = g
	}

	// The heap is not in delivery order — it is a heap. Sorting a copy keeps the
	// UI list stable between frames instead of reshuffling as messages land.
	flying := make([]*inFlight, len(c.net.queue))
	copy(flying, c.net.queue)
	sort.Slice(flying, func(a, b int) bool {
		if flying[a].deliverAt != flying[b].deliverAt {
			return flying[a].deliverAt < flying[b].deliverAt
		}
		return flying[a].seq < flying[b].seq
	})
	for _, it := range flying {
		m := it.msg
		idx := m.PrevLogIndex
		if m.Type == raft.MsgRequestVote {
			idx = m.LastLogIndex
		} else if m.Type == raft.MsgAppendEntriesResp {
			idx = m.MatchIndex
		}
		v.Wire = append(v.Wire, WireView{
			From: uint64(m.From), To: uint64(m.To), Type: m.Type.String(),
			Term: uint64(m.Term), Index: uint64(idx), Count: len(m.Entries),
			Ok: m.Success, Granted: m.VoteGranted, Arrives: it.deliverAt,
			Cut: c.net.partition[m.From] != c.net.partition[m.To],
		})
	}
	return v
}

/* ---- operator actions ------------------------------------------------ */

// Campaign forces one node to start an election now.
//
// Implemented by ticking that node alone until its randomized timeout expires,
// rather than by adding a back door to raft.Node. The election that results is
// the one the algorithm would have run on its own — just sooner.
func (c *Cluster) Campaign(id raft.NodeID) bool {
	n := c.nodes[id]
	if n == nil || n.crashed || n.node.Role() == raft.Leader {
		return false
	}
	// Drive until the TERM advances, not until the role is Candidate: a node
	// that is already campaigning is by that second test already done, and the
	// button would do nothing while reporting success.
	//
	// The timeout is drawn from [10,20), so 20 ticks always reaches it. The
	// bound exists so a node that cannot campaign — no longer a voter, say —
	// cannot spin here forever.
	start := n.node.Term()
	for i := 0; i < 20 && n.node.Term() == start; i++ {
		n.node.Tick()
		c.handleReady(n)
	}
	return n.node.Term() > start
}

// Isolate cuts one node off from every other node.
func (c *Cluster) Isolate(id raft.NodeID) {
	rest := make([]raft.NodeID, 0, len(c.ids)-1)
	for _, other := range c.ids {
		if other != id {
			rest = append(rest, other)
		}
	}
	if len(rest) == 0 {
		return
	}
	c.net.partitionInto([][]raft.NodeID{{id}, rest})
}

// SplitAt partitions the cluster so the named nodes are on one side.
func (c *Cluster) SplitAt(side []raft.NodeID) {
	rest := make([]raft.NodeID, 0, len(c.ids))
	for _, id := range c.ids {
		if !containsNodeID(side, id) {
			rest = append(rest, id)
		}
	}
	if len(side) == 0 || len(rest) == 0 {
		c.Heal()
		return
	}
	c.net.partitionInto([][]raft.NodeID{side, rest})
}

// SetLossy adjusts the network mid-run.
func (c *Cluster) SetLossy(drop, duplicate float64) {
	c.net.cfg.DropRate = drop
	c.net.cfg.DuplicateRate = duplicate
}

// Lossy reports the current network settings, so the UI can show what it set.
func (c *Cluster) Lossy() (drop, duplicate float64) {
	return c.net.cfg.DropRate, c.net.cfg.DuplicateRate
}

// ConfChange proposes a membership change through the current leader.
func (c *Cluster) ConfChange(add bool, id raft.NodeID) error {
	leader := c.Leader()
	if leader == 0 {
		return fmt.Errorf("no leader")
	}
	n := c.nodes[leader]
	if n.node.IsJoint() {
		return fmt.Errorf("a configuration change is already in flight")
	}
	t := raft.ConfChangeRemoveNode
	if add {
		t = raft.ConfChangeAddNode
	}
	_, err := n.node.ProposeConfChange(raft.ConfChange{Type: t, NodeID: id})
	return err
}

// Partitioned reports whether the network is currently split.
func (c *Cluster) Partitioned() bool { return len(c.partitionGroups()) > 1 }
