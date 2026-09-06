package sim

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/keshavsaini0311/raftkv/kv"
	"github.com/keshavsaini0311/raftkv/raft"
)

// settle runs until a leader exists, so a test can start from a working
// cluster without hard-coding how many ticks an election happens to take.
func settle(t *testing.T, c *Cluster, max int) raft.NodeID {
	t.Helper()
	for i := 0; i < max; i++ {
		c.Tick()
		if l := c.Leader(); l != 0 {
			return l
		}
	}
	t.Fatalf("no leader after %d ticks", max)
	return 0
}

// The lab shows what View reports, so View being wrong means the page lies —
// and a page that lies about consensus is worse than no page.
func TestViewReportsWhatTheClusterIsDoing(t *testing.T) {
	c := New(DefaultConfig(9, 3))
	leader := settle(t, c, 200)

	if _, ok := c.Propose(leader, kv.Command{
		Op: kv.OpPut, Key: "colour", Value: []byte("teal"), ClientID: 1, Seq: 1,
	}); !ok {
		t.Fatal("leader refused a proposal")
	}
	c.RunTicks(15)

	v := c.View([]string{"hello"}, 20)

	if v.Leader != uint64(leader) {
		t.Errorf("View.Leader = %d, want %d", v.Leader, leader)
	}
	if len(v.Nodes) != 3 {
		t.Fatalf("View has %d nodes, want 3", len(v.Nodes))
	}
	if v.T != c.Now() {
		t.Errorf("View.T = %d, want %d", v.T, c.Now())
	}
	if len(v.Log) != 1 || v.Log[0] != "hello" {
		t.Errorf("View.Log = %v, want the lines it was handed", v.Log)
	}

	// The write must be visible as a legible log entry AND in the state
	// machine. Either one alone would let a broken apply path look healthy.
	var sawPut bool
	for _, n := range v.Nodes {
		for _, e := range n.Entries {
			if strings.Contains(e.Desc, "put colour=teal") {
				sawPut = true
				if e.Kind != "normal" {
					t.Errorf("put entry classified as %q, want normal", e.Kind)
				}
			}
		}
	}
	if !sawPut {
		t.Error("the proposed write never appeared in any node's log view")
	}
	for _, n := range v.Nodes {
		if got := n.Keys["colour"]; got != "teal" {
			t.Errorf("n%d state machine has colour=%q, want teal", n.ID, got)
		}
		if n.Commit < 1 || n.Applied < 1 {
			t.Errorf("n%d reports commit=%d applied=%d, want both advanced", n.ID, n.Commit, n.Applied)
		}
	}

	// A new leader's first entry is a no-op. Naming it is the difference
	// between the page explaining an election and showing an unexplained gap.
	var sawNoop bool
	for _, e := range v.Nodes[0].Entries {
		if e.Kind == "noop" {
			sawNoop = true
		}
	}
	if !sawNoop {
		t.Error("the election no-op was never described")
	}
}

// A partition must be visible in the view, and the messages it kills must be
// marked rather than silently missing.
func TestViewShowsPartitionsAndWire(t *testing.T) {
	c := New(DefaultConfig(4, 5))
	settle(t, c, 200)

	// Split with traffic ALREADY on the wire. A message sent after the split is
	// dropped at send and never reaches the queue, so Cut can only ever describe
	// the packets that were mid-flight when the partition landed — which is
	// exactly the case the UI needs to show honestly.
	c.SplitAt([]raft.NodeID{1, 2})

	v := c.View(nil, 10)
	if len(v.Groups) != 2 {
		t.Fatalf("View.Groups has %d groups, want 2", len(v.Groups))
	}
	if !c.Partitioned() {
		t.Error("Partitioned() says the network is whole during a split")
	}

	var cut int
	for _, w := range v.Wire {
		if w.Cut {
			cut++
		}
		if w.Type == "" {
			t.Error("a message on the wire has no type")
		}
	}
	if len(v.Wire) == 0 {
		t.Fatal("nothing in flight when the partition landed")
	}
	if cut == 0 {
		t.Error("no message is marked as crossing the partition")
	}

	c.Heal()
	if c.Partitioned() {
		t.Error("still partitioned after Heal")
	}
}

// Campaign is an operator lever, not a back door: it must produce a real
// candidate through the real timeout path, and refuse when it cannot.
func TestCampaignStartsARealElection(t *testing.T) {
	c := New(DefaultConfig(11, 3))
	leader := settle(t, c, 200)

	var other raft.NodeID
	for _, id := range c.IDs() {
		if id != leader {
			other = id
			break
		}
	}

	before := c.nodes[other].node.Term()
	if !c.Campaign(other) {
		t.Fatalf("n%d would not campaign", other)
	}
	if got := c.nodes[other].node.Role(); got != raft.Candidate {
		t.Errorf("n%d is %v after Campaign, want Candidate", other, got)
	}
	if after := c.nodes[other].node.Term(); after <= before {
		t.Errorf("term went %d -> %d; a candidate must advance its term", before, after)
	}

	// Asking again must run a NEW election rather than reporting success for
	// the one already under way.
	mid := c.nodes[other].node.Term()
	if !c.Campaign(other) {
		t.Error("a candidate refused to start a fresh election")
	}
	if after := c.nodes[other].node.Term(); after <= mid {
		t.Errorf("second Campaign left the term at %d; it did nothing", after)
	}

	// A crashed node cannot campaign, and asking must not panic or hang.
	c.Crash(other)
	if c.Campaign(other) {
		t.Error("a crashed node campaigned")
	}
}

// Membership changes are the hardest thing to see, so the description a
// configuration entry carries has to be right.
func TestConfChangeEntriesAreDescribed(t *testing.T) {
	c := New(DefaultConfig(21, 5))
	settle(t, c, 200)

	if err := c.ConfChange(false, 5); err != nil {
		t.Fatalf("remove n5: %v", err)
	}
	c.RunTicks(40)

	v := c.View(nil, 40)
	var joint, committed bool
	for _, e := range v.Nodes[0].Entries {
		if e.Kind != "conf" {
			continue
		}
		// Two entries, and they say different things: the first names the
		// change and enters joint consensus, the second leaves it and states
		// the configuration that results.
		switch {
		case strings.Contains(e.Desc, "enter joint"):
			joint = true
			if !strings.Contains(e.Desc, "remove n5") {
				t.Errorf("joint entry described as %q, want it to name the change", e.Desc)
			}
		case strings.Contains(e.Desc, "leave joint"):
			committed = true
			if strings.Contains(e.Desc, "n5") {
				t.Errorf("final configuration %q still lists the removed node", e.Desc)
			}
		}
	}
	if !joint || !committed {
		t.Errorf("expected both transitions described; joint=%v leave=%v", joint, committed)
	}

	// n5 is still running — it just no longer counts. That distinction is the
	// whole reason the ring draws a dotted halo instead of removing the node.
	for _, n := range v.Nodes {
		if n.ID == 5 && n.Voter {
			t.Error("n5 still reports as a voter after being removed")
		}
	}
}

// A nil slice marshals to JSON null, and a page that iterates null crashes.
// This is not a hypothetical: it took down the whole render the first time the
// message queue happened to be empty.
func TestViewNeverMarshalsNullCollections(t *testing.T) {
	c := New(DefaultConfig(3, 3))
	// Deliberately BEFORE any tick: nothing has been sent, nothing logged,
	// no entries exist. Every collection is at its emptiest.
	blob, err := json.Marshal(c.View(nil, 10))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(blob, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"nodes", "wire", "log"} {
		if string(raw[key]) == "null" {
			t.Errorf("View.%s marshalled as null; it must be []", key)
		}
	}

	var v struct {
		Nodes []struct {
			Entries json.RawMessage `json:"entries"`
			Keys    json.RawMessage `json:"keys"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(blob, &v); err != nil {
		t.Fatalf("unmarshal nodes: %v", err)
	}
	for i, n := range v.Nodes {
		if string(n.Entries) == "null" {
			t.Errorf("node %d entries marshalled as null", i)
		}
		if string(n.Keys) == "null" {
			t.Errorf("node %d keys marshalled as null", i)
		}
	}
}
