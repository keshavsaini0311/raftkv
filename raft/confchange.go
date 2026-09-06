package raft

import "encoding/json"

// ConfChangeType is what a configuration change does.
type ConfChangeType uint8

const (
	ConfChangeAddNode ConfChangeType = iota
	ConfChangeRemoveNode
)

func (t ConfChangeType) String() string {
	if t == ConfChangeAddNode {
		return "AddNode"
	}
	return "RemoveNode"
}

// ConfChange is a request to add or remove one node.
//
// One node at a time. Joint consensus can express arbitrary changes, but
// restricting each transition to a single node keeps the two configurations
// close enough that a reader can reason about them, and it is what every
// production Raft actually does.
type ConfChange struct {
	Type   ConfChangeType `json:"type"`
	NodeID NodeID         `json:"node_id"`
}

// confState is the membership encoded into an EntryConfChange.
//
// Both configurations travel in the entry rather than being recomputed by each
// node. A node applies the configuration it finds in its log; if it had to
// derive the joint set itself, a node that missed an earlier change would
// derive a different one, and two nodes would disagree about who counts toward
// a majority — which is a split brain wearing a disguise.
type confState struct {
	Voters   []NodeID `json:"voters"`             // C_new
	Outgoing []NodeID `json:"outgoing,omitempty"` // C_old, set only while joint
	Change   *ConfChange
}

func (cs confState) encode() ([]byte, error) { return json.Marshal(cs) }

func decodeConfState(b []byte) (confState, error) {
	var cs confState
	err := json.Unmarshal(b, &cs)
	return cs, err
}

// config is a node's view of cluster membership.
//
// While joint (Outgoing non-empty) a decision needs a majority of BOTH
// configurations. That overlap is the whole mechanism: it makes it impossible
// for C_old and C_new to independently elect different leaders during the
// transition, which is exactly the hazard that makes naive membership changes
// unsafe.
type config struct {
	voters   []NodeID
	outgoing []NodeID
}

func newConfig(voters []NodeID) config {
	c := config{voters: append([]NodeID(nil), voters...)}
	sortNodeIDs(c.voters)
	return c
}

func (c config) isJoint() bool { return len(c.outgoing) > 0 }

// contains reports whether id votes in either configuration.
func (c config) contains(id NodeID) bool {
	for _, v := range c.voters {
		if v == id {
			return true
		}
	}
	for _, v := range c.outgoing {
		if v == id {
			return true
		}
	}
	return false
}

// all returns every node that votes in either configuration, sorted and
// deduplicated. Sorted because it is iterated, and iteration order that can
// reach an outcome must be fixed.
func (c config) all() []NodeID {
	seen := make(map[NodeID]bool, len(c.voters)+len(c.outgoing))
	out := make([]NodeID, 0, len(c.voters)+len(c.outgoing))
	for _, v := range c.voters {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	for _, v := range c.outgoing {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	sortNodeIDs(out)
	return out
}

// hasQuorum reports whether the nodes satisfying granted form a majority.
//
// While joint, a majority of BOTH configurations is required. A change that
// satisfied only one could be agreed by C_old while C_new independently agreed
// something else.
func (c config) hasQuorum(granted func(NodeID) bool) bool {
	if !majority(c.voters, granted) {
		return false
	}
	if c.isJoint() && !majority(c.outgoing, granted) {
		return false
	}
	return true
}

func majority(voters []NodeID, granted func(NodeID) bool) bool {
	if len(voters) == 0 {
		return false
	}
	count := 0
	for _, v := range voters {
		if granted(v) {
			count++
		}
	}
	return count >= len(voters)/2+1
}

// committedIndex returns the highest index a majority of BOTH configurations
// has reached. Used by the leader's commit rule.
func (c config) committedIndex(match func(NodeID) Index) Index {
	n := quorumIndex(c.voters, match)
	if c.isJoint() {
		if o := quorumIndex(c.outgoing, match); o < n {
			n = o
		}
	}
	return n
}

// quorumIndex is the largest index a majority of voters has reached.
func quorumIndex(voters []NodeID, match func(NodeID) Index) Index {
	if len(voters) == 0 {
		return 0
	}
	idx := make([]Index, 0, len(voters))
	for _, v := range voters {
		idx = append(idx, match(v))
	}
	// Descending insertion sort; the slice is tiny and this keeps sort out of
	// the core's imports.
	for i := 1; i < len(idx); i++ {
		for j := i; j > 0 && idx[j] > idx[j-1]; j-- {
			idx[j], idx[j-1] = idx[j-1], idx[j]
		}
	}
	return idx[len(voters)/2]
}

// enter builds the joint configuration for a change.
func (c config) enter(cc ConfChange) config {
	next := append([]NodeID(nil), c.voters...)
	switch cc.Type {
	case ConfChangeAddNode:
		if !containsID(next, cc.NodeID) {
			next = append(next, cc.NodeID)
		}
	case ConfChangeRemoveNode:
		out := next[:0]
		for _, v := range next {
			if v != cc.NodeID {
				out = append(out, v)
			}
		}
		next = out
	}
	sortNodeIDs(next)

	return config{
		voters:   next,
		outgoing: append([]NodeID(nil), c.voters...),
	}
}

// leave drops the outgoing configuration, completing the transition.
func (c config) leave() config {
	return config{voters: append([]NodeID(nil), c.voters...)}
}

func containsID(ids []NodeID, id NodeID) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}
