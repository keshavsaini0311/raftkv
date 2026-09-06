package raft

// Ready is the set of intentions the core hands to its driver.
//
// The ordering contract is not optional:
//
//	persist HardState and Entries  ->  send Messages  ->  apply CommittedEntries
//
// A node that replies to a RequestVote before its vote reaches disk can crash,
// restart having forgotten the vote, and vote a second time in the same term.
// Two leaders, one term.
type Ready struct {
	// HardState is nil when nothing durable changed. Non-nil means persist it
	// before sending anything in Messages.
	HardState *HardState

	// Entries to append to stable storage.
	Entries []Entry

	// Snapshot to persist and hand to the state machine, replacing its state.
	Snapshot *Snapshot

	// CommittedEntries are safe to apply to the state machine, in order.
	CommittedEntries []Entry

	// Messages to send to peers, after the above is durable.
	Messages []Message

	// SoftState is observational only: never persisted, safe to ignore.
	// Drivers use it to notice leadership changes.
	Lead NodeID
	Role Role
}

// IsEmpty reports whether there is nothing for the driver to do. Most ticks.
func (r Ready) IsEmpty() bool {
	return r.HardState == nil &&
		r.Snapshot.IsEmpty() &&
		len(r.Entries) == 0 &&
		len(r.CommittedEntries) == 0 &&
		len(r.Messages) == 0
}
