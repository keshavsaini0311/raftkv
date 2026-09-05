package raft

// Ready is the set of intentions the core hands to its driver.
//
// The core performs no I/O. It says what it WANTS done; server/ or sim/ does
// it. The ordering contract is not optional:
//
//	The driver MUST persist HardState and Entries before sending Messages.
//
// That is Figure 2's durability rule. A node that replies to a RequestVote
// before its vote reaches disk can crash, restart having forgotten the vote,
// and vote a second time in the same term — two leaders, one term.
type Ready struct {
	// HardState is nil when nothing durable changed this round. Non-nil means
	// the driver must persist it before sending anything in Messages.
	//
	// Detecting "changed" is a single == because HardState is comparable
	// (every field numeric). That is why it was designed that way.
	HardState *HardState

	// Entries to append to stable storage. Empty in milestone 1 — no log yet.
	Entries []Entry

	// CommittedEntries are safe to apply to the state machine.
	// Empty in milestone 1.
	CommittedEntries []Entry

	// Messages to send to peers, AFTER the above is durable.
	Messages []Message
}

// IsEmpty reports whether there is nothing for the driver to do. A driver loop
// calls this to skip the persist/send/apply cycle on a tick where nothing
// happened, which is most ticks.
func (r Ready) IsEmpty() bool {
	return r.HardState == nil &&
		len(r.Entries) == 0 &&
		len(r.CommittedEntries) == 0 &&
		len(r.Messages) == 0
}
