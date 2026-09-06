// Package storage persists the parts of a raft node that must survive a crash.
//
// The interface exists so the core's durability contract can be satisfied by a
// real file in production and by an in-memory fake in tests, without the raft
// package knowing either exists.
package storage

import "github.com/keshavsaini0311/raftkv/raft"

// Storage is what a driver needs to make a Ready durable.
//
// Every method must return only after the data is on stable storage. Returning
// early — buffering, or skipping the fsync — silently breaks Raft's safety
// argument: a node that acknowledges a vote it has not durably recorded can
// restart, forget, and vote twice in one term.
type Storage interface {
	// SaveHardState persists term, vote, and commit index.
	SaveHardState(hs raft.HardState) error

	// Append persists log entries. Entries whose indices already exist are
	// overwritten, and everything after the last appended index is discarded —
	// this is how a follower's divergent tail is truncated on disk.
	Append(entries []raft.Entry) error

	// SaveSnapshot persists a snapshot and discards log entries it covers.
	SaveSnapshot(snap *raft.Snapshot) error

	// Load recovers everything written before the last clean or unclean stop.
	Load() (raft.HardState, []raft.Entry, *raft.Snapshot, error)

	Close() error
}

// Memory is an in-memory Storage for tests and the simulator.
//
// It is not a fake in the misleading sense: it enforces the same truncation
// semantics as the file implementation, so a test that passes against it is
// exercising the same contract.
type Memory struct {
	hs      raft.HardState
	entries []raft.Entry
	snap    *raft.Snapshot
}

func NewMemory() *Memory { return &Memory{} }

func (m *Memory) SaveHardState(hs raft.HardState) error {
	m.hs = hs
	return nil
}

func (m *Memory) Append(entries []raft.Entry) error {
	for _, e := range entries {
		// Truncate at the first index we already hold, then append. A
		// follower repairing a divergent tail rewrites indices it already has.
		idx := -1
		for i := range m.entries {
			if m.entries[i].Index == e.Index {
				idx = i
				break
			}
		}
		if idx >= 0 {
			m.entries = append(m.entries[:idx:idx], e)
		} else {
			m.entries = append(m.entries, e)
		}
	}
	return nil
}

func (m *Memory) SaveSnapshot(snap *raft.Snapshot) error {
	m.snap = snap
	kept := m.entries[:0:0]
	for _, e := range m.entries {
		if e.Index > snap.Metadata.Index {
			kept = append(kept, e)
		}
	}
	m.entries = kept
	return nil
}

func (m *Memory) Load() (raft.HardState, []raft.Entry, *raft.Snapshot, error) {
	out := make([]raft.Entry, len(m.entries))
	copy(out, m.entries)
	return m.hs, out, m.snap, nil
}

func (m *Memory) Close() error { return nil }
