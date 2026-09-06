package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/keshavsaini0311/raftkv/raft"
)

// Both implementations must satisfy the same contract. Running one table
// against both is the point of having the interface at all: a test that passes
// against Memory is exercising the same semantics the file must provide.
func eachStorage(t *testing.T, fn func(t *testing.T, s Storage)) {
	t.Helper()

	t.Run("memory", func(t *testing.T) {
		fn(t, NewMemory())
	})

	t.Run("file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "raft.log")
		s, err := Open(path)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer s.Close()
		fn(t, s)
	})
}

func TestHardStateRoundTrip(t *testing.T) {
	eachStorage(t, func(t *testing.T, s Storage) {
		want := raft.HardState{Term: 7, VotedFor: 3, Commit: 12}
		if err := s.SaveHardState(want); err != nil {
			t.Fatalf("save: %v", err)
		}
		got, _, _, err := s.Load()
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if got != want {
			t.Errorf("hard state = %+v, want %+v", got, want)
		}
	})
}

func TestEntriesRoundTrip(t *testing.T) {
	eachStorage(t, func(t *testing.T, s Storage) {
		want := []raft.Entry{
			{Index: 1, Term: 1, Data: []byte("a")},
			{Index: 2, Term: 1, Data: []byte("b")},
			{Index: 3, Term: 2, Data: []byte("c")},
		}
		if err := s.Append(want); err != nil {
			t.Fatalf("append: %v", err)
		}
		_, got, _, err := s.Load()
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if len(got) != len(want) {
			t.Fatalf("loaded %d entries, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i].Index != want[i].Index || got[i].Term != want[i].Term ||
				string(got[i].Data) != string(want[i].Data) {
				t.Errorf("entry %d = %+v, want %+v", i, got[i], want[i])
			}
		}
	})
}

// Appending at an index that already exists must truncate everything after it.
//
// This is how a follower's divergent tail is repaired on disk. Storage that
// merely appended would replay both the old and new entry at that index on
// recovery, and the node would come back with a log that never existed.
func TestAppendAtExistingIndexTruncates(t *testing.T) {
	eachStorage(t, func(t *testing.T, s Storage) {
		if err := s.Append([]raft.Entry{
			{Index: 1, Term: 1, Data: []byte("a")},
			{Index: 2, Term: 1, Data: []byte("b")},
			{Index: 3, Term: 1, Data: []byte("c")},
		}); err != nil {
			t.Fatalf("append: %v", err)
		}

		// A new leader overwrites index 2 with a different term.
		if err := s.Append([]raft.Entry{
			{Index: 2, Term: 5, Data: []byte("B")},
		}); err != nil {
			t.Fatalf("append: %v", err)
		}

		_, got, _, err := s.Load()
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("loaded %d entries, want 2 — entry 3 should have been truncated: %+v", len(got), got)
		}
		if got[1].Term != 5 || string(got[1].Data) != "B" {
			t.Errorf("entry 2 = %+v, want term 5 / B", got[1])
		}
	})
}

func TestSnapshotDiscardsCoveredEntries(t *testing.T) {
	eachStorage(t, func(t *testing.T, s Storage) {
		s.Append([]raft.Entry{
			{Index: 1, Term: 1}, {Index: 2, Term: 1},
			{Index: 3, Term: 1}, {Index: 4, Term: 2},
		})
		snap := &raft.Snapshot{
			Metadata: raft.SnapshotMetadata{Index: 3, Term: 1},
			Data:     []byte(`{"data":{}}`),
		}
		if err := s.SaveSnapshot(snap); err != nil {
			t.Fatalf("save snapshot: %v", err)
		}

		_, ents, got, err := s.Load()
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if got == nil || got.Metadata.Index != 3 {
			t.Fatalf("snapshot = %+v, want index 3", got)
		}
		if len(ents) != 1 || ents[0].Index != 4 {
			t.Errorf("entries = %+v, want only index 4 (the rest are in the snapshot)", ents)
		}
	})
}

// A crash mid-write leaves a partial final line. Recovery must discard it and
// keep everything before, not refuse to start: an unclean shutdown is a normal
// event, and failing to boot would turn a survivable crash into an outage.
func TestFileRecoversFromATruncatedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.log")

	s, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	s.SaveHardState(raft.HardState{Term: 3, VotedFor: 1})
	s.Append([]raft.Entry{{Index: 1, Term: 3, Data: []byte("committed")}})
	s.Close()

	// Simulate the crash: append a half-written record.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	f.WriteString(`{"kind":"entry","entry":{"Index":2,"Te`)
	f.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer s2.Close()

	hs, ents, _, err := s2.Load()
	if err != nil {
		t.Fatalf("load after crash: %v", err)
	}
	if hs.Term != 3 || hs.VotedFor != 1 {
		t.Errorf("hard state = %+v, want term 3 vote 1 — durable state was lost", hs)
	}
	if len(ents) != 1 || string(ents[0].Data) != "committed" {
		t.Errorf("entries = %+v, want the one complete entry", ents)
	}
}

func TestEmptyStorageLoadsCleanly(t *testing.T) {
	eachStorage(t, func(t *testing.T, s Storage) {
		hs, ents, snap, err := s.Load()
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if !hs.IsEmpty() || len(ents) != 0 || snap != nil {
			t.Errorf("fresh storage returned hs=%+v ents=%d snap=%v", hs, len(ents), snap)
		}
	})
}
