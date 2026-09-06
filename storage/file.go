package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/keshavsaini0311/raftkv/raft"
)

// recordKind tags each line of the log file.
type recordKind string

const (
	kindHardState recordKind = "hardstate"
	kindEntry     recordKind = "entry"
	kindSnapshot  recordKind = "snapshot"
)

// record is one line of the append-only file.
//
// JSON lines rather than length-prefixed protobuf, following the design doc's
// open question: during a partition-induced debugging session, being able to
// `tail -f` the log and read it is worth more than the bytes.
type record struct {
	Kind      recordKind      `json:"kind"`
	HardState *raft.HardState `json:"hardstate,omitempty"`
	Entry     *raft.Entry     `json:"entry,omitempty"`
	Snapshot  *raft.Snapshot  `json:"snapshot,omitempty"`
}

// File is an append-only, fsync-on-write Storage.
//
// Append-only because it is the simplest structure that is crash-safe: a
// partial write at the tail is detectable and discardable on recovery, and
// nothing already written is ever mutated in place. Truncation is expressed by
// appending an entry at an index that already exists; recovery replays the file
// and lets later records win.
type File struct {
	path string
	f    *os.File
	w    *bufio.Writer
}

// Open opens or creates the log at path.
func Open(path string) (*File, error) {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("creating %s: %w", dir, err)
		}
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	return &File{path: path, f: f, w: bufio.NewWriter(f)}, nil
}

// write appends one record and fsyncs.
//
// The fsync is the entire point of this type. Without it the OS may hold the
// write in page cache for seconds, and a power loss in that window means a node
// that acknowledged a vote or an entry it no longer has — precisely the
// scenario Raft's durability rule exists to prevent. It is slow on purpose.
func (s *File) write(r record) error {
	b, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("encoding %s record: %w", r.Kind, err)
	}
	if _, err := s.w.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("buffering %s record: %w", r.Kind, err)
	}
	if err := s.w.Flush(); err != nil {
		return fmt.Errorf("flushing %s record: %w", r.Kind, err)
	}
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("fsync after %s record: %w", r.Kind, err)
	}
	return nil
}

func (s *File) SaveHardState(hs raft.HardState) error {
	return s.write(record{Kind: kindHardState, HardState: &hs})
}

func (s *File) Append(entries []raft.Entry) error {
	for i := range entries {
		if err := s.write(record{Kind: kindEntry, Entry: &entries[i]}); err != nil {
			return err
		}
	}
	return nil
}

func (s *File) SaveSnapshot(snap *raft.Snapshot) error {
	return s.write(record{Kind: kindSnapshot, Snapshot: snap})
}

// Load replays the file.
//
// A truncated final line is treated as a write that never completed and is
// discarded, not an error: a crash mid-fsync is a normal thing to recover
// from, and refusing to start would turn a survivable crash into an outage.
func (s *File) Load() (raft.HardState, []raft.Entry, *raft.Snapshot, error) {
	var (
		hs   raft.HardState
		ents []raft.Entry
		snap *raft.Snapshot
	)

	f, err := os.Open(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return hs, nil, nil, nil
		}
		return hs, nil, nil, fmt.Errorf("reopening %s: %w", s.path, err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)

	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var r record
		if err := json.Unmarshal(line, &r); err != nil {
			// A malformed line can only be the tail of an interrupted write,
			// because every complete write was fsynced before the next began.
			// Stop here and keep everything before it.
			break
		}
		switch r.Kind {
		case kindHardState:
			if r.HardState != nil {
				hs = *r.HardState
			}
		case kindEntry:
			if r.Entry != nil {
				ents = applyEntry(ents, *r.Entry)
			}
		case kindSnapshot:
			if r.Snapshot != nil {
				snap = r.Snapshot
				kept := ents[:0:0]
				for _, e := range ents {
					if e.Index > snap.Metadata.Index {
						kept = append(kept, e)
					}
				}
				ents = kept
			}
		}
	}
	if err := sc.Err(); err != nil && err != io.EOF {
		return hs, ents, snap, fmt.Errorf("scanning %s: %w", s.path, err)
	}
	return hs, ents, snap, nil
}

// applyEntry replays one entry record, honouring truncation: an entry at an
// index we already hold replaces it and discards everything after.
func applyEntry(ents []raft.Entry, e raft.Entry) []raft.Entry {
	for i := range ents {
		if ents[i].Index == e.Index {
			return append(ents[:i:i], e)
		}
	}
	return append(ents, e)
}

func (s *File) Close() error {
	if err := s.w.Flush(); err != nil {
		s.f.Close()
		return err
	}
	return s.f.Close()
}
