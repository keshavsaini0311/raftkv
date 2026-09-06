package raft

// raftLog is the replicated log plus its commit/apply bookkeeping.
//
// Layout: entries[0] is a SENTINEL, not a real entry. It carries the index and
// term of the last entry that was compacted away into a snapshot, so that the
// consistency check at the boundary works without special-casing. Real entries
// live at entries[1:]. On a fresh log the sentinel is {Index: 0, Term: 0},
// which makes "the entry before index 1" a well-defined thing to compare
// against and removes an entire class of off-by-one at the head of the log.
//
// Raft indices are 1-based. Index 0 never holds a real command.
type raftLog struct {
	entries []Entry

	// committed: the highest index known to be replicated on a majority.
	// applied: the highest index handed to the state machine.
	// applied <= committed <= lastIndex always holds.
	committed Index
	applied   Index

	// stable is the highest index the driver has reported persisted. Entries
	// past it are "unstable": they exist in memory and must be handed to the
	// driver through Ready before any message that depends on them is sent.
	stable Index

	// snapshot is retained until the driver reports it persisted.
	snapshot *Snapshot
}

// unstable returns entries appended but not yet persisted by the driver.
func (l *raftLog) unstable() []Entry {
	if l.stable >= l.lastIndex() {
		return nil
	}
	return l.slice(l.stable + 1)
}

// stableTo records that the driver persisted through index i.
func (l *raftLog) stableTo(i Index) {
	if i > l.stable && i <= l.lastIndex() {
		l.stable = i
	}
}

// lastIndexOfTerm returns the highest index whose entry has the given term.
// Used by the leader to skip an entire conflicting term in one round trip
// instead of walking nextIndex back one entry at a time.
func (l *raftLog) lastIndexOfTerm(t Term) (Index, bool) {
	for i := len(l.entries) - 1; i >= 0; i-- {
		if l.entries[i].Term == t {
			return l.entries[i].Index, true
		}
		if l.entries[i].Term < t {
			break // terms are non-decreasing along the log
		}
	}
	return 0, false
}

func newLog() *raftLog {
	return &raftLog{entries: []Entry{{Index: 0, Term: 0}}}
}

// compactedIndex is the index of the sentinel: everything at or below it lives
// in a snapshot rather than in entries.
func (l *raftLog) compactedIndex() Index { return l.entries[0].Index }
func (l *raftLog) compactedTerm() Term   { return l.entries[0].Term }

func (l *raftLog) firstIndex() Index { return l.compactedIndex() + 1 }
func (l *raftLog) lastIndex() Index  { return l.entries[len(l.entries)-1].Index }
func (l *raftLog) lastTerm() Term    { return l.entries[len(l.entries)-1].Term }

// term returns the term of the entry at index i. ok is false when i has been
// compacted away or lies past the end of the log.
func (l *raftLog) term(i Index) (Term, bool) {
	base := l.compactedIndex()
	if i < base || i > l.lastIndex() {
		return 0, false
	}
	return l.entries[i-base].Term, true
}

// at returns the entry at index i.
func (l *raftLog) at(i Index) (Entry, bool) {
	base := l.compactedIndex()
	if i <= base || i > l.lastIndex() {
		return Entry{}, false
	}
	return l.entries[i-base], true
}

// matches reports whether the log contains an entry at index i whose term is t.
// This is the AppendEntries consistency check: if it holds, the two logs are
// identical in every entry up to i (Log Matching Property, §5.3).
func (l *raftLog) matches(i Index, t Term) bool {
	term, ok := l.term(i)
	return ok && term == t
}

// slice returns a COPY of the entries in [lo, lastIndex].
//
// A copy, not a view. These entries are handed to a driver through Ready and
// may still be in flight to disk when a conflicting AppendEntries arrives and
// truncates the log. Sharing the backing array would let that truncation
// scribble on bytes the driver is mid-write on — a corrupted log file whose
// contents disagree with memory, discovered thousands of ticks later.
func (l *raftLog) slice(lo Index) []Entry {
	if lo < l.firstIndex() {
		lo = l.firstIndex()
	}
	if lo > l.lastIndex() {
		return nil
	}
	base := l.compactedIndex()
	src := l.entries[lo-base:]
	out := make([]Entry, len(src))
	copy(out, src)
	return out
}

// append adds entries to the end of the log, stamping their index.
func (l *raftLog) append(term Term, ents ...Entry) []Entry {
	next := l.lastIndex() + 1
	added := make([]Entry, 0, len(ents))
	for _, e := range ents {
		e.Term = term
		e.Index = next
		next++
		l.entries = append(l.entries, e)
		added = append(added, e)
	}
	return added
}

// maybeAppend implements AppendEntries receiver rules 2 through 5.
//
// It returns the index of the last entry now in the log and true on success.
// On failure it returns a conflict hint: the index the leader should retry
// from, and the term of the conflicting entry (0 when the log is simply too
// short), which lets the leader skip a whole term per round trip.
func (l *raftLog) maybeAppend(prevIndex Index, prevTerm Term, ents []Entry) (last Index, conflictIndex Index, conflictTerm Term, ok bool) {
	// Rule 2: reject if we have no entry at prevIndex with term prevTerm.
	if !l.matches(prevIndex, prevTerm) {
		// The log is shorter than the leader assumes: tell it where we end.
		if prevIndex > l.lastIndex() {
			return 0, l.lastIndex() + 1, 0, false
		}
		// We have that index but a different term. Report the term we hold and
		// the first index of it, so the leader can back up past the whole run
		// in one step rather than one entry at a time.
		badTerm, hasTerm := l.term(prevIndex)
		if !hasTerm {
			// prevIndex is compacted; ask for everything after the snapshot.
			return 0, l.firstIndex(), 0, false
		}
		first := l.firstIndex()
		idx := prevIndex
		for idx > first {
			if t, _ := l.term(idx - 1); t != badTerm {
				break
			}
			idx--
		}
		return 0, idx, badTerm, false
	}

	// Rules 3 and 4: find the first entry that genuinely conflicts, truncate
	// there, and append the remainder.
	for i, e := range ents {
		if l.matches(e.Index, e.Term) {
			continue // already present and identical; not a conflict
		}
		if e.Index <= l.lastIndex() {
			// Same index, different term: delete this entry and all that
			// follow. Rule 3 is only correct at a GENUINE conflict — a stale
			// or duplicated AppendEntries must not truncate entries the
			// follower already accepted, which is why the loop skips matches
			// above instead of truncating at prevIndex unconditionally.
			l.truncateFrom(e.Index)
		}
		l.appendAt(ents[i:])
		break
	}
	return prevIndex + Index(len(ents)), 0, 0, true
}

// truncateFrom deletes the entry at index i and everything after it.
//
// The new slice is freshly allocated rather than resliced. l.entries[:k] would
// share its backing array with every slice previously returned from the log, so
// the next append would overwrite entries a driver may still be persisting.
func (l *raftLog) truncateFrom(i Index) {
	base := l.compactedIndex()
	if i <= base || i > l.lastIndex() {
		return
	}
	keep := l.entries[:i-base]
	fresh := make([]Entry, len(keep))
	copy(fresh, keep)
	l.entries = fresh

	// committed can never exceed what we still hold. In a correct Raft this
	// cannot fire — a committed entry is never truncated — but clamping here
	// turns a would-be silent corruption into a visible inconsistency.
	if l.committed > l.lastIndex() {
		l.committed = l.lastIndex()
	}
	if l.applied > l.committed {
		l.applied = l.committed
	}
}

// appendAt appends entries that already carry their own index and term.
func (l *raftLog) appendAt(ents []Entry) {
	for _, e := range ents {
		if e.Index <= l.lastIndex() {
			continue
		}
		l.entries = append(l.entries, e)
	}
}

// commitTo raises the commit index. It never lowers it: commitment is
// permanent, and a stale LeaderCommit arriving late must not un-commit.
func (l *raftLog) commitTo(i Index) {
	if i > l.committed {
		if i > l.lastIndex() {
			i = l.lastIndex()
		}
		l.committed = i
	}
}

// nextApplicable returns committed entries not yet applied.
func (l *raftLog) nextApplicable() []Entry {
	if l.applied >= l.committed {
		return nil
	}
	lo := l.applied + 1
	if lo < l.firstIndex() {
		lo = l.firstIndex()
	}
	base := l.compactedIndex()
	if l.committed <= base {
		return nil
	}
	src := l.entries[lo-base : l.committed-base+1]
	out := make([]Entry, len(src))
	copy(out, src)
	return out
}

// appliedTo records that the state machine consumed up to index i.
func (l *raftLog) appliedTo(i Index) {
	if i > l.applied && i <= l.committed {
		l.applied = i
	}
}

// isUpToDate implements the §5.4.1 election restriction.
//
// Term first: a longer log is not a better log, because the extra entries may
// be uncommitted garbage from a leader that was partitioned before reaching a
// majority. An entry from a higher term was accepted more recently by a cluster
// that had moved on. Length only breaks ties within the same term.
//
// >= on the index, not >: an identical log is electable. With > a cluster whose
// nodes all agree could never elect anyone.
func (l *raftLog) isUpToDate(lastIndex Index, lastTerm Term) bool {
	return lastTerm > l.lastTerm() ||
		(lastTerm == l.lastTerm() && lastIndex >= l.lastIndex())
}

// compact discards entries up to and including i, replacing them with a
// sentinel. Used by milestone 5 after a snapshot is persisted.
func (l *raftLog) compact(i Index, term Term) {
	if i <= l.compactedIndex() || i > l.lastIndex() {
		return
	}
	base := l.compactedIndex()
	rest := l.entries[i-base+1:]

	fresh := make([]Entry, 0, len(rest)+1)
	fresh = append(fresh, Entry{Index: i, Term: term})
	fresh = append(fresh, rest...)
	l.entries = fresh
}

// restore replaces the entire log with a snapshot's boundary. Everything the
// node held is discarded: the snapshot came from a leader whose log is
// authoritative, so local entries past it are by definition uncommitted.
func (l *raftLog) restore(s *Snapshot) {
	l.entries = []Entry{{Index: s.Metadata.Index, Term: s.Metadata.Term}}
	l.committed = s.Metadata.Index
	l.applied = s.Metadata.Index
	l.snapshot = s
}
