// Package lab is the interactive simulator the browser drives.
//
// It lives here rather than inside cmd/raftwasm because cmd/raftwasm only
// builds under GOOS=js, and untestable code is where the bugs go to hide. Two
// were already found here by hand: a read that forked the timeline, and a
// rewind that landed on the wrong tick. Both are regression tests now, and
// neither could have been written against a js-only package.
package lab

import (
	"encoding/json"
	"fmt"

	"github.com/keshavsaini0311/raftkv/kv"
	"github.com/keshavsaini0311/raftkv/raft"
	"github.com/keshavsaini0311/raftkv/sim"
)

// lab is the whole mutable state of the page. Single-threaded by construction:
// WASM calls arrive on the JS event loop, one at a time, so there is nothing
// to lock — the same reason server/ gives the core its own goroutine.
type Lab struct {
	c     *sim.Cluster
	seed  int64
	n     int
	every int

	// log is what the operator has done and what the cluster did back. Kept
	// bounded so a long session cannot grow without limit.
	log []string

	clientSeq map[uint64]uint64 // per-key client sequence, for exactly-once puts
	pending   []*sim.Op

	// ops is every command that shaped this timeline, in order. Rewinding
	// replays a PREFIX of it from a fresh cluster rather than restoring a
	// snapshot — which is possible only because the simulator is deterministic,
	// and is far cheaper than deep-copying five raft nodes every tick.
	//
	// It is also exact. A snapshot can be subtly incomplete; a replay of the
	// same seed and the same commands cannot be, because it is the same run.
	ops []command

	ticks int // ticks applied so far: the cursor's position on the timeline
	max   int // furthest tick reached, so the scrubber knows its range
}

const maxLog = 200

func (l *Lab) note(format string, args ...any) {
	l.log = append(l.log, fmt.Sprintf("t%-5d %s", l.c.Now(), fmt.Sprintf(format, args...)))
	if len(l.log) > maxLog {
		l.log = l.log[len(l.log)-maxLog:]
	}
}

func (l *Lab) reset(seed int64, nodes int, snapshotEvery int) {
	l.seed, l.n, l.every = seed, nodes, snapshotEvery
	l.ops = nil
	l.max = 0
	l.rebuild(nil)
	l.note("cluster of %d created, seed %d", nodes, seed)
}

// rebuild starts from nothing and replays ops. Everything derived — the event
// log, in-flight client operations, client sequence numbers — is rebuilt too,
// so a rewound timeline reads exactly as it did the first time through.
func (l *Lab) rebuild(ops []command) {
	cfg := sim.DefaultConfig(l.seed, l.n)
	cfg.SnapshotEvery = l.every
	l.c = sim.New(cfg)
	l.log = nil
	l.clientSeq = map[uint64]uint64{}
	l.pending = nil
	l.ticks = 0
	for _, op := range ops {
		l.exec(op)
	}
}

// prefix returns the ops that produce exactly n ticks, splitting the tick
// command that straddles the boundary.
//
// Splitting rather than rounding is the whole point: "rewind to tick 137" has
// to mean tick 137, not the nearest command boundary, or stepping back one
// tick from 137 would land somewhere unpredictable.
func (l *Lab) prefix(n int) []command {
	out := make([]command, 0, len(l.ops))
	count := 0
	for _, op := range l.ops {
		if op.Do != "tick" {
			out = append(out, op)
			continue
		}
		k := op.N
		if k < 1 {
			k = 1
		}
		if count+k <= n {
			out = append(out, op)
			count += k
			continue
		}
		if rest := n - count; rest > 0 {
			out = append(out, command{Do: "tick", N: rest})
		}
		return out
	}
	return out
}

// seek moves the cursor. Non-destructive: the future is still there to scrub
// forward into, until an action rewrites it.
func (l *Lab) seek(n int) {
	if n < 0 {
		n = 0
	}
	if n > l.max {
		n = l.max
	}
	l.rebuild(l.prefix(n))
}

// command is everything the page can ask for. One entry point rather than a
// dozen exported functions: the JS side has one call to get right, and adding
// an action does not change the bridge.
type command struct {
	Do    string        `json:"do"`
	N     int           `json:"n"`
	ID    uint64        `json:"id"`
	IDs   []raft.NodeID `json:"ids"`
	Key   string        `json:"key"`
	Value string        `json:"value"`
	Seed  int64         `json:"seed"`
	Nodes int           `json:"nodes"`
	Drop  float64       `json:"drop"`
	Dup   float64       `json:"dup"`
	Every int           `json:"every"`
}

// mutating lists every command that changes the cluster and therefore belongs
// in the replayable history.
//
// An allowlist, not a denylist. Anything unrecognised — including the empty
// command the page sends to read state without doing anything — must be a pure
// read. Recorded instead, a read would append a no-op to the timeline and,
// while rewound, fork it: merely LOOKING at the past would destroy the future.
var mutating = map[string]bool{
	"tick": true, "crash": true, "restart": true, "isolate": true,
	"split": true, "heal": true, "campaign": true, "put": true,
	"del": true, "read": true, "add": true, "remove": true, "net": true,
}

// apply records a command and runs it. exec runs one without recording, which
// is what replay needs.
func (l *Lab) apply(cmd command) {
	switch cmd.Do {
	case "reset":
		nodes := cmd.Nodes
		if nodes < 1 {
			nodes = l.n
		}
		l.reset(cmd.Seed, nodes, cmd.Every)
		return
	case "seek":
		l.seek(cmd.N)
		return
	}

	if !mutating[cmd.Do] {
		return // a read, or nothing at all
	}

	// Acting while rewound forks the timeline: what used to follow this point
	// never happened. Keeping it would leave the page showing a future that
	// the commands in front of it no longer produce.
	if l.ticks < l.max {
		l.ops = l.prefix(l.ticks)
		l.max = l.ticks
		l.note("timeline forked here — what followed has been discarded")
	}

	l.ops = append(l.ops, cmd)
	l.exec(cmd)
	if l.ticks > l.max {
		l.max = l.ticks
	}
}

func (l *Lab) exec(cmd command) {
	switch cmd.Do {
	case "tick":
		n := cmd.N
		if n < 1 {
			n = 1
		}
		for i := 0; i < n; i++ {
			l.c.Tick()
			l.drainPending()
		}
		l.ticks += n

	case "crash":
		l.c.Crash(raft.NodeID(cmd.ID))
		l.note("n%d crashed — volatile state lost, disk kept", cmd.ID)

	case "restart":
		l.c.Restart(raft.NodeID(cmd.ID))
		l.note("n%d restarted from its persisted log", cmd.ID)

	case "isolate":
		l.c.Isolate(raft.NodeID(cmd.ID))
		l.note("n%d isolated from every peer", cmd.ID)

	case "split":
		l.c.SplitAt(cmd.IDs)
		if len(cmd.IDs) == 0 {
			l.note("network healed")
		} else {
			l.note("network split: %v on one side", cmd.IDs)
		}

	case "heal":
		l.c.Heal()
		l.note("network healed")

	case "campaign":
		if l.c.Campaign(raft.NodeID(cmd.ID)) {
			l.note("n%d timed out and started an election", cmd.ID)
		} else {
			l.note("n%d cannot campaign right now", cmd.ID)
		}

	case "put":
		l.propose(cmd.Key, cmd.Value, false)

	case "del":
		l.propose(cmd.Key, "", true)

	case "read":
		leader := l.c.Leader()
		if leader == 0 {
			l.note("read %q refused: no leader", cmd.Key)
			return
		}
		if op, ok := l.c.Read(leader, cmd.Key); ok {
			l.pending = append(l.pending, op)
			l.note("read %q via n%d — ReadIndex registered, waiting on a quorum", cmd.Key, leader)
		} else {
			l.note("read %q refused by n%d", cmd.Key, leader)
		}

	case "add", "remove":
		if err := l.c.ConfChange(cmd.Do == "add", raft.NodeID(cmd.ID)); err != nil {
			l.note("%s n%d refused: %v", cmd.Do, cmd.ID, err)
		} else {
			l.note("%s n%d proposed — enters joint consensus first", cmd.Do, cmd.ID)
		}

	case "net":
		l.c.SetLossy(cmd.Drop, cmd.Dup)
		l.note("network set to %.0f%% loss, %.0f%% duplication", cmd.Drop*100, cmd.Dup*100)

	}
}

// propose submits a write through whoever currently leads.
//
// Client sequence numbers are per KEY here rather than per client, which is
// enough to make a retry of the same write idempotent — the property the
// session table exists to provide.
func (l *Lab) propose(key, value string, del bool) {
	leader := l.c.Leader()
	if leader == 0 {
		l.note("write to %q refused: no leader to accept it", key)
		return
	}
	cid := uint64(1)
	l.clientSeq[cid]++
	op := kv.OpPut
	if del {
		op = kv.OpDelete
	}
	o, ok := l.c.Propose(leader, kv.Command{
		Op: op, Key: key, Value: []byte(value),
		ClientID: kv.ClientID(cid), Seq: l.clientSeq[cid],
	})
	if !ok {
		l.note("write to %q refused by n%d", key, leader)
		return
	}
	l.pending = append(l.pending, o)
	if del {
		l.note("del %q proposed to n%d — appended, not yet committed", key, leader)
	} else {
		l.note("put %s=%s proposed to n%d — appended, not yet committed", key, value, leader)
	}
}

// drainPending reports client operations that finished this tick.
//
// An ABANDONED operation is not a bug and is worth showing plainly: it means
// the entry was truncated by a new leader, or the node serving it went away.
// The client is told nothing rather than told it succeeded, which is the
// correct answer and the one people find surprising.
func (l *Lab) drainPending() {
	keep := l.pending[:0]
	for _, op := range l.pending {
		switch {
		case op.Returned:
			if op.Cmd.Op == kv.OpGet {
				if op.Result.Found {
					l.note("read %q = %s", op.Key, op.Result.Value)
				} else {
					l.note("read %q — not found", op.Key)
				}
			} else {
				l.note("%s %q committed and applied", op.Cmd.Op, op.Key)
			}
		case op.Abandoned:
			l.note("%s %q abandoned — the entry did not survive; the client is told nothing",
				op.Cmd.Op, op.Key)
		default:
			keep = append(keep, op)
		}
	}
	l.pending = keep
}

// payload is the view plus where the cursor sits on the timeline. Embedding
// inlines sim.View's fields, so the page sees one flat object.
type payload struct {
	sim.View
	Max  int   `json:"max"`
	Live bool  `json:"live"`
	Seed int64 `json:"seed"`
}

// New starts a lab with a default cluster.
func New(seed int64, nodes int) *Lab {
	l := &Lab{}
	l.reset(seed, nodes, 0)
	return l
}

// Do applies one JSON command and returns the resulting view as JSON. One
// entry point in, one blob out: the page never reasons about partial updates.
func (l *Lab) Do(raw string) string {
	if raw != "" {
		var cmd command
		if err := json.Unmarshal([]byte(raw), &cmd); err != nil {
			l.note("bad command: %v", err)
		} else {
			l.apply(cmd)
		}
	}
	out, err := json.Marshal(payload{
		View: l.c.View(l.log, 40),
		Max:  l.max,
		Live: l.ticks >= l.max,
		Seed: l.seed,
	})
	if err != nil {
		return `{"error":"encoding view"}`
	}
	return string(out)
}
