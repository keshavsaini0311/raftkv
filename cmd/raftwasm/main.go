//go:build js && wasm

// Command raftwasm exposes the simulator to a browser.
//
// The raft core compiles to WebAssembly unmodified — no clock, no goroutines,
// no I/O, no randomness of its own. That is not a coincidence: it is the
// property TestRaftCoreStaysPure enforces, and this is what it buys. The page
// is driving the SAME code the tests drive, not a JavaScript retelling of it.
package main

import (
	"encoding/json"
	"fmt"
	"syscall/js"

	"github.com/keshavsaini0311/raftkv/kv"
	"github.com/keshavsaini0311/raftkv/raft"
	"github.com/keshavsaini0311/raftkv/sim"
)

// lab is the whole mutable state of the page. Single-threaded by construction:
// WASM calls arrive on the JS event loop, one at a time, so there is nothing
// to lock — the same reason server/ gives the core its own goroutine.
type lab struct {
	c    *sim.Cluster
	seed int64
	n    int

	// log is what the operator has done and what the cluster did back. Kept
	// bounded so a long session cannot grow without limit.
	log []string

	clientSeq map[uint64]uint64 // per-key client sequence, for exactly-once puts
	pending   []*sim.Op
}

const maxLog = 200

func (l *lab) note(format string, args ...any) {
	l.log = append(l.log, fmt.Sprintf("t%-5d %s", l.c.Now(), fmt.Sprintf(format, args...)))
	if len(l.log) > maxLog {
		l.log = l.log[len(l.log)-maxLog:]
	}
}

func (l *lab) reset(seed int64, nodes int, snapshotEvery int) {
	cfg := sim.DefaultConfig(seed, nodes)
	cfg.SnapshotEvery = snapshotEvery
	l.c = sim.New(cfg)
	l.seed, l.n = seed, nodes
	l.log = nil
	l.clientSeq = map[uint64]uint64{}
	l.pending = nil
	l.note("cluster of %d created, seed %d", nodes, seed)
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

func (l *lab) apply(cmd command) {
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

	case "reset":
		nodes := cmd.Nodes
		if nodes < 1 {
			nodes = l.n
		}
		l.reset(cmd.Seed, nodes, cmd.Every)
	}
}

// propose submits a write through whoever currently leads.
//
// Client sequence numbers are per KEY here rather than per client, which is
// enough to make a retry of the same write idempotent — the property the
// session table exists to provide.
func (l *lab) propose(key, value string, del bool) {
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
func (l *lab) drainPending() {
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

func main() {
	l := &lab{}
	l.reset(1, 5, 0)

	// One exported function. It takes a JSON command and returns the whole
	// view as JSON: the page never has to reason about partial updates.
	js.Global().Set("raftAct", js.FuncOf(func(this js.Value, args []js.Value) any {
		if len(args) > 0 {
			var cmd command
			if err := json.Unmarshal([]byte(args[0].String()), &cmd); err != nil {
				l.note("bad command: %v", err)
			} else {
				l.apply(cmd)
			}
		}
		out, err := json.Marshal(l.c.View(l.log, 40))
		if err != nil {
			return `{"error":"encoding view"}`
		}
		return string(out)
	}))

	js.Global().Set("raftReady", js.ValueOf(true))
	select {} // keep the module alive; the page drives everything from here
}
