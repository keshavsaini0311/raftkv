package sim

import (
	"fmt"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/keshavsaini0311/raftkv/kv"
)

// The model. Linearizability is checked against a specification, not against
// the implementation — the whole point is that the checker has no idea how
// raftkv works and only asks whether the observed history COULD have been
// produced by a single-threaded map.

type kvInput struct {
	Op    kv.Op
	Key   string
	Value string

	// Indeterminate marks an operation whose outcome the client never learned:
	// the node crashed, or the request never came back.
	//
	// Its OUTPUT must not be asserted on — there wasn't one — but its EFFECT
	// must still be considered, because the write may well have committed.
	// Dropping such operations entirely would hide the exact bug where a write
	// is acknowledged, apparently lost, and then reappears in a later read.
	Indeterminate bool
}

type kvOutput struct {
	Value string
	Found bool
}

// kvState is one key's value. Comparable, so porcupine's default equality
// works and identical states get merged during the search.
type kvState struct {
	Value string
	Found bool
}

// registerModel specifies a single key.
//
// Checking per-key rather than whole-map is not a shortcut: operations on
// different keys are genuinely independent, so a history is linearizable iff
// every per-key sub-history is. It turns an exponential search into several
// small ones, which is the difference between checking a 2,000-operation
// history in milliseconds and not being able to check it at all.
var registerModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := make(map[string][]porcupine.Operation)
		var order []string
		for _, op := range history {
			k := op.Input.(kvInput).Key
			if _, seen := byKey[k]; !seen {
				order = append(order, k) // preserve first-seen order; no map ranging
			}
			byKey[k] = append(byKey[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(order))
		for _, k := range order {
			out = append(out, byKey[k])
		}
		return out
	},

	// The Events API needs its own partitioner. Without it, event-based
	// checking runs one exponential search over the whole history instead of
	// several small independent ones — the difference between milliseconds and
	// a timeout.
	PartitionEvent: func(history []porcupine.Event) [][]porcupine.Event {
		keyOf := make(map[int]string) // operation id -> key, from its call event
		byKey := make(map[string][]porcupine.Event)
		var order []string

		for _, e := range history {
			var k string
			if e.Kind == porcupine.CallEvent {
				k = e.Value.(kvInput).Key
				keyOf[e.Id] = k
			} else {
				k = keyOf[e.Id]
			}
			if _, seen := byKey[k]; !seen {
				order = append(order, k)
			}
			byKey[k] = append(byKey[k], e)
		}

		out := make([][]porcupine.Event, 0, len(order))
		for _, k := range order {
			out = append(out, byKey[k])
		}
		return out
	},

	Init: func() interface{} { return kvState{} },

	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(kvState)
		in := input.(kvInput)
		out := output.(kvOutput)

		switch in.Op {
		case kv.OpGet:
			if in.Indeterminate {
				return true, st // no observation, no constraint, no effect
			}
			// A read must return exactly what the state holds. This is the
			// assertion that catches a stale read from an isolated leader.
			return out.Value == st.Value && out.Found == st.Found, st

		case kv.OpPut:
			// The output is never asserted on: a put's result carries no
			// information beyond "it happened", so an indeterminate put needs
			// no special case here — only a return time that lets the checker
			// place it freely.
			return true, kvState{Value: in.Value, Found: true}

		case kv.OpDelete:
			if in.Indeterminate {
				return true, kvState{}
			}
			return out.Found == st.Found, kvState{}
		}
		return false, st
	},

	DescribeOperation: func(input, output interface{}) string {
		in := input.(kvInput)
		out := output.(kvOutput)
		switch in.Op {
		case kv.OpGet:
			return fmt.Sprintf("get(%s) -> %q found=%v", in.Key, out.Value, out.Found)
		case kv.OpPut:
			return fmt.Sprintf("put(%s, %q)", in.Key, in.Value)
		default:
			return fmt.Sprintf("delete(%s) -> found=%v", in.Key, out.Found)
		}
	},

	DescribeState: func(state interface{}) string {
		st := state.(kvState)
		if !st.Found {
			return "<absent>"
		}
		return fmt.Sprintf("%q", st.Value)
	},
}

// toOperations converts the simulator's history for porcupine.
//
// Two decisions here are load-bearing, and both were wrong in earlier versions
// of this file — each producing a linearizability failure that looked exactly
// like a raft bug and was not one.
//
// TIME. Call and Return use the history's monotonic EVENT counter, not the
// virtual tick. Several operations can be invoked or completed within one tick,
// and equal timestamps force the checker to treat them as overlapping when the
// simulator actually ran them in a definite order. The event counter is that
// order.
//
// INCOMPLETE OPERATIONS. An operation the client never saw a result for is
// given a return time after every other event and marked Indeterminate. It is
// NOT given a fabricated output — encoding a pending read as "returned: not
// found" is a false claim about what the client observed, and porcupine is
// right to reject a history containing it. It is also NOT dropped: a write that
// may have committed still constrains everything after it.
//
// (Porcupine's Events API does not accept a call with no matching return —
// verified directly — so "leave the return out" is not an option.)
func toOperations(ops []*Op) []porcupine.Operation {
	// A return time strictly after everything real, distinct per operation so
	// no two indeterminate ops tie.
	var maxSeq uint64
	for _, op := range ops {
		if op.CallSeq > maxSeq {
			maxSeq = op.CallSeq
		}
		if op.ReturnSeq > maxSeq {
			maxSeq = op.ReturnSeq
		}
	}

	out := make([]porcupine.Operation, 0, len(ops))
	for i, op := range ops {
		in := kvInput{Op: op.Cmd.Op, Key: op.Key, Value: string(op.Cmd.Value)}

		ret := int64(op.ReturnSeq)
		outVal := kvOutput{Value: string(op.Result.Value), Found: op.Result.Found}

		if !op.Returned {
			in.Indeterminate = true
			ret = int64(maxSeq) + 1 + int64(i)
			outVal = kvOutput{}
		}

		out = append(out, porcupine.Operation{
			ClientId: int(op.Cmd.ClientID),
			Input:    in,
			Call:     int64(op.CallSeq),
			Output:   outVal,
			Return:   ret,
		})
	}
	return out
}

// The headline claim: the same seed produces the same run, forever.
//
// Without this every other test in this file is worthless, because a failure
// found at seed 42891 could not be reproduced and the counterexample would be
// unusable.
func TestSameSeedReplaysIdentically(t *testing.T) {
	const seed = 42891

	run := func() string {
		c, nem, wl := Run(DefaultConfig(seed, 5), Chaos(), DefaultWorkload(), 3000)
		s := fmt.Sprintf("%s|%s|issued=%d rejected=%d|", c.Stats(), nem.Summary(), wl.Issued, wl.Rejected)
		for _, op := range c.History().Ops() {
			s += op.String() + ";"
		}
		return s
	}

	first := run()
	for i := 0; i < 3; i++ {
		if got := run(); got != first {
			// Find where they diverge, so the report is usable.
			n := 0
			for n < len(first) && n < len(got) && first[n] == got[n] {
				n++
			}
			lo := n - 120
			if lo < 0 {
				lo = 0
			}
			t.Fatalf("run %d diverged at offset %d\n  first: ...%.200s\n  got:   ...%.200s",
				i, n, first[lo:], got[lo:])
		}
	}
	t.Logf("3 replays of seed %d were byte-identical", seed)
}

// Two different seeds must produce different runs. Otherwise the first test
// passes trivially because the simulator is not actually varying anything.
func TestDifferentSeedsDiverge(t *testing.T) {
	summary := func(seed int64) string {
		c, nem, _ := Run(DefaultConfig(seed, 5), Chaos(), DefaultWorkload(), 2000)
		return c.Stats() + "|" + nem.Summary()
	}
	if summary(1) == summary(2) {
		t.Error("seeds 1 and 2 produced identical runs; the seed is not reaching the simulation")
	}
}

// Raft's central invariant, checked continuously under faults.
func TestNoTwoLeadersInOneTerm(t *testing.T) {
	for _, seed := range []int64{1, 7, 42, 1337, 42891} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			c := New(DefaultConfig(seed, 5))
			nem := NewNemesis(Chaos(), c.rng)
			wl := NewWorkload(DefaultWorkload(), c.rng)

			for i := 0; i < 4000; i++ {
				nem.Step(c)
				c.Tick()
				wl.Step(c)

				// Checked EVERY tick, not just at the end. A split brain that
				// heals before the run finishes would be invisible otherwise,
				// and a transient one is still a correctness violation.
				if bad := LeaderSafetyViolations(c); len(bad) > 0 {
					t.Fatalf("tick %d: two leaders in one term: %v\n  %s\n  %s",
						i, bad, c.Stats(), nem.Summary())
				}
			}
			t.Logf("%s | %s", c.Stats(), nem.Summary())
		})
	}
}

// The main event: every client history is linearizable under partitions,
// crashes, restarts, message loss, duplication, and reordering.
func TestLinearizableUnderChaos(t *testing.T) {
	seeds := []int64{1, 2, 3, 7, 42, 99, 1337, 42891}
	if testing.Short() {
		seeds = seeds[:2]
	}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			cfg := DefaultConfig(seed, 5)
			cfg.Network = NetworkConfig{
				MinLatency: 1, MaxLatency: 4, // a range, so messages reorder
				DropRate:      0.02,
				DuplicateRate: 0.02,
			}

			c, nem, wl := Run(cfg, Chaos(), DefaultWorkload(), 4000)

			ops := toOperations(c.History().Ops())
			if len(ops) < 20 {
				t.Fatalf("only %d operations recorded; the run was too quiet to prove "+
					"anything (%s)", len(ops), wl.summary())
			}
			// A green run that injected no faults proves nothing. Assert the
			// nemesis actually did each kind of damage.
			if nem.Partitions == 0 || nem.Crashes == 0 || nem.ConfChanges == 0 {
				t.Fatalf("nemesis was too quiet: %s", nem.Summary())
			}

			res, info := porcupine.CheckOperationsVerbose(registerModel, ops, 30*time.Second)
			switch res {
			case porcupine.Ok:
				t.Logf("linearizable: %s | %s | %s",
					c.History().Summary(), nem.Summary(), c.Stats())
			case porcupine.Illegal:
				// A counterexample, reproducible forever from this seed.
				t.Errorf("HISTORY IS NOT LINEARIZABLE at seed %d\n  %s\n  %s\n  %s\n"+
					"  reproduce: go test -run 'TestLinearizableUnderChaos/seed=%d' ./sim/",
					seed, c.History().Summary(), nem.Summary(), c.Stats(), seed)
				dumpFailure(t, info)
			case porcupine.Unknown:
				t.Logf("checker timed out at seed %d (history too large to decide); "+
					"not a failure, but not a proof either", seed)
			}
		})
	}
}

// A control. If the calm run were to fail, the chaos results would be
// meaningless — you would be looking at a bug in the simulator, not in raft.
func TestLinearizableWithoutFaults(t *testing.T) {
	c, _, _ := Run(DefaultConfig(7, 3), Calm(), DefaultWorkload(), 1500)

	ops := toOperations(c.History().Ops())
	if len(ops) < 20 {
		t.Fatalf("only %d operations in a calm run; the workload is not working", len(ops))
	}
	if res, info := porcupine.CheckOperationsVerbose(registerModel, ops, 20*time.Second); res == porcupine.Illegal {
		t.Errorf("a fault-free run was not linearizable — this is a simulator or "+
			"model bug, not a raft bug\n  %s", c.History().Summary())
		dumpFailure(t, info)
	}
}

// The pathological case the design doc names: the leader alone on one side of a
// partition. It still believes it leads. Any read it serves from local state is
// stale, and ReadIndex is what must prevent that.
func TestIsolatedLeaderCannotServeStaleReads(t *testing.T) {
	c := New(DefaultConfig(2024, 5))

	// Elect, then write a known value.
	c.RunTicks(60)
	leader := c.Leader()
	if leader == 0 {
		t.Fatal("no leader after 60 ticks")
	}
	if _, ok := c.Propose(leader, kv.Command{
		Op: kv.OpPut, Key: "k", Value: []byte("v1"), ClientID: 1, Seq: 1,
	}); !ok {
		t.Fatal("initial write was rejected")
	}
	c.RunTicks(40)

	// Strand the leader.
	old, ok := c.IsolateLeader()
	if !ok {
		t.Fatal("could not isolate the leader")
	}

	// The majority elects a new leader and overwrites the key.
	c.RunTicks(200)
	newLeader := c.Leader()
	if newLeader == 0 || newLeader == old {
		t.Fatalf("majority did not elect a new leader (old=%d new=%d)", old, newLeader)
	}
	if _, ok := c.Propose(newLeader, kv.Command{
		Op: kv.OpPut, Key: "k", Value: []byte("v2"), ClientID: 2, Seq: 1,
	}); !ok {
		t.Fatal("the new leader rejected a write")
	}
	c.RunTicks(60)

	// Now ask the STRANDED old leader for the key. It must not answer.
	before := len(c.History().Ops())
	c.Read(old, "k")
	c.RunTicks(100)

	for _, op := range c.History().Ops()[before:] {
		if op.Returned && string(op.Result.Value) == "v1" {
			t.Errorf("the isolated leader served a stale read (%q) — ReadIndex "+
				"failed to confirm leadership before answering", op.Result.Value)
		}
	}
	t.Logf("old leader %d isolated; new leader %d; stranded read did not return stale data",
		old, newLeader)
}

func dumpFailure(t *testing.T, info porcupine.LinearizationInfo) {
	t.Helper()
	// Porcupine renders an interactive HTML view of exactly where the history
	// stopped being explainable — far more useful than a stack trace. It is
	// best-effort: a visualisation failure must not replace the real error
	// message with a panic from the reporting path.
	defer func() {
		if r := recover(); r != nil {
			t.Logf("visualisation panicked (%v); the failure above still stands", r)
		}
	}()
	path := fmt.Sprintf("/tmp/raftkv-linearizability-%d.html", time.Now().UnixNano())
	if err := porcupine.VisualizePath(registerModel, info, path); err != nil {
		t.Logf("could not write the visualisation: %v", err)
		return
	}
	t.Logf("counterexample visualisation written to %s", path)
}

func (w *Workload) summary() string {
	return fmt.Sprintf("issued=%d rejected=%d completed=%d", w.Issued, w.Rejected, w.Completed)
}

// Linearizability with compaction ACTIVE.
//
// Snapshotting is where "the follower fell behind and could never catch up"
// bugs live: entries are discarded, so a slow node can no longer be repaired
// incrementally and must be caught up by a whole-state transfer. Running the
// same chaos with a deliberately tiny snapshot threshold exercises that path
// constantly instead of never.
func TestLinearizableWithCompaction(t *testing.T) {
	for _, seed := range []int64{1, 2, 3, 7, 42} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			cfg := DefaultConfig(seed, 5)
			cfg.Network = NetworkConfig{MinLatency: 1, MaxLatency: 4, DropRate: 0.02, DuplicateRate: 0.02}
			cfg.SnapshotEvery = 20 // absurdly small, on purpose

			c, nem, _ := Run(cfg, Chaos(), DefaultWorkload(), 4000)

			if c.Snapshots == 0 {
				t.Fatal("no snapshots were taken; this test proves nothing")
			}
			if c.SnapshotsInstalled == 0 {
				t.Errorf("snapshots were taken (%d) but none were ever INSTALLED on a "+
					"follower, so the InstallSnapshot path went untested", c.Snapshots)
			}

			ops := toOperations(c.History().Ops())
			if len(ops) < 20 {
				t.Fatalf("only %d operations; the run was too quiet", len(ops))
			}

			res, info := porcupine.CheckOperationsVerbose(registerModel, ops, 30*time.Second)
			switch res {
			case porcupine.Ok:
				t.Logf("linearizable under compaction: %s | %s | %s",
					c.History().Summary(), nem.Summary(), c.Stats())
			case porcupine.Illegal:
				t.Errorf("NOT LINEARIZABLE with compaction at seed %d\n  %s\n  %s\n  %s",
					seed, c.History().Summary(), nem.Summary(), c.Stats())
				dumpFailure(t, info)
			case porcupine.Unknown:
				t.Logf("checker timed out at seed %d", seed)
			}
		})
	}
}
