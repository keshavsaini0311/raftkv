package sim

import (
	"fmt"
	"sync"

	"github.com/keshavsaini0311/raftkv/kv"
	"github.com/keshavsaini0311/raftkv/raft"
)

// Op is one client operation as observed from outside the system.
//
// Linearizability is a property of this record, not of the implementation. An
// operation takes effect at some single instant between its invocation and its
// return; a history is linearizable if there EXISTS an ordering of those
// instants, consistent with real time, that a single-threaded store could have
// produced.
type Op struct {
	Node raft.NodeID
	Cmd  kv.Command
	Key  string

	CallAt   uint64 // virtual tick of invocation
	ReturnAt uint64 // virtual tick of response

	// CallSeq and ReturnSeq are a global monotonic counter over EVENTS, not
	// ticks. Several operations can be invoked or completed within one tick,
	// and a shared timestamp would force the checker to guess whether they
	// overlapped. The simulator is sequential, so the order events were
	// recorded IS the real order — recording it removes the ambiguity instead
	// of papering over it with a tie-break rule.
	CallSeq   uint64
	ReturnSeq uint64

	Result kv.Result

	// Abandoned marks an operation whose outcome the client never learned —
	// the node crashed, or the request timed out.
	//
	// These are NOT errors to be discarded. An abandoned write may or may not
	// have committed, so a correct checker must consider both possibilities.
	// Dropping them would hide exactly the bug where an acknowledged-then-lost
	// write reappears later.
	Abandoned bool
	Returned  bool
}

func (o *Op) String() string {
	kind := o.Cmd.Op.String()
	if o.Abandoned {
		return fmt.Sprintf("%s(%s) node=%d [%d..?] ABANDONED", kind, o.Key, o.Node, o.CallAt)
	}
	return fmt.Sprintf("%s(%s)=%q found=%v node=%d [%d..%d]",
		kind, o.Key, o.Result.Value, o.Result.Found, o.Node, o.CallAt, o.ReturnAt)
}

// History records every client operation with its virtual-time interval.
type History struct {
	mu  sync.Mutex // the simulator is single-threaded; this guards external readers
	ops []*Op
	seq uint64
}

func NewHistory() *History { return &History{} }

// Invoke records the start of an operation.
func (h *History) Invoke(node raft.NodeID, cmd kv.Command, now uint64) *Op {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seq++
	op := &Op{Node: node, Cmd: cmd, Key: cmd.Key, CallAt: now, CallSeq: h.seq}
	h.ops = append(h.ops, op)
	return op
}

// Return records a completed operation.
func (h *History) Return(op *Op, res kv.Result, now uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seq++
	op.Result = res
	op.ReturnAt = now
	op.ReturnSeq = h.seq
	op.Returned = true
}

// Abandon marks an operation whose result the client never saw.
func (h *History) Abandon(op *Op) {
	h.mu.Lock()
	defer h.mu.Unlock()
	op.Abandoned = true
}

// Ops returns every recorded operation.
func (h *History) Ops() []*Op {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]*Op, len(h.ops))
	copy(out, h.ops)
	return out
}

// Completed returns only operations the client actually saw a result for.
func (h *History) Completed() []*Op {
	var out []*Op
	for _, op := range h.Ops() {
		if op.Returned && !op.Abandoned {
			out = append(out, op)
		}
	}
	return out
}

// Summary is a one-line description for test output.
func (h *History) Summary() string {
	ops := h.Ops()
	var done, abandoned, reads, writes int
	for _, op := range ops {
		switch {
		case op.Abandoned:
			abandoned++
		case op.Returned:
			done++
		}
		if op.Cmd.Op == kv.OpGet {
			reads++
		} else {
			writes++
		}
	}
	return fmt.Sprintf("%d ops (%d completed, %d abandoned; %d reads, %d writes)",
		len(ops), done, abandoned, reads, writes)
}
