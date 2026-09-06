package sim

import (
	"fmt"
	"math/rand"

	"github.com/keshavsaini0311/raftkv/kv"
	"github.com/keshavsaini0311/raftkv/raft"
)

// WorkloadConfig describes the client traffic a run generates.
type WorkloadConfig struct {
	// Clients is the number of logical clients. Each has its own session, so
	// each gets exactly-once semantics independently.
	Clients int

	// Keys is the key space. A small space is deliberate: contention on the
	// same key is what makes a history interesting to check. Spread the writes
	// across a thousand keys and every operation is trivially independent, and
	// the checker proves nothing.
	Keys int

	// OpProb is the chance per client per tick of issuing an operation.
	OpProb float64

	// ReadRatio is the fraction of operations that are reads.
	ReadRatio float64
}

func DefaultWorkload() WorkloadConfig {
	return WorkloadConfig{Clients: 3, Keys: 3, OpProb: 0.08, ReadRatio: 0.4}
}

// client is one logical client with its own session.
type client struct {
	id  kv.ClientID
	seq uint64

	// inFlight is true while an operation is outstanding. Real clients are
	// sequential — one request at a time — and the session dedup logic depends
	// on that, since only the last sequence number is retained.
	inFlight *Op
}

// Workload issues client operations against whichever node currently leads.
type Workload struct {
	cfg     WorkloadConfig
	rng     *rand.Rand
	clients []*client
	keys    []string

	Issued, Rejected, Completed int
}

func NewWorkload(cfg WorkloadConfig, rng *rand.Rand) *Workload {
	w := &Workload{cfg: cfg, rng: rng}
	for i := 0; i < cfg.Clients; i++ {
		w.clients = append(w.clients, &client{id: kv.ClientID(i + 1)})
	}
	for i := 0; i < cfg.Keys; i++ {
		w.keys = append(w.keys, fmt.Sprintf("k%d", i))
	}
	return w
}

// Step may issue operations. Called once per tick, after the cluster ticks so
// that a leader elected this tick is visible.
func (w *Workload) Step(c *Cluster) {
	leader := c.Leader()

	// Iterate the client slice in order — never a map — so the sequence of rng
	// draws is fixed and the run replays.
	for _, cl := range w.clients {
		// Retire a finished operation before considering a new one.
		if cl.inFlight != nil {
			if cl.inFlight.Returned || cl.inFlight.Abandoned {
				if cl.inFlight.Returned {
					w.Completed++
				}
				cl.inFlight = nil
			} else {
				continue // still waiting; a sequential client does not pipeline
			}
		}

		if w.rng.Float64() >= w.cfg.OpProb {
			continue
		}

		key := w.keys[w.rng.Intn(len(w.keys))]
		isRead := w.rng.Float64() < w.cfg.ReadRatio

		if leader == 0 {
			// No leader right now. A real client would retry; we simply skip,
			// which models a client that backs off. Counted so a run with no
			// leader for most of its life is visible rather than silently
			// passing on an empty history.
			w.Rejected++
			continue
		}

		var (
			op *Op
			ok bool
		)
		if isRead {
			op, ok = c.Read(leader, key)
		} else {
			cl.seq++
			op, ok = c.Propose(leader, kv.Command{
				Op:       kv.OpPut,
				Key:      key,
				Value:    []byte(fmt.Sprintf("c%d-s%d", cl.id, cl.seq)),
				ClientID: cl.id,
				Seq:      cl.seq,
			})
		}

		if !ok {
			// The node stopped being leader between Leader() and here, or the
			// read index was not yet available. Both are normal.
			w.Rejected++
			continue
		}
		w.Issued++
		cl.inFlight = op
	}
}

// Run drives a full simulation: nemesis, cluster tick, workload.
//
// The order within a tick is fixed and meaningful. Faults land first so the
// tick observes them; the cluster then advances; the workload issues last so
// it sees the post-tick leader. Changing this order changes which rng draw
// serves which decision, and every recorded seed stops reproducing.
func Run(cfg Config, nemCfg NemesisConfig, wlCfg WorkloadConfig, ticks int) (*Cluster, *Nemesis, *Workload) {
	c := New(cfg)
	rng := rand.New(rand.NewSource(cfg.Seed ^ 0x5eed))
	nem := NewNemesis(nemCfg, rng)
	wl := NewWorkload(wlCfg, rng)

	for i := 0; i < ticks; i++ {
		nem.Step(c)
		c.Tick()
		wl.Step(c)
	}

	// Quiesce: heal everything, revive everyone, and run long enough for the
	// cluster to settle. Without this the history ends mid-flight and the tail
	// is all abandoned operations, which tells you nothing.
	c.Heal()
	for _, id := range c.IDs() {
		if c.Crashed(id) {
			c.Restart(id)
		}
	}
	for i := 0; i < 500; i++ {
		c.Tick()
		wl.Step(c)
	}
	return c, nem, wl
}

// LeaderSafetyViolations returns any term in which more than one node claimed
// leadership. This is Raft's central invariant and it must always be empty.
func LeaderSafetyViolations(c *Cluster) map[raft.Term][]raft.NodeID {
	bad := make(map[raft.Term][]raft.NodeID)
	for term, ids := range c.LeadersByTerm() {
		if len(ids) > 1 {
			bad[term] = ids
		}
	}
	return bad
}
