package sim

import (
	"fmt"
	"math/rand"

	"github.com/keshavsaini0311/raftkv/raft"
)

// NemesisConfig is a fault schedule. Every probability is per tick, and every
// draw comes from the cluster's seeded rng — so the entire fault sequence is a
// function of the seed and replays exactly.
type NemesisConfig struct {
	// PartitionProb splits the cluster into two random groups.
	PartitionProb float64
	// LeaderIsolateProb strands the leader alone. Called out separately
	// because a uniformly random partition rarely produces it, and it is the
	// pathological case: the isolated leader still believes it leads.
	LeaderIsolateProb float64
	// HealProb reconnects everything.
	HealProb float64
	// CrashProb kills a random live node, losing volatile state only.
	CrashProb float64
	// RestartProb revives a random crashed node from its storage.
	RestartProb float64

	// ConfChangeProb removes a node and adds it back, exercising membership
	// changes CONCURRENTLY with partitions and crashes. Membership under
	// concurrent faults is the combination that found the waiter bug; leaving
	// it untested would leave the same class of gap open.
	ConfChangeProb float64

	// MinLive is never violated. Below a quorum the cluster is correctly
	// unavailable, which is uninteresting to test for long stretches: it makes
	// every run pass by making nothing happen.
	MinLive int
}

// Chaos is an aggressive but survivable schedule.
func Chaos() NemesisConfig {
	return NemesisConfig{
		PartitionProb:     0.010,
		LeaderIsolateProb: 0.006,
		HealProb:          0.040,
		CrashProb:         0.008,
		RestartProb:       0.060,
		ConfChangeProb:    0.004,
		MinLive:           0, // computed from the cluster size if left at 0
	}
}

// Calm applies no faults. Used as a control: a test that passes only under
// Calm has proved nothing about fault tolerance.
func Calm() NemesisConfig { return NemesisConfig{} }

// Nemesis injects faults on a schedule.
type Nemesis struct {
	cfg NemesisConfig
	rng *rand.Rand

	// Counters so a test can assert the faults actually happened. A chaos run
	// that silently injected nothing is a green test that proves nothing, and
	// that failure mode is invisible without this.
	Partitions      int
	LeaderIsolates  int
	Heals           int
	Crashes         int
	Restarts        int
	ConfChanges     int
	SkippedForQuora int
}

func NewNemesis(cfg NemesisConfig, rng *rand.Rand) *Nemesis {
	return &Nemesis{cfg: cfg, rng: rng}
}

// Step may inject a fault. Called once per tick, before the cluster ticks.
func (nm *Nemesis) Step(c *Cluster) {
	minLive := nm.cfg.MinLive
	if minLive == 0 {
		minLive = len(c.ids)/2 + 1 // keep a quorum alive
	}

	// Order is fixed: heal, restart, partition, isolate, crash. Reordering
	// these changes which draws the rng serves to which decision, and the
	// seed would stop reproducing.

	if nm.cfg.HealProb > 0 && nm.rng.Float64() < nm.cfg.HealProb {
		c.Heal()
		nm.Heals++
	}

	if nm.cfg.RestartProb > 0 && nm.rng.Float64() < nm.cfg.RestartProb {
		if id, ok := nm.pickCrashed(c); ok {
			c.Restart(id)
			nm.Restarts++
		}
	}

	if nm.cfg.PartitionProb > 0 && nm.rng.Float64() < nm.cfg.PartitionProb {
		nm.randomPartition(c)
		nm.Partitions++
	}

	if nm.cfg.LeaderIsolateProb > 0 && nm.rng.Float64() < nm.cfg.LeaderIsolateProb {
		if _, ok := c.IsolateLeader(); ok {
			nm.LeaderIsolates++
		}
	}

	if nm.cfg.ConfChangeProb > 0 && nm.rng.Float64() < nm.cfg.ConfChangeProb {
		if c.proposeRandomConfChange(nm.rng) {
			nm.ConfChanges++
		}
	}

	if nm.cfg.CrashProb > 0 && nm.rng.Float64() < nm.cfg.CrashProb {
		live := nm.liveNodes(c)
		if len(live) > minLive {
			c.Crash(live[nm.rng.Intn(len(live))])
			nm.Crashes++
		} else {
			nm.SkippedForQuora++
		}
	}
}

// randomPartition splits the cluster in two. Each node is assigned by an
// independent coin flip, then the result is forced to be a genuine split —
// an "all on one side" draw is a no-op dressed up as a fault.
func (nm *Nemesis) randomPartition(c *Cluster) {
	var a, b []raft.NodeID
	for _, id := range c.ids { // sorted: the draw order is fixed
		if nm.rng.Intn(2) == 0 {
			a = append(a, id)
		} else {
			b = append(b, id)
		}
	}
	if len(a) == 0 || len(b) == 0 {
		mid := len(c.ids) / 2
		a = append([]raft.NodeID(nil), c.ids[:mid]...)
		b = append([]raft.NodeID(nil), c.ids[mid:]...)
	}
	c.Partition([][]raft.NodeID{a, b})
}

func (nm *Nemesis) liveNodes(c *Cluster) []raft.NodeID {
	var out []raft.NodeID
	for _, id := range c.ids {
		if !c.Crashed(id) {
			out = append(out, id)
		}
	}
	return out
}

func (nm *Nemesis) pickCrashed(c *Cluster) (raft.NodeID, bool) {
	var out []raft.NodeID
	for _, id := range c.ids {
		if c.Crashed(id) {
			out = append(out, id)
		}
	}
	if len(out) == 0 {
		return 0, false
	}
	return out[nm.rng.Intn(len(out))], true
}

// Summary reports what actually happened, so a green run can be checked for
// having been a real test rather than a quiet one.
func (nm *Nemesis) Summary() string {
	return fmt.Sprintf("partitions=%d leader-isolations=%d heals=%d crashes=%d restarts=%d confchanges=%d skipped=%d",
		nm.Partitions, nm.LeaderIsolates, nm.Heals, nm.Crashes, nm.Restarts,
		nm.ConfChanges, nm.SkippedForQuora)
}
