package sim

import (
	"fmt"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// Bisect which class of fault breaks linearizability.
//
// Running every combination is cheap here — the whole point of a deterministic
// simulator — and it turns "something is wrong" into "this specific fault is
// wrong", which is the difference between a hypothesis and a diagnosis.
func TestBisectFaults(t *testing.T) {
	lossy := NetworkConfig{MinLatency: 1, MaxLatency: 4, DropRate: 0.02, DuplicateRate: 0.02}
	clean := DefaultNetwork()

	partitionsOnly := NemesisConfig{PartitionProb: 0.01, LeaderIsolateProb: 0.006, HealProb: 0.04}
	crashesOnly := NemesisConfig{CrashProb: 0.008, RestartProb: 0.06}

	cases := []struct {
		name string
		net  NetworkConfig
		nem  NemesisConfig
	}{
		{"calm/clean-net", clean, Calm()},
		{"calm/lossy-net", lossy, Calm()},
		{"partitions/clean-net", clean, partitionsOnly},
		{"crashes/clean-net", clean, crashesOnly},
		{"chaos/clean-net", clean, Chaos()},
		{"chaos/lossy-net", lossy, Chaos()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bad := 0
			for _, seed := range []int64{1, 2, 3, 7} {
				cfg := DefaultConfig(seed, 5)
				cfg.Network = tc.net
				c, _, _ := Run(cfg, tc.nem, DefaultWorkload(), 3000)

				ops := toOperations(c.History().Ops())
				if len(ops) < 10 {
					t.Logf("  seed %d: only %d ops, skipping", seed, len(ops))
					continue
				}
				if res, _ := porcupine.CheckOperationsVerbose(registerModel, ops, 15*time.Second); res == porcupine.Illegal {
					bad++
					t.Logf("  seed %-6d NOT linearizable (%d ops)", seed, len(ops))
				} else {
					t.Logf("  seed %-6d ok (%d ops)", seed, len(ops))
				}
			}
			if bad > 0 {
				t.Errorf("%s: %d of 4 seeds not linearizable", tc.name, bad)
			}
		})
	}
}

// Shrink a failing seed to the smallest prefix of ticks that still fails.
//
// A 4,000-tick history with 150 operations is not something you can read. The
// same failure at 300 ticks with 8 operations usually is — and a deterministic
// simulator is what makes bisecting on tick count meaningful at all.
func TestShrinkFailingSeed(t *testing.T) {
	if testing.Short() {
		t.Skip("shrinking is slow")
	}
	const seed = 1

	cfg := DefaultConfig(seed, 5)
	cfg.Network = NetworkConfig{MinLatency: 1, MaxLatency: 4, DropRate: 0.02, DuplicateRate: 0.02}

	fails := func(ticks int) (bool, *Cluster, int) {
		c, _, _ := Run(cfg, Chaos(), DefaultWorkload(), ticks)
		ops := toOperations(c.History().Ops())
		if len(ops) < 2 {
			return false, c, len(ops)
		}
		res, _ := porcupine.CheckOperationsVerbose(registerModel, ops, 10*time.Second)
		return res == porcupine.Illegal, c, len(ops)
	}

	if bad, _, _ := fails(4000); !bad {
		t.Skip("seed no longer fails at 4000 ticks; nothing to shrink")
	}

	lo, hi := 0, 4000
	for lo+10 < hi {
		mid := (lo + hi) / 2
		if bad, _, _ := fails(mid); bad {
			hi = mid
		} else {
			lo = mid
		}
	}

	bad, c, n := fails(hi)
	t.Logf("smallest failing prefix: %d ticks, %d operations (still failing: %v)", hi, n, bad)
	for i, op := range c.History().Ops() {
		t.Logf("  %2d  %s", i, op)
	}
}

var _ = fmt.Sprintf
