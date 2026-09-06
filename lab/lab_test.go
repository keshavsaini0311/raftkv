package lab

import (
	"encoding/json"
	"testing"
)

// view is the fields a test needs out of the JSON blob.
type view struct {
	T     int  `json:"t"`
	Max   int  `json:"max"`
	Live  bool `json:"live"`
	Nodes []struct {
		ID      uint64 `json:"id"`
		Role    string `json:"role"`
		Term    uint64 `json:"term"`
		Last    uint64 `json:"last"`
		Commit  uint64 `json:"commit"`
		Crashed bool   `json:"crashed"`
	} `json:"nodes"`
}

func do(t *testing.T, l *Lab, cmd string) view {
	t.Helper()
	var v view
	if err := json.Unmarshal([]byte(l.Do(cmd)), &v); err != nil {
		t.Fatalf("decoding view after %s: %v", cmd, err)
	}
	return v
}

// nodesJSON is the part of the view that must be identical when a tick is
// revisited. The event log is excluded on purpose: it is rebuilt from the same
// commands, so comparing it would test the formatter, not the replay.
func nodesJSON(t *testing.T, l *Lab) string {
	t.Helper()
	var raw struct {
		Nodes json.RawMessage `json:"nodes"`
	}
	if err := json.Unmarshal([]byte(l.Do("")), &raw); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return string(raw.Nodes)
}

// build produces a timeline with real events in it — an election, an isolated
// leader, writes that commit and writes that cannot.
func build(t *testing.T) *Lab {
	t.Helper()
	l := New(7, 5)
	do(t, l, `{"do":"tick","n":25}`)
	do(t, l, `{"do":"put","key":"a","value":"1"}`)
	do(t, l, `{"do":"tick","n":10}`)
	v := do(t, l, `{"do":"tick","n":1}`)

	var leader uint64
	for _, n := range v.Nodes {
		if n.Role == "Leader" {
			leader = n.ID
		}
	}
	if leader == 0 {
		t.Fatal("no leader after 36 ticks")
	}
	do(t, l, `{"do":"isolate","id":`+itoa(leader)+`}`)
	do(t, l, `{"do":"tick","n":40}`)
	do(t, l, `{"do":"put","key":"b","value":"2"}`)
	do(t, l, `{"do":"tick","n":15}`)
	do(t, l, `{"do":"heal"}`)
	do(t, l, `{"do":"tick","n":20}`)
	return l
}

func itoa(u uint64) string {
	b, _ := json.Marshal(u)
	return string(b)
}

// Rewinding replays rather than restoring a snapshot, so every revisited tick
// must be byte-identical to the first time through. If it is not, the page
// shows a past that never happened.
func TestSeekReproducesEveryTickExactly(t *testing.T) {
	l := build(t)
	max := do(t, l, "").Max
	if max < 50 {
		t.Fatalf("timeline is only %d ticks; not much of a test", max)
	}

	forward := make([]string, max+1)
	for tick := 0; tick <= max; tick++ {
		v := do(t, l, `{"do":"seek","n":`+itoa(uint64(tick))+`}`)
		if v.T != tick {
			t.Fatalf("seek(%d) landed on t=%d", tick, v.T)
		}
		forward[tick] = nodesJSON(t, l)
	}

	// Walk back down. Order must not matter: a tick's state is a function of
	// the commands before it, nothing else.
	for tick := max; tick >= 0; tick-- {
		v := do(t, l, `{"do":"seek","n":`+itoa(uint64(tick))+`}`)
		if v.T != tick {
			t.Fatalf("rewind to %d landed on t=%d", tick, v.T)
		}
		if got := nodesJSON(t, l); got != forward[tick] {
			t.Fatalf("t=%d differs on the way back:\n forward: %s\nbackward: %s",
				tick, forward[tick], got)
		}
	}
}

// Looking at the past must not change it. This is not hypothetical: the first
// version recorded every command including the empty one the page sends to
// read state, so merely rendering while rewound truncated the future.
func TestReadingDoesNotDisturbTheTimeline(t *testing.T) {
	l := build(t)
	max := do(t, l, "").Max

	do(t, l, `{"do":"seek","n":10}`)
	for i := 0; i < 20; i++ {
		if v := do(t, l, ""); v.Max != max || v.T != 10 {
			t.Fatalf("after %d reads at t=10: t=%d max=%d, want 10 and %d", i+1, v.T, v.Max, max)
		}
	}
	// An unrecognised command is a read too, not a silent mutation.
	if v := do(t, l, `{"do":"nonsense"}`); v.Max != max || v.T != 10 {
		t.Errorf("an unknown command moved the timeline to t=%d max=%d", v.T, v.Max)
	}

	if v := do(t, l, `{"do":"seek","n":`+itoa(uint64(max))+`}`); !v.Live {
		t.Error("returning to the end did not report live")
	}
}

// Scrubbing is browsing; acting is editing. Acting in the past must discard
// the future, because the commands that produced it no longer apply.
func TestActingInThePastForksTheTimeline(t *testing.T) {
	l := build(t)
	max := do(t, l, "").Max

	do(t, l, `{"do":"seek","n":30}`)
	v := do(t, l, `{"do":"crash","id":2}`)

	if v.Max != 30 {
		t.Errorf("max = %d after forking at 30, want 30", v.Max)
	}
	if !v.Live {
		t.Error("not live after forking; the fork IS the new present")
	}
	if v.T != 30 {
		t.Errorf("forked at t=%d, want 30", v.T)
	}
	for _, n := range v.Nodes {
		if n.ID == 2 && !n.Crashed {
			t.Error("the action that forked the timeline was not applied")
		}
	}

	// The discarded future must be genuinely gone, not merely hidden.
	if v := do(t, l, `{"do":"seek","n":`+itoa(uint64(max))+`}`); v.T != 30 {
		t.Errorf("seeking past the fork reached t=%d, want to clamp at 30", v.T)
	}
}

// Seeking to a tick in the middle of a multi-tick command must land on that
// tick, not on the nearest command boundary.
func TestSeekSplitsAMultiTickCommand(t *testing.T) {
	l := New(3, 3)
	do(t, l, `{"do":"tick","n":50}`)

	for _, want := range []int{1, 7, 23, 49, 50} {
		if v := do(t, l, `{"do":"seek","n":`+itoa(uint64(want))+`}`); v.T != want {
			t.Errorf("seek(%d) landed on t=%d", want, v.T)
		}
	}
}

// Reset clears the timeline; a new seed must not inherit the old one's past.
func TestResetClearsHistory(t *testing.T) {
	l := build(t)
	v := do(t, l, `{"do":"reset","seed":99,"nodes":3}`)
	if v.T != 0 || v.Max != 0 {
		t.Errorf("after reset t=%d max=%d, want 0 and 0", v.T, v.Max)
	}
	if len(v.Nodes) != 3 {
		t.Errorf("reset to %d nodes, want 3", len(v.Nodes))
	}
}
