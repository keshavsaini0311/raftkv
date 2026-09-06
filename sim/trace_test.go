package sim

import (
	"encoding/json"
	"strings"
	"testing"
)

// The trace must be a faithful, replayable record — and, like everything else
// here, identical for a given seed. A visualisation built on a trace that
// varied run to run would show a different story each time it was opened.
func TestTraceIsRecordedAndDeterministic(t *testing.T) {
	run := func() *Trace {
		c := New(DefaultConfig(42891, 5))
		c.EnableTrace()
		nem := NewNemesis(Chaos(), c.rng)
		wl := NewWorkload(DefaultWorkload(), c.rng)
		for i := 0; i < 300; i++ {
			nem.Step(c)
			c.Tick()
			wl.Step(c)
		}
		return c.Trace()
	}

	tr := run()
	if len(tr.Frames) != 300 {
		t.Fatalf("recorded %d frames, want one per tick (300)", len(tr.Frames))
	}
	if tr.Seed != 42891 || len(tr.Nodes) != 5 {
		t.Errorf("trace header = seed %d, %d nodes; want 42891, 5", tr.Seed, len(tr.Nodes))
	}

	// It must actually capture activity, not 300 empty frames.
	var msgs, roles, groups int
	seenRole := map[string]bool{}
	events := map[string]int{}
	for _, f := range tr.Frames {
		msgs += len(f.Msgs)
		for _, kind := range []string{"elected leader", "crashed", "restarted", "partitioned", "healed"} {
			if strings.Contains(f.Event, kind) {
				events[kind]++
			}
		}
		if len(f.Groups) > 1 {
			groups++
		}
		for _, n := range f.Nodes {
			if len(f.Nodes) != 5 {
				t.Fatalf("frame at t=%d has %d nodes, want 5", f.T, len(f.Nodes))
			}
			seenRole[n.Role] = true
			roles++
		}
	}
	if msgs == 0 {
		t.Error("no messages recorded; the trace is not capturing the wire")
	}
	if groups == 0 {
		t.Error("no partitions recorded; the trace is not capturing network faults")
	}
	for _, want := range []string{"Follower", "Leader"} {
		if !seenRole[want] {
			t.Errorf("role %q never appeared in the trace", want)
		}
	}

	// Events are what make a replay legible: without them a viewer watches
	// colours change and has to infer why. Each kind is derived by a separate
	// branch, so each needs to be seen firing at least once.
	for _, want := range []string{"elected leader", "crashed", "restarted", "partitioned", "healed"} {
		if events[want] == 0 {
			t.Errorf("no %q event was ever derived", want)
		}
	}

	// Determinism: the whole trace must serialise identically.
	a, err := json.Marshal(tr)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	b, err := json.Marshal(run())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(a) != string(b) {
		t.Error("two traces of the same seed differ; a replay would not reproduce")
	}
	t.Logf("300 frames, %d messages, %d partitioned frames, %d bytes of JSON",
		msgs, groups, len(a))
}

// Tracing is opt-in and must cost a normal run nothing.
func TestTraceIsOffByDefault(t *testing.T) {
	c := New(DefaultConfig(1, 3))
	c.RunTicks(50)
	if c.Trace() != nil {
		t.Error("tracing was on without EnableTrace")
	}
}
