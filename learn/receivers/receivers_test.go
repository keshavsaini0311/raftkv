package receivers_test

import (
	"testing"

	"github.com/keshavsaini0311/raftkv/learn/receivers"
)

// Exercise 1: the method runs, increments its copy, and the copy dies.
// No error, no warning, no effect. This is the silent-failure mode.
func TestIncBrokenDoesNothing(t *testing.T) {
	c := receivers.NewCounter()

	c.IncBroken()
	c.IncBroken()
	c.IncBroken()

	if got := c.N(); got != 0 {
		t.Errorf("after 3x IncBroken, N() = %d, want 0 — a value receiver "+
			"cannot mutate the caller", got)
	}
}

// Exercise 2: same body, one character different in the signature.
func TestIncWorks(t *testing.T) {
	c := receivers.NewCounter()

	c.IncWorks()
	c.IncWorks()
	c.IncWorks()

	if got := c.N(); got != 3 {
		t.Errorf("after 3x IncWorks, N() = %d, want 3", got)
	}
}

// Exercise 3: the one that surprises people. Same VALUE receiver as exercise 1,
// but this time the caller DOES see the change — because the copy's slice
// header points at the caller's backing array.
func TestSetFirstBrokenMutatesAnyway(t *testing.T) {
	c := receivers.NewCounter("original", "second")

	c.SetFirstBroken("overwritten")

	if got := c.Log()[0]; got != "overwritten" {
		t.Errorf("Log()[0] = %q, want %q — a value receiver copies the slice "+
			"HEADER, so both share one backing array", got, "overwritten")
	}
	if got := c.Log()[1]; got != "second" {
		t.Errorf("Log()[1] = %q, want %q", got, "second")
	}
}

// And the boundary: it must not panic on an empty log.
func TestSetFirstBrokenEmptyLog(t *testing.T) {
	c := receivers.NewCounter()
	c.SetFirstBroken("anything") // must be a no-op, not a panic

	if got := len(c.Log()); got != 0 {
		t.Errorf("len(Log()) = %d, want 0", got)
	}
}
