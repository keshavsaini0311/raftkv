package slicing_test

import (
	"testing"

	"github.com/keshavsaini0311/raftkv/learn/slicing"
)

// Exercise 1: append onto nil is legal, and the return value is the answer.
func TestGrow(t *testing.T) {
	t.Run("nil slice", func(t *testing.T) {
		got := slicing.Grow(nil, 5)
		if len(got) != 1 || got[0] != 5 {
			t.Fatalf("Grow(nil, 5) = %v, want [5]", got)
		}
	})

	t.Run("existing slice", func(t *testing.T) {
		got := slicing.Grow([]int{1, 2}, 3)
		want := []int{1, 2, 3}
		if len(got) != len(want) {
			t.Fatalf("Grow = %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("Grow = %v, want %v", got, want)
			}
		}
	})
}

// Exercise 2: proof that s[:n] shares memory. We assert the CORRUPTION,
// because the point is to see it happen, not to avoid it yet.
func TestAppendToPrefixCorruptsInput(t *testing.T) {
	s := []int{1, 2, 3, 4, 5}

	got := slicing.AppendToPrefix(s, 2, 99)

	if len(got) != 3 || got[2] != 99 {
		t.Fatalf("returned %v, want [1 2 99]", got)
	}

	// The damage: s[2] was 3. Nobody assigned to s. It is now 99.
	if s[2] != 99 {
		t.Errorf("s[2] = %d, want 99 — appending to s[:2] should have written "+
			"through into s's backing array", s[2])
	}
	if s[3] != 4 || s[4] != 5 {
		t.Errorf("s = %v, want the tail past index 2 untouched", s)
	}
}

// Capacity is what makes exercise 2 possible: slicing keeps the original cap.
func TestSlicingKeepsCapacity(t *testing.T) {
	a := make([]int, 5, 5)
	b := a[:2]

	if len(b) != 2 {
		t.Errorf("len(a[:2]) = %d, want 2", len(b))
	}
	if cap(b) != 5 {
		t.Errorf("cap(a[:2]) = %d, want 5 — slicing does not shrink capacity, "+
			"which is exactly why append can reach past len", cap(b))
	}
	// The three-index form DOES shrink it. This is exercise 3's option 2.
	if c := a[:2:2]; cap(c) != 2 {
		t.Errorf("cap(a[:2:2]) = %d, want 2", cap(c))
	}
}

// The result must be INDEPENDENT of the input, not merely leave it unmodified
// during the call. This is the heartbeat case: zero entries appended, so a
// capacity-capped implementation has nothing to force a reallocation and hands
// back a slice that still points at the caller's array.
func TestTruncateAndAppendResultDoesNotAliasInput(t *testing.T) {
	log := []string{"a", "b", "c"}

	got := slicing.TruncateAndAppend(log, 1) // no entries — the heartbeat shape
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("got %v, want [a]", got)
	}

	got[0] = "MUTATED"

	if log[0] != "a" {
		t.Errorf("writing to the result changed the caller's log[0] to %q — "+
			"the returned slice shares a backing array with the input", log[0])
	}
}

// Exercise 3: the Raft operation. Correct result AND an untouched input.
func TestTruncateAndAppend(t *testing.T) {
	tests := []struct {
		name    string
		log     []string
		i       int
		entries []string
		want    []string
	}{
		{"replace tail", []string{"a", "b", "c", "d"}, 2, []string{"X", "Y"}, []string{"a", "b", "X", "Y"}},
		{"append at end", []string{"a", "b"}, 2, []string{"c"}, []string{"a", "b", "c"}},
		{"truncate to empty", []string{"a", "b"}, 0, []string{"Z"}, []string{"Z"}},
		{"no new entries", []string{"a", "b", "c"}, 1, nil, []string{"a"}},
		{"nil log", nil, 0, []string{"a"}, []string{"a"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Keep an independent copy so we can prove the input survived.
			before := make([]string, len(tt.log))
			copy(before, tt.log)

			got := slicing.TruncateAndAppend(tt.log, tt.i, tt.entries...)

			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for k := range tt.want {
				if got[k] != tt.want[k] {
					t.Fatalf("got %v, want %v", got, tt.want)
				}
			}

			// The requirement that makes this exercise real.
			for k := range before {
				if tt.log[k] != before[k] {
					t.Errorf("input log was mutated: got %v, want %v — the "+
						"caller's log must survive a truncation", tt.log, before)
					break
				}
			}
		})
	}
}
