package mapping_test

import (
	"testing"

	"github.com/keshavsaini0311/raftkv/learn/mapping"
)

func TestCount(t *testing.T) {
	got := mapping.Count([]string{"a", "b", "a", "c", "a"})

	want := map[string]int{"a": 3, "b": 1, "c": 1}
	if len(got) != len(want) {
		t.Fatalf("Count = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("Count[%q] = %d, want %d", k, got[k], v)
		}
	}
}

func TestCountEmptyIsNotNil(t *testing.T) {
	got := mapping.Count(nil)
	if got == nil {
		t.Error("Count(nil) returned a nil map; callers should be able to " +
			"write to the result without it panicking")
	}
	if len(got) != 0 {
		t.Errorf("len = %d, want 0", len(got))
	}
}

func TestGet(t *testing.T) {
	m := map[string]int{"present": 7, "zero": 0}

	tests := []struct {
		name    string
		key     string
		wantVal int
		wantOK  bool
	}{
		{"present non-zero", "present", 7, true},
		// The case plain m[k] cannot distinguish from the next one.
		{"present but zero", "zero", 0, true},
		{"absent", "missing", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotOK := mapping.Get(m, tt.key)
			if gotVal != tt.wantVal || gotOK != tt.wantOK {
				t.Errorf("Get(%q) = (%d, %v), want (%d, %v)",
					tt.key, gotVal, gotOK, tt.wantVal, tt.wantOK)
			}
		})
	}
}

// Reading from a nil map is legal and yields the zero value. (Writing to one
// panics — see TestNilMapWritePanics below.)
func TestGetNilMap(t *testing.T) {
	gotVal, gotOK := mapping.Get(nil, "anything")
	if gotVal != 0 || gotOK != false {
		t.Errorf("Get(nil, ...) = (%d, %v), want (0, false)", gotVal, gotOK)
	}
}

// Written for you: proof of the nil-map asymmetry. recover() catches a panic —
// errors get their own module, this is just the demonstration.
func TestNilMapWritePanics(t *testing.T) {
	var m map[string]int

	if v := m["read is fine"]; v != 0 {
		t.Errorf("reading a nil map gave %d, want 0", v)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("writing to a nil map did not panic, but it must")
		}
	}()
	m["write explodes"] = 1
	t.Error("unreachable — the line above must panic")
}

// Exercise 3: sorted, and therefore identical on every call and every run.
func TestKeysInOrder(t *testing.T) {
	m := map[string]int{"delta": 4, "alpha": 1, "charlie": 3, "bravo": 2, "echo": 5}
	want := []string{"alpha", "bravo", "charlie", "delta", "echo"}

	got := mapping.KeysInOrder(m)
	if len(got) != len(want) {
		t.Fatalf("KeysInOrder = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("KeysInOrder = %v, want %v", got, want)
		}
	}
}

// Called 1000 times, it must return the identical sequence 1000 times. A raw
// `for k := range m` fails this essentially always.
func TestKeysInOrderIsStable(t *testing.T) {
	m := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "g": 7, "h": 8}

	first := mapping.KeysInOrder(m)
	for i := 0; i < 1000; i++ {
		got := mapping.KeysInOrder(m)
		for k := range first {
			if got[k] != first[k] {
				t.Fatalf("iteration %d gave %v, first call gave %v — order "+
					"must not vary between calls", i, got, first)
			}
		}
	}
}

// Written for you: proof that Go really does randomize. We iterate the same map
// 1000 times and collect the distinct orderings seen. With 8 keys, getting only
// one ordering across 1000 runs is not something that happens.
func TestRangeOverMapIsRandomized(t *testing.T) {
	m := map[string]int{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "g": 7, "h": 8}

	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		order := ""
		for k := range m { // <- the dangerous line, on purpose
			order += k
		}
		seen[order] = true
	}

	if len(seen) < 2 {
		t.Errorf("saw only %d distinct iteration order(s) in 1000 passes; "+
			"Go is supposed to randomize this", len(seen))
	}
	t.Logf("1000 iterations of an 8-key map produced %d distinct orderings", len(seen))
}
