// Note the package name: basics_test, not basics.
//
// Go lets a _test.go file declare an external test package in the same
// directory. It compiles separately and can only see EXPORTED identifiers —
// which is exactly how a real caller sees your package. That is why exercise 1
// fails to compile until you export the function.
package basics_test

import (
	"testing"

	"github.com/keshavsaini0311/raftkv/learn/basics"
)

func TestGreeting(t *testing.T) {
	if got := basics.Greeting(); got != "hello, raft" {
		t.Errorf("Greeting() = %q, want %q", got, "hello, raft")
	}
}

func TestZeroConfig(t *testing.T) {
	c := basics.ZeroConfig()

	// Every one of these is Go handing you a typed zero, not undefined/null.
	if c.Name != "" {
		t.Errorf("Name = %q, want empty string", c.Name)
	}
	if c.Retries != 0 {
		t.Errorf("Retries = %d, want 0", c.Retries)
	}
	if c.Debug != false {
		t.Errorf("Debug = %v, want false", c.Debug)
	}
	if c.Tags != nil {
		t.Errorf("Tags = %v, want nil", c.Tags)
	}
	if c.Limits != nil {
		t.Errorf("Limits = %v, want nil", c.Limits)
	}
	if c.Parent != nil {
		t.Errorf("Parent = %v, want nil", c.Parent)
	}

	// A nil map is safe to READ from. (Writing to one panics — later module.)
	if n := len(c.Limits); n != 0 {
		t.Errorf("len(nil map) = %d, want 0", n)
	}
}

// Table-driven test: the dominant Go testing idiom. One slice of cases, one
// loop, one subtest each. `go test -run 'TestIsUnset/empty_but_non-nil_slice'`
// runs exactly one row.
func TestIsUnset(t *testing.T) {
	tests := []struct {
		name string
		cfg  basics.Config
		want bool
	}{
		{"zero value", basics.Config{}, true},
		{"name set", basics.Config{Name: "node1"}, false},
		{"retries set", basics.Config{Retries: 3}, false},
		{"debug set", basics.Config{Debug: true}, false},
		{"parent set", basics.Config{Parent: &basics.Config{}}, false},

		// The one that matters. An empty slice is NOT a nil slice. They have
		// the same length and print the same way, but they are different
		// values — and in raft/ that distinction decides whether a log is
		// "absent" or "present and empty".
		{"empty but non-nil slice", basics.Config{Tags: []string{}}, false},
		{"empty but non-nil map", basics.Config{Limits: map[string]int{}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := basics.IsUnset(tt.cfg); got != tt.want {
				t.Errorf("IsUnset(%+v) = %v, want %v", tt.cfg, got, tt.want)
			}
		})
	}
}
