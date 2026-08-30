package enums_test

import (
	"fmt"
	"testing"

	"github.com/keshavsaini0311/raftkv/learn/enums"
)

// A compile-time interface assertion. This declares a throwaway variable of
// type fmt.Stringer (the blank identifier _ means "discard") and assigns a Role
// to it. If Role does not satisfy fmt.Stringer, this line fails to COMPILE.
//
// It generates no code and costs nothing at runtime. You will see this idiom at
// the top of real Go packages as a guard: it pins the promise "this type
// implements this interface" so a later refactor cannot silently break it.
var _ fmt.Stringer = enums.Follower

func TestRoleValues(t *testing.T) {
	tests := []struct {
		name string
		role enums.Role
		want int
	}{
		{"Follower", enums.Follower, 0},
		{"Candidate", enums.Candidate, 1},
		{"Leader", enums.Leader, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// int(tt.role) is an explicit CONVERSION. Go will not compare a
			// Role to an int implicitly, even though Role's underlying type is
			// int. That is the nominal-typing lesson from exercise 1.
			if got := int(tt.role); got != tt.want {
				t.Errorf("%s = %d, want %d", tt.name, got, tt.want)
			}
		})
	}
}

func TestRoleString(t *testing.T) {
	tests := []struct {
		role enums.Role
		want string
	}{
		{enums.Follower, "Follower"},
		{enums.Candidate, "Candidate"},
		{enums.Leader, "Leader"},
		{enums.Role(99), "unknown"}, // out of range must not panic or print junk
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.role.String(); got != tt.want {
				t.Errorf("Role(%d).String() = %q, want %q", int(tt.role), got, tt.want)
			}
		})
	}
}

// This is the payoff: fmt finds String() on its own. Nothing here mentions
// Stringer, and no interface was declared or implemented explicitly.
func TestFmtUsesStringAutomatically(t *testing.T) {
	got := fmt.Sprintf("node is %v", enums.Leader)
	if want := "node is Leader"; got != want {
		t.Errorf("Sprintf = %q, want %q", got, want)
	}
}
