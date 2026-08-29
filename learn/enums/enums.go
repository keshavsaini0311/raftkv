// Package enums covers module 2 of the Go tour: named types, constants, and iota.
//
// This module builds the exact construct your design doc specifies for raft/:
//
//	type Role int
//	const (
//	    Follower Role = iota
//	    Candidate
//	    Leader
//	)
//
// Run: go test ./learn/enums/
package enums

// ---------------------------------------------------------------------------
// Exercise 1 — a named type is a NEW type, not an alias
//
// TypeScript:
//	type Role = number          // an ALIAS. Role and number are the same type,
//	                            // interchangeable everywhere, forever.
// Go:
//	type Role int               // a NEW type whose underlying type is int.
//	                            // An int is NOT a Role. Conversion is explicit.
//
// Go is NOMINALLY typed here: the name is the identity. TypeScript is
// STRUCTURALLY typed: the shape is the identity. This is why your design doc
// can declare NodeID, Term, and Index as three separate types even though all
// three are uint64 — the compiler will stop you passing a Term where a NodeID
// belongs, which is a real class of Raft bug deleted for free.
//
// TODO(keshav): declare Role as a new type with underlying type int.
// ---------------------------------------------------------------------------

// <-- your type declaration goes here

// ---------------------------------------------------------------------------
// Exercise 2 — iota
//
// Go has no `enum` keyword. The idiom is a const block plus iota.
//
// iota is a counter that resets to 0 at every `const (` and increments by one
// per line. Crucially, if a line omits its expression, it REPEATS the previous
// line's expression — with the new iota value. That is the whole trick:
//
//	const (
//	    A Thing = iota   // expression is "iota", value 0
//	    B                // repeats "Thing = iota", value 1
//	    C                // repeats "Thing = iota", value 2
//	)
//
// TODO(keshav): declare Follower, Candidate, and Leader as Roles, using iota,
// so that Follower == 0, Candidate == 1, Leader == 2. Write "iota" exactly once.
// ---------------------------------------------------------------------------

// <-- your const block goes here

// ---------------------------------------------------------------------------
// Exercise 3 — String(), your first method
//
// A Role is an int. Print one and you get "0", which is useless in a log line
// when you are trying to see why a node did not become leader.
//
// Go's fmt package checks whether a value has a String() string method. If it
// does, fmt uses it. That is the fmt.Stringer interface, and you satisfy it
// just by having the method — there is no `implements Stringer` to write.
//
// Syntax you have not seen yet:
//	func (r Role) String() string { ... }
//	     ^^^^^^^^ the RECEIVER: like `this`, but explicitly named and typed.
//
// TODO(keshav): implement String() so Follower -> "Follower",
// Candidate -> "Candidate", Leader -> "Leader", anything else -> "unknown".
// A switch statement is the natural fit. Go's switch needs no `break`.
//
// WARNING, and this one is a real Go footgun: inside String(), never use %v or
// %s on the receiver. fmt would call String() to format it, which calls fmt,
// which calls String()... until the stack dies. Use %d if you need the number.
// ---------------------------------------------------------------------------

// <-- your String method goes here
