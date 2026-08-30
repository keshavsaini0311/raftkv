// Package receivers covers module 3: value receivers vs pointer receivers.
//
// This is the module your design doc depends on most:
//
//	func (n *Node) Tick()                 // pointer — MUST mutate n
//	func (n *Node) Step(m Message) error  // pointer — MUST mutate n
//
// Drop one `*` and the method silently stops working. It still compiles, still
// runs, still returns. It just does nothing. There is no warning.
//
// Run: go test ./learn/receivers/
package receivers

// Counter is small on purpose. Note `log` is a slice — that matters in ex 3.
type Counter struct {
	n   int
	log []string
}

// NewCounter builds a Counter with a pre-populated log. (Written for you: Go
// has no constructors, just conventions. A `NewX` function returning X or *X is
// the idiom — there is no `new` keyword doing anything special for structs.)
func NewCounter(entries ...string) *Counter {
	return &Counter{log: entries}
}

// N and Log are read-only accessors so the test (in package receivers_test) can
// inspect unexported fields. Written for you — nothing to learn here.
func (c *Counter) N() int        { return c.n }
func (c *Counter) Log() []string { return c.log }

// ---------------------------------------------------------------------------
// Exercise 1 — a VALUE receiver gets a COPY
//
//	func (c Counter) ...   <- c is a fresh copy of the whole struct
//	func (c *Counter) ...  <- c points at the caller's struct
//
// Java and TypeScript have no equivalent: `this` is always a reference there.
// In Go the receiver is a normal parameter, and a non-pointer parameter is
// copied. So mutating it mutates the copy, and the copy is discarded on return.
//
// TODO(keshav): increment c.n by 1. Write the obvious thing.
// The test asserts this has NO visible effect on the caller. That is correct
// and intended — you are implementing the bug so you can see it.
// ---------------------------------------------------------------------------

func (c Counter) IncBroken() {
	c.n++
}

// ---------------------------------------------------------------------------
// Exercise 2 — a POINTER receiver mutates the real thing
//
// TODO(keshav): same one line of code, but this receiver is *Counter.
//
// Note you do NOT write (*c).n — Go auto-dereferences a pointer receiver, so
// c.n is correct. Unlike C, there is no `->` operator; the dot does both jobs.
// ---------------------------------------------------------------------------

func (c *Counter) IncWorks() {
	c.n++
}

// ---------------------------------------------------------------------------
// Exercise 3 — the copy is SHALLOW, and that is the trap
//
// Exercise 1 showed a value receiver cannot change the caller's data. That is
// only half true, and the other half causes real bugs.
//
// A struct copy copies each field. Copying a slice field copies the slice
// HEADER (a pointer, a length, a capacity) — not the array it points at. Both
// copies now point at the SAME backing array.
//
// So through a value receiver you cannot REPLACE the slice, but you can very
// much overwrite its ELEMENTS, and the caller sees that.
//
// TODO(keshav): on this VALUE receiver, set c.log[0] = s.
// Guard against an empty log first (indexing [0] of an empty slice panics).
// The test asserts the caller DOES see this change. Predict the result before
// you run it.
// ---------------------------------------------------------------------------

func (c Counter) SetFirstBroken(s string) {
	if len(c.log) > 0 {
		c.log[0] = s
	}
}
