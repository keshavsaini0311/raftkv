// Package slicing covers module 5: slice headers, append, aliasing, and nil.
//
// This is where your Raft log lives. Figure 2's AppendEntries receiver rule 3 —
// "delete the existing entry and all that follow it" — is a slice operation,
// and doing it naively corrupts data that other code is still holding.
//
// A slice is a 24-byte header, nothing more:
//
//	type slice struct {
//	    ptr *T    // start of the backing array
//	    len int   // elements you can index
//	    cap int   // elements before the array must be reallocated
//	}
//
// Copy a slice and you copy those 24 bytes. The array is NOT copied.
//
// Run: go test ./learn/slicing/
package slicing

// ---------------------------------------------------------------------------
// Exercise 1 — append, and why you must use its return value
//
// append may or may not reallocate:
//   - room left (len < cap)  -> writes in place, returns the SAME array
//   - full    (len == cap)   -> allocates a bigger array, copies, returns a NEW one
//
// You cannot tell which happened. That is why append RETURNS a slice, and why
// ignoring the return value is a real bug rather than a style issue:
//
//	append(s, v)        // useless — result discarded
//	s = append(s, v)    // correct
//
// Also: append works fine on a nil slice. A nil slice is a valid empty slice —
// ptr=nil, len=0, cap=0 — and appending to it allocates. You never need to
// initialize a slice before appending. This is the "useful zero value" idea
// from module 1 again.
//
// TODO(keshav): append v to s and return the result. One line.
// It must work when s is nil.
// ---------------------------------------------------------------------------

func Grow(s []int, v int) []int {
	return append(s, v)
}

// ---------------------------------------------------------------------------
// Exercise 2 — the aliasing bug, written on purpose
//
// s[:n] does NOT copy. It returns a new header pointing at the SAME array,
// with len = n — but capacity still runs to the end of the original array.
//
//	a := []int{1, 2, 3, 4, 5}
//	b := a[:2]              // len 2, cap 5  <- cap did not shrink
//	b = append(b, 99)       // len 2 < cap 5, so append writes IN PLACE...
//	                        // ...into a[2]. a is now {1, 2, 99, 4, 5}.
//
// Nothing warns you. b looks like its own slice and is not.
//
// TODO(keshav): implement the naive version — append v to s[:n] and return it.
// The test asserts that this CORRUPTS s. You are writing the bug so you can
// see it, exactly like IncBroken in module 3.
// ---------------------------------------------------------------------------

func AppendToPrefix(s []int, n, v int) []int {
	temp := s[:n]
	temp = append(temp, v)
	return temp
}

// ---------------------------------------------------------------------------
// Exercise 3 — the safe version. This IS the Raft operation.
//
// TruncateAndAppend keeps log[:i], then appends entries after it — the exact
// shape of AppendEntries receiver rule 3 followed by rule 4.
//
// The requirement that makes it hard: the caller's `log` must be UNCHANGED
// when you return. A follower's existing log may be referenced elsewhere (a
// Ready handed to a driver, a snapshot, a test assertion). Truncating must not
// reach back and scribble on it.
//
// Two ways to get this right — pick either:
//
//  1. Allocate and copy explicitly:
//     out := make([]string, 0, i+len(entries))
//     out = append(out, log[:i]...)     // copies into out's own array
//     out = append(out, entries...)
//
//     make([]T, len, cap) allocates. The `...` spreads a slice into append's
//     variadic parameter — like JS spread, but only for the final argument.
//
//  2. The full slice expression, log[:i:i], which caps capacity at i.
//     With len == cap, the next append is FORCED to reallocate, so it cannot
//     write into the original array. Three indices: [low:high:max].
//     Terse and idiomatic, but easy to misread — option 1 is clearer.
//
// TODO(keshav): implement it. i is guaranteed to be within 0..len(log).
// ---------------------------------------------------------------------------

func TruncateAndAppend(log []string, i int, entries ...string) []string {
	temp := log[:i:i]
	temp = append(temp, entries...)
	return temp
}
