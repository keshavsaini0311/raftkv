// Package mapping covers module 6: maps, the comma-ok idiom, and the one Go
// behaviour that can quietly destroy this entire project — randomized
// iteration order.
//
// Your design doc promises:
//
//	"Seed 42891 produces byte-identical behavior today and next year."
//
// Go promises the opposite about maps: iteration order is deliberately
// randomized on every run, of every map, every time. Those two promises meet
// inside raft/ the moment you write:
//
//	for peer := range n.matchIndex { ... }
//
// (Named `mapping`, not `maps`, to avoid shadowing the stdlib `maps` package.)
//
// Run: go test ./learn/mapping/
package mapping

import "sort"

// ---------------------------------------------------------------------------
// Exercise 1 — building a map, and the zero value doing work again
//
// Three ways to get a map:
//	var m map[string]int          // nil. Reads OK. WRITES PANIC.
//	m := map[string]int{}         // empty, usable
//	m := make(map[string]int)     // empty, usable — identical to above
//
// The nil-map asymmetry is the trap: reading a missing key returns the value
// type's ZERO, and reading from a nil map does too. But writing to a nil map
// panics with "assignment to entry in nil map". Unlike slices, where append
// handles nil for you, maps must be made before you write.
//
// The upside of zero-value reads: m[k]++ works on a key that does not exist.
// It reads 0, adds 1, stores 1. No "if key in map" guard needed — compare to
// Python's defaultdict or JS's `(m[k] ?? 0) + 1`.
//
// TODO(keshav): count how many times each word appears in words.
// Return an empty (non-nil) map for empty input.
// ---------------------------------------------------------------------------

func Count(words []string) map[string]int {
	m := map[string]int{}
	for i := 0; i < len(words); i++ {
		m[words[i]]++
	}
	return m
}

// ---------------------------------------------------------------------------
// Exercise 2 — comma-ok: telling "absent" from "present but zero"
//
// m[k] always succeeds. A missing key yields the zero value, so this is
// ambiguous — did the key exist with value 0, or not exist at all?
//
//	v := m[k]           // v == 0. Cannot tell which.
//	v, ok := m[k]       // ok == false only when the key is ABSENT.
//
// Go calls this the comma-ok idiom, and it appears in three other places you
// will meet: type assertions, channel receives, and this.
//
// It matters directly in Raft: `votedFor` uses 0 as a sentinel for "nobody"
// precisely because a zero value has to double as "unset". Anywhere you cannot
// spare a sentinel, you need comma-ok instead.
//
// TODO(keshav): return the value for k, and whether it was actually present.
// Must not panic when m is nil.
// ---------------------------------------------------------------------------

func Get(m map[string]int, k string) (int, bool) {
	v, ok := m[k]
	return v, ok
}

// ---------------------------------------------------------------------------
// Exercise 3 — the determinism exercise
//
// `for k := range m` visits keys in a RANDOM order. Not "unspecified but
// stable" — actively randomized, with a different order each run of the same
// binary over the same map. The Go team did this on purpose, to stop people
// writing code that accidentally depends on an order that was never promised.
//
// For raft/ this is not a style issue, it is a correctness issue. If the order
// you iterate peers decides the order messages enter the outbound queue, then
// two runs of the same seed produce different message orderings — and your
// deterministic simulator stops being deterministic. A failing seed would no
// longer replay. That is the whole thesis of the project, gone.
//
// The fix is always the same: collect the keys, sort them, iterate the sorted
// slice. Never iterate a map when order affects output.
//
// TODO(keshav): return every key of m, sorted ascending.
// Use slices.Sort from the standard library (Go 1.21+), or sort.Strings.
// Remember len(m) gives you the size — use it to pre-size with make.
// ---------------------------------------------------------------------------

func KeysInOrder(m map[string]int) []string {
	var temp []string
	for k := range m {
		temp = append(temp, k)
	}
	sort.Slice(temp, func(i, j int) bool {
		return temp[i] < temp[j]
	})
	return temp
}
