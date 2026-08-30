// Package ifaces covers module 7: interfaces, method sets, and the nil trap.
//
// Your design doc calls for exactly this shape:
//
//	storage/
//	  storage.go   interface: SaveHardState, Append, Entries, ...
//	  file.go      append-only file implementation
//
// One interface, a real implementation for server/, and a fake for tests —
// that is what makes the pure core testable without touching a disk.
//
// Run: go test ./learn/ifaces/
package ifaces

// Store is the contract. Note what is NOT here: no "implements" keyword
// anywhere, and no import linking implementations back to this file.
//
// In Java you write `class MemStore implements Store`. In Go, a type satisfies
// an interface by HAVING THE METHODS. Nothing is declared. The compiler checks
// it at the point of use — where you assign a concrete type to an interface
// variable, not where the type is defined.
//
// The practical consequence: you can define an interface that an existing type
// already satisfies, including a type from someone else's package that has
// never heard of you. This is why io.Reader works across the whole ecosystem.
//
// Idiom worth absorbing: Go interfaces are SMALL. io.Reader is one method.
// "The bigger the interface, the weaker the abstraction." Define interfaces
// where they are USED (the consumer), not next to the implementation.
type Store interface {
	Save(key string, val []byte)
	Load(key string) ([]byte, bool)
}

// ---------------------------------------------------------------------------
// Exercise 1 — satisfy the interface
//
// TODO(keshav): implement NewMemStore, Save, and Load so *MemStore satisfies
// Store. Use a map[string][]byte. Load returns comma-ok style, like module 6.
//
// Use POINTER receivers — you are mutating the struct's map, and by the
// module 3 rule, once one method needs a pointer receiver they all should.
// ---------------------------------------------------------------------------

type MemStore struct {
	data map[string][]byte
}

func NewMemStore() *MemStore {
	return &MemStore{
		data: make(map[string][]byte),
	}
}

func (m *MemStore) Save(key string, val []byte) {
	m.data[key] = val
}

func (m *MemStore) Load(key string) ([]byte, bool) {
	val, ok := m.data[key]
	return val, ok
}

// ---------------------------------------------------------------------------
// Exercise 2 — method sets: WHO satisfies the interface
//
// This is the rule that surprises everyone:
//
//	methods with VALUE   receivers -> in the method set of BOTH T and *T
//	methods with POINTER receivers -> in the method set of *T ONLY
//
// So with pointer receivers, *MemStore satisfies Store but MemStore does NOT.
// Go will not auto-take the address here, because it cannot: an interface
// stores a copy of the value, and there would be no addressable original to
// point at. (Method CALLS auto-address, as you saw in module 3. Interface
// SATISFACTION does not. Same-looking situation, different rule.)
//
// TODO(keshav): uncomment the line below, run `go build ./learn/ifaces/`,
// read the error — it names the rule explicitly — then comment it back out.
// ---------------------------------------------------------------------------

//var _ Store = MemStore{} // <- uncomment me, read the error, re-comment

// TODO(keshav): now write the assertion that DOES compile, for *MemStore.
// Use the blank-identifier form from module 2's test file:  var _ Store = ...

var _ Store = NewMemStore()

// ---------------------------------------------------------------------------
// Exercise 3 — the nil interface trap
//
// An interface value is a PAIR: (concrete type, value). It is nil only when
// BOTH halves are nil.
//
// Put a nil *MemStore into a Store and you get (type=*MemStore, value=nil).
// The value half is nil. The type half is not. So the interface != nil, even
// though the pointer inside it is.
//
//	var m *MemStore = nil
//	var s Store = m
//	s == nil            // FALSE. Everyone expects true.
//
// This is the most reported "wat" in Go, and it bites hardest with errors —
// a function returning a nil *MyError as an `error` produces a non-nil error,
// so `if err != nil` fires on success. You will meet that again in module 8.
//
// TODO(keshav): return a nil *MemStore, typed as a Store.
// Declare a *MemStore variable, leave it nil, return it. Do NOT return
// literal nil — that would produce a genuinely nil interface and defeat the
// demonstration. The test asserts the result is NOT nil.
// ---------------------------------------------------------------------------

func TypedNilStore() Store {
	var store *MemStore = nil
	return store
}
