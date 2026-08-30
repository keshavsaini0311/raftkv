package ifaces_test

import (
	"bytes"
	"testing"

	"github.com/keshavsaini0311/raftkv/learn/ifaces"
)

// The compile-time proof, from the outside. If *MemStore does not satisfy
// Store, this file does not build.
//
// (*ifaces.MemStore)(nil) is a conversion, not a call — it produces a typed nil
// pointer with zero runtime work. Calling NewMemStore() here instead would run
// at package-init time, before any test, and blow up on the stub's panic.
var _ ifaces.Store = (*ifaces.MemStore)(nil)

func TestMemStoreSaveLoad(t *testing.T) {
	// Declared as the INTERFACE, not the concrete type. Everything below goes
	// through the contract — which is what makes a fake swappable for a real
	// disk implementation later.
	var s ifaces.Store = ifaces.NewMemStore()

	if _, ok := s.Load("absent"); ok {
		t.Error("Load on an empty store returned ok=true")
	}

	s.Save("term", []byte("7"))

	got, ok := s.Load("term")
	if !ok {
		t.Fatal("Load after Save returned ok=false")
	}
	if !bytes.Equal(got, []byte("7")) {
		t.Errorf("Load = %q, want %q", got, "7")
	}

	s.Save("term", []byte("8")) // overwrite
	if got, _ := s.Load("term"); !bytes.Equal(got, []byte("8")) {
		t.Errorf("after overwrite Load = %q, want %q", got, "8")
	}
}

// bytes.Equal, not ==. []byte is a slice, and slices are not comparable —
// module 1's rule, still in force.
func TestMemStoreDistinguishesEmptyFromAbsent(t *testing.T) {
	s := ifaces.NewMemStore()
	s.Save("empty", []byte{})

	if v, ok := s.Load("empty"); !ok {
		t.Error(`Load("empty") ok=false; a stored empty value is still present`)
	} else if len(v) != 0 {
		t.Errorf("Load = %q, want empty", v)
	}

	if _, ok := s.Load("never-saved"); ok {
		t.Error("Load of an unsaved key returned ok=true")
	}
}

// Exercise 3: the trap. This test asserts the SURPRISING behaviour, because
// the surprising behaviour is what Go actually does.
func TestTypedNilIsNotNil(t *testing.T) {
	s := ifaces.TypedNilStore()

	if s == nil {
		t.Fatal("TypedNilStore() == nil — you probably returned a literal nil. " +
			"Return a nil *MemStore variable instead, so the interface carries " +
			"a type with a nil value")
	}

	// And the reason it matters: the interface is non-nil, so an `if s != nil`
	// guard passes... and then the call through it panics.
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected a nil-pointer panic when calling through the " +
				"typed-nil interface")
		}
	}()
	s.Save("boom", nil)
	t.Error("unreachable — the line above dereferences a nil *MemStore")
}

// For contrast: a genuinely nil interface. Both halves nil, so == nil is true.
func TestUntypedNilIsNil(t *testing.T) {
	var s ifaces.Store // never assigned
	if s != nil {
		t.Error("an unassigned interface variable should be nil")
	}
}
