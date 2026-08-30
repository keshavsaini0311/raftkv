// Package errs covers module 8: errors as values, wrapping, Is, and As.
//
// Go has no exceptions. There is no throw, no try/catch, no stack unwinding.
// `error` is an ordinary interface with one method:
//
//	type error interface {
//	    Error() string
//	}
//
// Functions that can fail return an error as their LAST result, and the caller
// checks it. That is the whole mechanism.
//
//	Java:  throw new NotFoundException();   // control jumps somewhere
//	JS:    throw new Error("nope");          // control jumps somewhere
//	Go:    return nil, ErrNotFound           // control returns to the caller
//
// The trade is explicit: Go code has more `if err != nil` and fewer places
// where control flow leaves the page. In a consensus implementation, where a
// half-applied state transition is a safety violation, "control jumps
// somewhere" is not a feature.
//
// (panic exists, but it is for PROGRAMMER errors — index out of range, nil
// dereference. Not for "the file was missing." Never use it for control flow.)
//
// Run: go test ./learn/errs/
package errs

import (
	"errors"
	"fmt"
)

// ---------------------------------------------------------------------------
// Exercise 1 — sentinel errors
//
// A sentinel is a package-level error value that callers can compare against.
// Convention: name it ErrXxx, declare it with errors.New, and export it —
// it is part of your API, exactly like a function signature.
//
//	var ErrNotFound = errors.New("not found")
//
// errors.New returns a pointer internally, so every call makes a DISTINCT
// value. Two errors.New("not found") are never equal. That is why the sentinel
// must be a single package-level var, not created fresh at each return.
//
// TODO(keshav): declare ErrNotFound with the message "not found".
// ---------------------------------------------------------------------------

var ErrNotFound = errors.New("not found")

// Fetch returns the value for key, or ErrNotFound.
//
// TODO(keshav): return ErrNotFound when the key is absent. On success return
// the value and a literal nil. Note the ordering convention: the error is the
// LAST return value, and on failure the other results are their zero values.
func Fetch(store map[string]string, key string) (string, error) {
	val, ok := store[key]
	if !ok {
		return "", ErrNotFound
	}
	return val, nil
}

// ---------------------------------------------------------------------------
// Exercise 2 — wrapping with %w
//
// A bare "not found" tells the caller nothing about WHERE. Wrapping adds
// context while keeping the original error reachable:
//
//	fmt.Errorf("loading config %q: %w", name, err)
//	                                    ^^ the verb that WRAPS
//
// %v would stringify the error and lose it. %w stores it, so errors.Is can
// still find the sentinel underneath any number of layers.
//
// errors.Is(err, ErrNotFound) unwraps the whole chain looking for a match.
// Always use errors.Is for sentinels, never err == ErrNotFound — the latter
// breaks the moment anyone wraps.
//
// Message convention: lowercase, no trailing punctuation, and read as a chain —
// "loading config "x": not found".
//
// TODO(keshav): call Fetch. If it fails, wrap the error with %w and the
// context: loading config %q. On success return the value.
// You will need to import "fmt".
// ---------------------------------------------------------------------------

func LoadConfig(store map[string]string, name string) (string, error) {
	val, err := Fetch(store, name)
	if err != nil {
		return "", fmt.Errorf("loading config %q: %w", name, err)
	}
	return val, nil
}

// ---------------------------------------------------------------------------
// Exercise 3 — a custom error type, and errors.As
//
// Sentinels answer "which error is this?". Custom types answer "which error,
// AND what were the details?" — a field name, an index, a term number.
//
// To be an error, a type needs one method: Error() string. By module 7's rule,
// use a POINTER receiver and return &ValidationError{...}, so *ValidationError
// is the thing that satisfies error.
//
// errors.As is errors.Is's counterpart: it unwraps looking for a matching TYPE
// and, on a hit, assigns it to your variable so you can read the fields.
//
//	var ve *ValidationError
//	if errors.As(err, &ve) { use(ve.Field) }
//
// TODO(keshav): give *ValidationError an Error() method returning
//	invalid field "Name": must not be empty
// using its Field and Reason. Then make Validate return &ValidationError{...}
// when name is empty (Reason: "must not be empty"), and nil otherwise.
// ---------------------------------------------------------------------------

type ValidationError struct {
	Field  string
	Reason string
}

// <-- your Error() method goes here

func (e *ValidationError) Error() string {
	return fmt.Sprintf(`invalid field "%s": %s`, e.Field, e.Reason)
}

func Validate(name string) error {
	if name == "" {
		return &ValidationError{
			Field:  "Name",
			Reason: "must not be empty",
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// Written for you — the typed-nil trap, now in its natural habitat.
//
// HEADS UP: until you write Error() in exercise 3, this function will not
// compile, and the error points HERE rather than at your TODO:
//
//	cannot use ve (variable of type *ValidationError) as error value in
//	return statement: *ValidationError does not implement error
//	(missing method Error)
//
// That is the compiler proving module 7's point — *ValidationError becomes an
// error the instant it has the method, and not one moment before. Nothing is
// wrong with the code below; finish exercise 3 and it compiles.
//
// This is module 7's trap, and this is where it actually bites people. The
// function looks like it returns nil on success. It does not: it returns an
// interface holding a nil *ValidationError, and `err != nil` is TRUE.
//
// The fix is the rule, not a cleverer check: never declare a variable of a
// CONCRETE error type and return it as `error`. Return literal nil, or return
// the concrete value at the point you build it — which is what Validate does.
// ---------------------------------------------------------------------------

func BrokenValidate(name string) error {
	var ve *ValidationError // nil *ValidationError
	if name == "" {
		ve = &ValidationError{Field: "Name", Reason: "must not be empty"}
	}
	return ve // returns a NON-NIL error even when ve is nil
}

// Compile-time proof that the sentinel is usable with errors.Is. Also keeps
// the errors import live before you write exercise 1.
var _ = errors.Is
