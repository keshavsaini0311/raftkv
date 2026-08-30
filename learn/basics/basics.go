// Package basics covers module 1 of the Go tour: packages, exports, and zero values.
//
// Fill in every function marked TODO(keshav), then run:
//
//	go test ./learn/basics/
package basics

// ---------------------------------------------------------------------------
// Exercise 1 — exports
//
// The test file lives in package basics_test, so it can only reach EXPORTED
// identifiers. This function is currently unexported, so the test will not
// compile. Fix it.
//
// TODO(keshav): make this reachable from basics_test. Return "hello, raft".
// ---------------------------------------------------------------------------

func Greeting() string {
	return "hello, raft"
}

// ---------------------------------------------------------------------------
// Exercise 2 & 3 — zero values
// ---------------------------------------------------------------------------

// Config is a deliberately mixed bag: one of each kind of field, so you can see
// what Go hands you when you set none of them.
type Config struct {
	Name    string
	Retries int
	Debug   bool
	Tags    []string
	Limits  map[string]int
	Parent  *Config
}

// ZeroConfig returns a Config with no field explicitly set.
//
// TODO(keshav): exercise 2. One line.
func ZeroConfig() Config {
	c := Config{}
	return c
}

// IsUnset reports whether every field of c still holds its zero value.
//
// TODO(keshav): exercise 3. Your first instinct will be `return c == Config{}`.
// Try it — read the compiler error carefully, it is the whole lesson.
func IsUnset(c Config) bool {
	return !c.Debug && c.Name == "" && c.Limits == nil && c.Parent == nil && c.Retries == 0 && c.Tags == nil
}
