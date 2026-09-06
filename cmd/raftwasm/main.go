//go:build js && wasm

// Command raftwasm exposes the simulator to a browser.
//
// The raft core compiles to WebAssembly unmodified — no clock, no goroutines,
// no I/O, no randomness of its own. That is not a coincidence: it is the
// property TestRaftCoreStaysPure enforces, and this is what it buys. The page
// is driving the SAME code the tests drive, not a JavaScript retelling of it.
//
// This file is only the bridge. Everything it calls lives in lab/, which
// builds and tests on any platform.
package main

import (
	"syscall/js"

	"github.com/keshavsaini0311/raftkv/lab"
)

func main() {
	l := lab.New(1, 5)

	js.Global().Set("raftAct", js.FuncOf(func(this js.Value, args []js.Value) any {
		if len(args) == 0 {
			return l.Do("")
		}
		return l.Do(args[0].String())
	}))

	js.Global().Set("raftReady", js.ValueOf(true))
	select {} // keep the module alive; the page drives everything from here
}
