// Package conc covers module 10: goroutines, channels, mutexes, and the race
// detector.
//
// Read this first, because it is the point of the whole module:
//
//	NONE OF THIS BELONGS IN raft/.
//
// Your design doc bans it outright — "no goroutines, no timers, no sockets".
// The pure core is single-threaded by construction, and that is what makes a
// five-node cluster replayable from a seed.
//
// So why learn it? Because the OTHER side of the boundary is made of this:
//
//	server/
//	  server.go      ticker goroutine, Ready loop
//	  transport.go   HTTP peer transport
//
// server/ is where goroutines live. sim/ deliberately has none — it advances a
// virtual clock on one thread, which is exactly why it is deterministic.
// Knowing where the line is drawn matters more than the syntax.
//
// Run: go test ./learn/conc/
package conc

import (
	"sort"
	"sync"
)

// ---------------------------------------------------------------------------
// Exercise 1 — a data race, on purpose
//
// A goroutine is a function call with `go` in front. It costs about 2KB and is
// scheduled by the Go runtime onto OS threads, not by the OS. Spawning
// thousands is normal; spawning thousands of OS threads is not.
//
//	go doSomething()        // returns IMMEDIATELY; the call runs concurrently
//
// sync.WaitGroup is how you wait for a batch to finish:
//
//	var wg sync.WaitGroup
//	wg.Add(1)               // before starting
//	go func() {
//	    defer wg.Done()     // defer runs when the function returns
//	    ...
//	}()
//	wg.Wait()               // blocks until every Done has fired
//
// `defer` schedules a call to run when the surrounding FUNCTION returns —
// like finally, but attached to a statement instead of a block.
//
// TODO(keshav): spawn n goroutines, each incrementing the SAME plain int
// variable, wait for all of them, and return it. Use no mutex. Return the
// counter.
//
// You would expect n. You will not reliably get n, because `count++` is three
// machine operations (read, add, write) and two goroutines can interleave
// between them. The test runs this under -race.
// ---------------------------------------------------------------------------
func CountRacy(n int) int {
	count := 0

	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			count++
		}()
	}

	wg.Wait()

	return count
}

// ---------------------------------------------------------------------------
// Exercise 2 — the mutex fix
//
// sync.Mutex has a useful zero value (module 1's theme, again): a plain
// `var mu sync.Mutex` is ready to lock, no constructor.
//
//	mu.Lock()
//	defer mu.Unlock()      // defer means it unlocks on EVERY return path
//	count++
//
// Always pair Lock with a deferred Unlock. An early return that skips Unlock
// deadlocks the whole program, and a mutex is not re-entrant in Go — locking
// one you already hold deadlocks instantly.
//
// TODO(keshav): same as exercise 1, but guard the increment with a mutex.
// The test asserts this returns exactly n, every time, under -race.
// ---------------------------------------------------------------------------

func CountSafe(n int) int {
	count := 0

	var mu sync.Mutex
	var wg sync.WaitGroup

	wg.Add(n)

	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()

			mu.Lock()
			defer mu.Unlock()

			count++
		}()
	}

	wg.Wait()

	return count
}

// ---------------------------------------------------------------------------
// Exercise 3 — channels
//
// A channel is a typed pipe. This is Go's headline idea:
//
//	"Do not communicate by sharing memory; share memory by communicating."
//
//	ch := make(chan int)      // UNBUFFERED: a send blocks until a receive
//	ch := make(chan int, 10)  // BUFFERED:   a send blocks only when full
//	ch <- v                   // send
//	v := <-ch                 // receive
//	close(ch)                 // no more sends; receivers can still drain
//	for v := range ch { }     // receives until the channel is CLOSED
//
// Two rules that cause most channel bugs:
//   - `range ch` blocks forever unless someone closes ch.
//   - only the SENDER closes, and only once. Sending on a closed channel panics.
//
// The standard shape, which you will write in server/ almost verbatim:
//
//	go func() { wg.Wait(); close(ch) }()   // closer waits for all senders
//	for v := range ch { ... }              // main drains until closed
//
// TODO(keshav): spawn n goroutines; goroutine i sends i*i on a channel.
// Collect every value and return them SORTED ascending.
//
// Sorted, because goroutines finish in nondeterministic order — module 6's
// lesson in a new costume. If you returned them in arrival order the result
// would differ run to run.
// ---------------------------------------------------------------------------

func SquaresConcurrent(n int) []int {
	ch := make(chan int)

	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()

			ch <- i * i
		}(i)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	var results []int

	for v := range ch {
		results = append(results, v)
	}

	sort.Ints(results)

	return results
}

// ---------------------------------------------------------------------------
// Written for you — select, and the shape server/ actually uses.
//
// select waits on several channel operations and takes whichever is ready
// first. This is the real structure of your design doc's driver loop:
//
//	server.go   ticker goroutine, Ready loop
//
// A real Raft server sits in exactly this loop: tick the core on a timer,
// deliver inbound messages as they arrive, drain Ready, and exit on shutdown.
// Note that every call into the raft core happens on THIS one goroutine —
// which is how a single-threaded core survives a concurrent server.
// ---------------------------------------------------------------------------

func DrainUntilDone(values <-chan int, done <-chan struct{}) []int {
	var got []int
	for {
		select {
		case v, ok := <-values:
			if !ok { // channel closed and drained
				return got
			}
			got = append(got, v)
		case <-done: // shutdown requested; stop immediately
			return got
		}
	}
}

// Keeps the sync import live before you write exercises 1 and 2.
var _ sync.Mutex
