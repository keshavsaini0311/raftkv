package conc_test

import (
	"os"
	"testing"

	"github.com/keshavsaini0311/raftkv/learn/conc"
)

// Exercise 1's test is GATED. CI runs `go test -race ./...`, and a deliberate
// data race would fail the build every time — a permanently red pipeline that
// everyone learns to ignore.
//
// Run it yourself, on purpose:
//
//	RACE_DEMO=1 go test -race -run TestCountRacy -v ./learn/conc/
//
// Expect a WARNING: DATA RACE report naming both goroutines and both stacks.
// Then run it without -race a few times and watch the number land under n.
func TestCountRacy(t *testing.T) {
	if os.Getenv("RACE_DEMO") == "" {
		t.Skip("set RACE_DEMO=1 to run the deliberate data race")
	}

	const n = 10000
	got := conc.CountRacy(n)
	t.Logf("CountRacy(%d) = %d (want %d — a mismatch IS the lesson)", n, got, n)
}

// Exercise 2: correct under -race, every run, no matter the scheduling.
func TestCountSafe(t *testing.T) {
	for _, n := range []int{0, 1, 100, 10000} {
		if got := conc.CountSafe(n); got != n {
			t.Errorf("CountSafe(%d) = %d, want %d", n, got, n)
		}
	}
}

// Exercise 3: concurrent producers, deterministic output.
func TestSquaresConcurrent(t *testing.T) {
	tests := []struct {
		name string
		n    int
		want []int
	}{
		{"zero", 0, nil},
		{"one", 1, []int{0}},
		{"five", 5, []int{0, 1, 4, 9, 16}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := conc.SquaresConcurrent(tt.n)
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("got %v, want %v", got, tt.want)
				}
			}
		})
	}
}

// Called repeatedly, it must give the identical answer every time — the same
// determinism demand as module 6's KeysInOrder, now with goroutines behind it.
func TestSquaresConcurrentIsStable(t *testing.T) {
	first := conc.SquaresConcurrent(50)
	for i := 0; i < 200; i++ {
		got := conc.SquaresConcurrent(50)
		for k := range first {
			if got[k] != first[k] {
				t.Fatalf("run %d differed at index %d: %d vs %d — goroutine "+
					"completion order must not reach the output", i, k, got[k], first[k])
			}
		}
	}
}

// Written for you: the select loop, drained to completion.
func TestDrainUntilDoneReadsAllValues(t *testing.T) {
	values := make(chan int)
	done := make(chan struct{})

	go func() {
		for i := 0; i < 5; i++ {
			values <- i
		}
		close(values) // only the SENDER closes
	}()

	got := conc.DrainUntilDone(values, done)
	if len(got) != 5 {
		t.Fatalf("got %v, want 5 values", got)
	}
}

// And the shutdown path: closing done stops the loop even with values pending.
func TestDrainUntilDoneStopsOnDone(t *testing.T) {
	values := make(chan int) // nobody ever sends
	done := make(chan struct{})
	close(done) // already shut down

	if got := conc.DrainUntilDone(values, done); len(got) != 0 {
		t.Errorf("got %v, want none", got)
	}
}
