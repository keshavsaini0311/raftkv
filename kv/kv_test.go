package kv

import (
	"bytes"
	"testing"
)

func apply(t *testing.T, s *Store, c Command) Result {
	t.Helper()
	b, err := c.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return s.Apply(b)
}

func TestPutGetDelete(t *testing.T) {
	s := New()

	apply(t, s, Command{Op: OpPut, Key: "a", Value: []byte("1")})
	if v, ok := s.Get("a"); !ok || string(v) != "1" {
		t.Errorf("Get(a) = %q, %v; want 1, true", v, ok)
	}

	apply(t, s, Command{Op: OpPut, Key: "a", Value: []byte("2")})
	if v, _ := s.Get("a"); string(v) != "2" {
		t.Errorf("after overwrite Get(a) = %q, want 2", v)
	}

	res := apply(t, s, Command{Op: OpDelete, Key: "a"})
	if !res.Found {
		t.Error("Delete of an existing key reported Found=false")
	}
	if _, ok := s.Get("a"); ok {
		t.Error("key survived a delete")
	}
	if res := apply(t, s, Command{Op: OpDelete, Key: "gone"}); res.Found {
		t.Error("Delete of a missing key reported Found=true")
	}
}

// The store must not hand out slices into its own map, and must not keep
// slices the caller can still mutate. Module 5's aliasing lesson: a []byte
// field is a header, and copying the struct does not copy the bytes.
func TestValuesAreCopiedInAndOut(t *testing.T) {
	s := New()

	in := []byte("original")
	s.ApplyCommand(Command{Op: OpPut, Key: "k", Value: in})
	in[0] = 'X' // caller mutates the slice it passed in
	if v, _ := s.Get("k"); string(v) != "original" {
		t.Errorf("stored value = %q; the store aliased the caller's slice", v)
	}

	out, _ := s.Get("k")
	out[0] = 'Y' // caller mutates the slice it got back
	if v, _ := s.Get("k"); string(v) != "original" {
		t.Errorf("stored value = %q; Get returned a view into the map", v)
	}
}

// Exactly-once semantics. Without sessions, a client that retries through a
// leader change applies its write twice and the store is no longer linearizable.
func TestClientSessionsDeduplicate(t *testing.T) {
	s := New()
	cmd := Command{Op: OpPut, Key: "k", Value: []byte("first"), ClientID: 7, Seq: 1}

	apply(t, s, cmd)

	// The same client retries the same sequence number with different bytes,
	// as a buggy or confused retry would.
	dup := cmd
	dup.Value = []byte("second")
	apply(t, s, dup)

	if v, _ := s.Get("k"); string(v) != "first" {
		t.Errorf("value = %q, want first — a duplicate sequence number was applied", v)
	}

	// A new sequence number is a genuinely new request.
	next := cmd
	next.Seq = 2
	next.Value = []byte("second")
	apply(t, s, next)
	if v, _ := s.Get("k"); string(v) != "second" {
		t.Errorf("value = %q, want second", v)
	}
}

// Commands without a client id are not deduplicated. That is correct for reads
// and for internal traffic, and it is why the check is keyed on ClientID != 0.
func TestNoSessionMeansNoDedup(t *testing.T) {
	s := New()
	apply(t, s, Command{Op: OpPut, Key: "k", Value: []byte("a")})
	apply(t, s, Command{Op: OpPut, Key: "k", Value: []byte("b")})
	if v, _ := s.Get("k"); string(v) != "b" {
		t.Errorf("value = %q, want b", v)
	}
}

func TestStaleSequenceIsRejected(t *testing.T) {
	s := New()
	apply(t, s, Command{Op: OpPut, Key: "k", Value: []byte("v"), ClientID: 1, Seq: 5})
	res := apply(t, s, Command{Op: OpPut, Key: "k", Value: []byte("old"), ClientID: 1, Seq: 3})
	if res.Err == "" {
		t.Error("a sequence number older than the last applied was accepted")
	}
	if v, _ := s.Get("k"); string(v) != "v" {
		t.Errorf("value = %q, want v", v)
	}
}

// Apply must be deterministic: identical command sequences must produce
// byte-identical snapshots on every replica, or the cluster silently diverges.
func TestApplyIsDeterministic(t *testing.T) {
	cmds := []Command{
		{Op: OpPut, Key: "z", Value: []byte("1")},
		{Op: OpPut, Key: "a", Value: []byte("2")},
		{Op: OpPut, Key: "m", Value: []byte("3")},
		{Op: OpDelete, Key: "z"},
		{Op: OpPut, Key: "b", Value: []byte("4"), ClientID: 9, Seq: 1},
	}

	var first []byte
	for run := 0; run < 50; run++ {
		s := New()
		for _, c := range cmds {
			apply(t, s, c)
		}
		snap, err := s.Snapshot()
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		if run == 0 {
			first = snap
			continue
		}
		if !bytes.Equal(snap, first) {
			t.Fatalf("run %d produced a different snapshot; Apply is not deterministic", run)
		}
	}
}

// Keys must be sorted. Go randomises map iteration, and any nondeterminism that
// reaches output breaks both snapshot equality and seeded replay.
func TestKeysAreSorted(t *testing.T) {
	s := New()
	for _, k := range []string{"delta", "alpha", "charlie", "bravo", "echo"} {
		apply(t, s, Command{Op: OpPut, Key: k, Value: []byte("v")})
	}
	want := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	for i := 0; i < 100; i++ {
		got := s.Keys()
		for j := range want {
			if got[j] != want[j] {
				t.Fatalf("Keys() = %v, want %v", got, want)
			}
		}
	}
}

// A snapshot must carry sessions, not just data. Restoring data alone would let
// every client's last write apply a second time — the exact duplicate the
// sessions exist to prevent.
func TestSnapshotRoundTripIncludesSessions(t *testing.T) {
	s := New()
	apply(t, s, Command{Op: OpPut, Key: "k", Value: []byte("v"), ClientID: 3, Seq: 1})

	snap, err := s.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	restored := New()
	if err := restored.Restore(snap); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if v, ok := restored.Get("k"); !ok || string(v) != "v" {
		t.Errorf("restored Get(k) = %q, %v", v, ok)
	}

	// The retry must still be deduplicated after a restore.
	apply(t, restored, Command{Op: OpPut, Key: "k", Value: []byte("dup"), ClientID: 3, Seq: 1})
	if v, _ := restored.Get("k"); string(v) != "v" {
		t.Errorf("value = %q, want v — sessions were lost across the snapshot, so "+
			"a retry reapplied", v)
	}
}

func TestMalformedCommandFailsIdentically(t *testing.T) {
	// Every replica sees the same committed bytes, so failing the same way
	// everywhere keeps them consistent. Skipping on some nodes would not.
	a, b := New(), New()
	ra := a.Apply([]byte("not json"))
	rb := b.Apply([]byte("not json"))
	if ra.Err == "" || ra.Err != rb.Err {
		t.Errorf("malformed command results differ: %q vs %q", ra.Err, rb.Err)
	}
}
