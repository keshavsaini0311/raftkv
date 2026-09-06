# raftkv — implementation notes

How this is built, why it is built that way, and what went wrong along the way.

The [design doc](specs/2026-08-29-raftkv-design.md) says what to build. This
says what was actually built, including the parts that were harder or different
than the design anticipated.

---

## 1. Reading order

If you read the code in this order it will make sense on the first pass:

| # | File | What it establishes |
|---|---|---|
| 1 | `raft/types.go` | the vocabulary: `Term`, `Index`, `Entry`, `HardState` |
| 2 | `raft/ready.go` | **the central contract** — read this before anything else |
| 3 | `raft/raft.go` | the algorithm: `Tick`, `Step`, the transitions, the handlers |
| 4 | `raft/log.go` | the log, the consistency check, compaction |
| 5 | `kv/kv.go` | the state machine, client sessions |
| 6 | `server/server.go` | the real driver: `run()` and `handleReady()` |
| 7 | `sim/cluster.go` | the deterministic driver — the same contract, no I/O |

`raft/ready.go` first is not arbitrary. Everything else is downstream of that
one struct.

---

## 2. The determinism boundary

### The rule

`raft/` is a pure function. It has no clock, no randomness of its own, no
network, no disk, and no goroutines. It consumes `Tick()` and `Step()`, and
emits *intentions* through `Ready()`. A driver performs them.

### Why it is worth the awkwardness

The intuitive Raft spawns a goroutine per peer and calls `time.After` for
timeouts. It works, and it is effectively untestable: races surface once in
~10,000 runs and vanish when you add a print statement.

With a pure core, five nodes run 4,000 logical ticks — with partitions,
crashes, message loss, duplication, and log compaction throughout — in **20
milliseconds**, and seed 42891 produces byte-identical behaviour every time.

That is the entire justification. Everything below is bookkeeping in service of
it.

### How it is enforced

Not by review. `raft/import_test.go` fails the build:

```go
TestRaftCoreStaysPure          // parses imports; bans time, math/rand, net,
                               // os, sync, syscall, context, log, and
                               // anything with a dot in its first path element
TestRaftCoreHasNoSideEffects   // walks the AST; bans fmt.Print*, fmt.Fprint*,
                               // print/println, recover, and `go` statements
```

Both were verified against deliberate violations, because a check that has
never failed is a check that might not work.

The three node types matter and each has a blind spot the others cover:

- `import "time"` → an **import**, invisible to a call check
- `fmt.Println` → a **call**, invisible to an import check (`fmt` must stay
  importable for `Sprintf` and `Errorf`)
- `go func(){}()` → a **statement**, invisible to both

That third one is the reason the second test exists. A single `go` inside
`raft/` would end determinism, produce no compile error, and pass every other
check.

### The one deviation from the design doc

The design doc sketches the injected randomness as:

```go
rand *rand.Rand   // INJECTED
```

That cannot compile under the doc's own dependency rule: naming the type
requires `import "math/rand"`. Injecting the *value* is not enough. The
implementation injects a **function** instead:

```go
randIntn func(n int) int
```

`server/` closes over a seeded `*rand.Rand`; `sim/` closes over its own; tests
pass a fixed stub. Same injection, no import.

---

## 3. The `Ready` contract

```go
for {
    n.Tick()                              // or n.Step(msg)

    rd := n.Ready()                       // "what do you want done?"

    storage.SaveHardState(rd.HardState)   // 1. PERSIST
    storage.Append(rd.Entries)            // 2.
    storage.SaveSnapshot(rd.Snapshot)     // 3.
    transport.Send(rd.Messages)           // 4. SEND — only after 1-3
    kv.Apply(rd.CommittedEntries)         // 5. APPLY
    serve(rd.ReadStates)                  // 6.

    n.Advance(rd)                         // 7. "handled"
}
```

**Steps 1–3 before step 4 is a safety requirement, not a performance tuning
choice.** A node grants a vote, replies "yes", crashes before the vote reaches
disk, restarts having forgotten, and grants a *second* vote in the same term.
Two candidates each collect a majority. Two leaders in one term — the exact
invariant Raft exists to protect, broken by reordering two lines.

Three properties of this design are load-bearing:

**`Ready()` does not mutate.** A driver may call it, decide it is busy, and call
again next loop. Both calls must return the same thing. Draining the queue there
would lose messages that were never sent.

**`Advance()` is the commit point.** A crash between `Ready` and `Advance`
means the work is handed back next time and retried, not lost. `server/`
depends on this: a failed `fsync` returns *without* calling `Advance`.

**`HardState` is `nil` when unchanged.** Persisting is an `fsync`; doing it on
every tick would dominate the cost of the whole system. The comparison is a
single `==` because every `HardState` field is numeric — which is *why*
`Entry` carries the `[]byte` and `HardState` does not.

---

## 4. Package walkthrough

### `raft/` — the core (1,413 lines)

| File | |
|---|---|
| `types.go` | scalar types, `Role`, `Entry`, `HardState`, snapshot types |
| `message.go` | six message types, one `Message` struct with a type tag |
| `raft.go` | `Node`, election, replication, ReadIndex, snapshots |
| `log.go` | the log: consistency check, truncation, compaction |
| `ready.go` | the driver contract |

**Nominal typing does real work here.** `NodeID`, `Term`, and `Index` are all
`uint64` underneath and all distinct types. Raft is dense with `uint64`s —
`commitIndex`, `matchIndex`, `PrevLogTerm`, `LastLogIndex` — and passing a term
where an index belongs is a classic bug. Here it does not compile, for free, at
zero runtime cost.

**The log keeps a sentinel at `entries[0]`.** Its index and term are those of
the last entry compacted into a snapshot. Real entries start at `entries[1]`.
This removes every special case at the head of the log: "the entry before index
1" is always a well-defined thing to compare against.

**Truncation allocates.** `l.entries[:k]` would share a backing array with every
slice previously returned from the log, so the next `append` would overwrite
entries a driver may still be writing to disk. `slice()` returns copies for the
same reason. This is the single most expensive correctness decision in the file
and it is deliberate.

### `kv/` — the state machine (233 lines)

**It does not import `raft`.** The state machine consumes opaque command bytes;
`raft` consumes opaque entry bytes; the two meet only in the driver. Each is
testable without the other.

`Apply` must be **deterministic**: every replica runs it on the same entries in
the same order and must reach byte-identical state. Hence sorted key iteration,
copied values in and out, and no clock or randomness anywhere.

**Client sessions** give exactly-once semantics. Each command carries
`(ClientID, Seq)`; the store keeps the last sequence number per client and the
response it produced, and replays the cached answer on a retry. Without this, a
client retrying through a leader change applies its write twice and the store is
no longer linearizable.

Only the *last* sequence number is kept, not every one ever seen. Clients are
strictly sequential — one outstanding request at a time — so a retry can only be
of the most recent command. Keeping full history would grow without bound.

Snapshots include sessions. Restoring data alone would let every client's last
write reapply after a restore — the exact duplicate the sessions prevent.

### `storage/` — durability (273 lines)

An interface with two implementations, run against the same test table so a test
that passes on the fake exercises the real contract.

`File` is append-only JSON lines with an `fsync` per record. Append-only because
it is the simplest crash-safe structure: a partial write at the tail is
detectable and discardable, and nothing already written is mutated in place.
Truncation is expressed by appending an entry at an index that already exists;
recovery replays and lets later records win.

JSON rather than length-prefixed protobuf, following the design doc's open
question: during a partition-induced debugging session, `cat`ing the log is
worth more than the bytes.

A truncated final record is **discarded on recovery, not treated as an error**.
An unclean shutdown is a normal event; refusing to boot would turn a survivable
crash into an outage.

### `server/` — the real driver (883 lines)

**Exactly one goroutine touches the core.** `run()` owns it; every other
goroutine — HTTP handlers, the ticker, inbound peer traffic — reaches it over
channels. That indirection is the entire reason the core needs no locks.

The transport has one send queue per peer and **drops under back-pressure**
rather than blocking. Raft already treats message loss as normal, and the
alternative is unbounded memory aimed at a machine that may never return. Drops
are counted and exposed at `/status`, so the choice is observable rather than
silent.

A follower returns **421 Misdirected Request** with the leader's address rather
than a 307 redirect. An automatic redirect would replay the request body
without the client's session bookkeeping noticing — which is precisely how a
retry becomes a duplicate write.

### `sim/` — the deterministic simulator (1,120 lines)

Three rules keep a run reproducible, and breaking any one silently destroys the
guarantee:

1. **One seeded PRNG**, drawn in a fixed order.
2. **No map is iterated where order can reach an outcome.** Nodes and links are
   sorted slices. Go deliberately randomises map iteration, and with a 3-node
   cluster a naive `range` gives exactly 3 possible orderings — so a replay
   matches about one time in three, which looks like flakiness rather than a
   bug.
3. **Every tie broken by an explicit sequence number**, including inside the
   message priority queue, where heap order for equal keys is an implementation
   detail.

The network models latency *ranges* (hence reordering), drops, duplication, and
partitions. The nemesis injects partitions, the pathological leader-isolated
partition, crashes that lose volatile state while keeping storage, and restarts.

Every fault is counted. A chaos run that silently injected nothing is a green
test that proves nothing, and that failure mode is invisible without counters.

---

## 5. Algorithm decisions worth knowing

**`Step` resolves the term first, unconditionally, and it applies to responses.**
A partitioned leader learns it has been deposed from a reply to its own
heartbeat. Check terms only on inbound requests and that leader never steps
down, serving stale reads forever.

**`votedFor` resets when the TERM advances, not when the role changes.** Clear
it on every stepdown and a candidate that voted for itself in term 5 can vote
again for someone else in term 5 — two majorities, two leaders. Never clear it
and nodes refuse to vote in terms they just joined, stalling elections silently.

**A candidate evaluates quorum when it records its own vote.** Not only when
counting a response. A one-node cluster has no peers, so no response ever
arrives; a candidate that only checks on response campaigns forever. This is not
an `N==1` special case — it is the general rule, and it makes `N=3` correct for
the same reason rather than a different one.

**`becomeLeader` appends an empty entry for the new term.** Figure 8 forbids
committing a previous-term entry by replica count alone, so entries inherited
from a deposed leader can only commit *indirectly*, carried along once a
current-term entry commits. Without the no-op, a quiet cluster can hold entries
replicated everywhere and committed nowhere. It is also what makes `ReadIndex`
available promptly after an election.

**Rule 3 truncates only at a genuine conflict.** The naive reading — "truncate
at `PrevLogIndex`, then append" — discards correct entries whenever a
retransmitted message arrives out of order, which under a lossy network is
constantly.

**Rule 5 clamps to the last NEW entry, not to `lastIndex`.** A follower holding
entries beyond what this `AppendEntries` carried must not mark them committed;
this leader did not confirm them.

**Rejections carry `ConflictIndex`/`ConflictTerm` (§5.3).** The follower reports
where its log actually diverges, so the leader skips a whole conflicting term
per round trip. Measured: **200 divergent entries across 4 terms repaired in 6
round trips**, versus 200 by decrementing one at a time.

**Reads use ReadIndex, not a leader lease.** Two conditions must hold before a
leader may answer from local state, and neither is about the data: it must have
committed an entry in its *own* term (the election no-op supplies it), and it
must confirm leadership with a heartbeat round *now*. A lease is faster but
assumes bounded clock drift, and the core has no clock to bound.

---

## 6. Testing strategy

### Mutation testing

Writing tests until they pass tells you nothing about whether they would catch a
bug. So: break the implementation deliberately, and count.

The first pass found **three of five mutations broke no test at all**:

| Mutation | Caught before | after |
|---|---|---|
| `quorum` off by one | 2 | 5 |
| term check skips responses | 1 | 1 |
| votes map not cleared between elections | **0** | 1 |
| `votedFor` cleared on every stepdown | **0** | 1 |
| no immediate heartbeat on election | **0** | 2 |
| `isUpToDate` uses `>` not `>=` | — | 5 |
| Figure 8 clause removed | — | 1 |
| commit takes max not quorum-th | — | 1 |
| follower commits `LeaderCommit` unclamped | **0** | 1 |
| `matchIndex` allowed to move backwards | **0** | 1 |

Every zero was a bug this repo has a *written paragraph* explaining. Prose
caught nothing, because prose cannot fail a build.

One of those zeros was a false negative in the mutation *harness* — a `sed` that
silently failed to match. Worth knowing: an unverified mutation is
indistinguishable from a caught one.

### The fault matrix

`sim/bisect_test.go` runs every combination of network condition and fault
class, so a future failure reports *which class* causes it rather than that
something is wrong:

```
calm/clean-net        partitions/clean-net      chaos/clean-net
calm/lossy-net        crashes/clean-net         chaos/lossy-net
```

This is what turned "all 8 seeds fail" into "only chaos fails, and calm passes"
— which immediately ruled out an entire family of hypotheses.

---

## 7. Debugging the checker

Three linearizability failures were investigated. **One was a real bug. Two were
bugs in the test harness that looked exactly like real bugs.** Telling them
apart is the hard part of this kind of testing, so all three are recorded.

### Failure 1 — incomplete operations encoded as completed

Every chaos seed failed. The fault-free control passed, which was the clue: a
run with no faults cannot produce a stale read.

The bisect narrowed it to a **single operation**: one `PUT` that never returned.

The harness encoded never-returned operations with `Return: math.MaxInt64` and a
zero `Output`. For a write that is harmless — the model ignores a put's output.
For a **read** it asserts the client observed `{"", false}`, which the client
never observed. A false claim, and Porcupine was right to reject it.

The fix is not to drop such operations — a write that may have committed still
constrains everything after it. They are marked **indeterminate**: their effect
is considered, their output is not asserted on.

> Porcupine's Events API turns out *not* to accept a call with no matching
> return; verified directly with a three-case test. "Leave the return out" is
> not an option, so the Operations API with an indeterminate marker is the
> correct encoding.

### Failure 2 — operation times used ticks

Several operations can be invoked or completed within one tick. Equal timestamps
force the checker to treat them as overlapping when the simulator actually ran
them in a definite order. Times now use the history's monotonic **event
counter**, which is the true order.

### Two problems found by benchmarks, not tests

`StepAppendEntries` measured 150,781 ns/op. Membership support had introduced a
backwards scan of the log on every `AppendEntries` to find the current
configuration — O(log length) per message, O(n²) overall. Every test passed;
none of them measure. Tracking the configuration entry's index instead made the
common path O(1): **38.98 ns/op, ~3,900× faster.**

`sendAppend` also had no cap on entries per message, so a follower 100,000
entries behind would be sent all of them at once, which the transport must
buffer whole while every other peer waits behind it. Now bounded at 256.

Neither is a correctness bug, which is exactly why no test caught them. A test
suite answers "is it right"; only a benchmark answers "is it viable".

### Failure 3 — the real one

After the two harness fixes, only `chaos` still failed, and only at seed 2 —
the one configuration combining partitions *and* crashes. The failing prefix:

```
50  PUT  node=1  value="c2-s34"  returned at tick 1811
55  PUT  node=1  value="c3-s28"  returned at tick 1867   ← completed
>>56 GET node=4  called at 1900  -> "c2-s34"             ← the pre-1867 value
```

A read that *began* after a write *completed* returned the older value.

Two hypotheses were tested and killed by instrumentation rather than argument:

- *Reads served before the state machine caught up to `ReadState.Index`?*
  Instrumented and counted: **zero**, across four seeds.
- *Proposals resolved by log index alone?* Yes.

`server/` and the simulator both tracked in-flight writes in
`map[Index]proposal`. When a leader is deposed before its entry commits, a new
leader can place a **different entry at the same index**. Applying it resolved
the old client's operation with the new entry's result — reporting success for a
write that had been truncated away.

Both now key on `(term, index)` and report failure when the term does not match,
so the client retries instead of believing a write that never happened.

**This is the payoff.** The bug is in the production driver, needs two fault
classes simultaneously, and was invisible to 56 unit tests and a 19-assertion
live-cluster test. The simulator found it reproducibly in 20 milliseconds.

### A note on minimisation

The first "minimal counterexample" was one `GET` returning a value nothing had
written — because the minimiser trimmed operations from the front, deleting the
`PUT` that produced it. **Removing writes from a history creates violations that
were never there.** Only prefix-shrinking is sound; the fix was to report the
shortest failing prefix and a window around the operation that breaks it.

---

## 8. Running everything

```bash
go test ./...                                    # all 56 tests
go test -race ./raft/                            # the algorithm
go test -run Linearizable -v ./sim/              # linearizability, all seeds
go test -run TestBisectFaults -v ./sim/          # the fault matrix
go test -run TestSameSeedReplays -v ./sim/       # the determinism claim
./scripts/manual-test.sh                         # a real 3-node cluster

go test -run TestRaftCore -v ./raft/             # the purity checks
go test -bench . -benchmem ./...                 # benchmarks
```

Reproducing a specific simulation failure:

```bash
go test -run 'TestLinearizableUnderChaos/seed=42891' ./sim/
```

That command produces the identical run every time, on any machine, forever.

---

## 9. Known gaps

Stated plainly rather than left to be discovered:

- **No live visualizer** (milestone 6). Benchmarks exist; the real-time view of
  election and partition healing does not.
- **Membership changes are not exercised by the simulator.** They are unit
  tested (7 tests, including the joint-quorum rule and configuration reversion
  on truncation), but the nemesis does not add or remove nodes mid-run, so
  membership under concurrent faults is untested — the exact combination that
  found the `server/` waiter bug.
- **`server/` has no unit tests.** It is covered end-to-end by
  `scripts/manual-test.sh` and, structurally, by `sim/` exercising the same
  driver contract — but its HTTP layer specifically is only tested by the
  script.
- **The linearizability checker can time out** on very long histories. It
  reports `Unknown`, which is logged as "not a failure, but not a proof either"
  rather than being quietly treated as a pass.
- **Reads are served on the leader only.** Follower reads with ReadIndex are a
  known extension and are not implemented.
- **No snapshot streaming.** A snapshot is one message; a multi-gigabyte state
  machine would need chunking.
- **The `calleeName` purity check is a heuristic.** A local variable named `fmt`
  with a `Println` method would false-positive. Nothing in `raft/` does that,
  and over-reporting on a rule this load-bearing is the right trade.
