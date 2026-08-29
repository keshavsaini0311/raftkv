# raftkv — Design

A distributed key-value store built on Raft consensus, in Go.

**Status:** design. Milestones 1–4 specified; 5–6 sketched.
**Date:** 2026-08-29

---

## 1. Goal

Build a linearizable, fault-tolerant key-value store on a from-scratch Raft
implementation — and be able to *prove* it correct rather than assert it.

The proof is the point. "I implemented Raft" is the most common distributed-systems
portfolio project in existence; interviewers have seen hundreds. What almost none of
them contain is an answer to the obvious follow-up: **"how do you know it works?"**

This project answers that with deterministic simulation testing: a seeded, virtual-clock
simulator that injects partitions, message loss, reordering, and crashes, and verifies
every resulting client history is linearizable. A failing seed replays identically
forever, so concurrency bugs stop being ghosts and become test cases.

### Non-goals

- Beating etcd on anything. This is a correctness exercise, not a production store.
- Multi-Raft / sharding. One replication group.
- Novel consensus. Raft as published, no variations, no "improvements."
- Fancy storage. A single append-only file and an in-memory map are sufficient.

### Learning model

**Keshav writes the algorithm.** Election, replication, and commit rules are implemented
by hand from the extended Raft paper — not generated, not copied from `etcd/raft`.
Claude scaffolds non-algorithmic plumbing (repo layout, test harness, CI), explains
concepts, and reviews.

The reason is practical, not pedagogical: a Raft repo you can't defend line-by-line in
an interview is worth less than no repo at all, because it invites questions you'll fail.

**Primary reference:** Ongaro & Ousterhout, *In Search of an Understandable Consensus
Algorithm (Extended Version)*. Figure 2 is the complete specification — the whole
algorithm fits on one page, and that page is the contract this implementation must meet.

---

## 2. The load-bearing decision: the determinism boundary

**The Raft core is a pure state machine.** No goroutines, no timers, no sockets,
no `time.Now()`, no `rand`, no disk. It is a function of `(state, input) → (state', outputs)`.

```go
func (n *Node) Tick()                 // advance logical time by one step
func (n *Node) Step(m Message) error  // deliver one inbound message
func (n *Node) Ready() Ready          // drain intentions: messages to send,
                                      // entries to persist, entries to apply
func (n *Node) Advance(r Ready)       // acknowledge the caller handled a Ready
```

Timeouts are counted in **ticks**, not durations. Randomness arrives through an injected
source. The core never learns that networks or disks exist — it only says what it *wants*
done, and a driver does it.

Two drivers wire the core to the world:

```
                     ┌──────────────────────┐
                     │   raft  (pure core)  │
                     │  no I/O, no clock,   │
                     │  no goroutines       │
                     └──────────┬───────────┘
              ┌─────────────────┴─────────────────┐
   ┌──────────▼──────────┐          ┌─────────────▼─────────────┐
   │  server  (real)     │          │  sim  (deterministic)     │
   │  goroutines, timers │          │  virtual clock            │
   │  TCP, append-only   │          │  seeded PRNG              │
   │  disk, HTTP API     │          │  partitions, drops,       │
   │                     │          │  reorder, crash/restart   │
   └─────────────────────┘          └───────────────────────────┘
```

### Why this and not the obvious design

The intuitive implementation spawns a goroutine per peer and calls `time.After` for
timeouts. It works, and it is effectively untestable: races surface once in ~10,000 runs
and vanish when you add a log line.

With a pure core, a test drives five nodes through 100,000 logical ticks — with
partitions and crashes throughout — in **milliseconds of wall time, with zero flakiness**.
Seed 42891 produces byte-identical behavior today and next year.

**This decision cannot be deferred.** Deterministic simulation cannot be bolted onto a
Raft that calls `time.Now()` internally; retrofitting it means rewriting the core. That
is precisely why milestone 4 is specified before milestone 1 is written.

Cost, stated honestly: it is less intuitive to write. The instinct to "just start a timer
here" has to be resisted every time. Roughly 20% more upfront thought, in exchange for a
system whose bugs are reproducible.

Prior art: this is the `etcd/raft` architecture, and the testing approach FoundationDB
and TigerBeetle are known for.

---

## 3. Package layout

```
raftkv/
├── raft/                  pure state machine — the learning core
│   ├── raft.go            Node, roles, state transitions, election
│   ├── log.go             log storage, index/term bookkeeping, conflict resolution
│   ├── message.go         RPC message types
│   ├── ready.go           Ready struct, Advance
│   └── raft_test.go       unit tests, hand-driven — no clock, no network
├── storage/               persistence
│   ├── storage.go         interface: SaveHardState, Append, Entries, ...
│   └── file.go            append-only file implementation
├── kv/                    the replicated state machine
│   └── kv.go              map + Apply(entry), client-session dedup
├── server/                real-world driver
│   ├── server.go          ticker goroutine, Ready loop
│   ├── transport.go       HTTP/gRPC peer transport
│   └── api.go             client API: Get, Put, Delete
├── sim/                   deterministic simulator  (milestone 4)
│   ├── cluster.go         virtual clock, node lifecycle
│   ├── network.go         latency, drop, reorder, partition
│   ├── nemesis.go         fault schedules
│   └── linearizability_test.go   Porcupine model + checker
├── cmd/
│   └── raftkv/main.go
└── docs/
    └── specs/2026-08-29-raftkv-design.md
```

**The dependency rule:** `raft/` imports nothing but the standard library — and not
`time`, `math/rand`, `net`, or `os`. If `raft/` ever needs one of those, the design has
been violated. This is checkable in CI with a simple import-graph test, and that test is
worth writing early.

---

## 4. Core types

```go
type NodeID uint64
type Term   uint64
type Index  uint64

type Role int
const (
    Follower Role = iota
    Candidate
    Leader
)

// Entry is one command in the replicated log.
type Entry struct {
    Term  Term
    Index Index
    Data  []byte   // opaque to raft/ — kv/ interprets it
}

// HardState is the subset that MUST be persisted before responding to any RPC.
type HardState struct {
    Term     Term
    VotedFor NodeID   // 0 == none
    Commit   Index
}

// Ready is the set of intentions the core hands to its driver.
// The driver MUST persist HardState and Entries before sending Messages.
type Ready struct {
    HardState   *HardState  // nil if unchanged
    Entries     []Entry     // to append to stable storage
    CommittedEntries []Entry // safe to apply to the state machine
    Messages    []Message   // to send to peers
}
```

**Message types** (mirroring Figure 2, plus responses):

| Message | Fields |
|---|---|
| `RequestVote` | `Term, CandidateID, LastLogIndex, LastLogTerm` |
| `RequestVoteResp` | `Term, VoteGranted` |
| `AppendEntries` | `Term, LeaderID, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit` |
| `AppendEntriesResp` | `Term, Success, ConflictIndex, ConflictTerm` |

`ConflictIndex`/`ConflictTerm` are the standard optimization: they let a leader skip a
whole conflicting term in one round trip instead of decrementing `nextIndex` one entry at
a time. Not in Figure 2, but described in §5.3 of the paper.

---

## 5. Milestone 1 — Leader election

**Deliverable:** a cluster of N nodes elects exactly one leader per term, and re-elects
after a leader is lost. No log replication yet — the log stays empty.

### State

```go
type Node struct {
    id      NodeID
    peers   []NodeID
    role    Role

    // persistent (Figure 2)
    currentTerm Term
    votedFor    NodeID

    // volatile
    votes map[NodeID]bool   // candidate: who has granted

    // tick-based timing — NOT durations
    electionElapsed  int
    heartbeatElapsed int
    electionTimeout  int   // randomized per election, from injected rand
    heartbeatTimeout int   // fixed

    rand *rand.Rand        // INJECTED — never rand.New(time.Now()) inside
    msgs []Message         // outbound queue, drained by Ready()
}
```

### Rules to implement (Figure 2)

**All servers:** if any RPC request *or response* carries `Term > currentTerm`, set
`currentTerm = Term`, clear `votedFor`, and become a follower. This check runs before
any message-specific handling — it is the single most commonly botched rule, and it
applies to responses too, not just requests.

**Follower:** on `electionElapsed >= electionTimeout` with no `AppendEntries` from the
current leader → become candidate.

**Candidate:** increment `currentTerm`, vote for self, reset the election timer, pick a
*new* random timeout, broadcast `RequestVote`. On a majority → leader. On `AppendEntries`
from a node with term ≥ ours → follower. On timeout → start a new election.

**Leader:** on election, immediately broadcast empty `AppendEntries` (heartbeat), and
repeat every `heartbeatTimeout` ticks.

**Granting a vote:** reply false if `Term < currentTerm`. Otherwise grant iff
`votedFor` is unset or already this candidate, **and** the candidate's log is at least as
up-to-date as ours. Up-to-date means: compare last entries — higher term wins; equal
terms, longer log wins. (Trivial in M1 with an empty log, but implement it now so M2
inherits it correctly.)

### Randomized timeouts

Election timeouts must be randomized per election, or split votes recur indefinitely.
Typical: `electionTimeout ∈ [10, 20)` ticks, `heartbeatTimeout = 1` tick — chosen so
`broadcastTime ≪ electionTimeout ≪ MTBF`.

Draw from the **injected** `rand`. `rand.New(rand.NewSource(time.Now().UnixNano()))`
anywhere in `raft/` breaks determinism and is the exact mistake this architecture exists
to prevent.

### Tests

Hand-driven — construct nodes, call `Tick()`/`Step()` directly, assert. No network, no
sleeps, no goroutines.

1. Single node elects itself immediately on timeout.
2. Three nodes: exactly one becomes leader; the other two are followers in the same term.
3. A leader receiving a higher term steps down to follower.
4. Split vote (all candidates simultaneously) → no leader that term → a later term
   resolves it.
5. Heartbeats prevent followers from starting elections.
6. A candidate that receives `AppendEntries` at an equal-or-higher term becomes a follower.
7. A node does not grant two votes in the same term.

### Done when

`go test ./raft/` passes all seven, and an import-graph test asserts `raft/` imports
neither `time` nor `math/rand`'s global source, nor `net`, nor `os`.

---

## 6. Milestone 2 — Log replication

**Deliverable:** entries proposed to the leader replicate to a majority, commit, and
apply in the same order on every node.

### Added state

```go
    log         *Log             // entries, with index/term bookkeeping
    commitIndex Index
    lastApplied Index

    // leader only, reinitialized on election
    nextIndex  map[NodeID]Index  // init: lastLogIndex + 1
    matchIndex map[NodeID]Index  // init: 0
```

### AppendEntries receiver (Figure 2, exactly)

1. Reply false if `Term < currentTerm`.
2. Reply false if the log has no entry at `PrevLogIndex` whose term equals `PrevLogTerm`
   — and return `ConflictIndex`/`ConflictTerm` so the leader can back up fast.
3. If an existing entry conflicts with a new one (same index, different term), delete
   that entry **and everything after it**.
4. Append any new entries not already present.
5. If `LeaderCommit > commitIndex`, set `commitIndex = min(LeaderCommit, index of last new entry)`.

Step 3 is subtle and worth stating explicitly: truncation is only correct when there is a
genuine conflict. A stale or duplicated `AppendEntries` must not truncate entries the
follower has already correctly accepted — a real bug that deterministic replay is very
good at catching, and that random testing usually misses.

### Leader commit rule

Advance `commitIndex` to the largest `N` such that `N > commitIndex`, a majority of
`matchIndex[i] >= N`, **and `log[N].Term == currentTerm`**.

That last clause is the Figure 8 condition. Without it, a leader can commit an entry
from a previous term that a later leader then overwrites — the safety violation the paper
devotes a full figure to. Entries from earlier terms commit *indirectly*, carried along
once a current-term entry commits.

### Tests

8. A single-node cluster commits and applies immediately.
9. Three nodes: an entry proposed to the leader reaches a majority and commits.
10. A follower with a divergent tail is truncated and repaired.
11. `nextIndex` backtracking converges on a follower far behind.
12. An entry replicated to a *minority* does not commit.
13. A previous-term entry does not commit on replica count alone (Figure 8).
14. All nodes apply the same entries in the same order.

### Done when

Tests 1–14 pass, and a `kv.Apply` stub shows identical state across all nodes.

---

## 7. Milestone 3 — KV state machine and client API *(designed, not planned)*

- `kv/` holds `map[string][]byte` plus an `Apply(Entry)` that decodes a command.
- Commands: `Put(k,v)`, `Delete(k)`, `Get(k)`.
- **Client sessions for exactly-once semantics:** each client sends `(ClientID, SeqNum)`.
  The state machine records the last applied `SeqNum` per client and returns the cached
  response on retry. Without this, a client that retries through a leader change applies
  its write twice — and the store is no longer linearizable.
- **Linearizable reads:** reads must not be served from a stale leader. Two options:
  *ReadIndex* (leader confirms leadership with a heartbeat round before serving) or
  *leader lease* (time-bound, faster, requires bounded clock drift). **Take ReadIndex** —
  it needs no clock assumption, which keeps the core pure.
- Non-leaders redirect clients to the current leader.

## 8. Milestone 4 — Deterministic simulator and linearizability *(designed, not planned)*

This is the differentiator; everything above is table stakes.

- **Virtual clock.** The simulator owns time. Advancing means calling `Tick()` on every
  node in a fixed order. No wall-clock time is consumed.
- **Network model.** A priority queue of in-flight messages keyed by virtual delivery
  time. Per-link configurable latency distribution, drop rate, and reorder rate.
  Partitions are link sets that drop everything.
- **Nemesis.** Scheduled faults from the seeded PRNG: random partitions (including the
  pathological leader-isolated case), node crash and restart with only persisted state
  surviving, and message storms.
- **History recording.** Every client operation records `invoke(op, t)` and
  `return(result, t)` on the virtual clock.
- **Linearizability checking.** Feed the history to
  [Porcupine](https://github.com/anishathalye/porcupine) with a register/KV model.
  Porcupine either proves the history linearizable or produces a counterexample.
- **Reproduction.** Every run is `(seed, config)`. A failure prints its seed; re-running
  that seed replays the identical failure. Add a shrinker that reduces a failing seed's
  fault schedule to the minimal one that still fails.

**The claim this unlocks:** *"seed 42891 reproduces a split-brain window; here is the
trace and here is the fix."* That is a falsifiable statement about correctness, and it is
what separates this repo from the hundreds of others.

## 9. Milestones 5–6 — sketch only

- **M5:** snapshots and log compaction (`InstallSnapshot` RPC); joint-consensus
  membership changes. These are the parts most hobby implementations skip, so finishing
  them is itself a signal.
- **M6:** a live visualizer (leader election, replication, partition healing rendered in
  real time) for the portfolio, plus a benchmark write-up published into `systems-lab`.

---

## 10. Testing strategy

Three layers, each catching what the one below cannot:

| Layer | Mechanism | Catches |
|---|---|---|
| Unit | hand-driven `Tick`/`Step`, no I/O | Figure 2 rule violations |
| Integration | in-process cluster, real driver | driver/core wiring bugs |
| Simulation | seeded virtual-clock cluster + Porcupine | safety violations under fault |

**No `time.Sleep` in any test.** A sleep in a distributed-systems test is a flake with a
delay attached; if a test needs to wait for something, the simulator advances the clock
instead.

## 11. Risks

| Risk | Mitigation |
|---|---|
| Scope death — Raft is bigger than it looks | Milestones ship independently; M1–M3 is a working store on its own |
| The purity discipline slips under deadline | CI import-graph test fails the build, not a code review |
| Porcupine checking blows up exponentially | Bound history length; Porcupine has a timeout mode |
| Figure 8 / commit-rule subtlety | Explicit test (13) before it can regress |

## 12. Open questions

- Peer transport: HTTP+JSON (debuggable, easy) or gRPC (realistic, more setup)?
  Leaning HTTP+JSON for M1–M3; it keeps focus on the algorithm.
- Storage format: length-prefixed protobuf, or JSON lines? JSON is inspectable by hand
  during debugging, which matters more than bytes here.
