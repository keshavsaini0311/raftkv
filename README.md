# raftkv

A linearizable, fault-tolerant key-value store built on a from-scratch
implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf),
in Go, with one external dependency.

> **Status:** all six milestones complete.
> See [the design doc](docs/specs/2026-08-29-raftkv-design.md) and
> [the implementation notes](docs/IMPLEMENTATION.md).

---

## What makes this one different

"I implemented Raft" is a common project. The interesting question is the one
that follows: **how do you know it's correct?**

This repo answers with a number rather than an assertion:

```
5 nodes · 4,000 logical ticks · ~33,000 messages
  ~40 network partitions      ~20 leader isolations
  ~30 crash/restart cycles    ~600 dropped, ~600 duplicated messages
  20-47 log compactions          3-11 membership changes

→ every client history checked linearizable by Porcupine
→ 0.02 seconds per seed, zero flakiness
→ seed 42891 replays byte-identically, forever
```

The last line is what makes the rest worth anything. A bug found once is a test
case forever, not a ghost that vanishes when you add a log line.

**And it caught a real bug.** See [below](#the-bug-the-simulator-found).

---

## Architecture

```
                  ┌────────────────────────────┐
                  │  raft/    THE PURE CORE     │
                  │                             │
                  │  no goroutines  no time     │
                  │  no rand        no sockets  │
                  │  no disk                    │
                  │                             │
                  │  (state, input) → (state',  │
                  │                    outputs) │
                  └────────┬────────────────────┘
                           │  Tick() Step() Ready() Advance()
             ┌─────────────┴──────────────┐
             ▼                            ▼
  ┌────────────────────┐      ┌────────────────────────┐
  │ server/   REAL     │      │ sim/  DETERMINISTIC     │
  │ time.Ticker        │      │ virtual clock           │
  │ goroutines         │      │ single-threaded         │
  │ HTTP transport     │      │ seeded PRNG             │
  │ fsync'd log file   │      │ partitions, crashes,    │
  │                    │      │ drops, duplication,     │
  │ production         │      │ reordering              │
  └────────────────────┘      └────────────────────────┘
```

The core is **the same code in both**. It cannot tell which driver it is under,
because it never observes anything that would reveal the difference.

That constraint is mechanically enforced, not documented and hoped for:

| Check | Catches |
|---|---|
| `TestRaftCoreStaysPure` | `import "time"`, `math/rand`, `net`, `os`, `sync`, anything non-stdlib |
| `TestRaftCoreHasNoSideEffects` | `fmt.Print*`, `print`/`println`, `recover`, and **`go f()`** |

The second exists because a goroutine needs no import and is not a function
call, so neither the import check nor a call check can see it — and one `go`
statement in `raft/` would silently end determinism.

---

## Quick start

```bash
go build -o raftkv ./cmd/raftkv

# three nodes on one machine
./raftkv -id 1 -raft :9001 -api :8001 -data /tmp/raftkv \
  -peers 2=http://127.0.0.1:9002,3=http://127.0.0.1:9003 &
./raftkv -id 2 -raft :9002 -api :8002 -data /tmp/raftkv \
  -peers 1=http://127.0.0.1:9001,3=http://127.0.0.1:9003 &
./raftkv -id 3 -raft :9003 -api :8003 -data /tmp/raftkv \
  -peers 1=http://127.0.0.1:9001,2=http://127.0.0.1:9002 &
```

```bash
curl -XPUT --data-binary 'hello' localhost:8001/kv/greeting
curl localhost:8001/kv/greeting            # -> hello   (linearizable read)
curl localhost:8001/status                 # -> {"role":"Leader","term":1,...}
```

Write to a follower and it tells you where to go, rather than silently
accepting:

```bash
$ curl -i -XPUT --data-binary 'x' localhost:8002/kv/k
HTTP/1.1 421 Misdirected Request
X-Raft-Leader: http://127.0.0.1:19001
{"error":"not the leader","leader":"http://127.0.0.1:19001"}
```

### API

| | |
|---|---|
| `GET /kv/{key}` | linearizable read, via ReadIndex |
| `PUT /kv/{key}` | write; body is the value |
| `DELETE /kv/{key}` | delete |
| `GET /status` | role, term, leader, dropped-message counters |
| `GET /keys` | all keys, sorted (**local** read — may be stale, for debugging) |

Send `X-Client-ID` and `X-Client-Seq` headers to get exactly-once semantics: a
retry through a leader change returns the cached response instead of applying
the write twice.

---

## Testing

Four layers, each catching what the one below cannot.

```bash
go test ./...                      # everything
go test -race ./raft/              # the algorithm
go test -run Linearizable ./sim/   # the differentiator
./scripts/manual-test.sh           # a real 3-node cluster over HTTP
```

| Layer | What it proves | Where |
|---|---|---|
| Unit | Figure 2 rule by rule, hand-driven ticks | `raft/*_test.go` |
| Contract | storage semantics, identical for the file and the fake | `storage/` |
| **Simulation** | linearizability under partitions, crashes, loss, compaction | `sim/` |
| **End-to-end** | 19 assertions against real processes, real HTTP, real fsync | `scripts/manual-test.sh` |

Current numbers:

| | src | test | coverage |
|---|---|---|---|
| `raft/` | 1,413 | 1,364 | 74.6% |
| `kv/` | 233 | 199 | 74.2% |
| `storage/` | 273 | 186 | 82.6% |
| `sim/` | 1,120 | 572 | 93.8% |
| `server/` | 920 | 380 | 68.4% |

**77 tests.** Several exist only because *mutation testing* showed the suite
missed them — deleting the votes-map reset, clearing `votedFor` on every
stepdown, or dropping the post-election heartbeat each broke **no test at all**,
despite each being a documented path to two leaders in one term.

---

## Driving it yourself

```sh
./scripts/build-lab.sh          # writes raftlab.html
open raftlab.html
```

One file, ~1.3 MB, no server. It contains the **actual raft core compiled to
WebAssembly** — not a JavaScript retelling of the algorithm, which would drift
from this implementation and quietly teach you something untrue.

**Six guided lessons** drive the cluster step by step and explain what to look
at — how a leader is chosen, what "committed" actually means, the leader that
doesn't know it was deposed, losing the majority, catching a follower up, and
changing membership safely. The lessons run the real simulator, so a lesson
cannot narrate over something the algorithm didn't do.

Or drive it yourself: crash a node, isolate the leader, force an election,
propose a write and watch it sit uncommitted until a majority acknowledges it.

**The timeline rewinds.** Scrub back to any tick and replay it. That works
because the simulator is deterministic — rewinding replays the recorded
commands from a fresh cluster rather than restoring a snapshot, so a revisited
tick is not an approximation of the past, it *is* the past. Act while rewound
and the timeline forks.

The centre of the page is the **replicated log**: every node's log, aligned by
index, coloured by the term that created each entry. Committed entries are
filled; appended-but-uncommitted ones are outlined, because an entry a leader
has written down can still be thrown away. Isolate a leader and its row simply
stops while the others advance — the whole of Raft's safety argument, visible.

That this is possible at all is the payoff for `raft/` having no clock, no
goroutines and no I/O. `TestRaftCoreStaysPure` enforces that; portability is
what it buys.

---

## Watching a recorded run

```sh
go run ./cmd/raftviz -seed 42891 -nodes 5 -ticks 700 -out replay.html
open replay.html
```

That writes one self-contained file — no server, no CDN, no build step — with
the whole trace inlined. Scrub the timeline, step tick by tick, watch the ring
partition and re-form. `#t=286` in the URL opens at that tick.

The same seed produces the same file, byte for byte. That is the point: a
replay you can send someone alongside a bug report, knowing they will see
exactly what you saw.

```
faults:   partitions=8 leader-isolations=3 heals=32 crashes=10 restarts=10 confchanges=1
network:  sent=5530 delivered=4705 partitioned=817 snapshots=14 installed=1
clients:  issued=91 completed=87 rejected=23 lost=0
```

It records only what an **observer** could see — roles, terms, log lengths,
commit indices, messages on the wire, which links are cut. Never `nextIndex`,
never the votes map. A picture drawn from internals shows the implementation
and breaks on every refactor; one drawn from observables shows the algorithm.

---

## The bug the simulator found

`server/` tracked in-flight client writes by **log index alone**:

```go
waiters map[raft.Index]proposal   // before
```

If a leader is deposed before its entry commits, a new leader can place a
*different* entry at that same index. The old driver would then hand the client
that other write's result — **reporting success for a write that had been
truncated away.** An acknowledged write that never happened is the worst thing
a store can do.

It requires partitions **and** crashes together to surface. The 19-assertion
manual cluster test never saw it. Neither did the unit tests. The simulator hit
it at seed 2, reproducibly, in 20 milliseconds.

```go
waiters map[raft.Index]waiter     // after: keyed by (term, index)
```

**Two data races in `server/`** were found the moment that package got tests,
by `go test -race`: the `/keys` and `/status` handlers read the state machine
from HTTP goroutines while the driver loop was writing to it, and
`Transport.Close` could close a send queue while `Send` was writing to it. Both
were violations of this repo's own stated invariant — exactly one goroutine
touches the state machine — and neither was reachable without concurrent load.

Two *harness* bugs were found on the way, both of which looked exactly like Raft
bugs and were not — see [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md#debugging-the-checker).
That distinction is the hardest part of this kind of testing, and worth reading
if you are building something similar.

---

## Milestones

| # | | Status |
|---|---|---|
| 1 | Leader election — randomized timeouts, term safety | ✅ |
| 2 | Log replication — consistency check, Figure 8 commit rule | ✅ |
| 3 | KV state machine, client sessions, ReadIndex linearizable reads | ✅ |
| 4 | Deterministic simulator + Porcupine linearizability | ✅ |
| 5 | Snapshots, log compaction, joint-consensus membership changes | ✅ |
| 6 | Benchmarks, a deterministic replay, and an interactive WASM lab | ✅ |

---

## Performance

The core does no I/O, so these measure the algorithm itself. Apple M1 Pro:

```
BenchmarkTick                  52 ns/op       0 allocs/op
BenchmarkStepAppendEntries     39 ns/op       0 allocs/op
BenchmarkReadyIdle             29 ns/op       1 alloc/op
BenchmarkLogTruncate        4,311 ns/op       1 alloc/op   (1,000-entry log)
```

`Ready` is called on every driver loop iteration, including the overwhelming
majority where nothing happened — so its idle cost is paid constantly and is
worth knowing.

Two problems the benchmarks caught, both invisible to the tests:

- **`StepAppendEntries` was 150,781 ns.** The membership work had added a
  backwards log scan on every message: O(log length) per message, O(n²)
  overall. Now tracked by index — **~3,900× faster.**
- **`AppendEntries` had no size cap.** A follower 100,000 entries behind was
  sent all of them in one message. Now bounded at 256 entries; repair takes
  more round trips and the transport never has to buffer an unbounded message.

## Prior art

The pure-state-machine core follows [`etcd/raft`](https://github.com/etcd-io/raft).
The deterministic-simulation approach follows
[FoundationDB](https://www.youtube.com/watch?v=4fFDFbi3toc) and
[TigerBeetle](https://tigerbeetle.com/blog/2023-03-28-random-fuzzy-thoughts/).
Linearizability checking uses
[Porcupine](https://github.com/anishathalye/porcupine) — the only external
dependency in the module.

## License

MIT
