# raftkv

A linearizable, fault-tolerant key-value store built on a from-scratch implementation of
the [Raft consensus algorithm](https://raft.github.io/raft.pdf), in Go.

> **Status:** design complete, implementation starting.
> See [the design doc](docs/specs/2026-08-29-raftkv-design.md).

## What makes this one different

"I implemented Raft" is a common project. The interesting question is the one that
follows it: **how do you know it's correct?**

The Raft core here is a **pure state machine** — no goroutines, no timers, no sockets, no
`time.Now()`, no `rand`. It only consumes inputs and emits intentions:

```go
func (n *Node) Tick()                 // advance logical time one step
func (n *Node) Step(m Message) error  // deliver one inbound message
func (n *Node) Ready() Ready          // messages to send, entries to persist / apply
```

Because the algorithm is decoupled from its effects, a test can drive a five-node cluster
through 100,000 logical ticks — with network partitions, dropped and reordered messages,
and crash/restart cycles — in **milliseconds of wall time, with zero flakiness**. Every run
is identified by a seed, so a failure found today replays identically a year from now.

Client histories are then checked for linearizability with
[Porcupine](https://github.com/anishathalye/porcupine), which either proves the history
correct or hands back a counterexample.

That turns "my Raft works" from an assertion into a falsifiable claim.

## Architecture

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

`raft/` imports nothing but the standard library — and not `time`, `math/rand`, `net`, or
`os`. A CI test enforces that, because the guarantee is only worth as much as its weakest
import.

## Roadmap

| # | Milestone | Status |
|---|-----------|--------|
| 1 | Leader election — randomized timeouts, term safety | in progress |
| 2 | Log replication — consistency check, commit rules | |
| 3 | KV state machine, client sessions, linearizable reads | |
| 4 | Deterministic simulator + linearizability checking | |
| 5 | Snapshots, log compaction, membership changes | |
| 6 | Live visualizer + benchmarks | |

## Prior art

The pure-state-machine core follows [`etcd/raft`](https://github.com/etcd-io/raft). The
deterministic-simulation approach follows
[FoundationDB](https://www.youtube.com/watch?v=4fFDFbi3toc) and
[TigerBeetle](https://tigerbeetle.com/blog/2023-03-28-random-fuzzy-thoughts/).

## License

MIT
