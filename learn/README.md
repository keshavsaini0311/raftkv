# Go foundations tour

Scratch exercises for learning Go before raft milestone 1. Not part of the raftkv
system — nothing here is imported by `raft/`, `server/`, or `sim/`. Safe to delete
once the tour is done.

Run everything: `go test ./learn/...`

| # | Package | Topic | Status |
|---|---------|-------|--------|
| 1 | `basics/` | Packages, exports, zero values | done |
| 2 | `enums/` | Named types, constants, iota, `String()` | in progress |
| 3 | | Structs & pointer vs value receivers | |
| 4 | | Pointers, nil, escape to heap | |
| 5 | | Slices — header, append, aliasing | |
| 6 | | Maps — comma-ok, random iteration order | |
| 7 | | Interfaces — implicit satisfaction, the nil trap | |
| 8 | | Errors as values, wrapping, sentinels | |
| 9 | | Testing — table-driven, subtests, `-race` | |
| 10 | | Concurrency — goroutines, channels, mutexes | |
