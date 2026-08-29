# Go foundations tour

Scratch exercises for learning Go before raft milestone 1. Not part of the raftkv
system — nothing here is imported by `raft/`, `server/`, or `sim/`. Safe to delete
once the tour is done.

Run everything: `go test ./learn/...`

| # | Package | Topic | Status |
|---|---------|-------|--------|
| 1 | `basics/` | Packages, exports, zero values | done |
| 2 | `enums/` | Named types, constants, iota, `String()` | done |
| 3 | `receivers/` | Structs & pointer vs value receivers | done |
| 4 | — | Pointers & nil — folded into 3 and 5 | merged |
| 5 | `slicing/` | Slices — header, append, aliasing, nil | in progress |
| 6 | | Maps — comma-ok, random iteration order | |
| 7 | | Interfaces — implicit satisfaction, the nil trap | |
| 8 | | Errors as values, wrapping, sentinels | |
| 9 | | Testing — table-driven, subtests, `-race` | |
| 10 | | Concurrency — goroutines, channels, mutexes | |
