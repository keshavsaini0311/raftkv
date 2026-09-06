// Command raftviz runs a seeded simulation and writes a self-contained HTML
// replay of it.
//
//	raftviz -seed 42891 -nodes 5 -ticks 400 -out replay.html
//
// The output has no external dependencies: the whole trace is inlined, so the
// file can be opened from disk, mailed, or committed. Same seed, same file.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	_ "embed"

	"github.com/keshavsaini0311/raftkv/sim"
)

// viz.html is a FRAGMENT — style, markup, script, no document wrapper. That is
// deliberate: the same file is both what this command wraps into a standalone
// page and what gets published directly to a host that supplies its own
// <head>. Two copies of one page would drift apart the first time either
// changed.
//
//go:embed viz.html
var page string

// tracePlaceholder is replaced with the trace JSON. Go's encoder escapes < >
// and & by default, so no script-closing sequence can survive into the page.
const tracePlaceholder = "__TRACE__"

const head = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>raftkv · consensus replay</title>
<style>*,*::before,*::after{box-sizing:border-box}body{margin:0}</style>
</head>
<body>
`

const tail = "\n</body>\n</html>\n"

func main() {
	var (
		seed  = flag.Int64("seed", 42891, "PRNG seed; the whole run is a function of this")
		nodes = flag.Int("nodes", 5, "cluster size")
		ticks = flag.Int("ticks", 700, "ticks to record")
		out   = flag.String("out", "replay.html", "output file")
		calm  = flag.Bool("calm", false, "no faults, for a clean baseline replay")
		frag  = flag.Bool("fragment", false, "omit the document wrapper, for a host that supplies its own <head>")
	)
	flag.Parse()

	nem := sim.Chaos()
	if *calm {
		nem = sim.Calm()
	}

	cfg := sim.DefaultConfig(*seed, *nodes)
	cfg.SnapshotEvery = 15 // small on purpose: makes compaction visible in a short run

	c, n, w := sim.RunTraced(cfg, nem, sim.DefaultWorkload(), *ticks)

	blob, err := json.Marshal(c.Trace())
	if err != nil {
		fatal(fmt.Sprintf("encoding trace: %v", err))
	}
	if !strings.Contains(page, tracePlaceholder) {
		fatal("viz.html no longer contains " + tracePlaceholder)
	}
	html := strings.Replace(page, tracePlaceholder, string(blob), 1)
	if !*frag {
		html = head + html + tail
	}

	if err := os.WriteFile(*out, []byte(html), 0o644); err != nil {
		fatal(fmt.Sprintf("writing %s: %v", *out, err))
	}

	fmt.Printf("%s — %d ticks, %d nodes, seed %d, %.0f KB\n",
		*out, *ticks, *nodes, *seed, float64(len(html))/1024)
	fmt.Println("  faults:  ", n.Summary())
	fmt.Println("  network: ", c.Stats())
	fmt.Printf("  clients:  issued=%d completed=%d rejected=%d lost=%d\n",
		w.Issued, w.Completed, w.Rejected, c.ProposalsLost)
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "raftviz:", msg)
	os.Exit(1)
}
