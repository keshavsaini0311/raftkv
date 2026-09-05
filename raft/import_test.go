// The purity check. Your design doc says:
//
//	"The dependency rule: raft/ imports nothing but the standard library — and
//	not time, math/rand, net, or os. If raft/ ever needs one of those, the
//	design has been violated. This is checkable in CI with a simple
//	import-graph test, and that test is worth writing early."
//
// This is that test, and it is written before any algorithm, on purpose:
//
//	"The purity discipline slips under deadline
//	 -> CI import-graph test fails the build, not a code review."
//
// A guarantee enforced by reviewer attention is not a guarantee.
package raft_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// nonTestGoFiles lists the production files of this package — the only ones the
// purity rules apply to. _test.go files are exempt: this file itself imports
// go/parser and path/filepath, which the core may not.
func nonTestGoFiles(t *testing.T) []string {
	t.Helper()

	all, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("globbing package files: %v", err)
	}

	var files []string
	for _, f := range all {
		if !strings.HasSuffix(f, "_test.go") {
			files = append(files, f)
		}
	}
	if len(files) == 0 {
		t.Fatal("no non-test .go files found; is this test running in raft/?")
	}
	return files
}

// Direct imports only — deliberately, and this is worth understanding.
//
// A TRANSITIVE check would be wrong here. fmt imports os internally, so
// "raft/ transitively reaches os" is true the moment you use fmt, and would
// make this test unpassable. What matters for determinism is whether raft/
// ITSELF calls time.Now or rand.Intn. What fmt does inside is not our problem.
var banned = map[string]string{
	"time":          "timeouts must be counted in ticks, not durations",
	"math/rand":     "randomness must arrive through an injected source",
	"math/rand/v2":  "randomness must arrive through an injected source",
	"crypto/rand":   "randomness must arrive through an injected source",
	"net":           "the core does not know networks exist; a driver sends",
	"net/http":      "the core does not know networks exist; a driver sends",
	"os":            "the core does not touch disk; a driver persists",
	"os/exec":       "the core does not touch the operating system",
	"syscall":       "the core does not touch the operating system",
	"sync":          "the core is single-threaded; no locks should be needed",
	"sync/atomic":   "the core is single-threaded; no atomics should be needed",
	"context":       "cancellation is a driver concern, not a core concern",
	"log":           "the core emits intentions, not side effects",
	"runtime/pprof": "the core does not profile itself",
}

func TestRaftCoreStaysPure(t *testing.T) {
	files := nonTestGoFiles(t)
	fset := token.NewFileSet()
	checked := 0

	for _, file := range files {
		checked++

		af, err := parser.ParseFile(fset, file, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parsing %s: %v", file, err)
		}

		for _, imp := range af.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			pos := fset.Position(imp.Pos())

			if reason, isBanned := banned[path]; isBanned {
				t.Errorf("%s:%d imports %q — %s",
					file, pos.Line, path, reason)
				continue
			}

			// Standard library paths have no dot in their first element.
			// Anything with a dot is a domain: a third-party module, or even
			// our own github.com/keshavsaini0311/raftkv/... packages, which
			// raft/ must not depend on either.
			if first, _, _ := strings.Cut(path, "/"); strings.Contains(first, ".") {
				t.Errorf("%s:%d imports %q — raft/ may import only the "+
					"standard library", file, pos.Line, path)
			}
		}
	}

	if checked == 0 {
		t.Error("no non-test .go files were checked")
	}
	t.Logf("checked %d non-test file(s) in raft/", checked)
}

// bannedCalls closes the hole the import check cannot.
//
// `fmt` must stay importable — Sprintf and Errorf are needed for String()
// methods and error messages. But fmt.Println WRITES TO STDOUT, and that is
// exactly the side effect the core is not allowed to have. The problem is the
// CALL, not the import, so no import-level rule can see it.
//
// The core emits intentions. It does not print, log, or write.
var bannedCalls = map[string]string{
	"fmt.Print":    "the core does not write to stdout; emit intentions instead",
	"fmt.Printf":   "the core does not write to stdout; emit intentions instead",
	"fmt.Println":  "the core does not write to stdout; emit intentions instead",
	"fmt.Fprint":   "the core does not write to any stream",
	"fmt.Fprintf":  "the core does not write to any stream",
	"fmt.Fprintln": "the core does not write to any stream",
	"print":        "builtin print writes to stderr",
	"println":      "builtin println writes to stderr",
	"recover":      "the core does not swallow panics; a bug should surface",
}

func TestRaftCoreHasNoSideEffects(t *testing.T) {
	files := nonTestGoFiles(t)
	fset := token.NewFileSet()

	for _, file := range files {
		// Full parse this time. ImportsOnly stops at the import block, and the
		// violations we are hunting live in function bodies.
		af, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", file, err)
		}

		ast.Inspect(af, func(node ast.Node) bool {
			switch v := node.(type) {
			case *ast.CallExpr:
				name := calleeName(v.Fun)
				if reason, banned := bannedCalls[name]; banned {
					pos := fset.Position(v.Pos())
					t.Errorf("%s:%d calls %s() — %s", file, pos.Line, name, reason)
				}

			case *ast.GoStmt:
				// `go f()` — invisible to both the import and call checks,
				// since a goroutine needs no import and is not a call.
				pos := fset.Position(v.Pos())
				t.Errorf("%s:%d spawns a goroutine — the core is single-threaded "+
					"by construction; concurrency belongs in server/", file, pos.Line)
			}
			return true
		})
	}
}

// calleeName renders a call target as "pkg.Func" or "Func".
//
// A heuristic, and knowingly so: a local variable named fmt with a Println
// method would be a false positive. Nothing in raft/ does that, and a check
// that occasionally over-reports on a rule this important is the right trade.
func calleeName(e ast.Expr) string {
	switch f := e.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		if pkg, ok := f.X.(*ast.Ident); ok {
			return pkg.Name + "." + f.Sel.Name
		}
	}
	return ""
}
