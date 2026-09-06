// Mechanical enforcement of the design doc's dependency rule:
//
//	"raft/ imports nothing but the standard library — and not time, math/rand,
//	 net, or os. If raft/ ever needs one of those, the design has been violated."
//
// Written before the algorithm, on purpose. A guarantee enforced by reviewer
// attention is not a guarantee.
package raft_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

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
		t.Fatal("no non-test .go files found; is this running in raft/?")
	}
	return files
}

// Direct imports only, deliberately. A transitive check would be wrong: fmt
// imports os internally, so "raft/ transitively reaches os" is true the moment
// you format a string. What matters is whether raft/ ITSELF calls time.Now.
var bannedImports = map[string]string{
	"time":          "timeouts are counted in ticks, not durations",
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
	"context":       "cancellation is a driver concern",
	"log":           "the core emits intentions, not side effects",
	"runtime/pprof": "the core does not profile itself",
}

func TestRaftCoreStaysPure(t *testing.T) {
	fset := token.NewFileSet()
	for _, file := range nonTestGoFiles(t) {
		af, err := parser.ParseFile(fset, file, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parsing %s: %v", file, err)
		}
		for _, imp := range af.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			pos := fset.Position(imp.Pos())
			if reason, banned := bannedImports[path]; banned {
				t.Errorf("%s:%d imports %q — %s", file, pos.Line, path, reason)
				continue
			}
			// Stdlib paths have no dot in their first element. A dot means a
			// domain: third-party, or our own module, neither of which raft/
			// may depend on.
			if first, _, _ := strings.Cut(path, "/"); strings.Contains(first, ".") {
				t.Errorf("%s:%d imports %q — raft/ may import only the standard library",
					file, pos.Line, path)
			}
		}
	}
}

// Closes the hole the import check cannot see. fmt must stay importable —
// Sprintf and Errorf are legitimate — but fmt.Println WRITES TO STDOUT, and the
// problem is the call, not the import.
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
	fset := token.NewFileSet()
	for _, file := range nonTestGoFiles(t) {
		af, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parsing %s: %v", file, err)
		}
		ast.Inspect(af, func(node ast.Node) bool {
			switch v := node.(type) {
			case *ast.CallExpr:
				if reason, banned := bannedCalls[calleeName(v.Fun)]; banned {
					t.Errorf("%s:%d calls %s() — %s",
						file, fset.Position(v.Pos()).Line, calleeName(v.Fun), reason)
				}
			case *ast.GoStmt:
				// Invisible to both other checks: a goroutine needs no import
				// and is not a call. It would also destroy determinism.
				t.Errorf("%s:%d spawns a goroutine — the core is single-threaded "+
					"by construction; concurrency belongs in server/",
					file, fset.Position(v.Pos()).Line)
			}
			return true
		})
	}
}

// A heuristic: a local variable named fmt with a Println method would
// false-positive. Over-reporting on a rule this load-bearing is the right trade.
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
