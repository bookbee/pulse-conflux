package housekeeping

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Housekeeping is authorised to do exactly two things: report read-only facts
// about the queue, and maintain the consumer groups this service owns. It must
// never delete, trim, expire or rename queue data outside its own groups
// (FR-020a) — stream trimming and the log list's expiry belong to the gateway,
// and the eviction policy is deliberately set so writes fail loudly rather than
// keys vanishing under a consumer.
//
// This guard reads the package's own source. It cannot prove intent, but it
// catches the mechanical way the rule gets broken: reaching for a destructive
// command by name.
func TestNoDestructiveQueueCommands(t *testing.T) {
	forbidden := map[string]string{
		"XTRIM":         "stream trimming belongs to the gateway",
		"XDEL":          "deleting entries destroys data another repo may not have read",
		"DEL":           "deleting keys destroys shared queue state",
		"UNLINK":        "same as DEL, asynchronously",
		"EXPIRE":        "the log list's expiry belongs to the gateway",
		"PEXPIRE":       "as EXPIRE",
		"FLUSHDB":       "never",
		"FLUSHALL":      "never",
		"RENAME":        "names are cross-repo contracts",
		"LTRIM":         "trimming the log list would discard entries the gateway wrote",
		"XGroupDestroy": "destroying a group orphans every pending entry in it",
	}

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no source files found — the guard would pass vacuously")
	}

	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		// Parse rather than grep, so a command name inside a comment explaining
		// why it is forbidden does not trip the guard.
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, f, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			var name string
			switch x := n.(type) {
			case *ast.SelectorExpr:
				name = x.Sel.Name
			case *ast.BasicLit:
				name = strings.Trim(x.Value, `"`)
			default:
				return true
			}
			for cmd, why := range forbidden {
				if strings.EqualFold(name, cmd) {
					t.Errorf("%s: uses %q — %s (FR-020a)", fset.Position(n.Pos()), cmd, why)
				}
			}
			return true
		})
	}
}
