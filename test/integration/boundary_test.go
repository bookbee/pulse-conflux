// This test has NO build tag: it must run in the default `go test ./...` suite,
// with no infrastructure, on every change.
//
// Constitution Principle II keeps source semantics behind the ingestion
// boundary. A boundary defended only by code review erodes, so it is defended
// here instead: if any package outside internal/ingest/... imports a Redis
// client, this fails.
package integration

import (
	"os/exec"
	"strings"
	"testing"
)

const (
	modulePath    = "in.dmart.pulse.conflux"
	boundaryPkg   = modulePath + "/internal/ingest"
	redisImport   = "github.com/redis/go-redis"
	datastoreHint = "database/sql"
)

func TestOnlyIngestMayImportRedis(t *testing.T) {
	for pkg, imports := range packageImports(t) {
		if strings.HasPrefix(pkg, boundaryPkg) {
			continue // the boundary itself is where Redis belongs
		}
		for _, imp := range imports {
			if strings.Contains(imp, redisImport) {
				t.Errorf("%s imports %s\n"+
					"Redis may only be imported inside %s/... (Constitution Principle II).\n"+
					"Consumption, acknowledgement, consumer-group lifecycle, pending recovery and\n"+
					"claim logic must not leak past the ingestion boundary.", pkg, imp, boundaryPkg)
			}
		}
	}
}

// FR-013 makes this feature dispatch-only: no datastore is introduced. Catching
// a SQL driver here is cheaper than discovering the scope change in review.
func TestNoDatastoreIntroduced(t *testing.T) {
	for pkg, imports := range packageImports(t) {
		for _, imp := range imports {
			if imp == datastoreHint {
				t.Errorf("%s imports %s, but FR-013 makes feature 001 dispatch-only:\n"+
					"no datastore is introduced and no envelope is durably stored.\n"+
					"Persistence is a later feature — change the spec before the code.", pkg, imp)
			}
		}
	}
}

// packageImports returns every package in the module with its direct imports,
// via `go list`. Using the toolchain keeps this honest about build tags and
// generated files in a way that grepping source never is.
func packageImports(t *testing.T) map[string][]string {
	t.Helper()

	out, err := exec.Command("go", "list", "-deps=false",
		"-f", "{{.ImportPath}} {{join .Imports \",\"}}", "../../...").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}

	result := make(map[string][]string)
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		pkg, imports, found := strings.Cut(line, " ")
		if !strings.HasPrefix(pkg, modulePath) {
			continue
		}
		if found {
			result[pkg] = strings.Split(imports, ",")
		} else {
			result[pkg] = nil
		}
	}
	if len(result) == 0 {
		t.Fatal("go list returned no packages from this module — the guard would pass vacuously")
	}
	return result
}
