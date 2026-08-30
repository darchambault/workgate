package db

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestPathEnvOverride(t *testing.T) {
	custom := filepath.Join(t.TempDir(), "override.db")
	t.Setenv("WORKGATE_DB", custom)
	got, err := Path()
	if err != nil {
		t.Fatal(err)
	}
	if got != custom {
		t.Fatalf("Path() = %q, want %q", got, custom)
	}
}

func TestPathDefaultsToUserCacheDir(t *testing.T) {
	t.Setenv("WORKGATE_DB", "")
	got, err := Path()
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join("Workgate", "workgate.db"); !strings.HasSuffix(got, want) {
		t.Fatalf("Path() = %q, want suffix %q", got, want)
	}
}

func TestOpenCreatesDirectoryAndIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "dir", "workgate.db")
	for i := 0; i < 2; i++ {
		d, err := Open(path)
		if err != nil {
			t.Fatalf("open #%d: %v", i+1, err)
		}
		if _, err := d.Exec(`SELECT COUNT(*) FROM workloads`); err != nil {
			t.Fatalf("schema missing on open #%d: %v", i+1, err)
		}
		if _, err := d.Exec(`SELECT COUNT(*) FROM completions`); err != nil {
			t.Fatalf("completions schema missing on open #%d: %v", i+1, err)
		}
		d.Close()
	}
}

// TestPartialIndexForbidsTwoOwners verifies the database-level backstop:
// even bypassing the queue logic, SQLite refuses a second running row.
func TestPartialIndexForbidsTwoOwners(t *testing.T) {
	d, err := Open(filepath.Join(t.TempDir(), "workgate.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	insert := `INSERT INTO workloads (id, resource, state, created_at, heartbeat_at)
	           VALUES (?, 'unity', 'running', 1, 1)`
	if _, err := d.Exec(insert, "aaa111"); err != nil {
		t.Fatalf("first running row: %v", err)
	}
	if _, err := d.Exec(insert, "bbb222"); err == nil {
		t.Fatal("second running row for same resource was allowed")
	}
	// A second running row for a different resource is fine.
	if _, err := d.Exec(`INSERT INTO workloads (id, resource, state, created_at, heartbeat_at)
	                     VALUES ('ccc333', 'steam-upload', 'running', 1, 1)`); err != nil {
		t.Fatalf("running row for different resource: %v", err)
	}
}

// TestOpenAddsCompletionsToAPreExistingDatabase is the upgrade proof: the
// machine-global database is shared between binary versions, so a new binary
// must add its table to a database an older one created, without disturbing
// the coordination rows already in it.
func TestOpenAddsCompletionsToAPreExistingDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workgate.db")
	old, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.Exec(`DROP TABLE completions`); err != nil {
		t.Fatalf("simulating an older schema: %v", err)
	}
	if _, err := old.Exec(`INSERT INTO workloads (id, resource, state, created_at, heartbeat_at)
	                       VALUES ('aaa111', 'gpu', 'running', 1, 1)`); err != nil {
		t.Fatalf("seeding a workload: %v", err)
	}
	old.Close()

	d, err := Open(path)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	defer d.Close()
	if _, err := d.Exec(`SELECT COUNT(*) FROM completions`); err != nil {
		t.Fatalf("completions missing after upgrade: %v", err)
	}
	var n int
	if err := d.QueryRow(`SELECT COUNT(*) FROM workloads WHERE id = 'aaa111'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("pre-existing workload row count = %d, want 1", n)
	}
}

// TestCompletionIDsMayRepeat guards a deliberate omission. Workload ids are
// three random bytes and unique only among live rows, so a UNIQUE constraint
// on completions.id would let a cosmetic collision fail a release transaction
// and strand a resource until the stale threshold.
func TestCompletionIDsMayRepeat(t *testing.T) {
	d, err := Open(filepath.Join(t.TempDir(), "workgate.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	insert := `INSERT INTO completions (id, resource, outcome, started_at, finished_at)
	           VALUES ('aaa111', 'gpu', 'ok', 1, 2)`
	if _, err := d.Exec(insert); err != nil {
		t.Fatalf("first completion: %v", err)
	}
	if _, err := d.Exec(insert); err != nil {
		t.Fatalf("repeated completion id was rejected: %v", err)
	}
}
