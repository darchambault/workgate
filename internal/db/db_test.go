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

func TestPathDefaultsToLocalAppData(t *testing.T) {
	t.Setenv("WORKGATE_DB", "")
	got, err := Path()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(got, filepath.Join("Workgate", "workgate.db")) {
		t.Fatalf("Path() = %q, want ...\\Workgate\\workgate.db", got)
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
