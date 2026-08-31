package db

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
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

// downgrade strips the priority column back off an open database, so a test
// can prove that reopening restores it. Dropping the index first is required:
// SQLite refuses to drop an indexed column.
func downgrade(t *testing.T, path string) {
	t.Helper()
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for _, q := range []string{
		`DROP INDEX IF EXISTS idx_workloads_resource_priority_seq`,
		`ALTER TABLE workloads DROP COLUMN priority`,
	} {
		if _, err := d.Exec(q); err != nil {
			t.Fatalf("simulating a pre-priority schema (%s): %v", q, err)
		}
	}
}

// TestOpenAddsPriorityToAPreExistingDatabase is the other half of the upgrade
// proof: unlike completions, priority is a column on a table that already
// exists, which CREATE TABLE IF NOT EXISTS cannot add.
func TestOpenAddsPriorityToAPreExistingDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workgate.db")
	if d, err := Open(path); err != nil {
		t.Fatal(err)
	} else {
		d.Close()
	}
	downgrade(t, path)

	old, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	// Seeded through the downgraded schema, exactly as an older binary would.
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
	var priority int
	if err := d.QueryRow(`SELECT priority FROM workloads WHERE id = 'aaa111'`).Scan(&priority); err != nil {
		t.Fatalf("priority missing after upgrade: %v", err)
	}
	// A row that predates priorities is neither urgent nor deferred.
	if priority != 3 {
		t.Fatalf("migrated row priority = %d, want 3", priority)
	}
}

// TestMigratingPriorityIsIdempotent covers the ordinary case: every open of an
// already-current database re-runs the migration and must be a no-op.
func TestMigratingPriorityIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workgate.db")
	for i := 0; i < 3; i++ {
		d, err := Open(path)
		if err != nil {
			t.Fatalf("open #%d: %v", i+1, err)
		}
		if _, err := d.Exec(`SELECT COUNT(*) FROM workloads WHERE priority = 3`); err != nil {
			t.Fatalf("priority missing on open #%d: %v", i+1, err)
		}
		d.Close()
	}
}

// TestConcurrentOpenMigratesPriorityOnce is the reason the migration runs in an
// immediate transaction. Several sessions routinely start at the same moment,
// and on a machine being upgraded they can all reach a pre-priority database
// together; none of them may fail.
func TestConcurrentOpenMigratesPriorityOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workgate.db")
	if d, err := Open(path); err != nil {
		t.Fatal(err)
	} else {
		d.Close()
	}
	downgrade(t, path)

	const openers = 8
	errs := make(chan error, openers)
	var wg sync.WaitGroup
	for i := 0; i < openers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := Open(path)
			if err != nil {
				errs <- err
				return
			}
			defer d.Close()
			_, err = d.Exec(`SELECT COUNT(*) FROM workloads WHERE priority = 3`)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent open: %v", err)
		}
	}
}

// downgradeCompletions strips the command column back off, so a test can prove
// that reopening restores it. No index to drop first: nothing indexes it.
func downgradeCompletions(t *testing.T, path string) {
	t.Helper()
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err := d.Exec(`ALTER TABLE completions DROP COLUMN command_display`); err != nil {
		t.Fatalf("simulating a pre-command completions schema: %v", err)
	}
}

// TestOpenAddsTheCommandToAPreExistingCompletions mirrors the priority proof:
// completions is a table that already exists on any database in use, so
// CREATE TABLE IF NOT EXISTS cannot add the column and a real ALTER must.
func TestOpenAddsTheCommandToAPreExistingCompletions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workgate.db")
	if d, err := Open(path); err != nil {
		t.Fatal(err)
	} else {
		d.Close()
	}
	downgradeCompletions(t, path)

	old, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	// Seeded through the downgraded schema, exactly as an older binary would.
	if _, err := old.Exec(`INSERT INTO completions
	                       (id, resource, outcome, started_at, finished_at)
	                       VALUES ('bbb222', 'gpu', 'ok', 1, 2)`); err != nil {
		t.Fatalf("seeding a completion: %v", err)
	}
	old.Close()

	d, err := Open(path)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	defer d.Close()
	var command sql.NullString
	if err := d.QueryRow(`SELECT command_display FROM completions WHERE id = 'bbb222'`).Scan(&command); err != nil {
		t.Fatalf("command_display missing after upgrade: %v", err)
	}
	// There is no command to invent for a row written before the column
	// existed, and the views render the blank line they always did.
	if command.Valid {
		t.Errorf("migrated completion command = %q, want NULL", command.String)
	}
}

// TestMigratingTheCompletionCommandIsIdempotent covers the ordinary case:
// every open of an already-current database re-runs the migration.
func TestMigratingTheCompletionCommandIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workgate.db")
	for i := 0; i < 3; i++ {
		d, err := Open(path)
		if err != nil {
			t.Fatalf("open #%d: %v", i+1, err)
		}
		if _, err := d.Exec(`SELECT COUNT(*) FROM completions WHERE command_display IS NULL`); err != nil {
			t.Fatalf("command_display missing on open #%d: %v", i+1, err)
		}
		d.Close()
	}
}

// TestConcurrentOpenMigratesTheCompletionCommandOnce is why this migration,
// like priority's, runs in an immediate transaction: several sessions can
// reach a pre-command database together, and none of them may fail.
func TestConcurrentOpenMigratesTheCompletionCommandOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workgate.db")
	if d, err := Open(path); err != nil {
		t.Fatal(err)
	} else {
		d.Close()
	}
	downgradeCompletions(t, path)

	const openers = 8
	errs := make(chan error, openers)
	var wg sync.WaitGroup
	for i := 0; i < openers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := Open(path)
			if err != nil {
				errs <- err
				return
			}
			defer d.Close()
			_, err = d.Exec(`SELECT COUNT(*) FROM completions WHERE command_display IS NULL`)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent open: %v", err)
		}
	}
}

// TestPriorityDefaultsToThree pins the compatibility contract that lets an
// older binary keep using a migrated database: its INSERT never mentions the
// column, and the row it writes must still be valid and neutrally ranked.
func TestPriorityDefaultsToThree(t *testing.T) {
	d, err := Open(filepath.Join(t.TempDir(), "workgate.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err := d.Exec(`INSERT INTO workloads (id, resource, state, created_at, heartbeat_at)
	                     VALUES ('aaa111', 'gpu', 'waiting', 1, 1)`); err != nil {
		t.Fatal(err)
	}
	var priority int
	if err := d.QueryRow(`SELECT priority FROM workloads WHERE id = 'aaa111'`).Scan(&priority); err != nil {
		t.Fatal(err)
	}
	if priority != 3 {
		t.Fatalf("default priority = %d, want 3", priority)
	}
}

// TestPriorityCheckRejectsOutOfRange verifies the database-level backstop on
// the range, on both schema paths: a fresh database gets the CHECK from CREATE
// TABLE, a migrated one from ALTER TABLE, and the two must not diverge.
func TestPriorityCheckRejectsOutOfRange(t *testing.T) {
	for _, tc := range []struct {
		desc     string
		migrated bool
	}{
		{"fresh schema", false},
		{"migrated schema", true},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "workgate.db")
			if d, err := Open(path); err != nil {
				t.Fatal(err)
			} else {
				d.Close()
			}
			if tc.migrated {
				downgrade(t, path)
			}
			d, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			for _, level := range []int{0, 6, -1} {
				_, err := d.Exec(`INSERT INTO workloads (id, resource, state, created_at, heartbeat_at, priority)
				                  VALUES (?, 'gpu', 'waiting', 1, 1, ?)`,
					fmt.Sprintf("id%04d", level+100), level)
				if err == nil {
					t.Errorf("priority %d was accepted", level)
				}
			}
			if _, err := d.Exec(`INSERT INTO workloads (id, resource, state, created_at, heartbeat_at, priority)
			                     VALUES ('aaa111', 'gpu', 'waiting', 1, 1, 1)`); err != nil {
				t.Errorf("priority 1 was rejected: %v", err)
			}
		})
	}
}
