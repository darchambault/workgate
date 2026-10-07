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
	// group_id and mode go too: both were added after priority, so a database
	// that predates priorities predates them as well.
	downgradeModesOn(t, d)
	for _, q := range []string{
		`ALTER TABLE workloads DROP COLUMN group_id`,
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

// downgradeGroups strips group_id back off, leaving priority in place: the
// shape of a database last opened by a binary from before multi-resource
// workloads.
func downgradeGroups(t *testing.T, path string) {
	t.Helper()
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	downgradeModesOn(t, d) // added after group_id, so older still
	if _, err := d.Exec(`ALTER TABLE workloads DROP COLUMN group_id`); err != nil {
		t.Fatalf("simulating a pre-group schema: %v", err)
	}
}

// TestOpenAddsGroupsToAPreExistingDatabase is the upgrade proof for group_id.
// A row written before the column existed must read back NULL, which is what
// makes it a workload of its own.
func TestOpenAddsGroupsToAPreExistingDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workgate.db")
	if d, err := Open(path); err != nil {
		t.Fatal(err)
	} else {
		d.Close()
	}
	downgradeGroups(t, path)

	// Seeded through raw SQL rather than Open, which would migrate the column
	// straight back: this is the old shape, written as an older binary would.
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`INSERT INTO workloads (id, resource, state, created_at, heartbeat_at)
	                       VALUES ('aaa111', 'gpu', 'running', 1, 1)`); err != nil {
		t.Fatalf("seeding a workload: %v", err)
	}
	raw.Close()

	d, err := Open(path)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	defer d.Close()
	var group sql.NullString
	if err := d.QueryRow(`SELECT group_id FROM workloads WHERE id = 'aaa111'`).Scan(&group); err != nil {
		t.Fatalf("group_id missing after upgrade: %v", err)
	}
	if group.Valid {
		t.Errorf("migrated row group_id = %q, want NULL", group.String)
	}
}

// TestConcurrentOpenMigratesGroupsOnce: several sessions can reach a pre-group
// database together, and none of them may fail.
func TestConcurrentOpenMigratesGroupsOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workgate.db")
	if d, err := Open(path); err != nil {
		t.Fatal(err)
	} else {
		d.Close()
	}
	downgradeGroups(t, path)

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
			_, err = d.Exec(`SELECT COUNT(*) FROM workloads WHERE group_id IS NULL`)
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

// downgradeModes strips the mode column back off and restores the original
// single-owner index: the shape of a database last opened by a binary from
// before tags.
func downgradeModes(t *testing.T, path string) {
	t.Helper()
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	downgradeModesOn(t, d)
}

// downgradeModesOn is downgradeModes on a database that is already open. The
// index goes first: SQLite refuses to drop a column an index still names.
func downgradeModesOn(t *testing.T, d *sql.DB) {
	t.Helper()
	for _, q := range []string{
		`DROP INDEX idx_one_running`,
		`CREATE UNIQUE INDEX idx_one_running ON workloads(resource) WHERE state = 'running'`,
		`ALTER TABLE workloads DROP COLUMN mode`,
	} {
		if _, err := d.Exec(q); err != nil {
			t.Fatalf("simulating a pre-tag schema (%s): %v", q, err)
		}
	}
}

// runningIndexSQL is the definition idx_one_running currently has.
func runningIndexSQL(t *testing.T, d *sql.DB) string {
	t.Helper()
	var def string
	if err := d.QueryRow(`SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'idx_one_running'`).Scan(&def); err != nil {
		t.Fatalf("reading idx_one_running: %v", err)
	}
	return def
}

// TestSharedRowsMayRunTogether is the other half of the backstop: the index
// is narrowed to exclusive rows, on a fresh database and on a migrated one
// alike, so several shared holders of a tag can run while two exclusive
// owners still cannot.
func TestSharedRowsMayRunTogether(t *testing.T) {
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
				downgradeModes(t, path)
			}
			d, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			shared := `INSERT INTO workloads (id, resource, state, created_at, heartbeat_at, mode)
			           VALUES (?, 'tag:p', 'running', 1, 1, 'shared')`
			for _, id := range []string{"aaa111", "bbb222"} {
				if _, err := d.Exec(shared, id); err != nil {
					t.Fatalf("running shared row %s: %v", id, err)
				}
			}
			exclusive := `INSERT INTO workloads (id, resource, state, created_at, heartbeat_at)
			              VALUES (?, 'gpu', 'running', 1, 1)`
			if _, err := d.Exec(exclusive, "ccc333"); err != nil {
				t.Fatalf("first exclusive row: %v", err)
			}
			if _, err := d.Exec(exclusive, "ddd444"); err == nil {
				t.Fatal("second running exclusive row for the same resource was allowed")
			}
			if _, err := d.Exec(`INSERT INTO workloads (id, resource, state, created_at, heartbeat_at, mode)
			                     VALUES ('eee555', 'gpu', 'waiting', 1, 1, 'sideways')`); err == nil {
				t.Fatal("an unknown mode was accepted")
			}
		})
	}
}

// TestModeDefaultsToExclusive pins the compatibility contract for an older
// binary: its INSERT never names the column, and the row it writes must claim
// its resource the way it always did.
func TestModeDefaultsToExclusive(t *testing.T) {
	d, err := Open(filepath.Join(t.TempDir(), "workgate.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err := d.Exec(`INSERT INTO workloads (id, resource, state, created_at, heartbeat_at)
	                     VALUES ('aaa111', 'gpu', 'waiting', 1, 1)`); err != nil {
		t.Fatal(err)
	}
	var mode string
	if err := d.QueryRow(`SELECT mode FROM workloads WHERE id = 'aaa111'`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != "exclusive" {
		t.Fatalf("default mode = %q, want exclusive", mode)
	}
}

// TestNarrowingTheRunningIndexIsIdempotent: every open runs the migration, and
// once the index names mode it must be left exactly as it is - including by
// an older binary, whose schema re-creates idx_one_running only IF NOT EXISTS.
func TestNarrowingTheRunningIndexIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workgate.db")
	var defs []string
	for i := 0; i < 3; i++ {
		d, err := Open(path)
		if err != nil {
			t.Fatalf("open #%d: %v", i+1, err)
		}
		defs = append(defs, runningIndexSQL(t, d))
		if i == 1 {
			// What an older binary runs on open.
			if _, err := d.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_one_running ON workloads(resource) WHERE state = 'running'`); err != nil {
				t.Fatalf("older binary's schema: %v", err)
			}
		}
		d.Close()
	}
	for i, def := range defs {
		if !strings.Contains(def, "mode") {
			t.Errorf("open #%d: idx_one_running = %q, want it narrowed to exclusive rows", i+1, def)
		}
		if def != defs[0] {
			t.Errorf("open #%d: idx_one_running changed to %q", i+1, def)
		}
	}
}

// TestConcurrentOpenMigratesModesOnce: several sessions can reach a pre-tag
// database together - with a running row already in it - and none may fail.
func TestConcurrentOpenMigratesModesOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workgate.db")
	if d, err := Open(path); err != nil {
		t.Fatal(err)
	} else {
		d.Close()
	}
	downgradeModes(t, path)
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`INSERT INTO workloads (id, resource, state, created_at, heartbeat_at)
	                       VALUES ('aaa111', 'gpu', 'running', 1, 1)`); err != nil {
		t.Fatalf("seeding a workload: %v", err)
	}
	raw.Close()

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
			_, err = d.Exec(`SELECT COUNT(*) FROM workloads WHERE mode = 'exclusive'`)
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
	d, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var mode string
	if err := d.QueryRow(`SELECT mode FROM workloads WHERE id = 'aaa111'`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != "exclusive" {
		t.Errorf("pre-existing row mode = %q, want exclusive", mode)
	}
}
