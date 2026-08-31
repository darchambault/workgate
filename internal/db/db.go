// Package db resolves the machine-global Workgate database location and opens
// it with the pragmas and schema the coordination protocol relies on.
package db

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// Path returns the machine-global database path for the current OS user,
// <user cache dir>/Workgate/workgate.db, resolved via os.UserCacheDir:
// %LOCALAPPDATA%\Workgate\workgate.db on Windows,
// ~/Library/Caches/Workgate/workgate.db on macOS, and
// $XDG_CACHE_HOME/Workgate/workgate.db (default ~/.cache/...) on Linux.
//
// The WORKGATE_DB environment variable overrides the path. This exists for
// tests (including multi-process integration tests) and is not intended as
// user-facing configuration.
func Path() (string, error) {
	if p := os.Getenv("WORKGATE_DB"); p != "" {
		return p, nil
	}
	base, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("resolving user cache directory: %w", err)
	}
	return filepath.Join(base, "Workgate", "workgate.db"), nil
}

// Open opens (creating if necessary) the coordination database at path.
//
// Non-default choices, all deliberate:
//   - journal_mode=WAL: readers (status, position checks) never block the
//     short write transactions used for acquisition.
//   - busy_timeout=5000: writers briefly wait out each other's transactions
//     instead of failing immediately with SQLITE_BUSY.
//   - synchronous=NORMAL: safe with WAL; this is live coordination state,
//     not durable history, so full fsync durability is unnecessary.
//   - _txlock=immediate: every transaction takes the write lock up front,
//     avoiding deferred-to-write upgrade deadlocks between processes.
//   - MaxOpenConns(1): each workgate process is low-traffic; a single
//     connection avoids intra-process lock contention entirely.
func Open(path string) (*sql.DB, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("creating database directory: %w", err)
		}
	}
	dsn := "file:" + filepath.ToSlash(path) +
		"?_txlock=immediate" +
		"&_pragma=busy_timeout(5000)" +
		"&_pragma=journal_mode(WAL)" +
		"&_pragma=synchronous(NORMAL)"
	d, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}
	d.SetMaxOpenConns(1)
	if err := migrate(d); err != nil {
		d.Close()
		return nil, err
	}
	return d, nil
}

const schema = `
CREATE TABLE IF NOT EXISTS workloads (
	seq               INTEGER PRIMARY KEY AUTOINCREMENT,
	id                TEXT UNIQUE NOT NULL,
	resource          TEXT NOT NULL,
	label             TEXT,
	state             TEXT NOT NULL CHECK (state IN ('waiting','running')),
	pid               INTEGER,
	created_at        INTEGER NOT NULL,
	acquired_at       INTEGER,
	heartbeat_at      INTEGER NOT NULL,
	working_directory TEXT,
	repository_root   TEXT,
	git_common_dir    TEXT,
	git_branch        TEXT,
	command_display   TEXT,
	hostname          TEXT,
	-- Declared last so a database created here and one brought up to date
	-- by migratePriority (which can only append) have the same column
	-- order. NOT NULL DEFAULT 3 is what makes the column compatible in
	-- both directions: rows that predate it read as the neutral level,
	-- and an older workgate binary, whose INSERT does not mention the
	-- column, still writes a valid row.
	priority          INTEGER NOT NULL DEFAULT 3 CHECK (priority BETWEEN 1 AND 5)
);
CREATE INDEX IF NOT EXISTS idx_workloads_resource_seq ON workloads(resource, seq);
-- Hard correctness backstop: SQLite itself refuses a second 'running' row
-- for the same resource, independent of application logic.
CREATE UNIQUE INDEX IF NOT EXISTS idx_one_running ON workloads(resource) WHERE state = 'running';

-- A small bounded ring of recently finished workloads. This is not history:
-- it is capped per resource and by age (see queue.CompletionsPerResource and
-- queue.CompletionRetention), and exists only so "monitor" and
-- "status --recent" can answer "what just finished?". Nothing here affects
-- locking; no code reads it to make a decision.
--
-- id is deliberately NOT UNIQUE: workloads.id is three random bytes, unique
-- only among live rows, so a cosmetic collision here would fail the release
-- transaction and strand a resource until the stale threshold.
CREATE TABLE IF NOT EXISTS completions (
	seq               INTEGER PRIMARY KEY AUTOINCREMENT,
	id                TEXT NOT NULL,
	resource          TEXT NOT NULL,
	label             TEXT,
	outcome           TEXT NOT NULL,
	exit_code         INTEGER NOT NULL DEFAULT 0,
	started_at        INTEGER NOT NULL,
	finished_at       INTEGER NOT NULL,
	working_directory TEXT,
	repository_root   TEXT,
	git_branch        TEXT,
	-- Declared last for the same reason priority is on workloads: a database
	-- created here and one brought up to date by migrateCompletionCommand
	-- (which can only append) must have the same column order.
	command_display   TEXT
);
-- Ordering is by seq everywhere, for display and for pruning alike: seq is
-- the real completion order, where finished_at is wall clock and can jump.
-- finished_at is indexed by nothing because the age sweep is a full scan,
-- which is only acceptable while the per-resource count cap bounds the table.
CREATE INDEX IF NOT EXISTS idx_completions_resource_seq ON completions(resource, seq);
`

// Statements that bring a database created before priorities existed up to
// date. Neither can live in schema: on such a database the schema Exec runs
// before the ALTER, so an index over priority would reference a column that is
// not there yet. The order is fixed - CREATE TABLE, then ALTER, then INDEX.
const (
	addPriorityColumn = `ALTER TABLE workloads ADD COLUMN priority INTEGER NOT NULL DEFAULT 3 CHECK (priority BETWEEN 1 AND 5)`

	// The command a completion ran. Nullable with no default, so rows written
	// before it existed read back as NULL and render as the blank line they
	// have always been - there is no command to invent for them.
	addCompletionCommand = `ALTER TABLE completions ADD COLUMN command_display TEXT`

	// Acquisition order is (priority, seq) within a resource; this index says
	// so. idx_workloads_resource_seq stays, because arrival order is still what
	// orders a level and what List falls back on.
	priorityIndex = `CREATE INDEX IF NOT EXISTS idx_workloads_resource_priority_seq ON workloads(resource, priority, seq)`
)

func migrate(d *sql.DB) error {
	if _, err := d.Exec(schema); err != nil {
		return fmt.Errorf("initializing schema: %w", err)
	}
	if err := migratePriority(d); err != nil {
		return err
	}
	return migrateCompletionCommand(d)
}

// migrateCompletionCommand adds the command column to a completions table that
// predates it, the same way and for the same reason migratePriority does.
// There is no index to create alongside it: the column is display-only and
// nothing ever selects or orders by it.
func migrateCompletionCommand(d *sql.DB) error {
	tx, err := d.Begin()
	if err != nil {
		return fmt.Errorf("beginning schema migration: %w", err)
	}
	defer tx.Rollback()

	has, err := hasColumn(tx, "completions", "command_display")
	if err != nil {
		return err
	}
	if has {
		return tx.Commit()
	}
	if _, alterErr := tx.Exec(addCompletionCommand); alterErr != nil {
		// As in migratePriority, the condition that matters is "the column
		// exists", not "my ALTER succeeded".
		if has, err = hasColumn(tx, "completions", "command_display"); err != nil {
			return err
		} else if !has {
			return fmt.Errorf("adding the command_display column: %w", alterErr)
		}
	}
	return tx.Commit()
}

// migratePriority adds the priority column to a database that predates it.
// CREATE TABLE IF NOT EXISTS cannot add a column to a table that already
// exists, so this is the one piece of schema that needs a real migration.
//
// Several workgate processes routinely open this database at the same moment,
// so the check and the ALTER run inside one immediate transaction (the DSN's
// _txlock=immediate takes the write lock at BEGIN): a second process blocks on
// that lock and then simply sees the column already there. Re-checking after a
// failed ALTER keeps this correct even if the DDL ever escaped that lock - the
// condition that matters is "the column exists", not "my ALTER succeeded", so
// nothing here depends on matching SQLite's error text.
func migratePriority(d *sql.DB) error {
	tx, err := d.Begin()
	if err != nil {
		return fmt.Errorf("beginning schema migration: %w", err)
	}
	defer tx.Rollback()

	has, err := hasColumn(tx, "workloads", "priority")
	if err != nil {
		return err
	}
	if !has {
		if _, alterErr := tx.Exec(addPriorityColumn); alterErr != nil {
			if has, err = hasColumn(tx, "workloads", "priority"); err != nil {
				return err
			} else if !has {
				return fmt.Errorf("adding the priority column: %w", alterErr)
			}
		}
	}
	if _, err := tx.Exec(priorityIndex); err != nil {
		return fmt.Errorf("creating the priority index: %w", err)
	}
	return tx.Commit()
}

func hasColumn(tx *sql.Tx, table, column string) (bool, error) {
	var one int
	err := tx.QueryRow(`SELECT 1 FROM pragma_table_info(?) WHERE name = ?`, table, column).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspecting the %s table: %w", table, err)
	}
	return true, nil
}
