// Package db resolves the machine-global Workgate database location and opens
// it with the pragmas and schema the coordination protocol relies on.
package db

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
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
//     short write transactions used for acquisition. Set by enableWAL rather
//     than in the DSN; see there.
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
		"&_pragma=synchronous(NORMAL)"
	d, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}
	d.SetMaxOpenConns(1)
	if err := enableWAL(d); err != nil {
		d.Close()
		return nil, err
	}
	if err := migrate(d); err != nil {
		d.Close()
		return nil, err
	}
	return d, nil
}

// walTimeout bounds how long enableWAL keeps retrying, and matches the DSN's
// busy_timeout: the switch is waited for exactly as long as any other lock.
// walRetryDelay is short because what it waits out is short - another
// process's own switch, or its first schema statements.
const (
	walTimeout    = 5 * time.Second
	walRetryDelay = 10 * time.Millisecond
)

// enableWAL switches the database to WAL mode, waiting out other processes
// doing the same.
//
// It cannot be a DSN pragma like the others. Switching a database into WAL
// needs an exclusive lock, and SQLite does not call the busy handler for it:
// while the file is still in rollback mode, the lock other processes hold to
// create the schema - or to make the same switch - fails the pragma at once
// with SQLITE_BUSY, busy_timeout notwithstanding. On a brand-new database,
// several `workgate run`s started together hit that routinely, and the DSN
// gives no way to retry: the pragma runs while the connection is being made,
// and its error is the first statement's.
//
// The mode is a property of the database file, not of a connection, so this
// is needed once per Open: a connection the pool makes later opens a file
// that is already in WAL. Once it is, the pragma takes no lock at all, which
// keeps the common case a single statement. As before, a file system that
// cannot do WAL leaves the mode where it was rather than failing the open.
func enableWAL(d *sql.DB) error {
	deadline := time.Now().Add(walTimeout)
	for {
		var mode string
		err := d.QueryRow(`PRAGMA journal_mode = WAL`).Scan(&mode)
		if err == nil {
			return nil
		}
		if !isBusy(err) || time.Now().After(deadline) {
			return fmt.Errorf("enabling WAL mode: %w", err)
		}
		time.Sleep(walRetryDelay)
	}
}

// isBusy reports whether err is SQLITE_BUSY, in any of its extended forms.
func isBusy(err error) bool {
	var se *sqlite.Error
	return errors.As(err, &se) && se.Code()&0xff == sqlite3.SQLITE_BUSY
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
	-- Declared after every original column so a database created here and
	-- one brought up to date by the priority migration (which can only
	-- append) have the same column order. NOT NULL DEFAULT 3 is what makes the column compatible in
	-- both directions: rows that predate it read as the neutral level,
	-- and an older workgate binary, whose INSERT does not mention the
	-- column, still writes a valid row.
	priority          INTEGER NOT NULL DEFAULT 3 CHECK (priority BETWEEN 1 AND 5),
	-- The workload a row belongs to, when one workload holds several
	-- resources: one row per resource, every one of them carrying the id of
	-- the first. NULL on rows written by a binary that predates it, which
	-- makes such a row a workload of its own - the group key everywhere is
	-- IFNULL(group_id, id). Declared after priority because its migration runs
	-- after the priority one, and both can only append.
	group_id          TEXT,
	-- How this row claims its resource. 'exclusive' is what every claim was
	-- before tags: one holder at a time. 'shared' rows - a --tag - may be held
	-- by any number of workloads at once, and conflict only with an exclusive
	-- row on the same resource. NOT NULL DEFAULT 'exclusive' does for an older
	-- binary what priority's default does: its INSERT does not name the
	-- column, and the row it writes claims what it always meant to.
	mode              TEXT NOT NULL DEFAULT 'exclusive' CHECK (mode IN ('exclusive','shared'))
);
CREATE INDEX IF NOT EXISTS idx_workloads_resource_seq ON workloads(resource, seq);
-- Hard correctness backstop: SQLite itself refuses a second 'running' row
-- for the same resource, independent of application logic. This is the form
-- that predates shared claims; migrate replaces it with exclusiveRunningIndex,
-- which cannot live here - see the note on the constants below.
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
	-- created here and one brought up to date by its migration
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

	// The workload a row belongs to; see the column in schema. Nullable with
	// no default, so a row written before it existed - or by an older binary
	// afterwards, whose INSERT does not mention it - is its own workload.
	// Nothing indexes it: the group key is IFNULL(group_id, id), which no index
	// on the column could serve, and the table holds a few dozen rows.
	addGroupColumn = `ALTER TABLE workloads ADD COLUMN group_id TEXT`

	// The command a completion ran. Nullable with no default, so rows written
	// before it existed read back as NULL and render as the blank line they
	// have always been - there is no command to invent for them.
	addCompletionCommand = `ALTER TABLE completions ADD COLUMN command_display TEXT`

	// Acquisition order is (priority, seq) within a resource; this index says
	// so. idx_workloads_resource_seq stays, because arrival order is still what
	// orders a level and what List falls back on.
	priorityIndex = `CREATE INDEX IF NOT EXISTS idx_workloads_resource_priority_seq ON workloads(resource, priority, seq)`

	// How a row claims its resource; see the column in schema.
	addModeColumn = `ALTER TABLE workloads ADD COLUMN mode TEXT NOT NULL DEFAULT 'exclusive' CHECK (mode IN ('exclusive','shared'))`

	// The single-owner backstop, narrowed to exclusive rows so that several
	// shared holders of a tag can run at once. What keeps a shared holder and
	// an exclusive one apart is TryAcquire's one immediate transaction; no
	// index can say "no exclusive row beside a shared one".
	//
	// It keeps the old index's name on purpose. An older binary runs
	// `CREATE UNIQUE INDEX IF NOT EXISTS idx_one_running` on every open, which
	// is a no-op while this one exists - under any other name it would try to
	// build the strict index over two running shared rows, and fail to open.
	exclusiveRunningIndex = `CREATE UNIQUE INDEX idx_one_running ON workloads(resource) WHERE state = 'running' AND mode = 'exclusive'`
)

func migrate(d *sql.DB) error {
	if _, err := d.Exec(schema); err != nil {
		return fmt.Errorf("initializing schema: %w", err)
	}
	// Order matters twice over: each ALTER appends, so running them in the
	// order the columns are declared in schema is what keeps a migrated table
	// and a fresh one in the same column order; and priorityIndex names a
	// column that only exists once the ALTER before it has run.
	for _, m := range []struct {
		table, column, alter string
		then                 []string
	}{
		{"workloads", "priority", addPriorityColumn, []string{priorityIndex}},
		{"workloads", "group_id", addGroupColumn, nil},
		{"workloads", "mode", addModeColumn, nil},
		// No index alongside it: the column is display-only and nothing ever
		// selects or orders by it.
		{"completions", "command_display", addCompletionCommand, nil},
	} {
		if err := addColumn(d, m.table, m.column, m.alter, m.then...); err != nil {
			return err
		}
	}
	return narrowRunningIndex(d)
}

// narrowRunningIndex replaces the original idx_one_running with
// exclusiveRunningIndex, once. Unlike the steps addColumn runs, this one is
// not idempotent by itself - CREATE cannot be IF NOT EXISTS when the old index
// holds the name - so it reads the index's definition first and does nothing
// once that already names mode. The read and the swap share one immediate
// transaction, so of several processes opening an old database together,
// exactly one swaps and the rest see it done.
//
// The schema Exec always creates the old form first, on a fresh database as
// on an old one, so there is a single path here rather than two to keep in
// step.
func narrowRunningIndex(d *sql.DB) error {
	tx, err := d.Begin()
	if err != nil {
		return fmt.Errorf("beginning index migration: %w", err)
	}
	defer tx.Rollback()

	var def string
	if err := tx.QueryRow(`SELECT IFNULL(sql,'') FROM sqlite_master
		WHERE type = 'index' AND name = 'idx_one_running'`).Scan(&def); err != nil {
		return fmt.Errorf("inspecting idx_one_running: %w", err)
	}
	if strings.Contains(def, "mode") {
		return nil
	}
	for _, q := range []string{`DROP INDEX idx_one_running`, exclusiveRunningIndex} {
		if _, err := tx.Exec(q); err != nil {
			return fmt.Errorf("narrowing idx_one_running to exclusive rows: %w", err)
		}
	}
	return tx.Commit()
}

// addColumn adds column to a table that predates it, then runs then.
// CREATE TABLE IF NOT EXISTS cannot add a column to a table that already
// exists, so every column added after the first release needs this.
//
// Several workgate processes routinely open this database at the same moment,
// so the check and the ALTER run inside one immediate transaction (the DSN's
// _txlock=immediate takes the write lock at BEGIN): a second process blocks on
// that lock and then simply sees the column already there. Re-checking after a
// failed ALTER keeps this correct even if the DDL ever escaped that lock - the
// condition that matters is "the column exists", not "my ALTER succeeded", so
// nothing here depends on matching SQLite's error text.
func addColumn(d *sql.DB, table, column, alter string, then ...string) error {
	tx, err := d.Begin()
	if err != nil {
		return fmt.Errorf("beginning schema migration: %w", err)
	}
	defer tx.Rollback()

	has, err := hasColumn(tx, table, column)
	if err != nil {
		return err
	}
	if !has {
		if _, alterErr := tx.Exec(alter); alterErr != nil {
			if has, err = hasColumn(tx, table, column); err != nil {
				return err
			} else if !has {
				return fmt.Errorf("adding the %s column: %w", column, alterErr)
			}
		}
	}
	for _, q := range then {
		if _, err := tx.Exec(q); err != nil {
			return fmt.Errorf("migrating the %s column: %w", column, err)
		}
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
