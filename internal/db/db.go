// Package db resolves the machine-global Workgate database location and opens
// it with the pragmas and schema the coordination protocol relies on.
package db

import (
	"database/sql"
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
	hostname          TEXT
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
	git_branch        TEXT
);
-- Ordering is by seq everywhere, for display and for pruning alike: seq is
-- the real completion order, where finished_at is wall clock and can jump.
-- finished_at is indexed by nothing because the age sweep is a full scan,
-- which is only acceptable while the per-resource count cap bounds the table.
CREATE INDEX IF NOT EXISTS idx_completions_resource_seq ON completions(resource, seq);
`

func migrate(d *sql.DB) error {
	if _, err := d.Exec(schema); err != nil {
		return fmt.Errorf("initializing schema: %w", err)
	}
	return nil
}
