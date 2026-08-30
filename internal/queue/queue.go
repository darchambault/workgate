// Package queue implements the FIFO coordination protocol: enqueueing,
// atomic acquisition, heartbeats, stale-workload recovery, release, and
// status queries. Every database transaction here is short; nothing holds
// a transaction open while waiting or while a child process runs.
package queue

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Timing constants for the coordination protocol. These are variables so
// tests (and the WORKGATE_*_MS environment overrides used by multi-process
// integration tests) can shorten them; production values are the defaults.
var (
	// HeartbeatInterval is how often a live workload refreshes heartbeat_at,
	// both while waiting and while its child process runs.
	HeartbeatInterval = 5 * time.Second

	// StaleThreshold is how old a heartbeat must be before other processes
	// may consider the workload abandoned. Deliberately conservative
	// (12x the heartbeat interval) so system sleep, debugger pauses, and
	// scheduling stalls do not cause false-positive stale removal.
	StaleThreshold = 60 * time.Second

	// PollInterval is how often a waiting workload re-attempts acquisition.
	PollInterval = 750 * time.Millisecond

	// LongWaitNotice is how often a still-waiting workload emits a
	// restrained progress message.
	LongWaitNotice = 60 * time.Second
)

// LoadEnvOverrides applies WORKGATE_HEARTBEAT_INTERVAL_MS,
// WORKGATE_STALE_THRESHOLD_MS and WORKGATE_POLL_INTERVAL_MS if set.
// Test hook only; not user-facing configuration.
func LoadEnvOverrides() {
	for _, o := range []struct {
		env string
		dst *time.Duration
	}{
		{"WORKGATE_HEARTBEAT_INTERVAL_MS", &HeartbeatInterval},
		{"WORKGATE_STALE_THRESHOLD_MS", &StaleThreshold},
		{"WORKGATE_POLL_INTERVAL_MS", &PollInterval},
	} {
		if v := os.Getenv(o.env); v != "" {
			if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
				*o.dst = time.Duration(ms) * time.Millisecond
			}
		}
	}
}

// Workload is one row of the coordination table.
type Workload struct {
	Seq              int64
	ID               string
	Resource         string
	Label            string
	State            string
	PID              int64
	CreatedAt        int64 // unix milliseconds
	AcquiredAt       int64 // unix milliseconds, 0 if never acquired
	HeartbeatAt      int64 // unix milliseconds
	WorkingDirectory string
	RepositoryRoot   string
	GitCommonDir     string
	GitBranch        string
	CommandDisplay   string
	Hostname         string
	Project          string // derived display name, informational only
}

// Meta is the diagnostic context recorded with a workload at enqueue time.
// None of it affects locking semantics.
type Meta struct {
	Label            string
	PID              int
	WorkingDirectory string
	RepositoryRoot   string
	GitCommonDir     string
	GitBranch        string
	CommandDisplay   string
	Hostname         string
	Project          string
}

// StaleRemoved describes an abandoned workload deleted during cleanup.
type StaleRemoved struct {
	ID       string
	Resource string
	State    string
}

// Outcome describes how a workload's turn ended. It is display-only: no
// coordination decision reads it.
type Outcome struct {
	Kind     string // one of the Outcome* constants
	ExitCode int    // meaningful only for OutcomeExit
}

// The outcome vocabulary. Each of these is rendered in a fixed-width column,
// so the words are kept short deliberately — see outcomeSpan in cmd/workgate,
// and the test that asserts they fit.
const (
	OutcomeOK       = "ok"       // the child exited 0
	OutcomeExit     = "exit"     // the child exited non-zero
	OutcomeKilled   = "killed"   // terminated by a signal, or crashed
	OutcomeCanceled = "canceled" // workgate interrupted while the child ran
	OutcomeStale    = "stale"    // owner stopped heartbeating; reclaimed
)

// Completion is one finished workload from the bounded ring. It records what
// a workload did, never what it may do: nothing in the coordination protocol
// reads these rows.
type Completion struct {
	Seq              int64
	ID               string
	Resource         string
	Label            string
	Outcome          string
	ExitCode         int64
	StartedAt        int64 // unix milliseconds; the workload's acquired_at
	FinishedAt       int64 // unix milliseconds
	WorkingDirectory string
	RepositoryRoot   string
	GitBranch        string
}

// Tuning for the completions ring. These are variables so tests can shorten
// them; unlike the timing constants above they have no WORKGATE_* override,
// because nothing about coordination depends on them.
//
// The two caps are mutually reinforcing. A count cap alone would leave the
// table proportional to every resource name ever used, typos included; an age
// cap alone would let a busy resource evict a quiet one entirely, which is
// exactly the scoped view the ring is most wanted for. Together they bound
// the table to CompletionsPerResource × (resources used within the retention
// window) — dozens of rows, which is what makes the unindexed age sweep in
// recordCompletionTx acceptable.
var (
	CompletionsPerResource = 10
	CompletionRetention    = 24 * time.Hour
)

var resourceRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`)

// ErrGone reports that this workload's row no longer exists — another
// process removed it as stale (e.g. after a long machine sleep).
var ErrGone = errors.New("workload entry no longer exists (removed as stale by another process)")

// ValidateResource normalizes a resource name to lowercase and rejects
// invalid identifiers.
func ValidateResource(name string) (string, error) {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return "", errors.New("resource name is empty")
	}
	if len(name) > 64 {
		return "", fmt.Errorf("resource name %q is too long (max 64 characters)", name)
	}
	if !resourceRe.MatchString(name) {
		return "", fmt.Errorf("invalid resource name %q: must match [a-zA-Z0-9][a-zA-Z0-9._-]*", name)
	}
	return name, nil
}

func nowMillis() int64 { return time.Now().UnixMilli() }

func newID() string {
	b := make([]byte, 3)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand failure is effectively impossible on Windows; fall
		// back to a time-derived value rather than aborting coordination.
		return fmt.Sprintf("%06x", nowMillis()&0xffffff)
	}
	return hex.EncodeToString(b)
}

// Enqueue inserts a new waiting workload for resource. heartbeat_at is
// initialized in the same INSERT, so there is no window in which another
// process could consider the fresh row stale.
func Enqueue(d *sql.DB, resource string, meta Meta) (*Workload, error) {
	now := nowMillis()
	var seq int64
	var id string
	for attempt := 0; ; attempt++ {
		id = newID()
		res, err := d.Exec(`
			INSERT INTO workloads
				(id, resource, label, state, pid, created_at, heartbeat_at,
				 working_directory, repository_root, git_common_dir, git_branch,
				 command_display, hostname)
			VALUES (?, ?, ?, 'waiting', ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			id, resource, meta.Label, meta.PID, now, now,
			meta.WorkingDirectory, meta.RepositoryRoot, meta.GitCommonDir,
			meta.GitBranch, meta.CommandDisplay, meta.Hostname)
		if err != nil {
			if attempt < 3 && strings.Contains(err.Error(), "UNIQUE") {
				continue // improbable id collision; retry with a fresh id
			}
			return nil, fmt.Errorf("enqueueing workload: %w", err)
		}
		seq, err = res.LastInsertId()
		if err != nil {
			return nil, fmt.Errorf("reading workload sequence: %w", err)
		}
		break
	}
	return &Workload{
		Seq: seq, ID: id, Resource: resource, Label: meta.Label,
		State: "waiting", PID: int64(meta.PID), CreatedAt: now, HeartbeatAt: now,
	}, nil
}

// Position returns this workload's 1-based place in the resource's queue,
// counting every earlier workload (running or waiting). Position 1 means
// next in line (or already eligible).
func Position(d *sql.DB, w *Workload) (int, error) {
	var ahead int
	err := d.QueryRow(
		`SELECT COUNT(*) FROM workloads WHERE resource = ? AND seq < ?`,
		w.Resource, w.Seq).Scan(&ahead)
	if err != nil {
		return 0, fmt.Errorf("querying queue position: %w", err)
	}
	return ahead + 1, nil
}

// TryAcquire attempts, in a single short immediate transaction, to
// transition w from waiting to running. The transaction:
//
//  1. deletes stale entries for the resource (returned for diagnostics);
//  2. verifies no running owner remains;
//  3. verifies w is the oldest waiting workload (lowest seq);
//  4. claims the resource.
//
// Because all four steps commit atomically, two processes can never both
// conclude "the resource is free and I am next", stale cleanup cannot race
// acquisition, and a newer workload can never overtake an older healthy
// waiter. The partial unique index on (resource) WHERE state='running'
// additionally enforces single ownership at the database level.
func TryAcquire(d *sql.DB, w *Workload) (acquired bool, removed []StaleRemoved, err error) {
	tx, err := d.Begin()
	if err != nil {
		return false, nil, fmt.Errorf("beginning acquisition transaction: %w", err)
	}
	defer tx.Rollback()

	now := nowMillis()
	removed, err = deleteStaleTx(tx, w.Resource, now)
	if err != nil {
		return false, nil, err
	}

	// Our own row may have been deleted as stale by another process while
	// this one was suspended (sleep, debugger). Detect that explicitly.
	var mySeq int64
	err = tx.QueryRow(`SELECT seq FROM workloads WHERE id = ?`, w.ID).Scan(&mySeq)
	if errors.Is(err, sql.ErrNoRows) {
		return false, removed, ErrGone
	}
	if err != nil {
		return false, removed, fmt.Errorf("checking own workload row: %w", err)
	}

	var owners int
	if err := tx.QueryRow(
		`SELECT COUNT(*) FROM workloads WHERE resource = ? AND state = 'running'`,
		w.Resource).Scan(&owners); err != nil {
		return false, removed, fmt.Errorf("checking resource owner: %w", err)
	}
	if owners > 0 {
		return false, removed, tx.Commit() // keep the stale deletions
	}

	var oldestWaiting int64
	if err := tx.QueryRow(
		`SELECT MIN(seq) FROM workloads WHERE resource = ? AND state = 'waiting'`,
		w.Resource).Scan(&oldestWaiting); err != nil {
		return false, removed, fmt.Errorf("finding oldest waiter: %w", err)
	}
	if oldestWaiting != mySeq {
		return false, removed, tx.Commit() // an older healthy waiter goes first
	}

	if _, err := tx.Exec(
		`UPDATE workloads SET state = 'running', acquired_at = ?, heartbeat_at = ? WHERE id = ?`,
		now, now, w.ID); err != nil {
		return false, removed, fmt.Errorf("claiming resource: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, removed, fmt.Errorf("committing acquisition: %w", err)
	}
	w.State = "running"
	w.AcquiredAt = now
	return true, removed, nil
}

// Heartbeat refreshes heartbeat_at for w. Returns ErrGone if the row has
// been removed by another process's stale cleanup.
func Heartbeat(d *sql.DB, w *Workload) error {
	res, err := d.Exec(`UPDATE workloads SET heartbeat_at = ? WHERE id = ?`, nowMillis(), w.ID)
	if err != nil {
		return fmt.Errorf("updating heartbeat: %w", err)
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return ErrGone
	}
	return nil
}

// Release deletes w's row, letting the next queued workload acquire the
// resource, and — if w actually held the resource — records how it ended in
// the bounded completions ring. Deleting an already-removed row is not an error.
//
// Both happen in one short transaction. Splitting them would force a bad
// choice: deleting first can lose a completion (harmless), but inserting
// first can leave the resource held if this process dies in between
// (catastrophic). One transaction removes the choice, and with
// MaxOpenConns(1) it also avoids taking the write lock twice while a waiter
// polls.
//
// DELETE ... RETURNING does three jobs at once here: it frees the resource,
// it supplies the label and paths — which the in-memory Workload does not
// carry, since Enqueue only populates the fields it knows — and its
// row-or-no-row result is an exactly-once guard, so a caller that releases
// twice can never write two completions.
func Release(d *sql.DB, w *Workload, out Outcome) error {
	tx, err := d.Begin()
	if err != nil {
		return fmt.Errorf("beginning release transaction: %w", err)
	}
	defer tx.Rollback()

	var (
		label, wd, root, branch string
		acquiredAt              sql.NullInt64
	)
	// QueryRow, not Query: with a single connection, executing the INSERT
	// below while a RETURNING result set is still open would deadlock.
	err = tx.QueryRow(`
		DELETE FROM workloads WHERE id = ?
		RETURNING IFNULL(label,''), acquired_at, IFNULL(working_directory,''),
		          IFNULL(repository_root,''), IFNULL(git_branch,'')`, w.ID).
		Scan(&label, &acquiredAt, &wd, &root, &branch)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// Another process already reclaimed the row as stale, and recorded
		// it. Nothing left to do.
		return commitRelease(tx)
	case err != nil:
		return fmt.Errorf("releasing workload: %w", err)
	}
	// A workload that never acquired the resource did not run, so it is not
	// a completion — it would only be noise in a view of the last few.
	if !acquiredAt.Valid || acquiredAt.Int64 == 0 {
		return commitRelease(tx)
	}
	now := nowMillis()
	if err := recordCompletionTx(tx, Completion{
		ID: w.ID, Resource: w.Resource, Label: label,
		Outcome: out.Kind, ExitCode: int64(out.ExitCode),
		StartedAt: acquiredAt.Int64, FinishedAt: now,
		WorkingDirectory: wd, RepositoryRoot: root, GitBranch: branch,
	}, now); err != nil {
		return err
	}
	return commitRelease(tx)
}

func commitRelease(tx *sql.Tx) error {
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing release: %w", err)
	}
	return nil
}

// recordCompletionTx appends c to the ring and applies both caps, inside the
// caller's transaction. It runs once per finished workload — against
// heartbeats every few seconds and acquisition polls under a second, the two
// pruning statements are not a hot path.
func recordCompletionTx(tx *sql.Tx, c Completion, now int64) error {
	if _, err := tx.Exec(`
		INSERT INTO completions
			(id, resource, label, outcome, exit_code, started_at, finished_at,
			 working_directory, repository_root, git_branch)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		c.ID, c.Resource, c.Label, c.Outcome, c.ExitCode, c.StartedAt, c.FinishedAt,
		c.WorkingDirectory, c.RepositoryRoot, c.GitBranch); err != nil {
		return fmt.Errorf("recording completion: %w", err)
	}
	// Count cap, scoped to the resource just written. The subselect yields
	// NULL when fewer than the cap plus one rows exist, and "seq <= NULL"
	// matches nothing, so the common case deletes nothing without a COUNT.
	if _, err := tx.Exec(`
		DELETE FROM completions
		WHERE resource = ? AND seq <= (
			SELECT seq FROM completions WHERE resource = ?
			ORDER BY seq DESC LIMIT 1 OFFSET ?)`,
		c.Resource, c.Resource, CompletionsPerResource); err != nil {
		return fmt.Errorf("trimming completions for %q: %w", c.Resource, err)
	}
	// Age cap, global: sweeps out resources that stopped being used at all,
	// which the per-resource count cap cannot reach.
	if _, err := tx.Exec(`DELETE FROM completions WHERE finished_at < ?`,
		now-CompletionRetention.Milliseconds()); err != nil {
		return fmt.Errorf("expiring completions: %w", err)
	}
	return nil
}

// RecentCompletions returns the most recently finished workloads (all
// resources if resource is empty), newest first. Ordering is by seq rather
// than by finished_at for the same reason the queue is: seq is the real
// order, where wall clock can jump.
func RecentCompletions(d *sql.DB, resource string, limit int) ([]Completion, error) {
	if limit <= 0 {
		return nil, nil
	}
	q := `SELECT seq, id, resource, IFNULL(label,''), outcome, exit_code,
	             started_at, finished_at, IFNULL(working_directory,''),
	             IFNULL(repository_root,''), IFNULL(git_branch,'')
	      FROM completions`
	var args []any
	if resource != "" {
		q += ` WHERE resource = ?`
		args = append(args, resource)
	}
	q += ` ORDER BY seq DESC LIMIT ?`
	args = append(args, limit)
	rows, err := d.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("listing completions: %w", err)
	}
	defer rows.Close()
	var out []Completion
	for rows.Next() {
		var c Completion
		if err := rows.Scan(&c.Seq, &c.ID, &c.Resource, &c.Label, &c.Outcome,
			&c.ExitCode, &c.StartedAt, &c.FinishedAt,
			&c.WorkingDirectory, &c.RepositoryRoot, &c.GitBranch); err != nil {
			return nil, fmt.Errorf("reading completion row: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// CleanupStale removes abandoned workloads (any resource if resource is
// empty) and reports what was removed. It never touches healthy rows.
func CleanupStale(d *sql.DB, resource string) ([]StaleRemoved, error) {
	tx, err := d.Begin()
	if err != nil {
		return nil, fmt.Errorf("beginning cleanup transaction: %w", err)
	}
	defer tx.Rollback()
	removed, err := deleteStaleTx(tx, resource, nowMillis())
	if err != nil {
		return nil, err
	}
	return removed, tx.Commit()
}

// deleteStaleTx removes abandoned rows and records the ones that held the
// resource as OutcomeStale, so a hard-killed workload leaves a trace instead
// of simply vanishing. Stale *waiting* rows never ran and are not recorded,
// the same rule Release applies.
//
// This runs inside TryAcquire's acquisition transaction, so the extra work
// matters. It is bounded to at most one insert-and-prune per resource: only
// running rows are recorded, and idx_one_running guarantees there is at most
// one of those per resource.
func deleteStaleTx(tx *sql.Tx, resource string, now int64) ([]StaleRemoved, error) {
	removed, reclaimed, err := takeStaleTx(tx, resource, now)
	if err != nil {
		return nil, err
	}
	// Recorded only after takeStaleTx has closed its result set: with a
	// single connection, inserting while RETURNING rows are still open would
	// deadlock.
	for _, c := range reclaimed {
		if err := recordCompletionTx(tx, c, now); err != nil {
			return nil, err
		}
	}
	return removed, nil
}

// takeStaleTx deletes the abandoned rows and splits them into what the caller
// reports and what is worth recording.
func takeStaleTx(tx *sql.Tx, resource string, now int64) ([]StaleRemoved, []Completion, error) {
	cutoff := now - StaleThreshold.Milliseconds()
	const cols = ` RETURNING id, resource, state, IFNULL(label,''), IFNULL(acquired_at,0),
	          IFNULL(working_directory,''), IFNULL(repository_root,''),
	          IFNULL(git_branch,'')`
	q := `DELETE FROM workloads WHERE heartbeat_at < ?` + cols
	args := []any{cutoff}
	if resource != "" {
		q = `DELETE FROM workloads WHERE resource = ? AND heartbeat_at < ?` + cols
		args = []any{resource, cutoff}
	}
	rows, err := tx.Query(q, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("removing stale workloads: %w", err)
	}
	defer rows.Close()
	var removed []StaleRemoved
	var reclaimed []Completion
	for rows.Next() {
		var r StaleRemoved
		var c Completion
		if err := rows.Scan(&r.ID, &r.Resource, &r.State, &c.Label, &c.StartedAt,
			&c.WorkingDirectory, &c.RepositoryRoot, &c.GitBranch); err != nil {
			return nil, nil, fmt.Errorf("reading stale workload row: %w", err)
		}
		removed = append(removed, r)
		if r.State == "running" {
			c.ID, c.Resource, c.Outcome, c.FinishedAt = r.ID, r.Resource, OutcomeStale, now
			reclaimed = append(reclaimed, c)
		}
	}
	return removed, reclaimed, rows.Err()
}

// List returns current workloads (all resources if resource is empty),
// ordered by resource, then running before waiting, then FIFO sequence.
func List(d *sql.DB, resource string) ([]Workload, error) {
	q := `SELECT seq, id, resource, IFNULL(label,''), state, IFNULL(pid,0),
	             created_at, IFNULL(acquired_at,0), heartbeat_at,
	             IFNULL(working_directory,''), IFNULL(repository_root,''),
	             IFNULL(git_common_dir,''), IFNULL(git_branch,''),
	             IFNULL(command_display,''), IFNULL(hostname,'')
	      FROM workloads`
	var args []any
	if resource != "" {
		q += ` WHERE resource = ?`
		args = append(args, resource)
	}
	q += ` ORDER BY resource, CASE state WHEN 'running' THEN 0 ELSE 1 END, seq`
	rows, err := d.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("listing workloads: %w", err)
	}
	defer rows.Close()
	var out []Workload
	for rows.Next() {
		var w Workload
		if err := rows.Scan(&w.Seq, &w.ID, &w.Resource, &w.Label, &w.State, &w.PID,
			&w.CreatedAt, &w.AcquiredAt, &w.HeartbeatAt,
			&w.WorkingDirectory, &w.RepositoryRoot, &w.GitCommonDir, &w.GitBranch,
			&w.CommandDisplay, &w.Hostname); err != nil {
			return nil, fmt.Errorf("reading workload row: %w", err)
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// AwaitEvents receives progress callbacks from Await. Any callback may be nil.
type AwaitEvents struct {
	OnStaleRemoved func(StaleRemoved)
	OnLongWait     func(position int, waited time.Duration)
}

// Await blocks until w acquires its resource, polling conservatively with
// short transactions. The caller is responsible for running heartbeats
// concurrently (see StartHeartbeat). Returns ctx.Err() if ctx is canceled
// and ErrGone if another process removed w as stale.
func Await(ctx context.Context, d *sql.DB, w *Workload, ev AwaitEvents) error {
	start := time.Now()
	nextNotice := start.Add(LongWaitNotice)
	for {
		acquired, removed, err := TryAcquire(d, w)
		if ev.OnStaleRemoved != nil {
			for _, r := range removed {
				ev.OnStaleRemoved(r)
			}
		}
		if err != nil {
			return err
		}
		if acquired {
			return nil
		}
		if ev.OnLongWait != nil && time.Now().After(nextNotice) {
			pos, perr := Position(d, w)
			if perr == nil {
				ev.OnLongWait(pos, time.Since(start))
			}
			nextNotice = time.Now().Add(LongWaitNotice)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(PollInterval):
		}
	}
}

// StartHeartbeat launches a goroutine refreshing w's heartbeat every
// HeartbeatInterval until ctx is canceled. onError (may be nil) receives
// heartbeat failures, including ErrGone; heartbeating continues on
// transient errors and stops on ErrGone.
func StartHeartbeat(ctx context.Context, d *sql.DB, w *Workload, onError func(error)) {
	go func() {
		t := time.NewTicker(HeartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := Heartbeat(d, w); err != nil {
					if onError != nil {
						onError(err)
					}
					if errors.Is(err, ErrGone) {
						return
					}
				}
			}
		}
	}()
}
