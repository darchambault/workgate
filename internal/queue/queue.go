// Package queue implements the coordination protocol: enqueueing, atomic
// acquisition, heartbeats, stale-workload recovery, release, and status
// queries. Acquisition order is (priority, seq) - the highest priority first,
// and arrival order within a level. Every database transaction here is short; nothing holds
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
	Priority         int // 1 (highest) .. 5 (lowest)
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
	CommandDisplay   string
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

// Priority levels. 1 is highest, 5 is lowest, and 3 is what a workload gets
// without --priority. Ordering is strict: a waiting workload with a lower
// number acquires before every waiting workload with a higher one, whatever
// their arrival order, and arrival order decides only within a level. A
// running workload is never preempted.
//
// There is no aging: a level-5 workload behind a steady supply of level-1
// work waits indefinitely. SetPriority is the remedy, deliberately a human
// decision rather than a scheduler heuristic that would make "who runs next"
// depend on the clock.
const (
	PriorityHighest = 1
	PriorityDefault = 3
	PriorityLowest  = 5
)

var resourceRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`)

// idRe matches an id exactly as status prints it. newID always produces six
// hex characters, including its crypto-failure fallback.
var idRe = regexp.MustCompile(`^[0-9a-f]{6}$`)

// ErrNoSuchWorkload reports that no queued workload has the requested id -
// it finished, was released, or was reclaimed as stale.
var ErrNoSuchWorkload = errors.New("no queued workload with that id")

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

// ValidateID normalizes a workload id as the user reads it off status.
// Matching is exact: ids are unique and only six characters, so a prefix would
// buy nothing and would introduce an ambiguity case that cannot otherwise
// occur. The only lookup failure is ErrNoSuchWorkload.
func ValidateID(id string) (string, error) {
	id = strings.ToLower(strings.TrimSpace(id))
	if !idRe.MatchString(id) {
		return "", fmt.Errorf("invalid workload id %q: expected the six characters shown by `workgate status`", id)
	}
	return id, nil
}

// ValidatePriority parses a user-supplied priority level.
func ValidatePriority(s string) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0, fmt.Errorf("invalid priority %q: expected a whole number from %d (highest) to %d (lowest)",
			s, PriorityHighest, PriorityLowest)
	}
	if err := checkPriority(n); err != nil {
		return 0, err
	}
	return n, nil
}

func checkPriority(n int) error {
	if n < PriorityHighest || n > PriorityLowest {
		return fmt.Errorf("priority %d is out of range: expected %d (highest) to %d (lowest)",
			n, PriorityHighest, PriorityLowest)
	}
	return nil
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

// Enqueue inserts a new waiting workload for resource at the given priority.
// heartbeat_at is initialized in the same INSERT, so there is no window in
// which another process could consider the fresh row stale.
//
// priority is a parameter rather than a Meta field because Meta is diagnostic
// context - none of it affects locking semantics - and priority decides who
// runs next. An invalid level is an error, never silently clamped: a zero
// value here means a caller forgot, not that it wanted PriorityDefault.
func Enqueue(d *sql.DB, resource string, priority int, meta Meta) (*Workload, error) {
	if err := checkPriority(priority); err != nil {
		return nil, err
	}
	now := nowMillis()
	var seq int64
	var id string
	for attempt := 0; ; attempt++ {
		id = newID()
		res, err := d.Exec(`
			INSERT INTO workloads
				(id, resource, label, state, pid, created_at, heartbeat_at,
				 working_directory, repository_root, git_common_dir, git_branch,
				 command_display, hostname, priority)
			VALUES (?, ?, ?, 'waiting', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			id, resource, meta.Label, meta.PID, now, now,
			meta.WorkingDirectory, meta.RepositoryRoot, meta.GitCommonDir,
			meta.GitBranch, meta.CommandDisplay, meta.Hostname, priority)
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
		State: "waiting", Priority: priority, PID: int64(meta.PID),
		CreatedAt: now, HeartbeatAt: now,
	}, nil
}

// positionQuery ranks one workload against everything queued for its resource,
// in exactly the order List displays and TryAcquire enforces: the running
// workload first - it holds the resource whatever its priority, because
// workgate never preempts - then by priority, then by arrival.
//
// Counting the running row explicitly is the part that is easy to get wrong. A
// plain (priority, seq) comparison would rank a waiting level-1 row above a
// running level-3 one and report position 1 to a workload that is in fact
// blocked behind it.
const positionQuery = `
SELECT COUNT(*) + 1
  FROM workloads t JOIN workloads me ON me.id = ?
 WHERE t.resource = me.resource AND t.id <> me.id
   AND ( t.state = 'running'
      OR ( me.state = 'waiting'
           AND ( t.priority < me.priority
              OR (t.priority = me.priority AND t.seq < me.seq) ) ) )`

// rowQuerier is satisfied by both *sql.DB and *sql.Tx, so a position can be
// read on its own or inside the transaction that just changed a priority.
type rowQuerier interface {
	QueryRow(query string, args ...any) *sql.Row
}

func positionOf(q rowQuerier, id string) (int, error) {
	var pos int
	if err := q.QueryRow(positionQuery, id).Scan(&pos); err != nil {
		return 0, fmt.Errorf("querying queue position: %w", err)
	}
	return pos, nil
}

// Position returns this workload's 1-based place in the resource's queue: one
// more than the number of workloads that will use the resource before it. The
// running workload counts as ahead of every waiter. Position 1 means next in
// line, or already running.
//
// The row is ranked by id rather than from w, because w's cached priority may
// be stale - see TryAcquire. And because a higher-priority workload can arrive
// at any time, a waiting workload's position can go up as well as down; this
// is the number to report now, not a promise about later.
//
// A row already removed as stale ranks against nothing and reports 1. That is
// diagnostic output only; reporting the row's disappearance is TryAcquire's
// and Heartbeat's job, on the same polling loop.
func Position(d *sql.DB, w *Workload) (int, error) {
	return positionOf(d, w.ID)
}

// TryAcquire attempts, in a single short immediate transaction, to
// transition w from waiting to running. The transaction:
//
//  1. deletes stale entries for the resource (returned for diagnostics);
//  2. verifies no running owner remains;
//  3. verifies that no waiting workload for the resource outranks w, where
//     rank is (priority, seq): the lower priority number first, arrival order
//     within a level;
//  4. claims the resource.
//
// Because all four steps commit atomically, two processes can never both
// conclude "the resource is free and I am next", and stale cleanup cannot race
// acquisition. seq is unique, so (priority, seq) is a strict total order and
// exactly one waiter can pass step 3. The partial unique index on (resource)
// WHERE state='running' additionally enforces single ownership at the database
// level.
//
// Ordering is strict priority, not FIFO: a newly arrived workload can overtake
// an older healthy waiter, but only by having a higher priority (a lower
// number). Within one level arrival order is absolute, and a running workload
// is never preempted. There is no aging, so a low-priority workload can be
// starved indefinitely by a stream of higher-priority ones; SetPriority is the
// deliberate escape hatch.
//
// Step 3 reads w's priority from the row rather than from w, because
// SetPriority may have changed it since this process enqueued. That re-read is
// also why a priority change needs no signalling: every waiter picks it up on
// its next poll.
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
	var myPriority int
	err = tx.QueryRow(`SELECT seq, priority FROM workloads WHERE id = ?`, w.ID).
		Scan(&mySeq, &myPriority)
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

	// Rank, not age: a waiter outranks us if its priority number is lower, or
	// equal and its seq is earlier. Asking "is anyone ahead of me" as a count
	// keeps this a single comparison against a strict total order, so exactly
	// one waiting row can ever see zero.
	var ahead int
	if err := tx.QueryRow(`
		SELECT COUNT(*) FROM workloads
		 WHERE resource = ? AND state = 'waiting'
		   AND (priority < ? OR (priority = ? AND seq < ?))`,
		w.Resource, myPriority, myPriority, mySeq).Scan(&ahead); err != nil {
		return false, removed, fmt.Errorf("checking for better-placed waiters: %w", err)
	}
	if ahead > 0 {
		return false, removed, tx.Commit() // a better-placed waiter goes first
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
	w.Priority = myPriority
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

// PriorityChange describes a completed priority mutation, with enough context
// for the caller to report it without a second query.
type PriorityChange struct {
	ID       string
	Resource string
	Label    string
	State    string // the row's state at the moment of the change
	From     int
	To       int
	Position int // 1-based place in the queue after the change
}

// SetPriority changes a queued workload's priority and reports where that
// leaves it. It is the only function here that writes a row belonging to
// another process, so it does all of its work - read, update, re-rank - in one
// immediate transaction: the position it reports is the one that held at the
// instant of the write.
//
// Nothing is notified, and nothing needs to be. Every waiter re-reads its own
// priority from the row on its next acquisition attempt, so a change takes
// effect within one poll interval without signalling, sockets, or a daemon.
//
// A running workload may be re-prioritized. The change is recorded so the
// caller can report it, but it has no scheduling effect: the resource is
// already held and workgate never preempts. Refusing would make the command
// fail for a race the user cannot see - a row can go from waiting to running
// between reading status and typing the id.
//
// heartbeat_at is deliberately untouched. It is a sign of life from the row's
// owner, not from whoever re-prioritized it, and re-prioritizing an abandoned
// workload must not resurrect it.
func SetPriority(d *sql.DB, id string, level int) (*PriorityChange, error) {
	if err := checkPriority(level); err != nil {
		return nil, err
	}
	tx, err := d.Begin()
	if err != nil {
		return nil, fmt.Errorf("beginning priority transaction: %w", err)
	}
	defer tx.Rollback()

	ch := PriorityChange{ID: id, To: level}
	err = tx.QueryRow(
		`SELECT resource, IFNULL(label,''), state, priority FROM workloads WHERE id = ?`, id).
		Scan(&ch.Resource, &ch.Label, &ch.State, &ch.From)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNoSuchWorkload
	}
	if err != nil {
		return nil, fmt.Errorf("reading workload %s: %w", id, err)
	}

	if _, err := tx.Exec(`UPDATE workloads SET priority = ? WHERE id = ?`, level, id); err != nil {
		return nil, fmt.Errorf("setting priority: %w", err)
	}
	if ch.Position, err = positionOf(tx, id); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("committing priority change: %w", err)
	}
	return &ch, nil
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
		label, wd, root, branch, command string
		acquiredAt                       sql.NullInt64
	)
	// QueryRow, not Query: with a single connection, executing the INSERT
	// below while a RETURNING result set is still open would deadlock.
	err = tx.QueryRow(`
		DELETE FROM workloads WHERE id = ?
		RETURNING IFNULL(label,''), acquired_at, IFNULL(working_directory,''),
		          IFNULL(repository_root,''), IFNULL(git_branch,''),
		          IFNULL(command_display,'')`, w.ID).
		Scan(&label, &acquiredAt, &wd, &root, &branch, &command)
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
		CommandDisplay: command,
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
			 working_directory, repository_root, git_branch, command_display)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		c.ID, c.Resource, c.Label, c.Outcome, c.ExitCode, c.StartedAt, c.FinishedAt,
		c.WorkingDirectory, c.RepositoryRoot, c.GitBranch,
		c.CommandDisplay); err != nil {
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
	             IFNULL(repository_root,''), IFNULL(git_branch,''),
	             IFNULL(command_display,'')
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
			&c.WorkingDirectory, &c.RepositoryRoot, &c.GitBranch,
			&c.CommandDisplay); err != nil {
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
	          IFNULL(git_branch,''), IFNULL(command_display,'')`
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
			&c.WorkingDirectory, &c.RepositoryRoot, &c.GitBranch,
			&c.CommandDisplay); err != nil {
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

// List returns current workloads (all resources if resource is empty), in the
// order they will use their resource: by resource, then running before
// waiting, then priority, then arrival. Display order and acquisition order
// are the same order deliberately - a view that sorted differently from
// TryAcquire would misreport who runs next.
func List(d *sql.DB, resource string) ([]Workload, error) {
	q := `SELECT seq, id, resource, IFNULL(label,''), state, IFNULL(pid,0),
	             created_at, IFNULL(acquired_at,0), heartbeat_at,
	             IFNULL(working_directory,''), IFNULL(repository_root,''),
	             IFNULL(git_common_dir,''), IFNULL(git_branch,''),
	             IFNULL(command_display,''), IFNULL(hostname,''), priority
	      FROM workloads`
	var args []any
	if resource != "" {
		q += ` WHERE resource = ?`
		args = append(args, resource)
	}
	q += ` ORDER BY resource, CASE state WHEN 'running' THEN 0 ELSE 1 END, priority, seq`
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
			&w.CommandDisplay, &w.Hostname, &w.Priority); err != nil {
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
