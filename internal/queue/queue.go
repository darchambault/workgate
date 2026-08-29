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
// resource. Deleting an already-removed row is not an error.
func Release(d *sql.DB, w *Workload) error {
	if _, err := d.Exec(`DELETE FROM workloads WHERE id = ?`, w.ID); err != nil {
		return fmt.Errorf("releasing workload: %w", err)
	}
	return nil
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

func deleteStaleTx(tx *sql.Tx, resource string, now int64) ([]StaleRemoved, error) {
	cutoff := now - StaleThreshold.Milliseconds()
	q := `DELETE FROM workloads WHERE heartbeat_at < ? RETURNING id, resource, state`
	args := []any{cutoff}
	if resource != "" {
		q = `DELETE FROM workloads WHERE resource = ? AND heartbeat_at < ? RETURNING id, resource, state`
		args = []any{resource, cutoff}
	}
	rows, err := tx.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("removing stale workloads: %w", err)
	}
	defer rows.Close()
	var removed []StaleRemoved
	for rows.Next() {
		var r StaleRemoved
		if err := rows.Scan(&r.ID, &r.Resource, &r.State); err != nil {
			return nil, fmt.Errorf("reading stale workload row: %w", err)
		}
		removed = append(removed, r)
	}
	return removed, rows.Err()
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
