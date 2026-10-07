// Package queue implements the coordination protocol: enqueueing, atomic
// acquisition, heartbeats, stale-workload recovery, release, and status
// queries. Acquisition order is (priority, seq) - the highest priority first,
// and arrival order within a level. Every database transaction here is short; nothing holds
// a transaction open while waiting or while a child process runs.
//
// A workload may need several resources at once. It is stored as one row per
// resource, every row carrying the id of the first in group_id, and it takes
// all of them in one transaction or none: it waits holding nothing, so two
// workloads can never each hold what the other is waiting for. Rows written
// by an older binary have no group_id and are a workload of one - which is
// why the group key everywhere below is IFNULL(group_id, id).
//
// A row claims its resource exclusively or shared. Resources named on the
// command line are exclusive: one holder at a time. A tag (see ValidateTag) is
// a resource of its own namespace that ordinary workloads claim shared - any
// number of them hold it at once - and that a workload wanting the others out
// of the way claims exclusively. Two rows conflict unless both are shared, and
// only conflicting rows stand in each other's way. A workload with an
// AllWorkloads row conflicts with every other workload, whatever it names.
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
	"sort"
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

// Workload is one row of the coordination table: one resource of one
// workload. ID is the workload's, shared by every row of a multi-resource
// workload; the rows' own ids are never shown.
type Workload struct {
	Seq              int64
	ID               string
	Resource         string   // this row's resource
	Mode             string   // how this row claims it: ModeExclusive or ModeShared
	Resources        []string // every resource of the workload, in the order named
	Modes            []string // how each of Resources is claimed; nil means all exclusive
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
	Resources        []string // every resource the workload held, in the order named
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

// MaxResources is how many resources one workload may name, tags included.
// Generous for real use - a project, a GPU, a tag - and small enough that an
// entry's "also holds" line stays readable. The AllWorkloads marker is not
// counted: it is a flag on the workload, not something it names.
const MaxResources = 4

// How a row claims its resource. The strings are what the mode column holds.
const (
	ModeExclusive = "exclusive"
	ModeShared    = "shared"
)

// TagPrefix starts the resource name a tag is stored under. resourceRe can
// never match a colon, so no resource can collide with a tag, and a tag is
// still a row like any other: queued, ranked, reclaimed and listed by the same
// code.
const TagPrefix = "tag:"

// AllWorkloads is the resource a --block-all workload claims exclusively. It
// is not a queue anyone else joins: what makes it block everything is
// blockedQuery, which treats a workload holding it as conflicting with every
// other workload. "*" can be neither a resource nor a tag name.
const AllWorkloads = "*"

// Claim is one resource a workload asks for, and how.
type Claim struct {
	Resource string
	Shared   bool
}

func (c Claim) mode() string {
	if c.Shared {
		return ModeShared
	}
	return ModeExclusive
}

// Exclusive claims every one of resources exclusively, which is how every
// resource named on the command line is claimed.
func Exclusive(resources []string) []Claim {
	out := make([]Claim, len(resources))
	for i, r := range resources {
		out[i] = Claim{Resource: r}
	}
	return out
}

// groupKey is the SQL expression for the workload a row belongs to. See the
// package comment: rows from older binaries have no group_id.
const groupKey = `IFNULL(group_id, id)`

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

// ValidateTag normalizes a tag name exactly as ValidateResource does a
// resource name, and returns the resource the tag is stored under.
func ValidateTag(name string) (string, error) {
	n, err := ValidateResource(name)
	if err != nil {
		return "", fmt.Errorf("invalid tag: %w", err)
	}
	return TagPrefix + n, nil
}

// ValidateScope normalizes what status and monitor accept to narrow their
// view: a resource name, or a tag written the way the views print it,
// "tag:<name>".
func ValidateScope(name string) (string, error) {
	n := strings.ToLower(strings.TrimSpace(name))
	if strings.HasPrefix(n, TagPrefix) {
		return ValidateTag(strings.TrimPrefix(n, TagPrefix))
	}
	return ValidateResource(name)
}

// ValidateResources parses a comma-separated resource list, as `run` takes it.
// Each name is validated as ValidateResource does; the list keeps the order it
// was written in, which is the order the views name the resources.
//
// A name given twice is an error rather than quietly merged, and so is an
// empty element: both are typing mistakes, and "a,,b" in particular is more
// likely a name that went missing than one that was never meant.
func ValidateResources(list string) ([]string, error) {
	parts := strings.Split(list, ",")
	if len(parts) > MaxResources {
		return nil, fmt.Errorf("too many resources in %q: a workload may name at most %d", list, MaxResources)
	}
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if strings.TrimSpace(p) == "" && len(parts) > 1 {
			return nil, fmt.Errorf("empty resource name in %q", list)
		}
		name, err := ValidateResource(p)
		if err != nil {
			return nil, err
		}
		for _, seen := range out {
			if seen == name {
				return nil, fmt.Errorf("resource %q is named twice in %q", name, list)
			}
		}
		out = append(out, name)
	}
	return out, nil
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

// Enqueue inserts a new waiting workload for resources at the given priority:
// one row per resource, all in one transaction. heartbeat_at is initialized in
// the same INSERTs, so there is no window in which another process could
// consider the fresh rows stale.
//
// One transaction does more than keep the rows together. It holds the write
// lock, so the rows' seqs are consecutive and no other workload's seq can fall
// between them: comparing any two rows on (priority, seq) is then the same as
// comparing their workloads. That is what lets every per-resource rank check
// below stay a comparison of rows, and still be one strict total order across
// all resources - the property that makes a cycle of waiters impossible.
//
// priority is a parameter rather than a Meta field because Meta is diagnostic
// context - none of it affects locking semantics - and priority decides who
// runs next. An invalid level is an error, never silently clamped: a zero
// value here means a caller forgot, not that it wanted PriorityDefault.
func Enqueue(d *sql.DB, resources []string, priority int, meta Meta) (*Workload, error) {
	return EnqueueClaims(d, Exclusive(resources), priority, meta)
}

// EnqueueClaims is Enqueue for a workload that claims some of its resources
// shared - tags - or that blocks every other workload with an AllWorkloads
// claim. Claims are stored, and reported, in the order given.
func EnqueueClaims(d *sql.DB, claims []Claim, priority int, meta Meta) (*Workload, error) {
	if err := checkPriority(priority); err != nil {
		return nil, err
	}
	named := 0
	for _, c := range claims {
		if c.Resource != AllWorkloads {
			named++
		} else if c.Shared {
			return nil, errors.New("enqueueing workload: the all-workloads claim cannot be shared")
		}
	}
	if named == 0 || named > MaxResources {
		return nil, fmt.Errorf("enqueueing workload: %d resources, want 1 to %d", named, MaxResources)
	}
	tx, err := d.Begin()
	if err != nil {
		return nil, fmt.Errorf("beginning enqueue transaction: %w", err)
	}
	defer tx.Rollback()

	now := nowMillis()
	var group string
	var headSeq int64
	resources := make([]string, len(claims))
	modes := make([]string, len(claims))
	for i, c := range claims {
		id, seq, err := insertRow(tx, c.Resource, c.mode(), group, priority, meta, now)
		if err != nil {
			return nil, err
		}
		if group == "" {
			group, headSeq = id, seq
		}
		resources[i], modes[i] = c.Resource, c.mode()
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("committing enqueue: %w", err)
	}
	return &Workload{
		Seq: headSeq, ID: group, Resource: resources[0], Mode: modes[0],
		Resources: resources, Modes: modes, Label: meta.Label,
		State: "waiting", Priority: priority, PID: int64(meta.PID),
		CreatedAt: now, HeartbeatAt: now,
	}, nil
}

// insertRow writes one waiting row. An empty group makes this the first row of
// its workload, whose own id becomes the group.
func insertRow(tx *sql.Tx, resource, mode, group string, priority int, meta Meta, now int64) (string, int64, error) {
	for attempt := 0; ; attempt++ {
		id := newID()
		g := group
		if g == "" {
			g = id
		}
		res, err := tx.Exec(`
			INSERT INTO workloads
				(id, resource, label, state, pid, created_at, heartbeat_at,
				 working_directory, repository_root, git_common_dir, git_branch,
				 command_display, hostname, priority, group_id, mode)
			VALUES (?, ?, ?, 'waiting', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			id, resource, meta.Label, meta.PID, now, now,
			meta.WorkingDirectory, meta.RepositoryRoot, meta.GitCommonDir,
			meta.GitBranch, meta.CommandDisplay, meta.Hostname, priority, g, mode)
		if err != nil {
			if attempt < 3 && strings.Contains(err.Error(), "UNIQUE") {
				continue // improbable id collision; retry with a fresh id
			}
			return "", 0, fmt.Errorf("enqueueing workload: %w", err)
		}
		seq, err := res.LastInsertId()
		if err != nil {
			return "", 0, fmt.Errorf("reading workload sequence: %w", err)
		}
		return id, seq, nil
	}
}

// resources is w.Resources, or w.Resource for a Workload built without them.
func (w *Workload) resources() []string {
	if len(w.Resources) > 0 {
		return w.Resources
	}
	return []string{w.Resource}
}

// conflicts is the SQL condition under which row t, of another workload, can
// stand in the way of row me. Two rows conflict when they are on the same
// resource and not both shared, and a workload holding the AllWorkloads marker
// conflicts with every other workload on every row - in both directions, so a
// --block-all waits for everything ahead of it and holds back everything
// behind it.
//
// Every claim was exclusive before tags, and then this is exactly the
// same-resource rule it replaces. The workloads are told apart by group rather
// than by row id because the AllWorkloads clauses match rows on different
// resources, where a workload's own rows would otherwise meet.
const conflicts = `IFNULL(t.group_id, t.id) <> IFNULL(me.group_id, me.id)
   AND ( ( t.resource = me.resource AND (me.mode = 'exclusive' OR t.mode = 'exclusive') )
      OR me.resource = '` + AllWorkloads + `' OR t.resource = '` + AllWorkloads + `' )`

// placesQuery ranks each row of one workload against everything queued for
// that row's resource, in exactly the order List displays and TryAcquire
// enforces: the running workload first - it holds the resource whatever its
// priority, because workgate never preempts - then by priority, then by
// arrival. Only workloads that conflict with the row count: a shared holder of
// a tag is not in the way of another, and a --block-all is in the way of
// everything.
//
// Counting the running row explicitly is the part that is easy to get wrong. A
// plain (priority, seq) comparison would rank a waiting level-1 row above a
// running level-3 one and report position 1 to a workload that is in fact
// blocked behind it. Workloads are counted, not rows: under the AllWorkloads
// rule one workload can conflict through several of its rows.
const placesQuery = `
SELECT me.resource, (
       SELECT COUNT(DISTINCT IFNULL(t.group_id, t.id)) + 1 FROM workloads t
        WHERE ` + conflicts + `
          AND ( t.state = 'running'
             OR ( me.state = 'waiting'
                  AND ( t.priority < me.priority
                     OR (t.priority = me.priority AND t.seq < me.seq) ) ) ))
  FROM workloads me
 WHERE IFNULL(me.group_id, me.id) = ?
 ORDER BY me.seq`

// Place is a workload's position in one resource's queue.
type Place struct {
	Resource string
	Position int
}

// querier is satisfied by both *sql.DB and *sql.Tx, so places can be read on
// their own or inside the transaction that just changed a priority.
type querier interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

func placesOf(q querier, id string) ([]Place, error) {
	rows, err := q.Query(placesQuery, id)
	if err != nil {
		return nil, fmt.Errorf("querying queue position: %w", err)
	}
	defer rows.Close()
	var out []Place
	for rows.Next() {
		var p Place
		if err := rows.Scan(&p.Resource, &p.Position); err != nil {
			return nil, fmt.Errorf("reading queue position: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// Positions returns this workload's 1-based place in each of its resources'
// queues, in the order the resources were named: one more than the number of
// workloads that will use that resource before it. The running workload counts
// as ahead of every waiter. Position 1 means next in line, or already running.
// A workload with several resources acquires once it is first in every queue
// and nothing is running on any of them.
//
// Rows are ranked by id rather than from w, because w's cached priority may be
// stale - see TryAcquire. And because a higher-priority workload can arrive at
// any time, a waiting workload's position can go up as well as down; this is
// the number to report now, not a promise about later.
//
// A workload already removed as stale has no rows and reports no places. That
// is diagnostic output only; reporting the disappearance is TryAcquire's and
// Heartbeat's job, on the same polling loop.
func Positions(d *sql.DB, w *Workload) ([]Place, error) {
	return placesOf(d, w.ID)
}

// blockedQuery counts what stands between a waiting workload and all of its
// resources: any conflicting row (see conflicts) that is running, or that is
// waiting and outranks this workload's row on (priority, seq). The workload's
// own rows never count against each other.
//
// Comparing rows on different resources - which the AllWorkloads rule does -
// is still comparing workloads: Enqueue keeps a workload's seqs consecutive,
// and TryAcquire keeps its rows at one priority.
const blockedQuery = `
SELECT COUNT(*)
  FROM workloads me JOIN workloads t
    ON ` + conflicts + `
 WHERE IFNULL(me.group_id, me.id) = ?
   AND ( t.state = 'running'
      OR ( t.state = 'waiting'
           AND ( t.priority < me.priority
              OR (t.priority = me.priority AND t.seq < me.seq) ) ) )`

// TryAcquire attempts, in a single short immediate transaction, to
// transition every row of w from waiting to running. The transaction:
//
//  1. deletes stale workloads touching w's resources (returned for
//     diagnostics);
//  2. verifies w still has a row for every resource it named;
//  3. verifies that, on every one of those resources, nothing that conflicts
//     with w's claim is running and no conflicting waiting workload outranks
//     w, where rank is (priority, seq): the lower priority number first,
//     arrival order within a level;
//  4. claims all of them.
//
// Because all four steps commit atomically, two processes can never both
// conclude "the resource is free and I am next", and stale cleanup cannot race
// acquisition. seq is unique, so (priority, seq) is a strict total order and
// of the waiters that conflict on a resource, exactly one can pass step 3. The
// partial unique index on (resource) WHERE the row is running and exclusive
// additionally enforces single exclusive ownership at the database level;
// keeping shared and exclusive holders apart rests on this transaction alone.
//
// Several resources are taken all at once or not at all, so a waiting
// workload holds nothing - the condition that rules out deadlock. And because
// Enqueue keeps a workload's seqs consecutive, its rank is the same on every
// resource it names: two waiters can never each be ahead of the other, and
// the best-ranked waiter of all is first in every queue it is in.
//
// That ordering is strict head-of-line. A waiter blocks everything ranked
// behind it that it conflicts with, on every resource it names, including one
// that is idle while it waits for another: letting later work take the idle
// one would let a stream of single-resource work starve a multi-resource
// workload indefinitely. This is what makes a --block-tag a hold: from the
// moment it is queued, tagged work queued after it waits, while what is
// already running, or queued ahead of it, finishes first.
//
// Rows that do not conflict do not hold each other back, even while one of
// them waits: a shared tag holder queued behind its own busy resource does not
// stop a later holder of the same tag, or one tag would chain every queue that
// uses it into one. Liveness is unaffected - the best-ranked waiter of all is
// still blocked only by what is running.
//
// Ordering is strict priority, not FIFO: a newly arrived workload can overtake
// an older healthy waiter, but only by having a higher priority (a lower
// number). Within one level arrival order is absolute, and a running workload
// is never preempted. There is no aging, so a low-priority workload can be
// starved indefinitely by a stream of higher-priority ones; SetPriority is the
// deliberate escape hatch.
//
// Step 3 reads w's priority from the rows rather than from w, because
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
	removed, err = deleteStaleTx(tx, w.resources(), now)
	if err != nil {
		return false, nil, err
	}

	// Our own rows may have been deleted as stale by another process while
	// this one was suspended (sleep, debugger). Detect that explicitly - and
	// a workload missing some of its rows is as gone as one missing all of
	// them: an older binary reclaims one resource at a time, and holding the
	// rest would be holding a set this workload never asked for.
	var rows, headPriority int
	if err := tx.QueryRow(`
		SELECT COUNT(*), IFNULL(MAX(CASE WHEN id = ? THEN priority END), 0)
		  FROM workloads WHERE `+groupKey+` = ?`, w.ID, w.ID).
		Scan(&rows, &headPriority); err != nil {
		return false, removed, fmt.Errorf("checking own workload rows: %w", err)
	}
	if rows < len(w.resources()) || headPriority == 0 {
		return false, removed, ErrGone
	}

	// Every row of a workload carries the same level, and the first row's is
	// the workload's. Only an older binary, re-prioritizing one row by its own
	// id, can make them differ - and differing levels would let this
	// workload's rank disagree between resources, the one thing that could
	// make two waiters each see the other ahead. A no-op in every other case.
	if _, err := tx.Exec(`UPDATE workloads SET priority = ?
		WHERE `+groupKey+` = ? AND priority <> ?`, headPriority, w.ID, headPriority); err != nil {
		return false, removed, fmt.Errorf("aligning workload priority: %w", err)
	}

	// Rank, not age, and every resource at once. Asking "is anything in my
	// way" as one count keeps this a single comparison against a strict total
	// order, so exactly one waiting workload can ever see zero on a resource.
	var blocked int
	if err := tx.QueryRow(blockedQuery, w.ID).Scan(&blocked); err != nil {
		return false, removed, fmt.Errorf("checking resource owners and better-placed waiters: %w", err)
	}
	if blocked > 0 {
		return false, removed, tx.Commit() // keep the stale deletions
	}

	if _, err := tx.Exec(
		`UPDATE workloads SET state = 'running', acquired_at = ?, heartbeat_at = ? WHERE `+groupKey+` = ?`,
		now, now, w.ID); err != nil {
		return false, removed, fmt.Errorf("claiming resource: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, removed, fmt.Errorf("committing acquisition: %w", err)
	}
	w.State = "running"
	w.AcquiredAt = now
	w.Priority = headPriority
	return true, removed, nil
}

// Heartbeat refreshes heartbeat_at on every row of w, in one statement - so a
// workload's rows always go stale together. Returns ErrGone if any of them
// has been removed by another process's stale cleanup.
func Heartbeat(d *sql.DB, w *Workload) error {
	res, err := d.Exec(`UPDATE workloads SET heartbeat_at = ? WHERE `+groupKey+` = ?`, nowMillis(), w.ID)
	if err != nil {
		return fmt.Errorf("updating heartbeat: %w", err)
	}
	if n, err := res.RowsAffected(); err == nil && n < int64(len(w.resources())) {
		return ErrGone
	}
	return nil
}

// PriorityChange describes a completed priority mutation, with enough context
// for the caller to report it without a second query.
type PriorityChange struct {
	ID     string
	Label  string
	State  string // the workload's state at the moment of the change
	From   int
	To     int
	Places []Place // the workload's place in each of its queues after the change
}

// SetPriority changes a queued workload's priority - on every one of its rows
// - and reports where that leaves it. It is the only function here that writes
// rows belonging to another process, so it does all of its work - read,
// update, re-rank - in one immediate transaction: the positions it reports are
// the ones that held at the instant of the write.
//
// Nothing is notified, and nothing needs to be. Every waiter re-reads its own
// priority from the rows on its next acquisition attempt, so a change takes
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

	// The workload is read off its first row, whose own id is the workload's.
	ch := PriorityChange{ID: id, To: level}
	err = tx.QueryRow(`
		SELECT IFNULL(label,''), state, priority FROM workloads
		 WHERE id = ? AND `+groupKey+` = id`, id).
		Scan(&ch.Label, &ch.State, &ch.From)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNoSuchWorkload
	}
	if err != nil {
		return nil, fmt.Errorf("reading workload %s: %w", id, err)
	}

	if _, err := tx.Exec(`UPDATE workloads SET priority = ? WHERE `+groupKey+` = ?`, level, id); err != nil {
		return nil, fmt.Errorf("setting priority: %w", err)
	}
	if ch.Places, err = placesOf(tx, id); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("committing priority change: %w", err)
	}
	return &ch, nil
}

// Release deletes every row of w, letting the next queued workloads acquire
// its resources, and — if w actually held them — records how it ended in the
// bounded completions ring, once per resource. Deleting an already-removed
// workload is not an error.
//
// Both happen in one short transaction. Splitting them would force a bad
// choice: deleting first can lose a completion (harmless), but inserting
// first can leave the resource held if this process dies in between
// (catastrophic). One transaction removes the choice, and with
// MaxOpenConns(1) it also avoids taking the write lock twice while a waiter
// polls.
//
// DELETE ... RETURNING does three jobs at once here: it frees the resources,
// it supplies the label and paths — which the in-memory Workload does not
// carry, since Enqueue only populates the fields it knows — and its
// rows-or-no-rows result is an exactly-once guard, so a caller that releases
// twice can never write two sets of completions.
//
// Every completion of one release shares its finished_at. Together with the
// id, that is what lets RecentCompletions tell them apart as one workload.
func Release(d *sql.DB, w *Workload, out Outcome) error {
	tx, err := d.Begin()
	if err != nil {
		return fmt.Errorf("beginning release transaction: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.Query(`
		DELETE FROM workloads WHERE `+groupKey+` = ?
		RETURNING seq, resource, IFNULL(label,''), IFNULL(acquired_at,0),
		          IFNULL(working_directory,''), IFNULL(repository_root,''),
		          IFNULL(git_branch,''), IFNULL(command_display,'')`, w.ID)
	if err != nil {
		return fmt.Errorf("releasing workload: %w", err)
	}
	var held []Completion
	for rows.Next() {
		var c Completion
		if err := rows.Scan(&c.Seq, &c.Resource, &c.Label, &c.StartedAt,
			&c.WorkingDirectory, &c.RepositoryRoot, &c.GitBranch,
			&c.CommandDisplay); err != nil {
			rows.Close()
			return fmt.Errorf("releasing workload: %w", err)
		}
		// A row that never acquired its resource did not run, so it is not a
		// completion — it would only be noise in a view of the last few.
		// (No row reached here means another process already reclaimed the
		// workload as stale, and recorded it.)
		if c.StartedAt != 0 {
			held = append(held, c)
		}
	}
	// Closed before recording: with a single connection, inserting while the
	// RETURNING result set is still open would deadlock.
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("releasing workload: %w", err)
	}
	now := nowMillis()
	for _, c := range inSeqOrder(held) {
		c.ID, c.Outcome, c.ExitCode, c.FinishedAt = w.ID, out.Kind, int64(out.ExitCode), now
		if err := recordCompletionTx(tx, c, now); err != nil {
			return err
		}
	}
	return commitRelease(tx)
}

// inSeqOrder sorts completions taken off workload rows into the order the rows
// were written, which RETURNING does not promise. Recording them in that order
// is what keeps a workload's resources in the order they were named.
func inSeqOrder(cs []Completion) []Completion {
	sort.Slice(cs, func(i, j int) bool { return cs[i].Seq < cs[j].Seq })
	return cs
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
//
// A workload that held several resources finished once but left a completion
// on each of them, all sharing its id and finished_at. Every entry carries
// all of them in Resources. Scoped to one resource, that resource's own entry
// is returned; unscoped, the workload is returned once - as its last-recorded
// entry, which is where it sits in completion order - so one finish is not
// shown as several, and limit counts workloads rather than rows.
func RecentCompletions(d *sql.DB, resource string, limit int) ([]Completion, error) {
	if limit <= 0 {
		return nil, nil
	}
	// The window runs over the whole table before the outer WHERE narrows
	// it, which is how a scoped read still learns the other resources.
	q := `SELECT seq, id, resource, label, outcome, exit_code, started_at,
	             finished_at, working_directory, repository_root, git_branch,
	             command_display, members
	      FROM (SELECT seq, id, resource, IFNULL(label,'') AS label, outcome,
	                   exit_code, started_at, finished_at,
	                   IFNULL(working_directory,'') AS working_directory,
	                   IFNULL(repository_root,'') AS repository_root,
	                   IFNULL(git_branch,'') AS git_branch,
	                   IFNULL(command_display,'') AS command_display,
	                   group_concat(resource, ',') OVER (
	                       PARTITION BY id, finished_at ORDER BY seq
	                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS members,
	                   row_number() OVER (
	                       PARTITION BY id, finished_at ORDER BY seq DESC) AS from_last
	            FROM completions)`
	var args []any
	if resource != "" {
		q += ` WHERE resource = ?`
		args = append(args, resource)
	} else {
		q += ` WHERE from_last = 1`
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
		var members string
		if err := rows.Scan(&c.Seq, &c.ID, &c.Resource, &c.Label, &c.Outcome,
			&c.ExitCode, &c.StartedAt, &c.FinishedAt,
			&c.WorkingDirectory, &c.RepositoryRoot, &c.GitBranch,
			&c.CommandDisplay, &members); err != nil {
			return nil, fmt.Errorf("reading completion row: %w", err)
		}
		c.Resources = strings.Split(members, ",")
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
	var scope []string
	if resource != "" {
		scope = []string{resource}
	}
	removed, err := deleteStaleTx(tx, scope, nowMillis())
	if err != nil {
		return nil, err
	}
	return removed, tx.Commit()
}

// CleanupAbandoned is CleanupStale for a caller that should not be making
// judgement calls about other sessions' workloads: it removes a stale row only
// once the process that owns it is known to have exited. exited is asked about
// each stale row's owner - its pid, and the time the row was enqueued, which
// the owner was already running by - and must answer false whenever it cannot
// be sure.
//
// A stale heartbeat alone is what every other cleanup acts on, and it is
// enough for a process that is about to take the resource or has just been
// asked to report the queue. It is not enough for something that runs
// unattended for hours: after a machine sleep every heartbeat is stale at once,
// and removing a running row whose owner is merely late would let a second
// workload acquire the resource while the first child still runs. A dead owner
// cannot come back, so its row can go at once.
//
// A pid means nothing on another machine, so only rows recorded under host are
// judged; an empty host judges none. A workload is reclaimed whole, every
// resource of it, whichever of its rows was found.
//
// The candidates are read before any transaction is opened, so a queue with
// nothing to reclaim - the ordinary case - costs one read and no write lock.
// The delete then re-checks staleness inside the transaction: an owner that
// heartbeated in between is not removed.
func CleanupAbandoned(d *sql.DB, resource, host string, exited func(pid int, enqueued time.Time) bool) ([]StaleRemoved, error) {
	if host == "" {
		return nil, nil
	}
	q := `SELECT DISTINCT ` + groupKey + `, pid, created_at FROM workloads
	       WHERE heartbeat_at < ? AND hostname = ? AND pid > 0`
	args := []any{nowMillis() - StaleThreshold.Milliseconds(), host}
	if resource != "" {
		q += ` AND resource = ?`
		args = append(args, resource)
	}
	rows, err := d.Query(q, args...)
	if err != nil {
		return nil, fmt.Errorf("listing stale workloads: %w", err)
	}
	type candidate struct {
		id             string
		pid, createdAt int64
	}
	var stale []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.id, &c.pid, &c.createdAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("reading stale workload row: %w", err)
		}
		stale = append(stale, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("listing stale workloads: %w", err)
	}

	var dead []any
	for _, c := range stale {
		if exited(int(c.pid), time.UnixMilli(c.createdAt)) {
			dead = append(dead, c.id)
		}
	}
	if len(dead) == 0 {
		return nil, nil
	}

	tx, err := d.Begin()
	if err != nil {
		return nil, fmt.Errorf("beginning cleanup transaction: %w", err)
	}
	defer tx.Rollback()
	now := nowMillis()
	cond := groupKey + ` IN (` + placeholders(len(dead)) + `) AND heartbeat_at < ?`
	removed, err := reclaimTx(tx, cond, append(dead, now-StaleThreshold.Milliseconds()), now)
	if err != nil {
		return nil, err
	}
	return removed, tx.Commit()
}

// deleteStaleTx removes abandoned workloads - every workload with a row whose
// heartbeat is older than StaleThreshold, on any of resources (on any resource
// at all if resources is empty) - and records them as reclaimTx does.
//
// The workload is removed whole, not just its rows on resources. Its rows
// heartbeat in one statement and go stale together anyway; deleting by
// workload says so outright, so reclaiming one resource can never leave a
// dead workload's row holding another.
//
// This runs inside TryAcquire's acquisition transaction, so the extra work
// matters. It is bounded to one insert-and-prune per running row reclaimed:
// only running rows are recorded, idx_one_running allows one exclusive one per
// resource, and a workload has at most MaxResources rows besides its
// AllWorkloads marker. Shared holders of a tag are the exception - any number
// can be running - but each is a whole workload that stopped heartbeating, and
// reclaiming them is the work there is to do.
func deleteStaleTx(tx *sql.Tx, resources []string, now int64) ([]StaleRemoved, error) {
	inner := `SELECT ` + groupKey + ` FROM workloads WHERE heartbeat_at < ?`
	args := []any{now - StaleThreshold.Milliseconds()}
	if len(resources) > 0 {
		inner += ` AND resource IN (` + placeholders(len(resources)) + `)`
		for _, r := range resources {
			args = append(args, r)
		}
	}
	return reclaimTx(tx, groupKey+` IN (`+inner+`)`, args, now)
}

// placeholders returns n comma-separated SQL parameters.
func placeholders(n int) string {
	return `?` + strings.Repeat(`, ?`, n-1)
}

// reclaimTx deletes the rows matching cond and records the ones that held the
// resource as OutcomeStale, so a hard-killed workload leaves a trace instead
// of simply vanishing. Stale *waiting* rows never ran and are not recorded,
// the same rule Release applies.
func reclaimTx(tx *sql.Tx, cond string, args []any, now int64) ([]StaleRemoved, error) {
	removed, reclaimed, err := takeStaleTx(tx, cond, args, now)
	if err != nil {
		return nil, err
	}
	// Recorded only after takeStaleTx has closed its result set: with a
	// single connection, inserting while RETURNING rows are still open would
	// deadlock.
	for _, c := range inSeqOrder(reclaimed) {
		if err := recordCompletionTx(tx, c, now); err != nil {
			return nil, err
		}
	}
	return removed, nil
}

// takeStaleTx deletes the abandoned rows matching cond and splits them into
// what the caller reports and what is worth recording. Both carry the
// workload's id, never a row's own: that is the id every view has shown.
func takeStaleTx(tx *sql.Tx, cond string, args []any, now int64) ([]StaleRemoved, []Completion, error) {
	rows, err := tx.Query(`DELETE FROM workloads WHERE `+cond+`
		RETURNING `+groupKey+`, seq, resource, state, IFNULL(label,''), IFNULL(acquired_at,0),
		          IFNULL(working_directory,''), IFNULL(repository_root,''),
		          IFNULL(git_branch,''), IFNULL(command_display,'')`, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("removing stale workloads: %w", err)
	}
	defer rows.Close()
	var removed []StaleRemoved
	var reclaimed []Completion
	for rows.Next() {
		var r StaleRemoved
		var c Completion
		if err := rows.Scan(&r.ID, &c.Seq, &r.Resource, &r.State, &c.Label, &c.StartedAt,
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

// List returns current workloads (all resources if resource is empty, and
// always including any --block-all), in the order they will use their
// resource: by resource, then running before waiting, then priority, then
// arrival. Several shared holders of a tag can all be running at once. Display order and acquisition order
// are the same order deliberately - a view that sorted differently from
// TryAcquire would misreport who runs next.
//
// A workload with several resources is listed once per resource, as the row
// for that resource, and every one of its rows carries the workload's ID and
// all of its Resources - including when the list is narrowed to one resource,
// which is how a view of one queue knows what else the entry holds.
func List(d *sql.DB, resource string) ([]Workload, error) {
	// The window runs over the whole table before the outer WHERE narrows it.
	q := `SELECT seq, gid, resource, mode, label, state, pid, created_at, acquired_at,
	             heartbeat_at, working_directory, repository_root, git_common_dir,
	             git_branch, command_display, hostname, priority, members, member_modes
	      FROM (SELECT seq, ` + groupKey + ` AS gid, resource, mode, IFNULL(label,'') AS label,
	                   state, IFNULL(pid,0) AS pid, created_at,
	                   IFNULL(acquired_at,0) AS acquired_at, heartbeat_at,
	                   IFNULL(working_directory,'') AS working_directory,
	                   IFNULL(repository_root,'') AS repository_root,
	                   IFNULL(git_common_dir,'') AS git_common_dir,
	                   IFNULL(git_branch,'') AS git_branch,
	                   IFNULL(command_display,'') AS command_display,
	                   IFNULL(hostname,'') AS hostname, priority,
	                   group_concat(resource, ',') OVER (
	                       PARTITION BY ` + groupKey + ` ORDER BY seq
	                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS members,
	                   group_concat(mode, ',') OVER (
	                       PARTITION BY ` + groupKey + ` ORDER BY seq
	                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS member_modes
	            FROM workloads)`
	var args []any
	if resource != "" {
		// A --block-all is in the way of every queue, so a view of one queue
		// lists it too: without it, a waiter there would be stuck behind
		// something the view does not show.
		q += ` WHERE resource = ? OR resource = '` + AllWorkloads + `'`
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
		var members, modes string
		if err := rows.Scan(&w.Seq, &w.ID, &w.Resource, &w.Mode, &w.Label, &w.State, &w.PID,
			&w.CreatedAt, &w.AcquiredAt, &w.HeartbeatAt,
			&w.WorkingDirectory, &w.RepositoryRoot, &w.GitCommonDir, &w.GitBranch,
			&w.CommandDisplay, &w.Hostname, &w.Priority, &members, &modes); err != nil {
			return nil, fmt.Errorf("reading workload row: %w", err)
		}
		w.Resources = strings.Split(members, ",")
		w.Modes = strings.Split(modes, ",")
		out = append(out, w)
	}
	return out, rows.Err()
}

// AwaitEvents receives progress callbacks from Await. Any callback may be nil.
type AwaitEvents struct {
	// OnStaleRemoved receives everything one acquisition attempt reclaimed,
	// together: a workload of several resources is reclaimed as several rows
	// at once, and a caller handed them one by one could not tell that from
	// several workloads.
	OnStaleRemoved func([]StaleRemoved)
	OnLongWait     func(places []Place, waited time.Duration)
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
		if ev.OnStaleRemoved != nil && len(removed) > 0 {
			ev.OnStaleRemoved(removed)
		}
		if err != nil {
			return err
		}
		if acquired {
			return nil
		}
		if ev.OnLongWait != nil && time.Now().After(nextNotice) {
			places, perr := Positions(d, w)
			if perr == nil {
				ev.OnLongWait(places, time.Since(start))
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
