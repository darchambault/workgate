package queue

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"workgate/internal/db"
)

func testDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "workgate.db")
	d, err := db.Open(path)
	if err != nil {
		t.Fatalf("opening test db: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d, path
}

func enqueue(t *testing.T, d *sql.DB, resource string) *Workload {
	t.Helper()
	return enqueueAt(t, d, resource, PriorityDefault)
}

func enqueueAt(t *testing.T, d *sql.DB, resource string, priority int) *Workload {
	t.Helper()
	return enqueueAll(t, d, priority, resource)
}

// enqueueAll enqueues one workload that needs every one of resources.
func enqueueAll(t *testing.T, d *sql.DB, priority int, resources ...string) *Workload {
	t.Helper()
	w, err := Enqueue(d, resources, priority, Meta{Label: "test", PID: 1234})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return w
}

// position is a single-resource workload's place in its queue.
func position(t *testing.T, d *sql.DB, w *Workload) int {
	t.Helper()
	places, err := Positions(d, w)
	if err != nil {
		t.Fatal(err)
	}
	if len(places) != 1 {
		t.Fatalf("Positions(%s) = %+v, want exactly one place", w.ID, places)
	}
	return places[0].Position
}

func mustAcquire(t *testing.T, d *sql.DB, w *Workload) {
	t.Helper()
	ok, _, err := TryAcquire(d, w)
	if err != nil {
		t.Fatalf("TryAcquire(%s): %v", w.ID, err)
	}
	if !ok {
		t.Fatalf("workload %s (seq %d) should have acquired but did not", w.ID, w.Seq)
	}
}

func mustNotAcquire(t *testing.T, d *sql.DB, w *Workload) {
	t.Helper()
	ok, _, err := TryAcquire(d, w)
	if err != nil {
		t.Fatalf("TryAcquire(%s): %v", w.ID, err)
	}
	if ok {
		t.Fatalf("workload %s (seq %d) acquired but should have waited", w.ID, w.Seq)
	}
}

func backdateHeartbeat(t *testing.T, d *sql.DB, w *Workload, age time.Duration) {
	t.Helper()
	old := time.Now().Add(-age).UnixMilli()
	if _, err := d.Exec(`UPDATE workloads SET heartbeat_at = ? WHERE `+groupKey+` = ?`, old, w.ID); err != nil {
		t.Fatalf("backdating heartbeat: %v", err)
	}
}

func TestFirstWorkloadAcquiresImmediately(t *testing.T) {
	d, _ := testDB(t)
	w := enqueue(t, d, "unity")
	mustAcquire(t, d, w)
	if w.State != "running" {
		t.Fatalf("state = %q, want running", w.State)
	}
}

func TestSecondWorkloadWaits(t *testing.T) {
	d, _ := testDB(t)
	a := enqueue(t, d, "unity")
	mustAcquire(t, d, a)
	b := enqueue(t, d, "unity")
	mustNotAcquire(t, d, b)
}

func TestStrictFIFOOrder(t *testing.T) {
	d, _ := testDB(t)
	a := enqueue(t, d, "unity")
	b := enqueue(t, d, "unity")
	c := enqueue(t, d, "unity")

	mustAcquire(t, d, a)
	mustNotAcquire(t, d, b)
	mustNotAcquire(t, d, c)

	if err := Release(d, a, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatal(err)
	}
	mustNotAcquire(t, d, c) // newer workload must not overtake b
	mustAcquire(t, d, b)
	mustNotAcquire(t, d, c)

	if err := Release(d, b, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatal(err)
	}
	mustAcquire(t, d, c)
}

// Arrival order is still absolute within a priority level, which is what this
// and TestStrictFIFOOrder cover: everything here is enqueued at the default.
func TestNewerCannotOvertakeHealthyWaiter(t *testing.T) {
	d, _ := testDB(t)
	a := enqueue(t, d, "unity")
	mustAcquire(t, d, a)
	b := enqueue(t, d, "unity")
	c := enqueue(t, d, "unity")

	if err := Release(d, a, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatal(err)
	}
	// Resource is free; c polls first but must still lose to healthy b.
	mustNotAcquire(t, d, c)
	mustAcquire(t, d, b)
}

func TestDifferentResourcesRunConcurrently(t *testing.T) {
	d, _ := testDB(t)
	a := enqueue(t, d, "unity")
	b := enqueue(t, d, "steam-upload")
	mustAcquire(t, d, a)
	mustAcquire(t, d, b)
}

func TestReleaseAllowsNextWaiter(t *testing.T) {
	d, _ := testDB(t)
	a := enqueue(t, d, "unity")
	mustAcquire(t, d, a)
	b := enqueue(t, d, "unity")
	mustNotAcquire(t, d, b)
	if err := Release(d, a, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatal(err)
	}
	mustAcquire(t, d, b)
}

func TestStaleWaitingWorkloadIsRemoved(t *testing.T) {
	d, _ := testDB(t)
	a := enqueue(t, d, "unity")
	mustAcquire(t, d, a)
	b := enqueue(t, d, "unity")
	c := enqueue(t, d, "unity")
	backdateHeartbeat(t, d, b, StaleThreshold+time.Minute)

	if err := Release(d, a, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatal(err)
	}
	ok, removed, err := TryAcquire(d, c)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("c should acquire after stale b is removed")
	}
	if len(removed) != 1 || removed[0].ID != b.ID || removed[0].State != "waiting" {
		t.Fatalf("removed = %+v, want stale waiting workload %s", removed, b.ID)
	}
}

func TestStaleRunningWorkloadIsRemoved(t *testing.T) {
	d, _ := testDB(t)
	a := enqueue(t, d, "unity")
	mustAcquire(t, d, a)
	b := enqueue(t, d, "unity")
	backdateHeartbeat(t, d, a, StaleThreshold+time.Minute)

	ok, removed, err := TryAcquire(d, b)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("b should acquire after stale running a is removed")
	}
	if len(removed) != 1 || removed[0].ID != a.ID || removed[0].State != "running" {
		t.Fatalf("removed = %+v, want stale running workload %s", removed, a.ID)
	}
}

func TestHealthyHeartbeatPreventsStaleRemoval(t *testing.T) {
	origStale := StaleThreshold
	StaleThreshold = 1 * time.Second
	defer func() { StaleThreshold = origStale }()

	d, _ := testDB(t)
	a := enqueue(t, d, "unity")
	mustAcquire(t, d, a)

	// Heartbeat well within the shortened threshold for twice its length —
	// a long-running healthy workload must survive repeated cleanups.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := Heartbeat(d, a); err != nil {
			t.Fatalf("heartbeat: %v", err)
		}
		if removed, err := CleanupStale(d, "unity"); err != nil {
			t.Fatal(err)
		} else if len(removed) != 0 {
			t.Fatalf("healthy workload removed as stale: %+v", removed)
		}
		time.Sleep(100 * time.Millisecond)
	}
	ws, err := List(d, "unity")
	if err != nil {
		t.Fatal(err)
	}
	if len(ws) != 1 || ws[0].ID != a.ID || ws[0].State != "running" {
		t.Fatalf("workloads = %+v, want healthy running %s", ws, a.ID)
	}
}

func TestValidateResource(t *testing.T) {
	valid := map[string]string{
		"unity":                "unity",
		"Unity":                "unity",
		"UNITY":                "unity",
		"steam-upload":         "steam-upload",
		"asset.import_2":       "asset.import_2",
		"9lives":               "9lives",
		"  build  ":            "build",
		"airport-tycoon-build": "airport-tycoon-build",
	}
	for in, want := range valid {
		got, err := ValidateResource(in)
		if err != nil {
			t.Errorf("ValidateResource(%q) unexpected error: %v", in, err)
		} else if got != want {
			t.Errorf("ValidateResource(%q) = %q, want %q", in, got, want)
		}
	}
	invalid := []string{"", "-unity", ".hidden", "has space", "uni/ty", "uni\\ty", "ünïty",
		"x" + string(make([]byte, 70))}
	for _, in := range invalid {
		if got, err := ValidateResource(in); err == nil {
			t.Errorf("ValidateResource(%q) = %q, want error", in, got)
		}
	}
}

func TestNormalizedNamesShareOneQueue(t *testing.T) {
	d, _ := testDB(t)
	r1, _ := ValidateResource("Unity")
	r2, _ := ValidateResource("UNITY")
	a := enqueue(t, d, r1)
	mustAcquire(t, d, a)
	b := enqueue(t, d, r2)
	mustNotAcquire(t, d, b)
}

// TestConcurrentAcquireProducesSingleOwner simulates independent processes:
// each contender uses its own database connection to the same file and
// races TryAcquire. Exactly one may win, and it must be the oldest.
func TestConcurrentAcquireProducesSingleOwner(t *testing.T) {
	d, path := testDB(t)

	const n = 8
	workloads := make([]*Workload, n)
	for i := range workloads {
		workloads[i] = enqueue(t, d, "unity")
	}

	var mu sync.Mutex
	var winners []string
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(w *Workload) {
			defer wg.Done()
			conn, err := db.Open(path)
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			ok, _, err := TryAcquire(conn, w)
			if err != nil {
				errs <- fmt.Errorf("TryAcquire(%s): %w", w.ID, err)
				return
			}
			if ok {
				mu.Lock()
				winners = append(winners, w.ID)
				mu.Unlock()
			}
		}(workloads[i])
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if len(winners) != 1 {
		t.Fatalf("winners = %v, want exactly one", winners)
	}
	if winners[0] != workloads[0].ID {
		t.Fatalf("winner = %s, want oldest workload %s", winners[0], workloads[0].ID)
	}
	var running int
	if err := d.QueryRow(
		`SELECT COUNT(*) FROM workloads WHERE resource = 'unity' AND state = 'running'`).
		Scan(&running); err != nil {
		t.Fatal(err)
	}
	if running != 1 {
		t.Fatalf("running rows = %d, want 1", running)
	}
}

func TestPositionCountsEarlierWorkloads(t *testing.T) {
	d, _ := testDB(t)
	a := enqueue(t, d, "unity")
	mustAcquire(t, d, a)
	b := enqueue(t, d, "unity")
	c := enqueue(t, d, "unity")

	for w, want := range map[*Workload]int{a: 1, b: 2, c: 3} {
		if got := position(t, d, w); got != want {
			t.Errorf("Position(%s) = %d, want %d", w.ID, got, want)
		}
	}
}

func TestHeartbeatOnRemovedRowReturnsErrGone(t *testing.T) {
	d, _ := testDB(t)
	a := enqueue(t, d, "unity")
	if err := Release(d, a, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatal(err)
	}
	if err := Heartbeat(d, a); err != ErrGone {
		t.Fatalf("Heartbeat after removal = %v, want ErrGone", err)
	}
}

func TestReleaseIsIdempotent(t *testing.T) {
	d, _ := testDB(t)
	a := enqueue(t, d, "unity")
	if err := Release(d, a, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatal(err)
	}
	if err := Release(d, a, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatalf("second release: %v", err)
	}
}

// completions reads the whole ring, oldest first, for readable assertions.
func completions(t *testing.T, d *sql.DB) []Completion {
	t.Helper()
	all, err := RecentCompletions(d, "", 1000)
	if err != nil {
		t.Fatalf("reading completions: %v", err)
	}
	for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
		all[i], all[j] = all[j], all[i]
	}
	return all
}

func TestReleaseRecordsCompletion(t *testing.T) {
	d, _ := testDB(t)
	w, err := Enqueue(d, []string{"gpu"}, PriorityDefault, Meta{
		Label: "build wheels", PID: 4321,
		WorkingDirectory: "/src/proj", RepositoryRoot: "/src/proj", GitBranch: "feature-x",
		CommandDisplay: "make wheels -j8",
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	mustAcquire(t, d, w)
	time.Sleep(20 * time.Millisecond)
	if err := Release(d, w, Outcome{Kind: OutcomeExit, ExitCode: 7}); err != nil {
		t.Fatalf("release: %v", err)
	}

	got := completions(t, d)
	if len(got) != 1 {
		t.Fatalf("completion count = %d, want 1", len(got))
	}
	c := got[0]
	if c.ID != w.ID || c.Resource != "gpu" || c.Label != "build wheels" {
		t.Errorf("identity = %s/%s/%q, want %s/gpu/build wheels", c.ID, c.Resource, c.Label, w.ID)
	}
	if c.Outcome != OutcomeExit || c.ExitCode != 7 {
		t.Errorf("outcome = %s/%d, want exit/7", c.Outcome, c.ExitCode)
	}
	// The paths and branch must come from the deleted row: Enqueue does not
	// populate them on the in-memory Workload, so a completion built from
	// that value would silently record an empty context.
	if c.RepositoryRoot != "/src/proj" || c.WorkingDirectory != "/src/proj" || c.GitBranch != "feature-x" {
		t.Errorf("context = %q/%q/%q, want /src/proj, /src/proj, feature-x",
			c.RepositoryRoot, c.WorkingDirectory, c.GitBranch)
	}
	// The command comes off the deleted row for the same reason, and is what
	// the LAST COMPLETED section shows under a finished entry.
	if c.CommandDisplay != "make wheels -j8" {
		t.Errorf("command = %q, want %q", c.CommandDisplay, "make wheels -j8")
	}
	if c.StartedAt != w.AcquiredAt {
		t.Errorf("started_at = %d, want the acquisition time %d", c.StartedAt, w.AcquiredAt)
	}
	if held := c.FinishedAt - c.StartedAt; held < 10 || held > 5000 {
		t.Errorf("held duration = %dms, want roughly the 20ms the workload ran", held)
	}
	var live int
	if err := d.QueryRow(`SELECT COUNT(*) FROM workloads`).Scan(&live); err != nil {
		t.Fatal(err)
	}
	if live != 0 {
		t.Errorf("workload rows after release = %d, want 0", live)
	}
}

func TestReleaseDoesNotRecordAWorkloadThatNeverAcquired(t *testing.T) {
	d, _ := testDB(t)
	a := enqueue(t, d, "gpu")
	mustAcquire(t, d, a)
	b := enqueue(t, d, "gpu") // queued behind a, never runs
	if err := Release(d, b, Outcome{Kind: OutcomeCanceled}); err != nil {
		t.Fatalf("release: %v", err)
	}
	if got := completions(t, d); len(got) != 0 {
		t.Fatalf("completion count = %d, want 0: a workload that never ran is not a completion", len(got))
	}
}

// TestReleaseRecordsAtMostOnce covers the double-call-safe release path in
// cmdRun: DELETE ... RETURNING finds no row the second time, so there is
// nothing left to record.
func TestReleaseRecordsAtMostOnce(t *testing.T) {
	d, _ := testDB(t)
	a := enqueue(t, d, "gpu")
	mustAcquire(t, d, a)
	for i := 0; i < 2; i++ {
		if err := Release(d, a, Outcome{Kind: OutcomeOK}); err != nil {
			t.Fatalf("release #%d: %v", i+1, err)
		}
	}
	if got := completions(t, d); len(got) != 1 {
		t.Fatalf("completion count = %d, want 1", len(got))
	}
}

func TestStaleRunningWorkloadIsRecordedAsStale(t *testing.T) {
	d, _ := testDB(t)
	a, err := Enqueue(d, []string{"gpu"}, PriorityDefault, Meta{
		Label: "doomed", PID: 1234, CommandDisplay: "sleep 900",
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	mustAcquire(t, d, a)
	backdateHeartbeat(t, d, a, 2*StaleThreshold)
	if _, err := CleanupStale(d, ""); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	got := completions(t, d)
	if len(got) != 1 {
		t.Fatalf("completion count = %d, want 1: a hard-killed workload should leave a trace", len(got))
	}
	if got[0].Outcome != OutcomeStale || got[0].ID != a.ID {
		t.Errorf("completion = %s/%s, want %s/stale", got[0].ID, got[0].Outcome, a.ID)
	}
	// Nobody was there to release this one, so the reclaim path is the only
	// thing that can preserve what it was running.
	if got[0].CommandDisplay != "sleep 900" {
		t.Errorf("reclaimed command = %q, want %q", got[0].CommandDisplay, "sleep 900")
	}
}

func TestStaleWaitingWorkloadIsNotRecorded(t *testing.T) {
	d, _ := testDB(t)
	a := enqueue(t, d, "gpu")
	mustAcquire(t, d, a)
	b := enqueue(t, d, "gpu")
	backdateHeartbeat(t, d, b, 2*StaleThreshold)
	if _, err := CleanupStale(d, ""); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if got := completions(t, d); len(got) != 0 {
		t.Fatalf("completion count = %d, want 0: a stale waiter never ran", len(got))
	}
}

// TestStaleRecordingHappensInsideTryAcquire covers the path that matters for
// cost: stale cleanup also runs inside the acquisition transaction.
func TestStaleRecordingHappensInsideTryAcquire(t *testing.T) {
	d, _ := testDB(t)
	doomed := enqueue(t, d, "gpu")
	mustAcquire(t, d, doomed)
	backdateHeartbeat(t, d, doomed, 2*StaleThreshold)

	next := enqueue(t, d, "gpu")
	mustAcquire(t, d, next) // reclaims the abandoned row on the way in
	got := completions(t, d)
	if len(got) != 1 || got[0].ID != doomed.ID || got[0].Outcome != OutcomeStale {
		t.Fatalf("completions = %+v, want one stale entry for %s", got, doomed.ID)
	}
}

// enqueueOwned enqueues a workload as if pid on host had run it.
func enqueueOwned(t *testing.T, d *sql.DB, resource, host string, pid int) *Workload {
	t.Helper()
	w, err := Enqueue(d, []string{resource}, PriorityDefault, Meta{Label: "owned", PID: pid, Hostname: host})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return w
}

// exitedPids is an exited callback that knows exactly which pids are dead, and
// remembers what it was asked.
type exitedPids struct {
	dead  map[int]bool
	asked []int
}

func (e *exitedPids) exited(pid int, _ time.Time) bool {
	e.asked = append(e.asked, pid)
	return e.dead[pid]
}

func TestCleanupAbandonedRemovesAStaleRowWhoseOwnerExited(t *testing.T) {
	d, _ := testDB(t)
	a := enqueueOwned(t, d, "gpu", "here", 100)
	mustAcquire(t, d, a)
	backdateHeartbeat(t, d, a, 2*StaleThreshold)

	e := &exitedPids{dead: map[int]bool{100: true}}
	removed, err := CleanupAbandoned(d, "", "here", e.exited)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0].ID != a.ID || removed[0].State != "running" {
		t.Fatalf("removed = %+v, want running workload %s", removed, a.ID)
	}
	if ws, _ := List(d, ""); len(ws) != 0 {
		t.Fatalf("workloads remain: %+v", ws)
	}
	// Moved, not just deleted: this is the whole point for a view that shows
	// what finished.
	got := completions(t, d)
	if len(got) != 1 || got[0].ID != a.ID || got[0].Outcome != OutcomeStale {
		t.Fatalf("completions = %+v, want one stale entry for %s", got, a.ID)
	}
}

// The case CleanupAbandoned exists to refuse. After a machine sleep every
// heartbeat is stale at once; an owner that is still alive is only late.
func TestCleanupAbandonedKeepsAStaleRowWhoseOwnerIsAlive(t *testing.T) {
	d, _ := testDB(t)
	a := enqueueOwned(t, d, "gpu", "here", 100)
	mustAcquire(t, d, a)
	backdateHeartbeat(t, d, a, 2*StaleThreshold)

	e := &exitedPids{}
	removed, err := CleanupAbandoned(d, "", "here", e.exited)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 0 {
		t.Fatalf("removed %+v, but its owner is alive", removed)
	}
	if len(e.asked) != 1 || e.asked[0] != 100 {
		t.Errorf("asked about pids %v, want [100]", e.asked)
	}
	if got := completions(t, d); len(got) != 0 {
		t.Fatalf("completions = %+v, want none", got)
	}
}

// A dead owner is not enough on its own: the heartbeat threshold still decides
// when, so this path cannot reclaim anything sooner than every other one does.
func TestCleanupAbandonedIgnoresAFreshHeartbeat(t *testing.T) {
	d, _ := testDB(t)
	a := enqueueOwned(t, d, "gpu", "here", 100)
	mustAcquire(t, d, a)

	e := &exitedPids{dead: map[int]bool{100: true}}
	removed, err := CleanupAbandoned(d, "", "here", e.exited)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 0 {
		t.Fatalf("removed %+v with a fresh heartbeat", removed)
	}
	if len(e.asked) != 0 {
		t.Errorf("asked about pids %v for a healthy row", e.asked)
	}
}

// A pid recorded on another machine names nothing here, and neither does a row
// whose host was never recorded.
func TestCleanupAbandonedJudgesOnlyThisHost(t *testing.T) {
	d, _ := testDB(t)
	other := enqueueOwned(t, d, "gpu", "elsewhere", 100)
	unknown := enqueueOwned(t, d, "cpu", "", 100)
	for _, w := range []*Workload{other, unknown} {
		mustAcquire(t, d, w)
		backdateHeartbeat(t, d, w, 2*StaleThreshold)
	}

	e := &exitedPids{dead: map[int]bool{100: true}}
	for _, host := range []string{"here", ""} {
		removed, err := CleanupAbandoned(d, "", host, e.exited)
		if err != nil {
			t.Fatal(err)
		}
		if len(removed) != 0 {
			t.Fatalf("host %q: removed %+v", host, removed)
		}
	}
	if len(e.asked) != 0 {
		t.Errorf("asked about pids %v recorded on no host of ours", e.asked)
	}
}

func TestCleanupAbandonedIsScopedToTheResource(t *testing.T) {
	d, _ := testDB(t)
	a := enqueueOwned(t, d, "gpu", "here", 100)
	b := enqueueOwned(t, d, "cpu", "here", 200)
	for _, w := range []*Workload{a, b} {
		mustAcquire(t, d, w)
		backdateHeartbeat(t, d, w, 2*StaleThreshold)
	}

	e := &exitedPids{dead: map[int]bool{100: true, 200: true}}
	removed, err := CleanupAbandoned(d, "gpu", "here", e.exited)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0].ID != a.ID {
		t.Fatalf("removed = %+v, want only %s", removed, a.ID)
	}
	if ws, _ := List(d, "cpu"); len(ws) != 1 || ws[0].ID != b.ID {
		t.Fatalf("cpu workloads = %+v, want %s untouched", ws, b.ID)
	}
}

// A waiter whose owner died never ran: removed, like any stale waiter, and not
// recorded, like any stale waiter.
func TestCleanupAbandonedDoesNotRecordAWaiter(t *testing.T) {
	d, _ := testDB(t)
	a := enqueueOwned(t, d, "gpu", "here", 100)
	mustAcquire(t, d, a)
	b := enqueueOwned(t, d, "gpu", "here", 200)
	backdateHeartbeat(t, d, b, 2*StaleThreshold)

	e := &exitedPids{dead: map[int]bool{200: true}}
	removed, err := CleanupAbandoned(d, "gpu", "here", e.exited)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0].ID != b.ID || removed[0].State != "waiting" {
		t.Fatalf("removed = %+v, want waiting workload %s", removed, b.ID)
	}
	if got := completions(t, d); len(got) != 0 {
		t.Fatalf("completions = %+v, want none: a stale waiter never ran", got)
	}
}

func TestCompletionsRingIsBoundedPerResource(t *testing.T) {
	d, _ := testDB(t)
	// A quiet resource, written first: a busy one must not evict it.
	quiet := enqueue(t, d, "steam-upload")
	mustAcquire(t, d, quiet)
	if err := Release(d, quiet, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatalf("release: %v", err)
	}

	var ids []string
	for i := 0; i < 2*CompletionsPerResource; i++ {
		w := enqueue(t, d, "gpu")
		mustAcquire(t, d, w)
		if err := Release(d, w, Outcome{Kind: OutcomeOK}); err != nil {
			t.Fatalf("release #%d: %v", i, err)
		}
		ids = append(ids, w.ID)
	}

	gpu, err := RecentCompletions(d, "gpu", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(gpu) != CompletionsPerResource {
		t.Fatalf("gpu completions = %d, want the cap of %d", len(gpu), CompletionsPerResource)
	}
	// Newest first, and the newest are the ones kept.
	for i, c := range gpu {
		if want := ids[len(ids)-1-i]; c.ID != want {
			t.Fatalf("gpu completion %d = %s, want %s (newest first, oldest evicted)", i, c.ID, want)
		}
	}
	other, err := RecentCompletions(d, "steam-upload", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(other) != 1 {
		t.Fatalf("steam-upload completions = %d, want 1: the count cap is per resource", len(other))
	}
}

func TestCompletionsAgeCapDropsOldRows(t *testing.T) {
	d, _ := testDB(t)
	old := enqueue(t, d, "gpu")
	mustAcquire(t, d, old)
	if err := Release(d, old, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, err := d.Exec(`UPDATE completions SET finished_at = ?`,
		time.Now().Add(-2*CompletionRetention).UnixMilli()); err != nil {
		t.Fatalf("ageing the row: %v", err)
	}
	// The sweep runs when the next completion is written, on any resource.
	fresh := enqueue(t, d, "rig")
	mustAcquire(t, d, fresh)
	if err := Release(d, fresh, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatalf("release: %v", err)
	}
	got := completions(t, d)
	if len(got) != 1 || got[0].ID != fresh.ID {
		t.Fatalf("completions = %+v, want only the fresh %s", got, fresh.ID)
	}
}

func TestRecentCompletionsOrderAndScope(t *testing.T) {
	d, _ := testDB(t)
	var gpu []string
	for _, res := range []string{"gpu", "rig", "gpu", "gpu"} {
		w := enqueue(t, d, res)
		mustAcquire(t, d, w)
		if err := Release(d, w, Outcome{Kind: OutcomeOK}); err != nil {
			t.Fatalf("release: %v", err)
		}
		if res == "gpu" {
			gpu = append(gpu, w.ID)
		}
	}
	all, err := RecentCompletions(d, "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("limit not honoured: got %d rows, want 2", len(all))
	}
	if all[0].ID != gpu[len(gpu)-1] {
		t.Errorf("first row = %s, want the newest completion %s", all[0].ID, gpu[len(gpu)-1])
	}
	scoped, err := RecentCompletions(d, "gpu", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(scoped) != len(gpu) {
		t.Fatalf("scoped completions = %d, want %d", len(scoped), len(gpu))
	}
	for _, c := range scoped {
		if c.Resource != "gpu" {
			t.Errorf("scoped read returned a %q row", c.Resource)
		}
	}
	if n, err := RecentCompletions(d, "", 0); err != nil || n != nil {
		t.Errorf("RecentCompletions(limit 0) = %v, %v; want nil, nil", n, err)
	}
}
