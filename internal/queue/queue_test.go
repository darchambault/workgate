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
	w, err := Enqueue(d, resource, Meta{Label: "test", PID: 1234})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return w
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
	if _, err := d.Exec(`UPDATE workloads SET heartbeat_at = ? WHERE id = ?`, old, w.ID); err != nil {
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

	if err := Release(d, a); err != nil {
		t.Fatal(err)
	}
	mustNotAcquire(t, d, c) // newer workload must not overtake b
	mustAcquire(t, d, b)
	mustNotAcquire(t, d, c)

	if err := Release(d, b); err != nil {
		t.Fatal(err)
	}
	mustAcquire(t, d, c)
}

func TestNewerCannotOvertakeHealthyWaiter(t *testing.T) {
	d, _ := testDB(t)
	a := enqueue(t, d, "unity")
	mustAcquire(t, d, a)
	b := enqueue(t, d, "unity")
	c := enqueue(t, d, "unity")

	if err := Release(d, a); err != nil {
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
	if err := Release(d, a); err != nil {
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

	if err := Release(d, a); err != nil {
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
		got, err := Position(d, w)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("Position(%s) = %d, want %d", w.ID, got, want)
		}
	}
}

func TestHeartbeatOnRemovedRowReturnsErrGone(t *testing.T) {
	d, _ := testDB(t)
	a := enqueue(t, d, "unity")
	if err := Release(d, a); err != nil {
		t.Fatal(err)
	}
	if err := Heartbeat(d, a); err != ErrGone {
		t.Fatalf("Heartbeat after removal = %v, want ErrGone", err)
	}
}

func TestReleaseIsIdempotent(t *testing.T) {
	d, _ := testDB(t)
	a := enqueue(t, d, "unity")
	if err := Release(d, a); err != nil {
		t.Fatal(err)
	}
	if err := Release(d, a); err != nil {
		t.Fatalf("second release: %v", err)
	}
}
