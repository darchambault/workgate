package queue

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

// rowsOf returns a workload's rows as (resource, seq, priority, state), in seq
// order, read straight off the table.
func rowsOf(t *testing.T, d *sql.DB, id string) []struct {
	resource, state string
	seq             int64
	priority        int
} {
	t.Helper()
	rows, err := d.Query(`SELECT resource, state, seq, priority FROM workloads
	                       WHERE `+groupKey+` = ? ORDER BY seq`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []struct {
		resource, state string
		seq             int64
		priority        int
	}
	for rows.Next() {
		var r struct {
			resource, state string
			seq             int64
			priority        int
		}
		if err := rows.Scan(&r.resource, &r.state, &r.seq, &r.priority); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}

func TestValidateResources(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    []string
		wantErr string
	}{
		{in: "gpu", want: []string{"gpu"}},
		{in: "MyProject,GPU", want: []string{"myproject", "gpu"}},
		{in: " a , b ", want: []string{"a", "b"}},
		{in: "a,b,c,d", want: []string{"a", "b", "c", "d"}},
		{in: "a,b,c,d,e", wantErr: "at most 4"},
		{in: "gpu,GPU", wantErr: "named twice"},
		{in: "a,,b", wantErr: "empty resource name"},
		{in: "a,", wantErr: "empty resource name"},
		{in: ",a", wantErr: "empty resource name"},
		{in: "a,bad!name", wantErr: "invalid resource name"},
		{in: "", wantErr: "empty"},
	} {
		got, err := ValidateResources(tc.in)
		if tc.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("ValidateResources(%q) error = %v, want one containing %q", tc.in, err, tc.wantErr)
			}
			continue
		}
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("ValidateResources(%q) = %v, %v; want %v", tc.in, got, err, tc.want)
		}
	}
}

// TestEnqueueWritesOneRowPerResource pins the shape every rank comparison
// relies on: consecutive seqs, one workload id, one level.
func TestEnqueueWritesOneRowPerResource(t *testing.T) {
	d, _ := testDB(t)
	w := enqueueAll(t, d, 2, "proj", "gpu", "seat")
	if !reflect.DeepEqual(w.Resources, []string{"proj", "gpu", "seat"}) || w.Resource != "proj" {
		t.Errorf("workload resources = %s / %v", w.Resource, w.Resources)
	}
	rows := rowsOf(t, d, w.ID)
	if len(rows) != 3 {
		t.Fatalf("rows = %+v, want 3", rows)
	}
	for i, r := range rows {
		if r.seq != w.Seq+int64(i) || r.priority != 2 || r.state != "waiting" || r.resource != w.Resources[i] {
			t.Errorf("row %d = %+v, want seq %d, priority 2, waiting, %s", i, r, w.Seq+int64(i), w.Resources[i])
		}
	}
}

// TestMultiResourceAcquiresAllOrNothing is the property that rules out
// deadlock: a waiter holds none of its resources until it can hold them all.
func TestMultiResourceAcquiresAllOrNothing(t *testing.T) {
	d, _ := testDB(t)
	holder := enqueue(t, d, "proj")
	mustAcquire(t, d, holder)

	multi := enqueueAll(t, d, PriorityDefault, "proj", "gpu")
	mustNotAcquire(t, d, multi)
	for _, r := range rowsOf(t, d, multi.ID) {
		if r.state != "waiting" {
			t.Fatalf("%s row is %s while proj is held elsewhere", r.resource, r.state)
		}
	}

	if err := Release(d, holder, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatal(err)
	}
	mustAcquire(t, d, multi)
	for _, r := range rowsOf(t, d, multi.ID) {
		if r.state != "running" {
			t.Errorf("%s row is %s after acquiring", r.resource, r.state)
		}
	}
	// It now blocks both of its resources.
	mustNotAcquire(t, d, enqueue(t, d, "proj"))
	mustNotAcquire(t, d, enqueue(t, d, "gpu"))
}

// TestMultiResourceWaiterHoldsItsPlaceOnAnIdleResource is strict head-of-line:
// gpu is free, but a later gpu-only workload may not take it while an earlier
// workload waits for gpu and proj together.
func TestMultiResourceWaiterHoldsItsPlaceOnAnIdleResource(t *testing.T) {
	d, _ := testDB(t)
	holder := enqueue(t, d, "proj")
	mustAcquire(t, d, holder)
	multi := enqueueAll(t, d, PriorityDefault, "proj", "gpu")
	mustNotAcquire(t, d, multi)

	later := enqueue(t, d, "gpu")
	mustNotAcquire(t, d, later)

	if err := Release(d, holder, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatal(err)
	}
	mustNotAcquire(t, d, later) // still behind multi, which goes first
	mustAcquire(t, d, multi)
	if err := Release(d, multi, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatal(err)
	}
	mustAcquire(t, d, later)
}

// TestHigherPriorityMayOvertakeAMultiResourceWaiter: head-of-line is by rank,
// not arrival, exactly as for one resource.
func TestHigherPriorityMayOvertakeAMultiResourceWaiter(t *testing.T) {
	d, _ := testDB(t)
	holder := enqueue(t, d, "proj")
	mustAcquire(t, d, holder)
	multi := enqueueAll(t, d, PriorityDefault, "proj", "gpu")
	urgent := enqueueAt(t, d, "gpu", PriorityHighest)
	mustNotAcquire(t, d, multi)
	mustAcquire(t, d, urgent)
}

// TestCrossedMultiResourceWorkloadsDoNotDeadlock is the textbook deadlock -
// one workload wanting a then b, another b then a - which nesting runs would
// produce. Whatever order they name their resources in, one rank orders them.
func TestCrossedMultiResourceWorkloadsDoNotDeadlock(t *testing.T) {
	d, _ := testDB(t)
	holder := enqueueAll(t, d, PriorityDefault, "a", "b")
	mustAcquire(t, d, holder)
	ab := enqueueAll(t, d, PriorityDefault, "a", "b")
	ba := enqueueAll(t, d, PriorityDefault, "b", "a")
	mustNotAcquire(t, d, ab)
	mustNotAcquire(t, d, ba)

	if err := Release(d, holder, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatal(err)
	}
	// The later one sees the earlier one ahead on both resources ...
	mustNotAcquire(t, d, ba)
	// ... and the earlier one sees nothing ahead on either.
	mustAcquire(t, d, ab)
	if err := Release(d, ab, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatal(err)
	}
	mustAcquire(t, d, ba)
}

func TestPositionsReportEveryQueue(t *testing.T) {
	d, _ := testDB(t)
	holder := enqueue(t, d, "proj")
	mustAcquire(t, d, holder)
	enqueue(t, d, "proj")
	multi := enqueueAll(t, d, PriorityDefault, "proj", "gpu")

	places, err := Positions(d, multi)
	if err != nil {
		t.Fatal(err)
	}
	if want := []Place{{"proj", 3}, {"gpu", 1}}; !reflect.DeepEqual(places, want) {
		t.Errorf("places = %+v, want %+v", places, want)
	}
}

func TestReleaseFreesEveryResourceAndRecordsEach(t *testing.T) {
	d, _ := testDB(t)
	multi := enqueueAll(t, d, PriorityDefault, "proj", "gpu")
	mustAcquire(t, d, multi)
	if err := Release(d, multi, Outcome{Kind: OutcomeExit, ExitCode: 3}); err != nil {
		t.Fatal(err)
	}
	if rows := rowsOf(t, d, multi.ID); len(rows) != 0 {
		t.Fatalf("rows left after release: %+v", rows)
	}
	for _, r := range []string{"proj", "gpu"} {
		cs, err := RecentCompletions(d, r, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(cs) != 1 || cs[0].ID != multi.ID || cs[0].Resource != r ||
			cs[0].Outcome != OutcomeExit || cs[0].ExitCode != 3 ||
			!reflect.DeepEqual(cs[0].Resources, []string{"proj", "gpu"}) {
			t.Errorf("completions on %s = %+v", r, cs)
		}
	}
	// Unscoped, one finish is one entry.
	all, err := RecentCompletions(d, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || !reflect.DeepEqual(all[0].Resources, []string{"proj", "gpu"}) {
		t.Errorf("unscoped completions = %+v, want the workload once", all)
	}
	mustAcquire(t, d, enqueue(t, d, "proj"))
	mustAcquire(t, d, enqueue(t, d, "gpu"))
}

func TestReleaseOfAMultiResourceWaiterRecordsNothing(t *testing.T) {
	d, _ := testDB(t)
	holder := enqueue(t, d, "gpu")
	mustAcquire(t, d, holder)
	multi := enqueueAll(t, d, PriorityDefault, "proj", "gpu")
	if err := Release(d, multi, Outcome{Kind: OutcomeCanceled}); err != nil {
		t.Fatal(err)
	}
	if rows := rowsOf(t, d, multi.ID); len(rows) != 0 {
		t.Fatalf("rows left after release: %+v", rows)
	}
	if cs := completions(t, d); len(cs) != 0 {
		t.Errorf("a workload that never ran was recorded: %+v", cs)
	}
}

// TestStaleMultiResourceWorkloadIsReclaimedWhole: reclaiming through one of its
// resources must free the others too, and leave a trace on each.
func TestStaleMultiResourceWorkloadIsReclaimedWhole(t *testing.T) {
	d, _ := testDB(t)
	multi := enqueueAll(t, d, PriorityDefault, "proj", "gpu")
	mustAcquire(t, d, multi)
	backdateHeartbeat(t, d, multi, StaleThreshold+time.Minute)

	next := enqueue(t, d, "gpu")
	ok, removed, err := TryAcquire(d, next)
	if err != nil || !ok {
		t.Fatalf("TryAcquire after stale owner = %v, %v", ok, err)
	}
	if len(removed) != 2 || removed[0].ID != multi.ID || removed[1].ID != multi.ID {
		t.Errorf("removed = %+v, want both rows reported under %s", removed, multi.ID)
	}
	if rows := rowsOf(t, d, multi.ID); len(rows) != 0 {
		t.Fatalf("rows left after reclaim: %+v", rows)
	}
	mustAcquire(t, d, enqueue(t, d, "proj"))
	for _, r := range []string{"proj", "gpu"} {
		cs, err := RecentCompletions(d, r, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(cs) != 1 || cs[0].ID != multi.ID || cs[0].Outcome != OutcomeStale {
			t.Errorf("completions on %s = %+v, want one stale entry for %s", r, cs, multi.ID)
		}
	}
}

func TestCleanupAbandonedReclaimsAMultiResourceWorkloadWhole(t *testing.T) {
	d, _ := testDB(t)
	w, err := Enqueue(d, []string{"proj", "gpu"}, PriorityDefault, Meta{Label: "owned", PID: 4242, Hostname: "here"})
	if err != nil {
		t.Fatal(err)
	}
	mustAcquire(t, d, w)
	backdateHeartbeat(t, d, w, StaleThreshold+time.Minute)

	e := &exitedPids{dead: map[int]bool{4242: true}}
	removed, err := CleanupAbandoned(d, "gpu", "here", e.exited)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 2 {
		t.Errorf("removed = %+v, want both rows", removed)
	}
	if len(e.asked) != 1 {
		t.Errorf("owner judged %d times, want once", len(e.asked))
	}
	if rows := rowsOf(t, d, w.ID); len(rows) != 0 {
		t.Fatalf("rows left after reclaim: %+v", rows)
	}
}

// TestPartialWorkloadIsGone covers the mixed-version case: an older binary
// reclaims one resource at a time, and a workload that has lost any of its
// rows must not go on to hold the rest.
func TestPartialWorkloadIsGone(t *testing.T) {
	d, _ := testDB(t)
	multi := enqueueAll(t, d, PriorityDefault, "proj", "gpu")
	if _, err := d.Exec(`DELETE FROM workloads WHERE resource = 'gpu'`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := TryAcquire(d, multi); !errors.Is(err, ErrGone) {
		t.Errorf("TryAcquire on a partial workload: %v, want ErrGone", err)
	}
	if err := Heartbeat(d, multi); !errors.Is(err, ErrGone) {
		t.Errorf("Heartbeat on a partial workload: %v, want ErrGone", err)
	}
	// Releasing what is left still cleans up.
	if err := Release(d, multi, Outcome{Kind: OutcomeCanceled}); err != nil {
		t.Fatal(err)
	}
	if rows := rowsOf(t, d, multi.ID); len(rows) != 0 {
		t.Errorf("rows left after release: %+v", rows)
	}
}

func TestHeartbeatRefreshesEveryRow(t *testing.T) {
	d, _ := testDB(t)
	multi := enqueueAll(t, d, PriorityDefault, "proj", "gpu")
	backdateHeartbeat(t, d, multi, time.Hour)
	if err := Heartbeat(d, multi); err != nil {
		t.Fatal(err)
	}
	var stale int
	if err := d.QueryRow(`SELECT COUNT(*) FROM workloads WHERE heartbeat_at < ?`,
		time.Now().Add(-time.Minute).UnixMilli()).Scan(&stale); err != nil {
		t.Fatal(err)
	}
	if stale != 0 {
		t.Errorf("%d rows left stale after a heartbeat", stale)
	}
}

func TestSetPriorityMovesTheWholeWorkload(t *testing.T) {
	d, _ := testDB(t)
	holder := enqueue(t, d, "gpu")
	mustAcquire(t, d, holder)
	enqueue(t, d, "proj")
	enqueue(t, d, "gpu")
	multi := enqueueAll(t, d, PriorityDefault, "proj", "gpu")

	ch, err := SetPriority(d, multi.ID, PriorityHighest)
	if err != nil {
		t.Fatal(err)
	}
	if want := []Place{{"proj", 1}, {"gpu", 2}}; !reflect.DeepEqual(ch.Places, want) {
		t.Errorf("places = %+v, want %+v", ch.Places, want)
	}
	for _, r := range rowsOf(t, d, multi.ID) {
		if r.priority != PriorityHighest {
			t.Errorf("%s row priority = %d, want %d", r.resource, r.priority, PriorityHighest)
		}
	}
}

// TestDivergentPrioritiesAreAligned: only an older binary, re-prioritizing one
// row by its own id, can split a workload's levels. The next acquisition
// attempt puts every row back on the first row's level.
func TestDivergentPrioritiesAreAligned(t *testing.T) {
	d, _ := testDB(t)
	holder := enqueue(t, d, "proj")
	mustAcquire(t, d, holder)
	multi := enqueueAll(t, d, 2, "proj", "gpu")
	if _, err := d.Exec(`UPDATE workloads SET priority = 5 WHERE resource = 'gpu' AND id <> ?`, multi.ID); err != nil {
		t.Fatal(err)
	}
	mustNotAcquire(t, d, multi)
	for _, r := range rowsOf(t, d, multi.ID) {
		if r.priority != 2 {
			t.Errorf("%s row priority = %d, want 2", r.resource, r.priority)
		}
	}
}

func TestListCarriesEveryResource(t *testing.T) {
	d, _ := testDB(t)
	enqueue(t, d, "gpu")
	multi := enqueueAll(t, d, PriorityDefault, "proj", "gpu")

	ws, err := List(d, "gpu")
	if err != nil {
		t.Fatal(err)
	}
	if len(ws) != 2 {
		t.Fatalf("List(gpu) = %+v", ws)
	}
	got := ws[1]
	if got.ID != multi.ID || got.Resource != "gpu" || !reflect.DeepEqual(got.Resources, []string{"proj", "gpu"}) {
		t.Errorf("multi-resource row = %s %s %v", got.ID, got.Resource, got.Resources)
	}
	if !reflect.DeepEqual(ws[0].Resources, []string{"gpu"}) {
		t.Errorf("single-resource row resources = %v", ws[0].Resources)
	}

	all, err := List(d, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Errorf("List() = %d rows, want one per resource row (3)", len(all))
	}
}

// TestRowFromAnOlderBinaryIsAWorkloadOfOne: no group_id, as an INSERT that
// predates the column writes it.
func TestRowFromAnOlderBinaryIsAWorkloadOfOne(t *testing.T) {
	d, _ := testDB(t)
	now := time.Now().UnixMilli()
	if _, err := d.Exec(`INSERT INTO workloads (id, resource, state, created_at, heartbeat_at)
	                     VALUES ('0ld000', 'gpu', 'waiting', ?, ?)`, now, now); err != nil {
		t.Fatal(err)
	}
	old := &Workload{ID: "0ld000", Resource: "gpu"}
	multi := enqueueAll(t, d, PriorityDefault, "proj", "gpu")

	ws, err := List(d, "gpu")
	if err != nil {
		t.Fatal(err)
	}
	if ws[0].ID != "0ld000" || !reflect.DeepEqual(ws[0].Resources, []string{"gpu"}) {
		t.Errorf("old row listed as %s %v", ws[0].ID, ws[0].Resources)
	}
	mustNotAcquire(t, d, multi) // the old row arrived first
	mustAcquire(t, d, old)
	if err := Heartbeat(d, old); err != nil {
		t.Errorf("Heartbeat on an old row: %v", err)
	}
	if _, err := SetPriority(d, "0ld000", 1); err != nil {
		t.Errorf("SetPriority on an old row: %v", err)
	}
	if err := Release(d, old, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatal(err)
	}
	mustAcquire(t, d, multi)
}
