package queue

import (
	"errors"
	"strings"
	"sync"
	"testing"
)

// TestHigherPriorityOvertakesWaitingWorkload is the whole point of the feature:
// arrival order no longer settles the queue on its own.
func TestHigherPriorityOvertakesWaitingWorkload(t *testing.T) {
	d, _ := testDB(t)
	a := enqueue(t, d, "unity")
	mustAcquire(t, d, a)
	b := enqueue(t, d, "unity")                    // default, arrived first
	c := enqueueAt(t, d, "unity", PriorityHighest) // urgent, arrived second

	if err := Release(d, a, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatal(err)
	}
	mustNotAcquire(t, d, b)
	mustAcquire(t, d, c)
}

// TestLowerPriorityYieldsToALaterDefaultWorkload is the same rule read from the
// other end, and the more surprising direction: deferring a workload lets
// ordinary work that has not even been queued yet go ahead of it.
func TestLowerPriorityYieldsToALaterDefaultWorkload(t *testing.T) {
	d, _ := testDB(t)
	holder := enqueue(t, d, "unity")
	mustAcquire(t, d, holder)
	deferred := enqueueAt(t, d, "unity", PriorityLowest)
	ordinary := enqueue(t, d, "unity")

	if err := Release(d, holder, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatal(err)
	}
	mustNotAcquire(t, d, deferred)
	mustAcquire(t, d, ordinary)
}

// TestEqualPriorityKeepsArrivalOrder pins the tiebreak. Priority reorders
// levels against each other and nothing else: within one level the queue is
// exactly as strict as it was before priorities existed.
func TestEqualPriorityKeepsArrivalOrder(t *testing.T) {
	d, _ := testDB(t)
	holder := enqueue(t, d, "unity")
	mustAcquire(t, d, holder)
	first := enqueueAt(t, d, "unity", PriorityHighest)
	second := enqueueAt(t, d, "unity", PriorityHighest)

	if err := Release(d, holder, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatal(err)
	}
	mustNotAcquire(t, d, second)
	mustAcquire(t, d, first)
}

// TestTryAcquireReadsTheStoredPriorityNotTheCallersCopy is the invariant the
// whole on-the-fly change rests on. The waiting process holds a Workload value
// that still says PriorityDefault; only the row knows better, and acquisition
// must believe the row. Without this, a priority change would need signalling.
func TestTryAcquireReadsTheStoredPriorityNotTheCallersCopy(t *testing.T) {
	d, _ := testDB(t)
	holder := enqueue(t, d, "unity")
	mustAcquire(t, d, holder)
	first := enqueue(t, d, "unity")
	second := enqueue(t, d, "unity")

	if _, err := SetPriority(d, second.ID, PriorityHighest); err != nil {
		t.Fatalf("SetPriority: %v", err)
	}
	// The in-memory copy is deliberately left stale, as a waiting process's
	// would be: it has no way to learn of the change except by re-reading.
	if second.Priority != PriorityDefault {
		t.Fatalf("test setup: cached priority = %d, want it left stale at %d",
			second.Priority, PriorityDefault)
	}

	if err := Release(d, holder, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatal(err)
	}
	mustNotAcquire(t, d, first)
	mustAcquire(t, d, second)
	// Acquiring refreshes the caller's copy, so later reporting is accurate.
	if second.Priority != PriorityHighest {
		t.Errorf("priority after acquiring = %d, want %d", second.Priority, PriorityHighest)
	}
}

// TestConcurrentAcquireRespectsPriority repeats the single-owner race with one
// urgent contender. Priority changes who wins; it must not weaken the
// guarantee that exactly one does.
func TestConcurrentAcquireRespectsPriority(t *testing.T) {
	d, _ := testDB(t)
	const contenders = 8
	const urgent = 5 // neither first nor last in arrival order

	workloads := make([]*Workload, contenders)
	for i := range workloads {
		if i == urgent {
			workloads[i] = enqueueAt(t, d, "unity", PriorityHighest)
			continue
		}
		workloads[i] = enqueue(t, d, "unity")
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var winners []string
	for _, w := range workloads {
		wg.Add(1)
		go func(w *Workload) {
			defer wg.Done()
			ok, _, err := TryAcquire(d, w)
			if err != nil {
				t.Errorf("TryAcquire(%s): %v", w.ID, err)
				return
			}
			if ok {
				mu.Lock()
				winners = append(winners, w.ID)
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	if len(winners) != 1 {
		t.Fatalf("winners = %v, want exactly one", winners)
	}
	if winners[0] != workloads[urgent].ID {
		t.Fatalf("winner = %s, want the urgent workload %s", winners[0], workloads[urgent].ID)
	}
}

// TestPositionCountsTheRunningWorkloadAhead guards the trap in ranking by
// (priority, seq) alone: an urgent waiter sorts above a running default one,
// and would be told it is next while it is in fact blocked behind it.
func TestPositionCountsTheRunningWorkloadAhead(t *testing.T) {
	d, _ := testDB(t)
	running := enqueue(t, d, "unity")
	mustAcquire(t, d, running)
	waiting := enqueue(t, d, "unity")
	urgent := enqueueAt(t, d, "unity", PriorityHighest)

	for w, want := range map[*Workload]int{running: 1, urgent: 2, waiting: 3} {
		got, err := Position(d, w)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("Position(%s) = %d, want %d", w.ID, got, want)
		}
	}
}

func TestSetPriorityReordersWaitingQueue(t *testing.T) {
	d, _ := testDB(t)
	holder := enqueue(t, d, "unity")
	mustAcquire(t, d, holder)
	first := enqueue(t, d, "unity")
	second := enqueue(t, d, "unity")

	ch, err := SetPriority(d, second.ID, PriorityHighest)
	if err != nil {
		t.Fatalf("SetPriority: %v", err)
	}
	if ch.From != PriorityDefault || ch.To != PriorityHighest {
		t.Errorf("change = %d -> %d, want %d -> %d", ch.From, ch.To, PriorityDefault, PriorityHighest)
	}
	// Position 2: behind the holder, ahead of the waiter it just overtook.
	if ch.Position != 2 {
		t.Errorf("position = %d, want 2", ch.Position)
	}
	if ch.Resource != "unity" || ch.Label != "test" || ch.State != "waiting" {
		t.Errorf("context = %s/%q/%s, want unity/test/waiting", ch.Resource, ch.Label, ch.State)
	}

	if err := Release(d, holder, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatal(err)
	}
	mustNotAcquire(t, d, first)
	mustAcquire(t, d, second)
}

// TestSetPriorityLoweringYieldsTheQueue covers the other direction: standing
// aside is as valid a use of the command as pushing ahead.
func TestSetPriorityLoweringYieldsTheQueue(t *testing.T) {
	d, _ := testDB(t)
	holder := enqueue(t, d, "unity")
	mustAcquire(t, d, holder)
	first := enqueue(t, d, "unity")
	second := enqueue(t, d, "unity")

	if _, err := SetPriority(d, first.ID, PriorityLowest); err != nil {
		t.Fatalf("SetPriority: %v", err)
	}
	if err := Release(d, holder, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatal(err)
	}
	mustNotAcquire(t, d, first)
	mustAcquire(t, d, second)
}

// TestSetPriorityOnRunningWorkloadChangesNoOrder documents the deliberate
// no-op. It is accepted rather than refused because a row can go from waiting
// to running between reading status and typing the id, and failing on that
// race would be worse than doing nothing visible.
func TestSetPriorityOnRunningWorkloadChangesNoOrder(t *testing.T) {
	d, _ := testDB(t)
	holder := enqueue(t, d, "unity")
	mustAcquire(t, d, holder)
	waiter := enqueue(t, d, "unity")

	ch, err := SetPriority(d, holder.ID, PriorityLowest)
	if err != nil {
		t.Fatalf("SetPriority: %v", err)
	}
	if ch.State != "running" {
		t.Errorf("state = %q, want running", ch.State)
	}
	if ch.Position != 1 {
		t.Errorf("position = %d, want 1 (a running workload is never preempted)", ch.Position)
	}
	// The waiter must not be promoted past a holder that merely lowered itself.
	mustNotAcquire(t, d, waiter)
}

// TestSetPriorityLeavesHeartbeatUntouched pins a subtle safety property:
// re-prioritizing is not a sign of life from the row's owner, so it must not
// rescue an abandoned workload from stale cleanup.
func TestSetPriorityLeavesHeartbeatUntouched(t *testing.T) {
	d, _ := testDB(t)
	holder := enqueue(t, d, "unity")
	mustAcquire(t, d, holder)
	abandoned := enqueue(t, d, "unity")
	backdateHeartbeat(t, d, abandoned, 2*StaleThreshold)

	var before int64
	if err := d.QueryRow(`SELECT heartbeat_at FROM workloads WHERE id = ?`, abandoned.ID).
		Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err := SetPriority(d, abandoned.ID, PriorityHighest); err != nil {
		t.Fatalf("SetPriority: %v", err)
	}
	var after int64
	if err := d.QueryRow(`SELECT heartbeat_at FROM workloads WHERE id = ?`, abandoned.ID).
		Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Errorf("heartbeat_at = %d, want it left at %d", after, before)
	}
	// And it is still reaped, urgent or not.
	removed, err := CleanupStale(d, "unity")
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0].ID != abandoned.ID {
		t.Errorf("removed = %v, want just the abandoned workload %s", removed, abandoned.ID)
	}
}

func TestSetPriorityUnknownWorkload(t *testing.T) {
	d, _ := testDB(t)
	if _, err := SetPriority(d, "000000", PriorityHighest); !errors.Is(err, ErrNoSuchWorkload) {
		t.Errorf("err = %v, want ErrNoSuchWorkload", err)
	}
}

func TestSetPriorityRejectsOutOfRangeLevel(t *testing.T) {
	d, _ := testDB(t)
	w := enqueue(t, d, "unity")
	for _, level := range []int{0, 6, -1} {
		if _, err := SetPriority(d, w.ID, level); err == nil {
			t.Errorf("level %d was accepted", level)
		}
	}
	var got int
	if err := d.QueryRow(`SELECT priority FROM workloads WHERE id = ?`, w.ID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != PriorityDefault {
		t.Errorf("priority after rejected changes = %d, want it left at %d", got, PriorityDefault)
	}
}

func TestEnqueueRejectsInvalidPriority(t *testing.T) {
	d, _ := testDB(t)
	for _, level := range []int{0, 6, -1} {
		if _, err := Enqueue(d, "unity", level, Meta{Label: "test"}); err == nil {
			t.Errorf("level %d was accepted", level)
		}
	}
	var n int
	if err := d.QueryRow(`SELECT COUNT(*) FROM workloads`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("rows written = %d, want 0", n)
	}
}

func TestValidatePriority(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    int
		wantErr bool
	}{
		{"1", 1, false},
		{"3", 3, false},
		{"5", 5, false},
		{" 2 ", 2, false},
		{"0", 0, true},
		{"6", 0, true},
		{"-1", 0, true},
		{"", 0, true},
		{"high", 0, true},
		{"1.5", 0, true},
	} {
		got, err := ValidatePriority(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ValidatePriority(%q) = %d, want an error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ValidatePriority(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ValidatePriority(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestValidateID(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"1eabb7", "1eabb7", false},
		{"1EABB7", "1eabb7", false},
		{" 49ce3e ", "49ce3e", false},
		{"", "", true},
		{"1eabb", "", true},    // too short
		{"1eabb77", "", true},  // too long
		{"1eabbz", "", true},   // not hex
		{"%", "", true},        // a LIKE wildcard must never be accepted
		{"1eabb7 x", "", true}, // trailing junk, not just whitespace
	} {
		got, err := ValidateID(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ValidateID(%q) = %q, want an error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ValidateID(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ValidateID(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestListOrdersByPriorityWithinASection keeps the views honest: status and
// monitor read this order straight out, so it has to match acquisition.
func TestListOrdersByPriorityWithinASection(t *testing.T) {
	d, _ := testDB(t)
	holder := enqueue(t, d, "unity")
	mustAcquire(t, d, holder)
	ordinary := enqueue(t, d, "unity")
	urgent := enqueueAt(t, d, "unity", PriorityHighest)
	deferred := enqueueAt(t, d, "unity", PriorityLowest)

	got, err := List(d, "unity")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{holder.ID, urgent.ID, ordinary.ID, deferred.ID}
	var ids []string
	for _, w := range got {
		ids = append(ids, w.ID)
	}
	if strings.Join(ids, ",") != strings.Join(want, ",") {
		t.Errorf("List order = %v, want %v", ids, want)
	}
	if got[1].Priority != PriorityHighest {
		t.Errorf("priority was not read back: got %d, want %d", got[1].Priority, PriorityHighest)
	}
}
