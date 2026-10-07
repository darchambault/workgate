package queue

import (
	"database/sql"
	"reflect"
	"strings"
	"testing"
)

// claims builds a workload's claims from a compact spec: "wt" is an exclusive
// resource, "+p" a shared tag p (--tag), "!p" an exclusive tag p
// (--block-tag), and "*" the AllWorkloads marker (--block-all).
func claims(spec ...string) []Claim {
	out := make([]Claim, len(spec))
	for i, s := range spec {
		switch {
		case strings.HasPrefix(s, "+"):
			out[i] = Claim{Resource: TagPrefix + s[1:], Shared: true}
		case strings.HasPrefix(s, "!"):
			out[i] = Claim{Resource: TagPrefix + s[1:]}
		default:
			out[i] = Claim{Resource: s}
		}
	}
	return out
}

func enqueueClaims(t *testing.T, d *sql.DB, spec ...string) *Workload {
	t.Helper()
	w, err := EnqueueClaims(d, claims(spec...), PriorityDefault, Meta{Label: "test", PID: 1234})
	if err != nil {
		t.Fatalf("enqueue %v: %v", spec, err)
	}
	return w
}

func release(t *testing.T, d *sql.DB, w *Workload) {
	t.Helper()
	if err := Release(d, w, Outcome{Kind: OutcomeOK}); err != nil {
		t.Fatal(err)
	}
}

func TestValidateTagAndScope(t *testing.T) {
	if got, err := ValidateTag(" MyProj "); err != nil || got != "tag:myproj" {
		t.Errorf("ValidateTag = %q, %v; want tag:myproj", got, err)
	}
	if _, err := ValidateTag("bad:name"); err == nil || !strings.Contains(err.Error(), "invalid tag") {
		t.Errorf("ValidateTag(bad:name) error = %v", err)
	}
	for in, want := range map[string]string{"GPU": "gpu", "Tag:MyProj": "tag:myproj", "tag:p": "tag:p"} {
		if got, err := ValidateScope(in); err != nil || got != want {
			t.Errorf("ValidateScope(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"tag:", "*", "other:x"} {
		if _, err := ValidateScope(in); err == nil {
			t.Errorf("ValidateScope(%q) was accepted", in)
		}
	}
}

func TestEnqueueClaimsRecordsModes(t *testing.T) {
	d, _ := testDB(t)
	w := enqueueClaims(t, d, "wt", "+p", "!q", AllWorkloads)
	wantRes := []string{"wt", "tag:p", "tag:q", "*"}
	wantModes := []string{ModeExclusive, ModeShared, ModeExclusive, ModeExclusive}
	if !reflect.DeepEqual(w.Resources, wantRes) || !reflect.DeepEqual(w.Modes, wantModes) {
		t.Errorf("workload = %v %v, want %v %v", w.Resources, w.Modes, wantRes, wantModes)
	}
	ws, err := List(d, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range ws {
		if !reflect.DeepEqual(l.Resources, wantRes) || !reflect.DeepEqual(l.Modes, wantModes) {
			t.Errorf("listed %s: %v %v, want %v %v", l.Resource, l.Resources, l.Modes, wantRes, wantModes)
		}
	}
}

func TestEnqueueClaimsLimits(t *testing.T) {
	d, _ := testDB(t)
	// The AllWorkloads marker is not one of the four.
	if _, err := EnqueueClaims(d, claims("a", "b", "+c", "!d", AllWorkloads), PriorityDefault, Meta{}); err != nil {
		t.Errorf("four resources and a block-all: %v", err)
	}
	if _, err := EnqueueClaims(d, claims("a", "b", "c", "+d", "+e"), PriorityDefault, Meta{}); err == nil {
		t.Error("five resources were accepted")
	}
	if _, err := EnqueueClaims(d, claims(AllWorkloads), PriorityDefault, Meta{}); err == nil {
		t.Error("a workload naming nothing but block-all was accepted")
	}
	if _, err := EnqueueClaims(d, []Claim{{Resource: "a"}, {Resource: AllWorkloads, Shared: true}}, PriorityDefault, Meta{}); err == nil {
		t.Error("a shared block-all was accepted")
	}
}

// TestSharedTagHoldersRunTogether is the point of a tag: any number of
// workloads hold it at once, each still exclusive on its own resource.
func TestSharedTagHoldersRunTogether(t *testing.T) {
	d, _ := testDB(t)
	a := enqueueClaims(t, d, "wt-a", "+p")
	b := enqueueClaims(t, d, "wt-b", "+p")
	mustAcquire(t, d, a)
	mustAcquire(t, d, b)
	// Their own resources are as exclusive as ever.
	mustNotAcquire(t, d, enqueueClaims(t, d, "wt-a", "+p"))

	ws, err := List(d, "tag:p")
	if err != nil {
		t.Fatal(err)
	}
	running := 0
	for _, w := range ws {
		if w.State == "running" {
			running++
		}
	}
	if running != 2 {
		t.Errorf("running holders of tag:p = %d, want 2", running)
	}
}

// TestBlockTagWaitsForRunningHolders: an exclusive claim on a tag starts only
// once every shared holder has finished.
func TestBlockTagWaitsForRunningHolders(t *testing.T) {
	d, _ := testDB(t)
	a := enqueueClaims(t, d, "wt-a", "+p")
	b := enqueueClaims(t, d, "wt-b", "+p")
	mustAcquire(t, d, a)
	mustAcquire(t, d, b)

	bench := enqueueClaims(t, d, "wt-c", "!p")
	mustNotAcquire(t, d, bench)
	release(t, d, a)
	mustNotAcquire(t, d, bench)
	release(t, d, b)
	mustAcquire(t, d, bench)

	// And while it runs, nothing else with the tag starts.
	mustNotAcquire(t, d, enqueueClaims(t, d, "wt-a", "+p"))
	// Work without the tag is not held.
	mustAcquire(t, d, enqueueClaims(t, d, "wt-b"))
}

// TestBlockTagHoldsFromTheMomentItIsQueued is the hold: tagged work queued
// after a waiting blocker does not start, even on an idle resource of its
// own, while the blocker is still behind its own busy queue.
func TestBlockTagHoldsFromTheMomentItIsQueued(t *testing.T) {
	d, _ := testDB(t)
	first := enqueueClaims(t, d, "wt-a", "+p")
	mustAcquire(t, d, first)
	bench := enqueueClaims(t, d, "wt-a", "!p") // behind first on wt-a
	mustNotAcquire(t, d, bench)

	later := enqueueClaims(t, d, "wt-b", "+p")
	mustNotAcquire(t, d, later)

	release(t, d, first)
	mustNotAcquire(t, d, later)
	mustAcquire(t, d, bench)
	mustNotAcquire(t, d, later)
	release(t, d, bench)
	mustAcquire(t, d, later)
}

// TestTagHolderQueuedAheadOfABlockerGoesFirst: arrival order still decides. A
// holder queued before the blocker, but stuck behind its own busy resource,
// runs before the blocker does.
func TestTagHolderQueuedAheadOfABlockerGoesFirst(t *testing.T) {
	d, _ := testDB(t)
	busy := enqueueClaims(t, d, "wt-b")
	mustAcquire(t, d, busy)
	early := enqueueClaims(t, d, "wt-b", "+p")
	mustNotAcquire(t, d, early)
	bench := enqueueClaims(t, d, "wt-a", "!p")
	mustNotAcquire(t, d, bench) // early is ahead of it on tag:p

	release(t, d, busy)
	mustNotAcquire(t, d, bench)
	mustAcquire(t, d, early)
	mustNotAcquire(t, d, bench)
	release(t, d, early)
	mustAcquire(t, d, bench)
}

// TestWaitingTagHolderDoesNotHoldBackAnother: shared claims never stand in
// each other's way, waiting or not - otherwise one tag would chain every
// queue that uses it into a single line.
func TestWaitingTagHolderDoesNotHoldBackAnother(t *testing.T) {
	d, _ := testDB(t)
	busy := enqueueClaims(t, d, "wt-a")
	mustAcquire(t, d, busy)
	stuck := enqueueClaims(t, d, "wt-a", "+p")
	mustNotAcquire(t, d, stuck)
	mustAcquire(t, d, enqueueClaims(t, d, "wt-b", "+p"))
}

// TestCrossedTagWorkloadsDoNotDeadlock mixes worktrees and both kinds of tag
// claim in crossed orders; one rank still orders them all.
func TestCrossedTagWorkloadsDoNotDeadlock(t *testing.T) {
	d, _ := testDB(t)
	holder := enqueueClaims(t, d, "a", "+p")
	mustAcquire(t, d, holder)
	x := enqueueClaims(t, d, "!p", "b")
	y := enqueueClaims(t, d, "b", "+p", "a")
	z := enqueueClaims(t, d, "+p", "c")
	for _, w := range []*Workload{x, y, z} {
		mustNotAcquire(t, d, w)
	}
	release(t, d, holder)
	mustNotAcquire(t, d, y)
	mustNotAcquire(t, d, z)
	mustAcquire(t, d, x)
	release(t, d, x)
	mustAcquire(t, d, y)
	mustAcquire(t, d, z) // shared with y, and on its own resource
}

// TestRowWithoutAModeIsExclusive: a row an older binary writes names no mode,
// and must keep shared holders out exactly as it always kept everyone out.
func TestRowWithoutAModeIsExclusive(t *testing.T) {
	d, _ := testDB(t)
	if _, err := d.Exec(`INSERT INTO workloads (id, resource, state, created_at, heartbeat_at)
	                     VALUES ('aaa111', 'tag:p', 'running', ?, ?)`, nowMillis(), nowMillis()); err != nil {
		t.Fatal(err)
	}
	mustNotAcquire(t, d, enqueueClaims(t, d, "wt", "+p"))
}

func TestPositionsCountOnlyConflictingWorkloads(t *testing.T) {
	d, _ := testDB(t)
	for _, wt := range []string{"wt-a", "wt-b"} {
		mustAcquire(t, d, enqueueClaims(t, d, wt, "+p"))
	}
	shared := enqueueClaims(t, d, "wt-c", "+p")
	if want := []Place{{"wt-c", 1}, {"tag:p", 1}}; !reflect.DeepEqual(placesFor(t, d, shared), want) {
		t.Errorf("shared holder places = %+v, want %+v", placesFor(t, d, shared), want)
	}
	release(t, d, shared)

	bench := enqueueClaims(t, d, "wt-c", "!p")
	if want := []Place{{"wt-c", 1}, {"tag:p", 3}}; !reflect.DeepEqual(placesFor(t, d, bench), want) {
		t.Errorf("blocker places = %+v, want %+v", placesFor(t, d, bench), want)
	}
	later := enqueueClaims(t, d, "wt-d", "+p")
	if want := []Place{{"wt-d", 1}, {"tag:p", 2}}; !reflect.DeepEqual(placesFor(t, d, later), want) {
		t.Errorf("later holder places = %+v, want %+v", placesFor(t, d, later), want)
	}
}

func placesFor(t *testing.T, d *sql.DB, w *Workload) []Place {
	t.Helper()
	places, err := Positions(d, w)
	if err != nil {
		t.Fatal(err)
	}
	return places
}

// TestBlockAllWaitsForEverything: a --block-all starts only once nothing at
// all is running, on any resource.
func TestBlockAllWaitsForEverything(t *testing.T) {
	d, _ := testDB(t)
	gpu := enqueueClaims(t, d, "gpu")
	tagged := enqueueClaims(t, d, "wt-b", "+p")
	mustAcquire(t, d, gpu)
	mustAcquire(t, d, tagged)

	quiet := enqueueClaims(t, d, "wt-a", AllWorkloads)
	mustNotAcquire(t, d, quiet)
	release(t, d, gpu)
	mustNotAcquire(t, d, quiet)
	release(t, d, tagged)
	mustAcquire(t, d, quiet)
}

// TestBlockAllHoldsEverythingQueuedAfterIt: from the moment it is queued,
// nothing queued after it starts, whatever it names - while what was queued
// before it still goes first.
func TestBlockAllHoldsEverythingQueuedAfterIt(t *testing.T) {
	d, _ := testDB(t)
	running := enqueueClaims(t, d, "wt-a")
	mustAcquire(t, d, running)
	earlier := enqueueClaims(t, d, "wt-a") // behind running
	quiet := enqueueClaims(t, d, "wt-b", AllWorkloads)
	later := enqueueClaims(t, d, "unrelated")

	mustNotAcquire(t, d, quiet)
	mustNotAcquire(t, d, later)
	release(t, d, running)
	mustNotAcquire(t, d, quiet) // earlier is ahead of it
	mustAcquire(t, d, earlier)
	mustNotAcquire(t, d, later)
	release(t, d, earlier)
	mustAcquire(t, d, quiet)
	mustNotAcquire(t, d, later)
	release(t, d, quiet)
	mustAcquire(t, d, later)
}

func TestTwoBlockAllsRunOneAfterTheOther(t *testing.T) {
	d, _ := testDB(t)
	first := enqueueClaims(t, d, "a", AllWorkloads)
	second := enqueueClaims(t, d, "b", AllWorkloads)
	mustAcquire(t, d, first)
	mustNotAcquire(t, d, second)
	release(t, d, first)
	mustAcquire(t, d, second)
}

// TestScopedListShowsABlockAll: a view of one queue must show the --block-all
// that is holding it back.
func TestScopedListShowsABlockAll(t *testing.T) {
	d, _ := testDB(t)
	quiet := enqueueClaims(t, d, "wt-a", AllWorkloads)
	enqueueClaims(t, d, "gpu")
	ws, err := List(d, "gpu")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, w := range ws {
		got = append(got, w.ID+"/"+w.Resource)
	}
	if len(ws) != 2 || ws[0].Resource != AllWorkloads || ws[0].ID != quiet.ID {
		t.Errorf("scoped list = %v, want the block-all first and then the gpu waiter", got)
	}
}

func TestBlockAllPositionCountsEveryWorkloadAhead(t *testing.T) {
	d, _ := testDB(t)
	mustAcquire(t, d, enqueueClaims(t, d, "a"))
	mustAcquire(t, d, enqueueClaims(t, d, "b", "+p"))
	enqueueClaims(t, d, "c") // waiting ahead, though it could run
	quiet := enqueueClaims(t, d, "a", AllWorkloads)
	// Its own queue counts what is on a; the "*" place is where everything
	// else it waits for shows.
	if want := []Place{{"a", 2}, {"*", 4}}; !reflect.DeepEqual(placesFor(t, d, quiet), want) {
		t.Errorf("block-all places = %+v, want %+v", placesFor(t, d, quiet), want)
	}
	later := enqueueClaims(t, d, "d")
	if want := []Place{{"d", 2}}; !reflect.DeepEqual(placesFor(t, d, later), want) {
		t.Errorf("later places = %+v, want %+v", placesFor(t, d, later), want)
	}
}
