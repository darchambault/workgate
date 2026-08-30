package main

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestHigherPriorityOvertakesAcrossProcesses is the FIFO test with one urgent
// workload in the middle. Three real processes contend for one resource; the
// last to arrive must run second, and ordering is read off marker files rather
// than the clock.
func TestHigherPriorityOvertakesAcrossProcesses(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wg.db")
	marker := filepath.Join(dir, "markers.txt")
	env := fastEnv(dbPath)
	d := openTestDB(t, dbPath)

	run := func(name, sleep string, args ...string) *wgProc {
		full := append([]string{"run", "prio-res", "--label", name}, args...)
		full = append(full, "--", helperExe, "-marker", marker, "-name", name, "-sleep", sleep)
		return startWG(t, dir, env, full...)
	}

	a := run("A", "1200ms")
	waitFor(t, 15*time.Second, "A running", func() bool {
		ws := listState(t, d, "prio-res")
		return len(ws) == 1 && ws[0].State == "running"
	})
	b := run("B", "300ms")
	waitFor(t, 15*time.Second, "B enqueued", func() bool {
		return len(listState(t, d, "prio-res")) == 2
	})
	c := run("C", "300ms", "--priority", "1")
	waitFor(t, 15*time.Second, "C enqueued", func() bool {
		return len(listState(t, d, "prio-res")) == 3
	})

	for name, p := range map[string]*wgProc{"A": a, "B": b, "C": c} {
		if code := p.waitExit(t, 60*time.Second); code != 0 {
			t.Fatalf("%s exit = %d, want 0\noutput:\n%s", name, code, p.output())
		}
	}

	// A holds the resource and is never preempted; C overtakes B on priority.
	want := []string{"start", "A", "end", "A", "start", "C", "end", "C", "start", "B", "end", "B"}
	got := readMarkers(t, marker)
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("execution order = %v, want %v", got, want)
	}
	if !strings.Contains(c.output(), "at priority 1") {
		t.Errorf("the queued notice should name a non-default level:\n%s", c.output())
	}
	if strings.Contains(b.output(), "at priority") {
		t.Errorf("the default level should go unmentioned:\n%s", b.output())
	}
}

// TestPriorityCommandReordersQueuedWorkloads exercises the whole point of the
// command: a fourth process reaches into two other sessions' queue and changes
// which of them runs next, with no signalling between them.
func TestPriorityCommandReordersQueuedWorkloads(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wg.db")
	marker := filepath.Join(dir, "markers.txt")
	env := fastEnv(dbPath)
	d := openTestDB(t, dbPath)

	run := func(name, sleep string) *wgProc {
		return startWG(t, dir, env, "run", "bump-res", "--label", name, "--",
			helperExe, "-marker", marker, "-name", name, "-sleep", sleep)
	}

	a := run("A", "1500ms")
	waitFor(t, 15*time.Second, "A running", func() bool {
		ws := listState(t, d, "bump-res")
		return len(ws) == 1 && ws[0].State == "running"
	})
	b := run("B", "300ms")
	waitFor(t, 15*time.Second, "B enqueued", func() bool {
		return len(listState(t, d, "bump-res")) == 2
	})
	c := run("C", "300ms")
	waitFor(t, 15*time.Second, "C enqueued", func() bool {
		return len(listState(t, d, "bump-res")) == 3
	})

	// The id is what a user reads off `workgate status`.
	var cID string
	for _, w := range listState(t, d, "bump-res") {
		if w.Label == "C" {
			cID = w.ID
		}
	}
	if cID == "" {
		t.Fatal("could not find C's workload id")
	}

	bump := startWG(t, dir, env, "priority", cID, "1")
	if code := bump.waitExit(t, 15*time.Second); code != 0 {
		t.Fatalf("priority exit = %d, want 0\noutput:\n%s", code, bump.output())
	}
	for _, want := range []string{cID, `"C"`, "priority 3 -> 1", "position 2"} {
		if !strings.Contains(bump.output(), want) {
			t.Errorf("priority output missing %q:\n%s", want, bump.output())
		}
	}

	for name, p := range map[string]*wgProc{"A": a, "B": b, "C": c} {
		if code := p.waitExit(t, 60*time.Second); code != 0 {
			t.Fatalf("%s exit = %d, want 0\noutput:\n%s", name, code, p.output())
		}
	}

	// B was ahead of C and arrived first; the bump is the only reason C wins.
	want := []string{"start", "A", "end", "A", "start", "C", "end", "C", "start", "B", "end", "B"}
	got := readMarkers(t, marker)
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("execution order = %v, want %v", got, want)
	}
}

// TestPriorityCommandOnARunningWorkload covers the deliberate no-op, end to
// end: it succeeds, says plainly that nothing will change, and leaves the
// queue alone.
func TestPriorityCommandOnARunningWorkload(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wg.db")
	env := fastEnv(dbPath)
	d := openTestDB(t, dbPath)

	a := startWG(t, dir, env, "run", "hold-res", "--label", "Holder", "--",
		helperExe, "-sleep", "20s")
	waitFor(t, 15*time.Second, "A running", func() bool {
		ws := listState(t, d, "hold-res")
		return len(ws) == 1 && ws[0].State == "running"
	})
	id := listState(t, d, "hold-res")[0].ID

	bump := startWG(t, dir, env, "priority", id, "5")
	if code := bump.waitExit(t, 15*time.Second); code != 0 {
		t.Fatalf("priority exit = %d, want 0\noutput:\n%s", code, bump.output())
	}
	if !strings.Contains(bump.output(), "already running") {
		t.Errorf("output should say the change has no scheduling effect:\n%s", bump.output())
	}
	ws := listState(t, d, "hold-res")
	if len(ws) != 1 || ws[0].State != "running" || ws[0].Priority != 5 {
		t.Errorf("queue after the change = %+v, want one running row at priority 5", ws)
	}
	a.cmd.Process.Kill()
}

func TestPriorityCommandRejectsBadArguments(t *testing.T) {
	for _, tc := range []struct {
		desc string
		args []string
	}{
		{"no arguments", []string{"priority"}},
		{"id only", []string{"priority", "1eabb7"}},
		{"level out of range", []string{"priority", "1eabb7", "9"}},
		{"level not a number", []string{"priority", "1eabb7", "urgent"}},
		{"malformed id", []string{"priority", "zzz", "1"}},
		// Well formed, but no such workload: it finished, or was mistyped.
		{"unknown workload", []string{"priority", "000000", "1"}},
		{"unknown flag", []string{"priority", "--now", "1eabb7", "1"}},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			env := fastEnv(filepath.Join(t.TempDir(), "wg.db"))
			p := startWG(t, t.TempDir(), env, tc.args...)
			if code := p.waitExit(t, 15*time.Second); code != 2 {
				t.Fatalf("exit = %d, want 2\noutput:\n%s", code, p.output())
			}
		})
	}
}

// TestStatusShowsPriority is the user-visible end of the feature: the level
// has to reach the view a human actually reads.
func TestStatusShowsPriority(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wg.db")
	env := fastEnv(dbPath)
	d := openTestDB(t, dbPath)

	a := startWG(t, dir, env, "run", "show-res", "--label", "Held workload", "--",
		helperExe, "-sleep", "20s")
	waitFor(t, 15*time.Second, "A running", func() bool {
		ws := listState(t, d, "show-res")
		return len(ws) == 1 && ws[0].State == "running"
	})
	b := startWG(t, dir, env, "run", "show-res", "--label", "Urgent workload",
		"--priority", "1", "--", helperExe, "-exit", "0")
	waitFor(t, 15*time.Second, "B waiting", func() bool {
		return len(listState(t, d, "show-res")) == 2
	})

	st := startWG(t, dir, env, "status", "show-res")
	if code := st.waitExit(t, 15*time.Second); code != 0 {
		t.Fatalf("status exit = %d, want 0\noutput:\n%s", code, st.output())
	}
	text := st.output()
	for _, want := range []string{"RESOURCE: show-res", "P3", "P1", "Held workload", "Urgent workload"} {
		if !strings.Contains(text, want) {
			t.Errorf("status output missing %q:\n%s", want, text)
		}
	}

	a.cmd.Process.Kill()
	b.cmd.Process.Kill()
}
