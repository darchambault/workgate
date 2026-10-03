package main

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestMultiResourceRunWaitsHoldingNothing is the feature end to end: a run
// naming proj and gpu waits for proj without taking gpu, and a gpu-only run
// that arrives later queues behind it anyway - strict head-of-line - rather
// than slipping onto the idle GPU.
func TestMultiResourceRunWaitsHoldingNothing(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wg.db")
	marker := filepath.Join(dir, "markers.txt")
	env := fastEnv(dbPath)
	d := openTestDB(t, dbPath)

	run := func(resources, name, sleep string) *wgProc {
		return startWG(t, dir, env, "run", resources, "--label", name, "--",
			helperExe, "-marker", marker, "-name", name, "-sleep", sleep)
	}

	a := run("proj", "A", "1500ms")
	waitFor(t, 15*time.Second, "A running", func() bool {
		ws := listState(t, d, "proj")
		return len(ws) == 1 && ws[0].State == "running"
	})
	m := run("proj,gpu", "M", "300ms")
	waitFor(t, 15*time.Second, "M enqueued on both", func() bool {
		return len(listState(t, d, "proj")) == 2 && len(listState(t, d, "gpu")) == 1
	})
	if ws := listState(t, d, "gpu"); ws[0].State != "waiting" {
		t.Fatalf("M took gpu while proj was held: %+v", ws)
	}
	g := run("gpu", "G", "100ms")
	waitFor(t, 15*time.Second, "G enqueued", func() bool {
		return len(listState(t, d, "gpu")) == 2
	})

	for name, p := range map[string]*wgProc{"A": a, "M": m, "G": g} {
		if code := p.waitExit(t, 60*time.Second); code != 0 {
			t.Fatalf("%s exit = %d, want 0\noutput:\n%s", name, code, p.output())
		}
	}
	want := []string{"start", "A", "end", "A", "start", "M", "end", "M", "start", "G", "end", "G"}
	if got := readMarkers(t, marker); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("execution order = %v, want %v", got, want)
	}
	for _, msg := range []string{
		`Queued for "proj" (position 2) and "gpu" (position 1): M`,
		`Acquired "proj" and "gpu"`,
		`Released "proj" and "gpu"`,
	} {
		if !strings.Contains(m.output(), msg) {
			t.Errorf("M output missing %q:\n%s", msg, m.output())
		}
	}
}

// TestRunningMultiResourceRunBlocksEachResource: while it runs, it holds both,
// and the views say so from either queue.
func TestRunningMultiResourceRunBlocksEachResource(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wg.db")
	marker := filepath.Join(dir, "markers.txt")
	env := fastEnv(dbPath)
	d := openTestDB(t, dbPath)

	run := func(resources, name, sleep string) *wgProc {
		return startWG(t, dir, env, "run", resources, "--label", name, "--",
			helperExe, "-marker", marker, "-name", name, "-sleep", sleep)
	}

	m := run("proj,gpu", "M", "2s")
	waitFor(t, 15*time.Second, "M running", func() bool {
		ws := listState(t, d, "")
		return len(ws) == 2 && ws[0].State == "running" && ws[1].State == "running"
	})
	p := run("proj", "P", "100ms")
	g := run("gpu", "G", "100ms")
	waitFor(t, 15*time.Second, "P and G enqueued", func() bool {
		return len(listState(t, d, "")) == 4
	})

	text := runStatus(t, dir, env, "gpu")
	for _, want := range []string{"RUNNING", `"M"`, "also holds: proj", "WAITING", `"G"`} {
		if !strings.Contains(text, want) {
			t.Errorf("status gpu missing %q:\n%s", want, text)
		}
	}

	for name, proc := range map[string]*wgProc{"M": m, "P": p, "G": g} {
		if code := proc.waitExit(t, 60*time.Second); code != 0 {
			t.Fatalf("%s exit = %d, want 0\noutput:\n%s", name, code, proc.output())
		}
	}
	got := readMarkers(t, marker)
	if len(got) != 12 || strings.Join(got[:4], " ") != "start M end M" {
		t.Fatalf("execution order = %v, want M to finish before P or G starts", got)
	}

	// Once finished, an unscoped view shows the workload once, naming both.
	recent := runStatus(t, dir, env, "--recent")
	if n := strings.Count(recent, `"M"`); n != 1 {
		t.Errorf("M appears %d times in the unscoped completions, want once:\n%s", n, recent)
	}
	if !strings.Contains(recent, "proj,gpu") {
		t.Errorf("unscoped completion does not name both resources:\n%s", recent)
	}
}

// TestCrossedMultiResourceRunsBothFinish is the deadlock nesting would cause:
// one session wanting a then b, another b then a, both blocked at once.
func TestCrossedMultiResourceRunsBothFinish(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wg.db")
	env := fastEnv(dbPath)
	d := openTestDB(t, dbPath)

	h := startWG(t, dir, env, "run", "res-a,res-b", "--", helperExe, "-sleep", "800ms")
	waitFor(t, 15*time.Second, "holder running", func() bool {
		ws := listState(t, d, "res-a")
		return len(ws) == 1 && ws[0].State == "running"
	})
	x := startWG(t, dir, env, "run", "res-a,res-b", "--", helperExe, "-sleep", "200ms")
	y := startWG(t, dir, env, "run", "res-b,res-a", "--", helperExe, "-sleep", "200ms")
	for name, p := range map[string]*wgProc{"holder": h, "X": x, "Y": y} {
		if code := p.waitExit(t, 30*time.Second); code != 0 {
			t.Fatalf("%s exit = %d, want 0\noutput:\n%s", name, code, p.output())
		}
	}
}

// TestHardKilledMultiResourceRunFreesEveryResource: the next run on either
// resource reclaims the whole workload, and says so once.
func TestHardKilledMultiResourceRunFreesEveryResource(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wg.db")
	env := append([]string{
		"WORKGATE_DB=" + dbPath,
		"WORKGATE_POLL_INTERVAL_MS=100",
		"WORKGATE_HEARTBEAT_INTERVAL_MS=300",
		"WORKGATE_STALE_THRESHOLD_MS=2500",
	}, noConfigEnv(dbPath)...)
	d := openTestDB(t, dbPath)

	a := startWG(t, dir, env, "run", "proj,gpu", "--label", "doomed", "--",
		helperExe, "-sleep", "120s")
	var id string
	waitFor(t, 15*time.Second, "A running", func() bool {
		ws := listState(t, d, "gpu")
		if len(ws) == 1 && ws[0].State == "running" {
			id = ws[0].ID
			return true
		}
		return false
	})
	if err := a.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-a.done

	b := startWG(t, dir, env, "run", "gpu", "--", helperExe, "-exit", "0")
	if code := b.waitExit(t, 30*time.Second); code != 0 {
		t.Fatalf("B exit = %d, want 0\noutput:\n%s", code, b.output())
	}
	notice := `Removed stale workload ` + id + ` from "proj" and "gpu"`
	if strings.Count(b.output(), "Removed stale workload") != 1 || !strings.Contains(b.output(), notice) {
		t.Errorf("B output, want exactly %q:\n%s", notice, b.output())
	}
	if ws := listState(t, d, ""); len(ws) != 0 {
		t.Fatalf("workloads remain: %+v", ws)
	}
}

func TestInvalidResourceListsRejected(t *testing.T) {
	env := fastEnv(filepath.Join(t.TempDir(), "wg.db"))
	for _, list := range []string{"a,,b", "a,", "gpu,GPU", "a,b,c,d,e", "a,bad!name"} {
		p := startWG(t, t.TempDir(), env, "run", list, "--", helperExe)
		if code := p.waitExit(t, 15*time.Second); code != 2 {
			t.Errorf("run %q: exit = %d, want 2\noutput:\n%s", list, code, p.output())
		}
	}
}

// TestPriorityCommandOnAMultiResourceWorkload reports a position in every
// queue the workload is in.
func TestPriorityCommandOnAMultiResourceWorkload(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wg.db")
	env := fastEnv(dbPath)
	d := openTestDB(t, dbPath)

	a := startWG(t, dir, env, "run", "proj", "--", helperExe, "-sleep", "20s")
	waitFor(t, 15*time.Second, "A running", func() bool {
		ws := listState(t, d, "proj")
		return len(ws) == 1 && ws[0].State == "running"
	})
	m := startWG(t, dir, env, "run", "proj,gpu", "--label", "M", "--", helperExe, "-exit", "0")
	var id string
	waitFor(t, 15*time.Second, "M enqueued", func() bool {
		ws := listState(t, d, "gpu")
		if len(ws) == 1 {
			id = ws[0].ID
			return true
		}
		return false
	})

	bump := startWG(t, dir, env, "priority", id, "1")
	if code := bump.waitExit(t, 15*time.Second); code != 0 {
		t.Fatalf("priority exit = %d, want 0\noutput:\n%s", code, bump.output())
	}
	want := `priority 3 -> 1 (now position 2 on "proj", 1 on "gpu")`
	if !strings.Contains(bump.output(), want) {
		t.Errorf("priority output missing %q:\n%s", want, bump.output())
	}
	for _, w := range listState(t, d, "") {
		if w.ID == id && w.Priority != 1 {
			t.Errorf("%s row priority = %d, want 1", w.Resource, w.Priority)
		}
	}
	a.cmd.Process.Kill()
	m.cmd.Process.Kill()
}
