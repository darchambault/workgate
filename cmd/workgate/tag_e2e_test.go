package main

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// markerEvents pairs readMarkers' tokens into "start A", "end A", ... .
func markerEvents(t *testing.T, path string) []string {
	t.Helper()
	tokens := readMarkers(t, path)
	var out []string
	for i := 0; i+1 < len(tokens); i += 2 {
		out = append(out, tokens[i]+" "+tokens[i+1])
	}
	return out
}

// TestBlockTagRunsAloneAmongTaggedRuns is the quiet-machine case end to end:
// two tagged runs on their own worktrees overlap; a --block-tag run queued
// while they run waits for both; a tagged run queued after it waits for it.
func TestBlockTagRunsAloneAmongTaggedRuns(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wg.db")
	marker := filepath.Join(dir, "markers.txt")
	env := fastEnv(dbPath)
	d := openTestDB(t, dbPath)

	run := func(name, sleep string, flags ...string) *wgProc {
		args := append([]string{"run"}, flags...)
		args = append(args, "--label", name, "--",
			helperExe, "-marker", marker, "-name", name, "-sleep", sleep)
		return startWG(t, dir, env, args...)
	}

	a := run("A", "1500ms", "wt-a", "--tag", "proj")
	b := run("B", "1500ms", "wt-b", "--tag", "proj")
	waitFor(t, 15*time.Second, "A and B running together", func() bool {
		ws := listState(t, d, "tag:proj")
		return len(ws) == 2 && ws[0].State == "running" && ws[1].State == "running"
	})
	bench := run("Bench", "300ms", "wt-c", "--block-tag", "proj")
	waitFor(t, 15*time.Second, "Bench enqueued", func() bool {
		return len(listState(t, d, "tag:proj")) == 3
	})
	c := run("C", "100ms", "wt-d", "--tag", "proj")
	waitFor(t, 15*time.Second, "C enqueued", func() bool {
		return len(listState(t, d, "tag:proj")) == 4
	})

	text := runStatus(t, dir, env, "tag:proj")
	for _, want := range []string{"TAG: proj", "RUNNING", "WAITING", "[BLOCKS TAG]"} {
		if !strings.Contains(text, want) {
			t.Errorf("status tag:proj missing %q:\n%s", want, text)
		}
	}

	for name, p := range map[string]*wgProc{"A": a, "B": b, "Bench": bench, "C": c} {
		if code := p.waitExit(t, 60*time.Second); code != 0 {
			t.Fatalf("%s exit = %d, want 0\noutput:\n%s", name, code, p.output())
		}
	}
	got := markerEvents(t, marker)
	if len(got) != 8 {
		t.Fatalf("events = %v, want 8", got)
	}
	// A and B both started before either ended: they overlapped.
	if first := strings.Join(got[:2], ","); !strings.Contains(first, "start A") || !strings.Contains(first, "start B") {
		t.Errorf("A and B did not run together: %v", got)
	}
	if tail := strings.Join(got[4:], ","); tail != "start Bench,end Bench,start C,end C" {
		t.Errorf("events = %v, want Bench alone after A and B, then C", got)
	}
	if !strings.Contains(bench.output(), `Acquired "wt-c" and "tag:proj"`) {
		t.Errorf("Bench output does not name the tag:\n%s", bench.output())
	}
}

// TestBlockAllRunsAloneAmongAllRuns: --block-all waits for an unrelated
// running workload, and holds back one queued after it on yet another
// resource.
func TestBlockAllRunsAloneAmongAllRuns(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wg.db")
	marker := filepath.Join(dir, "markers.txt")
	env := fastEnv(dbPath)
	d := openTestDB(t, dbPath)

	run := func(name, sleep string, flags ...string) *wgProc {
		args := append([]string{"run"}, flags...)
		args = append(args, "--label", name, "--",
			helperExe, "-marker", marker, "-name", name, "-sleep", sleep)
		return startWG(t, dir, env, args...)
	}

	x := run("X", "1200ms", "gpu")
	waitFor(t, 15*time.Second, "X running", func() bool {
		ws := listState(t, d, "gpu")
		return len(ws) == 1 && ws[0].State == "running"
	})
	q := run("Q", "300ms", "wt-a", "--block-all")
	waitFor(t, 15*time.Second, "Q enqueued", func() bool {
		return len(listState(t, d, "wt-a")) == 2 // its own row and its block-all
	})
	y := run("Y", "100ms", "unrelated")
	waitFor(t, 15*time.Second, "Y enqueued", func() bool {
		return len(listState(t, d, "unrelated")) == 2 // its own row and Q's block-all
	})

	text := runStatus(t, dir, env, "unrelated")
	for _, want := range []string{"ALL WORKLOADS (--block-all)", `"Q"`, `"Y"`} {
		if !strings.Contains(text, want) {
			t.Errorf("status unrelated missing %q:\n%s", want, text)
		}
	}

	for name, p := range map[string]*wgProc{"X": x, "Q": q, "Y": y} {
		if code := p.waitExit(t, 60*time.Second); code != 0 {
			t.Fatalf("%s exit = %d, want 0\noutput:\n%s", name, code, p.output())
		}
	}
	want := "start X,end X,start Q,end Q,start Y,end Y"
	if got := strings.Join(markerEvents(t, marker), ","); got != want {
		t.Errorf("events = %s, want %s", got, want)
	}
}
