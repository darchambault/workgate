package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"workgate/internal/queue"
)

// testNow is a fixed reference point so elapsed times in expectations are
// stable regardless of when the suite runs.
const testNow = int64(1_700_000_000_000)

// plainTexts strips styling, which is how status renders and how most of
// these assertions want to read a frame.
func plainTexts(lines []line) []string {
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = l.plain()
	}
	return out
}

func plainText(lines []line) string { return strings.Join(plainTexts(lines), "\n") }

// styleOf returns the style of the first span whose text contains want, so a
// test can assert how a particular piece of a line is rendered.
func styleOf(lines []line, want string) (string, bool) {
	for _, l := range lines {
		for _, s := range l {
			if strings.Contains(s.text, want) {
				return s.style, true
			}
		}
	}
	return "", false
}

func testWorkload(id, resource, label, state string, ageMS, heartbeatAgeMS int64) queue.Workload {
	w := queue.Workload{
		ID: id, Resource: resource, Label: label, State: state,
		PID:         4242,
		CreatedAt:   testNow - ageMS,
		HeartbeatAt: testNow - heartbeatAgeMS,
	}
	if state == "running" {
		w.AcquiredAt = testNow - ageMS
	}
	return w
}

func TestStatusLinesGroupsByResourceAndSection(t *testing.T) {
	ws := []queue.Workload{
		testWorkload("aaa", "gpu", "Holder", "running", 5000, 0),
		testWorkload("bbb", "gpu", "Waiter", "waiting", 2000, 0),
		testWorkload("ccc", "rig", "Other", "running", 1000, 0),
	}
	got := plainText(statusLines(ws, testNow, false))
	for _, want := range []string{
		"RESOURCE: gpu", "RESOURCE: rig", "RUNNING", "WAITING",
		`"Holder"`, `"Waiter"`, "pid 4242",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("statusLines missing %q:\n%s", want, got)
		}
	}
	if strings.Index(got, "RUNNING") > strings.Index(got, "WAITING") {
		t.Errorf("RUNNING section should precede WAITING:\n%s", got)
	}
}

// The non-compact layout is what `workgate status` prints, so it must keep
// putting the worktree on a continuation line of its own.
func TestStatusLinesNonCompactUsesContinuationLines(t *testing.T) {
	w := testWorkload("aaa", "gpu", "Holder", "running", 5000, 0)
	w.RepositoryRoot = "/somewhere/MyProject"
	w.GitBranch = "feature-x"
	lines := plainTexts(statusLines([]queue.Workload{w}, testNow, false))
	entry := -1
	for i, l := range lines {
		if strings.Contains(l, `"Holder"`) {
			entry = i
		}
	}
	if entry < 0 || entry+1 >= len(lines) {
		t.Fatalf("no entry line with a continuation after it:\n%s", strings.Join(lines, "\n"))
	}
	if next := strings.TrimSpace(lines[entry+1]); next != "project: MyProject [feature-x]" {
		t.Errorf("expected a worktree continuation line after the entry, got %q", next)
	}
}

// Agents that name every worktree after the main one make the directory alone
// ambiguous, so the branch has to be there to tell two workloads apart - and
// the name must be the worktree's own, not the repository it was cut from.
func TestStatusLinesNamesTheWorktreeNotTheRepository(t *testing.T) {
	one := testWorkload("aaa", "gpu", "First", "running", 5000, 0)
	one.GitCommonDir = "/somewhere/MyProject/.git"
	one.RepositoryRoot = "/somewhere/MyProject"
	one.GitBranch = "main"
	two := testWorkload("bbb", "gpu", "Second", "waiting", 2000, 0)
	two.GitCommonDir = "/somewhere/MyProject/.git"
	two.RepositoryRoot = "/elsewhere/MyProject-codex-42"
	two.GitBranch = "codex/rework-queue"

	for _, compact := range []bool{false, true} {
		got := plainText(statusLines([]queue.Workload{one, two}, testNow, compact))
		for _, want := range []string{
			"MyProject [main]", "MyProject-codex-42 [codex/rework-queue]",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("compact=%v: missing %q:", compact, want)
				t.Error(got)
			}
		}
	}
}

// The branch is decoration around a name, so it only shows when it says
// something: no branch recorded, or the literal "HEAD" that
// `git rev-parse --abbrev-ref` reports for a detached head, leaves the name
// bare rather than bracketing a non-answer.
func TestStatusLinesWorktreeFallbacks(t *testing.T) {
	for _, tc := range []struct {
		desc, root, cwd, branch, want string
	}{
		{"no branch recorded", "/somewhere/MyProject", "", "", "MyProject"},
		{"detached head", "/somewhere/MyProject", "", "HEAD", "MyProject"},
		{"outside git", "", "/somewhere/scratch", "", "scratch"},
		{"no location at all", "", "", "feature-x", ""},
	} {
		w := testWorkload("aaa", "gpu", "Holder", "running", 5000, 0)
		w.RepositoryRoot, w.WorkingDirectory, w.GitBranch = tc.root, tc.cwd, tc.branch
		if got := displayContext(w); got != tc.want {
			t.Errorf("%s: displayContext = %q, want %q", tc.desc, got, tc.want)
		}
		// An unnamed worktree gets no continuation line at all.
		got := strings.Contains(plainText(statusLines([]queue.Workload{w}, testNow, false)), "project: ")
		if got != (tc.want != "") {
			t.Errorf("%s: continuation line present = %v, want %v", tc.desc, got, tc.want != "")
		}
	}
}

// Splitting the entry into spans so the id and pid can be dimmed must not
// disturb the column layout: the plain text has to be exactly the columns.
func TestStatusLinesEntryLayoutIsUnchangedBySpans(t *testing.T) {
	w := testWorkload("aaa", "gpu", "Holder", "running", 5000, 0)
	lines := plainTexts(statusLines([]queue.Workload{w}, testNow, false))
	want := fmt.Sprintf("  %-8s %-10s %-8s %s", "aaa", "pid 4242", "00:05", `"Holder"`)
	for _, l := range lines {
		if strings.Contains(l, "Holder") {
			if l != want {
				t.Errorf("entry line = %q, want %q", l, want)
			}
			return
		}
	}
	t.Fatalf("no entry line found:\n%s", strings.Join(lines, "\n"))
}

// The compact layout is the monitor's: one line per workload, so a fixed
// screen height holds as many as possible.
func TestStatusLinesCompactIsOneLinePerWorkload(t *testing.T) {
	ws := []queue.Workload{
		testWorkload("aaa", "gpu", "Holder", "running", 5000, 0),
		testWorkload("bbb", "gpu", "Waiter", "waiting", 2000, 0),
	}
	lines := plainTexts(statusLines(ws, testNow, true))
	entries := 0
	for _, l := range lines {
		if strings.Contains(l, `"`) {
			entries++
		}
		if strings.Contains(l, "pid:") {
			t.Errorf("compact mode should not emit a pid continuation line: %q", l)
		}
	}
	if entries != 2 {
		t.Errorf("entries = %d, want 2:\n%s", entries, strings.Join(lines, "\n"))
	}
	if !strings.Contains(strings.Join(lines, "\n"), "pid 4242") {
		t.Errorf("compact entry should fold the pid inline:\n%s", strings.Join(lines, "\n"))
	}
}

func TestStatusLinesCompactFlagsStaleEntries(t *testing.T) {
	stale := queue.StaleThreshold.Milliseconds() + 1000
	ws := []queue.Workload{
		testWorkload("aaa", "gpu", "Alive", "running", 5000, 0),
		testWorkload("bbb", "gpu", "Abandoned", "waiting", 90000, stale),
	}
	for _, l := range plainTexts(statusLines(ws, testNow, true)) {
		switch {
		case strings.Contains(l, `"Alive"`) && strings.Contains(l, "[STALE]"):
			t.Errorf("healthy entry marked stale: %q", l)
		case strings.Contains(l, `"Abandoned"`) && !strings.Contains(l, "[STALE]"):
			t.Errorf("stale entry not marked: %q", l)
		}
	}
	// Non-compact is `status` output, which never shows the marker.
	if strings.Contains(plainText(statusLines(ws, testNow, false)), "[STALE]") {
		t.Error("non-compact statusLines should not emit [STALE]")
	}
}

// A narrow terminal truncates the end of a line, so the stale marker has to
// come before the incidental metadata: losing "[STALE]" would hide the whole
// point of the row, while losing a worktree and its branch costs little.
func TestStatusLinesStaleMarkerSurvivesTruncation(t *testing.T) {
	stale := queue.StaleThreshold.Milliseconds() + 1000
	w := testWorkload("fd2b09", "gpu", "A workload with a fairly long label", "waiting", 751000, stale)
	w.RepositoryRoot = "/somewhere/AProjectWithALongName"
	w.GitBranch = "a-branch-with-a-fairly-long-name"
	entry := statusLines([]queue.Workload{w}, testNow, true)[3]

	full := entry.plain()
	if strings.Index(full, "[STALE]") > strings.Index(full, "AProjectWithALongName") {
		t.Errorf("the stale marker must precede the worktree: %q", full)
	}
	// 80 columns is the assumed width when the terminal cannot be measured,
	// so the marker has to survive a fit to it.
	fitted := fitFrame([]line{entry}, 80, 24)[0]
	if !strings.Contains(fitted.plain(), "[STALE]") {
		t.Errorf("fitting to 80 columns lost the stale marker: %q", fitted.plain())
	}
}

// Colour is a second channel, never the only one: every state the palette
// distinguishes is also stated in the text.
func TestStatusLinesStyles(t *testing.T) {
	stale := queue.StaleThreshold.Milliseconds() + 1000
	ws := []queue.Workload{
		testWorkload("aaa", "gpu", "Holder", "running", 5000, 0),
		testWorkload("bbb", "gpu", "Abandoned", "waiting", 90000, stale),
	}
	lines := statusLines(ws, testNow, true)
	for _, tc := range []struct{ text, style, desc string }{
		{"RESOURCE: gpu", styleBold, "resource heading"},
		{"RUNNING", styleRunning, "running section"},
		{"WAITING", styleWaiting, "waiting section"},
		{"aaa", styleDim, "workload id"},
		{"pid 4242", styleDim, "inline pid"},
		{"[STALE]", styleAlert, "stale marker"},
	} {
		got, ok := styleOf(lines, tc.text)
		if !ok {
			t.Errorf("%s: no span containing %q", tc.desc, tc.text)
			continue
		}
		if got != tc.style {
			t.Errorf("%s (%q): style = %q, want %q", tc.desc, tc.text, got, tc.style)
		}
	}
	// The label is the point of the line; it keeps the default colour.
	if got, _ := styleOf(lines, `"Holder"`); got != "" {
		t.Errorf("label style = %q, want the terminal default", got)
	}
}

// Styles must never reach status, whose output is byte-for-byte the same as
// before colour existed.
func TestStatusLinesPlainTextCarriesNoEscapes(t *testing.T) {
	ws := []queue.Workload{testWorkload("aaa", "gpu", "Holder", "running", 5000, 0)}
	for _, compact := range []bool{false, true} {
		if got := plainText(statusLines(ws, testNow, compact)); strings.Contains(got, "\x1b") {
			t.Errorf("compact=%v: plain text contains an escape sequence: %q", compact, got)
		}
	}
}

func TestStatusLinesEmpty(t *testing.T) {
	if got := statusLines(nil, testNow, true); len(got) != 0 {
		t.Errorf("statusLines(nil) = %v, want no lines", got)
	}
}

func TestMonitorBodyEmptyStates(t *testing.T) {
	if got := monitorBody(nil, "", testNow); !strings.Contains(got[0].plain(), "No active workgate workloads.") {
		t.Errorf("all-resources empty message = %q", got[0].plain())
	}
	if got := monitorBody(nil, "gpu", testNow); !strings.Contains(got[0].plain(), `"gpu"`) {
		t.Errorf("single-resource empty message should name the resource, got %q", got[0].plain())
	}
}

// A failed read must not blank the view: the last good body stays on screen
// with a warning appended.
func TestMonitorFrameKeepsBodyOnReadError(t *testing.T) {
	body := []line{plainLine("RESOURCE: gpu"), plainLine("RUNNING"), plainLine(`  aaa  "Holder"  00:05`)}
	frame := monitorFrame("gpu", body, errors.New("database is locked"), time.Second, time.Now())
	text := plainText(frame)
	for _, want := range []string{"workgate monitor - gpu", `"Holder"`,
		"warning: last refresh failed: database is locked", "Ctrl+C to stop"} {
		if !strings.Contains(text, want) {
			t.Errorf("frame missing %q:\n%s", want, text)
		}
	}
	if got, _ := styleOf(frame, "last refresh failed"); got != styleAlert {
		t.Errorf("refresh warning style = %q, want %q", got, styleAlert)
	}
}

func TestMonitorFrameShowsInterval(t *testing.T) {
	frame := monitorFrame("all resources", []line{plainLine("x")}, nil, 2500*time.Millisecond, time.Now())
	last := frame[len(frame)-1]
	if !strings.Contains(last.plain(), "refreshing every 2.5s") {
		t.Errorf("footer should state the interval: %q", last.plain())
	}
	if last[0].style != styleDim {
		t.Errorf("footer style = %q, want dim", last[0].style)
	}
}

// The header and footer are chrome; the queue is the content.
func TestMonitorFrameDimsChrome(t *testing.T) {
	frame := monitorFrame("gpu", []line{plainLine("body")}, nil, time.Second, time.Now())
	if frame[0][0].style != styleDim {
		t.Errorf("header style = %q, want dim", frame[0][0].style)
	}
}

func TestFitFrameTruncatesWidth(t *testing.T) {
	got := fitFrame([]line{plainLine(strings.Repeat("x", 200))}, 20, 10)
	if n := got[0].width(); n != 20 {
		t.Errorf("line width = %d, want 20: %q", n, got[0].plain())
	}
	if !strings.HasSuffix(got[0].plain(), "...") {
		t.Errorf("truncated line should end in an ellipsis: %q", got[0].plain())
	}
}

func TestFitFrameCapsHeightAndReportsOverflow(t *testing.T) {
	lines := make([]line, 20)
	for i := range lines {
		lines[i] = plainLine("line")
	}
	got := fitFrame(lines, 80, 5)
	if len(got) != 5 {
		t.Fatalf("frame height = %d, want 5", len(got))
	}
	// Four lines are kept plus the overflow notice, so 16 are hidden.
	if !strings.Contains(got[4].plain(), "16 more") {
		t.Errorf("overflow notice = %q, want a count of 16", got[4].plain())
	}
}

// A frame filling the full height must stop one column short of the last
// cell, which some terminals treat as a scroll trigger.
func TestFitFrameLeavesLastCellUnwritten(t *testing.T) {
	got := fitFrame([]line{
		plainLine(strings.Repeat("x", 100)),
		plainLine(strings.Repeat("y", 100)),
	}, 10, 2)
	if n := got[0].width(); n != 10 {
		t.Errorf("non-final line = %d columns, want 10", n)
	}
	if n := got[1].width(); n != 9 {
		t.Errorf("final line = %d columns, want 9", n)
	}
}

func TestFitFrameFallsBackOnBogusSize(t *testing.T) {
	got := fitFrame([]line{plainLine("hello")}, 0, 0)
	if len(got) != 1 || got[0].plain() != "hello" {
		t.Errorf("fitFrame with a zero size = %q, want the line unchanged", plainTexts(got))
	}
}

// Width is measured on text alone. If styles were counted, a coloured line
// would measure wider than it draws and be truncated far too early — and a
// cut landing inside an escape sequence would smear that style onward.
func TestTruncateLineIgnoresStyleWidth(t *testing.T) {
	l := line{
		{text: "1234567890", style: styleDim},
		{text: "abcdefghij", style: styleAlert},
	}
	if got := truncateLine(l, 20).width(); got != 20 {
		t.Errorf("an exactly-fitting styled line was truncated to %d columns", got)
	}
	got := truncateLine(l, 15)
	if got.width() != 15 {
		t.Errorf("width = %d, want 15", got.width())
	}
	if want := "1234567890ab..."; got.plain() != want {
		t.Errorf("plain = %q, want %q", got.plain(), want)
	}
	// Spans that survive intact keep their style, and the ellipsis adopts
	// the style of the span it interrupted.
	if got[0].style != styleDim {
		t.Errorf("first span style = %q, want dim", got[0].style)
	}
	if got[len(got)-1].style != styleAlert {
		t.Errorf("ellipsis style = %q, want the interrupted span's", got[len(got)-1].style)
	}
}

func TestTruncateLinePreservesRunes(t *testing.T) {
	// Byte-based truncation would slice these two-byte runes in half.
	l := plainLine("\u03b1\u03b1\u03b1\u03b1\u03b1\u03b1")
	if got := truncateLine(l, 5).plain(); got != "\u03b1\u03b1..." {
		t.Errorf("truncateLine = %q, want two runes plus an ellipsis", got)
	}
	if got := truncateLine(plainLine("abc"), 5).plain(); got != "abc" {
		t.Errorf("short lines should pass through unchanged, got %q", got)
	}
	if got := truncateLine(plainLine("abcdef"), 2).plain(); got != ".." {
		t.Errorf("truncateLine to 2 = %q, want %q", got, "..")
	}
	if got := truncateLine(plainLine("abc"), 0); got != nil {
		t.Errorf("truncateLine to 0 = %v, want nothing", got)
	}
}

func TestParseMonitorArgs(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		resource string
		interval time.Duration
		wantErr  bool
	}{
		{name: "no arguments", interval: defaultMonitorInterval},
		{name: "resource only", args: []string{"gpu"}, resource: "gpu", interval: defaultMonitorInterval},
		{name: "separate interval", args: []string{"gpu", "--interval", "2s"},
			resource: "gpu", interval: 2 * time.Second},
		{name: "joined interval", args: []string{"--interval=500ms", "gpu"},
			resource: "gpu", interval: 500 * time.Millisecond},
		{name: "missing interval value", args: []string{"--interval"}, wantErr: true},
		{name: "unparseable interval", args: []string{"--interval", "soon"}, wantErr: true},
		{name: "interval below the floor", args: []string{"--interval", "10ms"}, wantErr: true},
		{name: "unknown flag", args: []string{"--watch"}, wantErr: true},
		{name: "second positional", args: []string{"gpu", "rig"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resource, interval, err := parseMonitorArgs(tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got resource=%q interval=%s", resource, interval)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resource != tc.resource || interval != tc.interval {
				t.Errorf("= (%q, %s), want (%q, %s)", resource, interval, tc.resource, tc.interval)
			}
		})
	}
}
