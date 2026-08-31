package main

import (
	"context"
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
		Priority:    queue.PriorityDefault,
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

// An entry is a header row, then its label, then the command that was run.
// Both views stack the same way: the split exists so a long label and a long
// command can be read whole, which is not something one view needs and the
// other does not.
func TestEntryStacksHeaderLabelAndCommand(t *testing.T) {
	w := testWorkload("aaa", "gpu", "Holder", "running", 5000, 0)
	w.RepositoryRoot = "/somewhere/MyProject"
	w.GitBranch = "feature-x"
	w.CommandDisplay = "go test ./..."

	for _, flagStale := range []bool{false, true} {
		lines := plainTexts(statusLines([]queue.Workload{w}, testNow, flagStale))
		header := -1
		for i, l := range lines {
			if strings.HasPrefix(l, "  aaa") {
				header = i
			}
		}
		if header < 0 || header+2 >= len(lines) {
			t.Fatalf("flagStale=%v: expected a header and two lines under it:\n%s",
				flagStale, strings.Join(lines, "\n"))
		}
		// The header carries the columns and the worktree and nothing else:
		// the label and the command no longer compete for its tail.
		if got := lines[header]; !strings.HasSuffix(got, "MyProject [feature-x]") {
			t.Errorf("flagStale=%v: header = %q, want it to end at the worktree", flagStale, got)
		}
		if strings.Contains(lines[header], "Holder") {
			t.Errorf("flagStale=%v: the label belongs on its own line, not %q", flagStale, lines[header])
		}
		if got := lines[header+1]; got != continuationIndent+`"Holder"` {
			t.Errorf("flagStale=%v: label line = %q", flagStale, got)
		}
		if got := lines[header+2]; got != continuationIndent+"go test ./..." {
			t.Errorf("flagStale=%v: command line = %q", flagStale, got)
		}
	}
}

// Each continuation is skipped when there is nothing to put on it, rather
// than spending a line on a placeholder standing in for one.
func TestEntryOmitsMissingLabelAndCommand(t *testing.T) {
	for _, tc := range []struct {
		desc, label, command string
		want                 []string
	}{
		{"both", "Holder", "go test", []string{`"Holder"`, "go test"}},
		{"no label", "", "go test", []string{"go test"}},
		{"no command", "Holder", "", []string{`"Holder"`}},
		{"neither", "", "", nil},
	} {
		w := testWorkload("aaa", "gpu", tc.label, "running", 5000, 0)
		w.CommandDisplay = tc.command
		lines := plainTexts(statusLines([]queue.Workload{w}, testNow, true))
		var got []string
		for _, l := range lines {
			if strings.HasPrefix(l, continuationIndent) {
				got = append(got, strings.TrimSpace(l))
			}
		}
		if strings.Join(got, "|") != strings.Join(tc.want, "|") {
			t.Errorf("%s: continuations = %q, want %q", tc.desc, got, tc.want)
		}
		if strings.Contains(strings.Join(lines, "\n"), "no label") {
			t.Errorf("%s: an absent label should cost no line at all", tc.desc)
		}
	}
}

// The two views drifting apart is what the shared renderer exists to prevent,
// and flagStale is the only thing left that can tell them apart.
func TestStatusAndMonitorRenderTheSameEntry(t *testing.T) {
	w := testWorkload("aaa", "gpu", "Holder", "running", 5000, 0)
	w.RepositoryRoot = "/somewhere/MyProject"
	w.GitBranch = "main"
	w.CommandDisplay = "go test ./..."
	ws := []queue.Workload{w}
	monitor, status := plainText(statusLines(ws, testNow, true)), plainText(statusLines(ws, testNow, false))
	if monitor != status {
		t.Errorf("the views differ on a healthy workload:\n monitor:\n%s\n status:\n%s", monitor, status)
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

	for _, flagStale := range []bool{false, true} {
		got := plainText(statusLines([]queue.Workload{one, two}, testNow, flagStale))
		for _, want := range []string{
			"MyProject [main]", "MyProject-codex-42 [codex/rework-queue]",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("flagStale=%v: missing %q:", flagStale, want)
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
		// An unnamed worktree leaves the header row ending at its columns.
		bare := false
		for _, l := range plainTexts(statusLines([]queue.Workload{w}, testNow, false)) {
			if strings.HasPrefix(l, "  aaa") {
				bare = strings.HasSuffix(l, "P3")
			}
		}
		if bare != (tc.want == "") {
			t.Errorf("%s: bare header row = %v, want %v", tc.desc, bare, tc.want == "")
		}
	}
}

// Splitting the header row into spans so the id and pid can be dimmed must
// not disturb the column layout: the plain text has to be exactly the columns.
func TestEntryHeaderLayoutIsUnchangedBySpans(t *testing.T) {
	w := testWorkload("aaa", "gpu", "Holder", "running", 5000, 0)
	lines := plainTexts(statusLines([]queue.Workload{w}, testNow, false))
	want := fmt.Sprintf("  %-8s %-10s %-8s %-2s", "aaa", "pid 4242", "00:05", "P3")
	for _, l := range lines {
		if strings.HasPrefix(l, "  aaa") {
			if l != want {
				t.Errorf("header row = %q, want %q", l, want)
			}
			return
		}
	}
	t.Fatalf("no header row found:\n%s", strings.Join(lines, "\n"))
}

func TestMonitorFlagsStaleEntries(t *testing.T) {
	stale := queue.StaleThreshold.Milliseconds() + 1000
	ws := []queue.Workload{
		testWorkload("aaa", "gpu", "Alive", "running", 5000, 0),
		testWorkload("bbb", "gpu", "Abandoned", "waiting", 90000, stale),
	}
	// The marker rides the header row, which is the line carrying the id.
	for _, l := range plainTexts(statusLines(ws, testNow, true)) {
		switch {
		case strings.HasPrefix(l, "  aaa") && strings.Contains(l, "[STALE]"):
			t.Errorf("healthy entry marked stale: %q", l)
		case strings.HasPrefix(l, "  bbb") && !strings.Contains(l, "[STALE]"):
			t.Errorf("stale entry not marked: %q", l)
		}
	}
	// status reclaims stale rows before it lists, so it never has one to mark.
	if strings.Contains(plainText(statusLines(ws, testNow, false)), "[STALE]") {
		t.Error("statusLines with flagStale off should not emit [STALE]")
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
	var entry line
	for _, l := range statusLines([]queue.Workload{w}, testNow, true) {
		if strings.HasPrefix(l.plain(), "  fd2b09") {
			entry = l
		}
	}
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
	ws[0].CommandDisplay = "go test ./..."
	lines := statusLines(ws, testNow, true)
	for _, tc := range []struct{ text, style, desc string }{
		{"RESOURCE: gpu", styleBold, "resource heading"},
		{"RUNNING", styleRunning, "running section"},
		{"WAITING", styleWaiting, "waiting section"},
		{"aaa", styleDim, "workload id"},
		{"pid 4242", styleDim, "inline pid"},
		{"[STALE]", styleAlert, "stale marker"},
		{"go test ./...", styleDim, "command line"},
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
	for _, flagStale := range []bool{false, true} {
		if got := plainText(statusLines(ws, testNow, flagStale)); strings.Contains(got, "\x1b") {
			t.Errorf("flagStale=%v: plain text contains an escape sequence: %q", flagStale, got)
		}
	}
}

func TestStatusLinesEmpty(t *testing.T) {
	if got := statusLines(nil, testNow, true); len(got) != 0 {
		t.Errorf("statusLines(nil) = %v, want no lines", got)
	}
}

func TestMonitorBodyEmptyStates(t *testing.T) {
	if got := monitorBody(nil, nil, "", testNow, ""); !strings.Contains(got[0].plain(), "No active workgate workloads.") {
		t.Errorf("all-resources empty message = %q", got[0].plain())
	}
	if got := monitorBody(nil, nil, "gpu", testNow, ""); !strings.Contains(got[0].plain(), `"gpu"`) {
		t.Errorf("single-resource empty message should name the resource, got %q", got[0].plain())
	}
}

// A failed read must not blank the view: the last good body stays on screen
// with a warning appended.
func TestMonitorFrameKeepsBodyOnReadError(t *testing.T) {
	body := []line{plainLine("RESOURCE: gpu"), plainLine("RUNNING"), plainLine(`  aaa  "Holder"  00:05`)}
	frame := monitorFrame(body, frameState{scope: "gpu", readErr: errors.New("database is locked"),
		interval: time.Second, now: time.Now()})
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
	frame := monitorFrame([]line{plainLine("x")}, frameState{scope: "all resources",
		interval: 2500 * time.Millisecond, now: time.Now()})
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
	frame := monitorFrame([]line{plainLine("body")}, frameState{scope: "gpu",
		interval: time.Second, now: time.Now()})
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

func testCompletion(id, resource, label, outcome string, exitCode int64, ranMS, agoMS int64) queue.Completion {
	return queue.Completion{
		ID: id, Resource: resource, Label: label,
		Outcome: outcome, ExitCode: exitCode,
		FinishedAt:     testNow - agoMS,
		StartedAt:      testNow - agoMS - ranMS,
		RepositoryRoot: "/src/proj",
		GitBranch:      "main",
	}
}

// TestCompletionLinesShareTheEntryGrid is the mirror of
// TestStatusLinesEntryLayoutIsUnchangedBySpans: a finished row must land on
// exactly the same columns as a live one, so a frame reads as one table.
func TestCompletionLinesShareTheEntryGrid(t *testing.T) {
	live := statusLines([]queue.Workload{
		testWorkload("aaa", "gpu", "Holder", "running", 5000, 0),
	}, testNow, true)
	done := completionLines([]queue.Completion{
		testCompletion("bbb", "gpu", "Finished", queue.OutcomeExit, 1, 5000, 0),
	}, testNow, false)

	liveEntry, doneEntry := "", ""
	liveLabel, doneLabel := "", ""
	for _, l := range plainTexts(live) {
		if strings.HasPrefix(l, "  aaa") {
			liveEntry = l
		}
		if strings.Contains(l, "Holder") {
			liveLabel = l
		}
	}
	for _, l := range plainTexts(done) {
		if strings.HasPrefix(l, "  bbb") {
			doneEntry = l
		}
		if strings.Contains(l, "Finished") {
			doneLabel = l
		}
	}
	want := fmt.Sprintf("  %-8s %-10s %-8s %-2s", "bbb", "exit 1", "00:05", "")
	if !strings.HasPrefix(doneEntry, want) {
		t.Fatalf("completion header = %q, want it to start %q", doneEntry, want)
	}
	// Both ran five seconds, so the timer column is directly comparable.
	if strings.Index(liveEntry, "00:05") != strings.Index(doneEntry, "00:05") {
		t.Errorf("timer columns differ:\n live %q\n done %q", liveEntry, doneEntry)
	}
	// And the labels underneath start at the same column, which is the
	// property that actually matters on screen.
	if strings.Index(liveLabel, `"Holder"`) != strings.Index(doneLabel, `"Finished"`) {
		t.Errorf("label columns differ:\n live %q\n done %q", liveLabel, doneLabel)
	}
}

// TestOutcomeTextFitsTheColumn guards the invariant the fixed grid rests on:
// %-*s pads but never truncates, so one over-long outcome would silently
// shift every later column on that row.
func TestOutcomeTextFitsTheColumn(t *testing.T) {
	kinds := []string{
		queue.OutcomeOK, queue.OutcomeExit, queue.OutcomeKilled,
		queue.OutcomeCanceled, queue.OutcomeStale,
	}
	codes := []int64{0, 1, 255, -1, 3221225477}
	for _, kind := range kinds {
		for _, code := range codes {
			s := outcomeSpan(queue.Completion{Outcome: kind, ExitCode: code})
			if len(s.text) != pidWidth {
				t.Errorf("outcomeSpan(%s, %d) width = %d, want exactly %d",
					kind, code, len(s.text), pidWidth)
			}
		}
	}
	// outcomeFor is what keeps the wide codes out of the "exit N" branch.
	for _, code := range []int{-1, 3221225477} {
		if got := outcomeFor(code, nil); got.Kind != queue.OutcomeKilled {
			t.Errorf("outcomeFor(%d) = %+v, want killed", code, got)
		}
	}
}

func TestOutcomeForClassifiesRuns(t *testing.T) {
	tests := []struct {
		name string
		code int
		err  error
		want queue.Outcome
	}{
		{"clean exit", 0, nil, queue.Outcome{Kind: queue.OutcomeOK}},
		{"failure", 3, nil, queue.Outcome{Kind: queue.OutcomeExit, ExitCode: 3}},
		{"interrupted", 130, context.Canceled, queue.Outcome{Kind: queue.OutcomeCanceled}},
		{"signalled", -1, nil, queue.Outcome{Kind: queue.OutcomeKilled}},
		// A command that could not be launched is honestly described by the
		// 126/127 the shell itself would report.
		{"not found", 127, nil, queue.Outcome{Kind: queue.OutcomeExit, ExitCode: 127}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := outcomeFor(tc.code, tc.err); got != tc.want {
				t.Errorf("outcomeFor(%d, %v) = %+v, want %+v", tc.code, tc.err, got, tc.want)
			}
		})
	}
}

func TestCompletionLinesStyles(t *testing.T) {
	cs := []queue.Completion{
		testCompletion("aaa", "gpu", "Ok", queue.OutcomeOK, 0, 1000, 0),
		testCompletion("bbb", "gpu", "Failed", queue.OutcomeExit, 1, 1000, 0),
		testCompletion("ccc", "gpu", "Crashed", queue.OutcomeStale, 0, 1000, 0),
		testCompletion("ddd", "gpu", "Stopped", queue.OutcomeCanceled, 0, 1000, 0),
	}
	got := completionLines(cs, testNow, false)
	for _, tc := range []struct{ text, style string }{
		{"ok", styleDim},
		{"exit 1", styleAlert},
		{"stale", styleAlert},
		{"canceled", styleDim},
	} {
		style, ok := styleOf(got, tc.text)
		if !ok {
			t.Fatalf("no span containing %q", tc.text)
		}
		if style != tc.style {
			t.Errorf("%q style = %q, want %q", tc.text, style, tc.style)
		}
	}
	if style, ok := styleOf(got, completionsHeading); !ok || style != styleDim {
		t.Errorf("heading style = %q (found %v), want %q", style, ok, styleDim)
	}
	// An unlabelled completion spends no line on a placeholder, exactly as
	// an unlabelled workload does not.
	if got := plainTexts(completionLines(
		[]queue.Completion{testCompletion("eee", "gpu", "", queue.OutcomeOK, 0, 0, 0)},
		testNow, false)); len(got) != 3 {
		t.Errorf("an unlabelled completion should be one row, got %q", got)
	}
}

// TestCompletionSuffixOrderSurvivesTruncation mirrors
// TestStatusLinesStaleMarkerSurvivesTruncation: a narrow terminal eats the
// end of a line, so the suffixes are ordered by what costs least to lose.
func TestCompletionSuffixOrderSurvivesTruncation(t *testing.T) {
	cs := []queue.Completion{
		testCompletion("aaa", "gpu", "a fairly long label here", queue.OutcomeExit, 9, 1000, 120000),
	}
	full := plainText(completionLines(cs, testNow, true))
	res, age := strings.Index(full, "  gpu"), strings.Index(full, "(2m ago)")
	proj := strings.Index(full, "proj [main]")
	if res < 0 || age < 0 || proj < 0 {
		t.Fatalf("expected resource, age and worktree on the row:\n%s", full)
	}
	// Without the resource an unscoped row is unattributable; the age is
	// next; the worktree costs the least to lose, so it goes last.
	if !(res < age && age < proj) {
		t.Fatalf("suffix order = resource %d, age %d, worktree %d:\n%s", res, age, proj, full)
	}
	// Squeezed, the worktree is what the truncation takes, while the
	// outcome and resource stay.
	narrow := plainText(fitFrame(completionLines(cs, testNow, true), 55, 24))
	if !strings.Contains(narrow, "exit 9") || !strings.Contains(narrow, "gpu") {
		t.Errorf("the outcome and resource must survive a narrow frame:\n%s", narrow)
	}
	if strings.Contains(narrow, "proj [main]") {
		t.Errorf("the worktree should have been the part dropped:\n%s", narrow)
	}
}

func TestCompletionLinesOmitResourceWhenScoped(t *testing.T) {
	cs := []queue.Completion{testCompletion("aaa", "gpu", "Build", queue.OutcomeOK, 0, 1000, 0)}
	if got := plainText(completionLines(cs, testNow, false)); strings.Contains(got, "gpu") {
		t.Errorf("a scoped view should not repeat the resource on every row:\n%s", got)
	}
	if got := plainText(completionLines(cs, testNow, true)); !strings.Contains(got, "gpu") {
		t.Errorf("an unscoped view needs the resource to attribute a row:\n%s", got)
	}
}

// A completion stacks exactly like a live entry: header, label, command.
func TestCompletionStacksHeaderLabelAndCommand(t *testing.T) {
	c := testCompletion("aaa", "gpu", "Build", queue.OutcomeOK, 0, 1000, 0)
	c.CommandDisplay = "make all"
	got := plainTexts(completionLines([]queue.Completion{c}, testNow, false))
	if len(got) != 5 {
		t.Fatalf("expected blank, heading, header, label and command; got %d lines: %q", len(got), got)
	}
	if !strings.HasPrefix(got[2], "  aaa") || strings.Contains(got[2], "Build") {
		t.Errorf("header row = %q, want the columns without the label", got[2])
	}
	if want := continuationIndent + `"Build"`; got[3] != want {
		t.Errorf("label line = %q, want %q", got[3], want)
	}
	if want := continuationIndent + "make all"; got[4] != want {
		t.Errorf("command line = %q, want %q", got[4], want)
	}
	// No row may leave trailing whitespace behind.
	for _, l := range got[2:] {
		if strings.TrimRight(l, " ") != l {
			t.Errorf("row has trailing whitespace: %q", l)
		}
	}
}

// A completion recorded before the command column existed reads back empty,
// and renders the one line it always did rather than a blank continuation.
func TestCompletionWithoutACommandKeepsItsOldShape(t *testing.T) {
	got := plainTexts(completionLines([]queue.Completion{
		testCompletion("aaa", "gpu", "Build", queue.OutcomeOK, 0, 1000, 0),
	}, testNow, false))
	if len(got) != 4 {
		t.Fatalf("expected blank, heading, header and label; got %d lines: %q", len(got), got)
	}
	if want := continuationIndent + `"Build"`; got[3] != want {
		t.Errorf("label line = %q, want %q", got[3], want)
	}
}

// And a finished entry sits on the same lines as a live one, which is what
// keeps a monitor frame reading as a single table.
func TestLiveAndFinishedEntriesStackAlike(t *testing.T) {
	w := testWorkload("aaa", "gpu", "Holder", "running", 5000, 0)
	w.CommandDisplay = "make all"
	c := testCompletion("bbb", "gpu", "Holder", queue.OutcomeOK, 0, 5000, 0)
	c.CommandDisplay = "make all"

	continuations := func(lines []line) []string {
		var out []string
		for _, l := range plainTexts(lines) {
			if strings.HasPrefix(l, continuationIndent) {
				out = append(out, l)
			}
		}
		return out
	}
	live := continuations(statusLines([]queue.Workload{w}, testNow, true))
	done := continuations(completionLines([]queue.Completion{c}, testNow, false))
	if strings.Join(live, "|") != strings.Join(done, "|") {
		t.Errorf("continuations differ:\n live %q\n done %q", live, done)
	}
}

// TestCompletionLinesReportAnEmptyRing covers the difference between the two
// callers: status was asked for the section by name, so it gets an answer.
func TestCompletionLinesReportAnEmptyRing(t *testing.T) {
	got := plainText(completionLines(nil, testNow, false))
	if !strings.Contains(got, completionsHeading) || !strings.Contains(got, "(none recorded)") {
		t.Errorf("an empty ring should still answer:\n%s", got)
	}
}

// TestMonitorBodyShowsCompletionsWithAnEmptyQueue guards the case the section
// is most useful in: nothing is running, and the question is what finished.
func TestMonitorBodyShowsCompletionsWithAnEmptyQueue(t *testing.T) {
	done := []queue.Completion{testCompletion("aaa", "gpu", "Build", queue.OutcomeOK, 0, 1000, 0)}
	got := plainText(monitorBody(nil, done, "", testNow, ""))
	if !strings.Contains(got, "No active workgate workloads.") {
		t.Errorf("the empty-queue message should remain:\n%s", got)
	}
	if !strings.Contains(got, completionsHeading) || !strings.Contains(got, "Build") {
		t.Errorf("completions should survive an empty queue:\n%s", got)
	}
}

func TestMonitorBodyOmitsTheSectionWhenThereAreNoCompletions(t *testing.T) {
	got := plainTexts(monitorBody([]queue.Workload{
		testWorkload("aaa", "gpu", "Holder", "running", 5000, 0),
	}, nil, "gpu", testNow, ""))
	for _, l := range got {
		if strings.Contains(l, completionsHeading) {
			t.Fatalf("the monitor was not asked for the section by name:\n%s", strings.Join(got, "\n"))
		}
	}
	if last := got[len(got)-1]; strings.TrimSpace(last) == "" {
		t.Errorf("a stray separator was left behind:\n%s", strings.Join(got, "\n"))
	}
}

func TestFmtAgo(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want string
	}{
		{0, "(just now)"},
		{59 * time.Second, "(just now)"},
		{90 * time.Second, "(1m ago)"},
		{59 * time.Minute, "(59m ago)"},
		{3 * time.Hour, "(3h ago)"},
		// The day rollover is why fmtElapsed cannot serve here: it would
		// render this as 25:00:00.
		{25 * time.Hour, "(1d ago)"},
	}
	for _, tc := range tests {
		if got := fmtAgo(tc.d); got != tc.want {
			t.Errorf("fmtAgo(%s) = %q, want %q", tc.d, got, tc.want)
		}
	}
}

func TestParseStatusArgs(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		resource string
		recent   int
		wantErr  bool
	}{
		{name: "no arguments"},
		{name: "resource only", args: []string{"gpu"}, resource: "gpu"},
		{name: "bare flag", args: []string{"--recent"}, recent: defaultRecentCount},
		{name: "flag and resource", args: []string{"gpu", "--recent"},
			resource: "gpu", recent: defaultRecentCount},
		{name: "joined count", args: []string{"--recent=5", "gpu"}, resource: "gpu", recent: 5},
		{name: "count at the cap", args: []string{"--recent=10"}, recent: 10},
		// A bare number would otherwise be taken as a resource name, since
		// "5" is a valid one; say so rather than doing the wrong thing.
		{name: "separate count", args: []string{"--recent", "5"}, wantErr: true},
		{name: "zero count", args: []string{"--recent=0"}, wantErr: true},
		{name: "count beyond what is retained", args: []string{"--recent=99"}, wantErr: true},
		{name: "unparseable count", args: []string{"--recent=lots"}, wantErr: true},
		{name: "unknown flag", args: []string{"--verbose"}, wantErr: true},
		{name: "second positional", args: []string{"gpu", "rig"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resource, recent, err := parseStatusArgs(tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got resource=%q recent=%d", resource, recent)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resource != tc.resource || recent != tc.recent {
				t.Errorf("= (%q, %d), want (%q, %d)", resource, recent, tc.resource, tc.recent)
			}
		})
	}
}

// TestCompletionsAreDroppedBeforeTheLiveQueue pins the priority a short
// window imposes. The section is last on screen, so fitFrame eats it first —
// which is the right way round: the live queue is what the tool is for.
func TestCompletionsAreDroppedBeforeTheLiveQueue(t *testing.T) {
	ws := []queue.Workload{
		testWorkload("aaa", "gpu", "Holder", "running", 5000, 0),
		testWorkload("bbb", "gpu", "Waiter", "waiting", 2000, 0),
	}
	done := []queue.Completion{
		testCompletion("ccc", "gpu", "Finished", queue.OutcomeOK, 0, 1000, 0),
	}
	body := monitorBody(ws, done, "gpu", testNow, "")
	if !strings.Contains(plainText(body), "Finished") {
		t.Fatalf("the section should be present before fitting:\n%s", plainText(body))
	}
	// One line short of the whole body: the overflow counter replaces the
	// last line, which belongs to the completions.
	got := plainText(fitFrame(body, 80, len(body)-1))
	for _, want := range []string{"Holder", "Waiter"} {
		if !strings.Contains(got, want) {
			t.Errorf("the live queue must survive height pressure, missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "Finished") {
		t.Errorf("the completion should have been the line dropped:\n%s", got)
	}
	if !strings.Contains(got, "more (enlarge the window") {
		t.Errorf("what did not fit should be counted, not silently dropped:\n%s", got)
	}
}
