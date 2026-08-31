package main

import (
	"fmt"
	"strings"
	"testing"

	"workgate/internal/queue"
)

// TestStatusLinesShowsPriority checks the column in both layouts, and that the
// waiting section is printed in the order List returns — which is the order the
// workloads will actually run in.
func TestStatusLinesShowsPriority(t *testing.T) {
	holder := testWorkload("aaa", "gpu", "Holder", "running", 5000, 0)
	urgent := testWorkload("bbb", "gpu", "Urgent", "waiting", 2000, 0)
	urgent.Priority = queue.PriorityHighest
	deferred := testWorkload("ccc", "gpu", "Deferred", "waiting", 9000, 0)
	deferred.Priority = queue.PriorityLowest
	ws := []queue.Workload{holder, urgent, deferred}

	for _, flagStale := range []bool{false, true} {
		got := plainText(statusLines(ws, testNow, flagStale))
		for _, want := range []string{"P3", "P1", "P5"} {
			if !strings.Contains(got, want) {
				t.Errorf("flagStale=%v: missing %q:\n%s", flagStale, want, got)
			}
		}
		if strings.Index(got, `"Urgent"`) > strings.Index(got, `"Deferred"`) {
			t.Errorf("flagStale=%v: waiting rows must print in run order:\n%s", flagStale, got)
		}
	}
}

// A workload from a database written before priorities existed reads back as 0.
// It must still occupy the column, or every row after it shifts left.
func TestStatusLinesKeepsTheColumnForAnUnknownPriority(t *testing.T) {
	known := testWorkload("aaa", "gpu", "Known", "running", 5000, 0)
	unknown := testWorkload("bbb", "gpu", "Unknown", "waiting", 2000, 0)
	unknown.Priority = 0

	known.RepositoryRoot, unknown.RepositoryRoot = "/somewhere/Proj", "/somewhere/Proj"
	lines := plainTexts(statusLines([]queue.Workload{known, unknown}, testNow, true))
	var a, b string
	for _, l := range lines {
		switch {
		case strings.HasPrefix(l, "  aaa"):
			a = l
		case strings.HasPrefix(l, "  bbb"):
			b = l
		}
	}
	if a == "" || b == "" {
		t.Fatalf("missing header rows:\n%s", strings.Join(lines, "\n"))
	}
	if strings.Index(a, "Proj") != strings.Index(b, "Proj") {
		t.Errorf("worktree columns differ:\n known   %q\n unknown %q", a, b)
	}
}

func TestStatusLinesPriorityStyles(t *testing.T) {
	urgent := testWorkload("aaa", "gpu", "Urgent", "running", 5000, 0)
	urgent.Priority = queue.PriorityHighest
	ordinary := testWorkload("bbb", "gpu", "Ordinary", "waiting", 2000, 0)
	deferred := testWorkload("ccc", "gpu", "Deferred", "waiting", 2000, 0)
	deferred.Priority = queue.PriorityLowest
	lines := statusLines([]queue.Workload{urgent, ordinary, deferred}, testNow, true)

	for _, tc := range []struct{ text, style, desc string }{
		{"P1", styleBold, "top priority stands out"},
		{"P3", styleDim, "the default recedes"},
		{"P5", "", "other levels take the default colour"},
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
}

// A label is no longer clamped anywhere: it ends its own line, so a long one
// cannot push the worktree or the [STALE] marker off a narrow screen. See
// TestStatusLinesStaleMarkerSurvivesTruncation, which is what used to need it.
func TestLabelIsShownWholeInBothViews(t *testing.T) {
	long := strings.Repeat("x", 200)
	w := testWorkload("aaa", "gpu", long, "running", 5000, 0)
	w.RepositoryRoot = "/somewhere/MyProject"
	for _, flagStale := range []bool{false, true} {
		lines := plainTexts(statusLines([]queue.Workload{w}, testNow, flagStale))
		if !strings.Contains(strings.Join(lines, "\n"), long) {
			t.Errorf("flagStale=%v: the label was clamped:\n%s", flagStale, strings.Join(lines, "\n"))
		}
		// And it cannot have moved the header row's own columns.
		for _, l := range lines {
			if strings.HasPrefix(l, "  aaa") && !strings.HasSuffix(l, "MyProject") {
				t.Errorf("flagStale=%v: header row = %q", flagStale, l)
			}
		}
	}
}

// The label and command under an entry start where continuationIndent says,
// and that indent is computed from the column widths rather than measured.
// Adding or resizing a column is exactly when that arithmetic goes wrong.
func TestContinuationsStartAtTheComputedIndent(t *testing.T) {
	w := testWorkload("aaa", "gpu", "Holder", "running", 5000, 0)
	w.CommandDisplay = "go test ./..."
	lines := plainTexts(statusLines([]queue.Workload{w}, testNow, false))
	for i, l := range lines {
		if !strings.HasPrefix(l, "  aaa") {
			continue
		}
		if i+2 >= len(lines) {
			t.Fatal("no label and command under the header row")
		}
		// The header's own columns end exactly where a continuation begins is
		// not the claim: the indent is deliberately shorter, so a continuation
		// cannot be misread as a header row but a long command keeps its tail.
		if len(continuationIndent) >= len(l) {
			t.Errorf("continuationIndent %d is not inside the header row %q", len(continuationIndent), l)
		}
		if got := strings.Index(lines[i+1], `"Holder"`); got != len(continuationIndent) {
			t.Errorf("label starts at column %d, continuationIndent is %d", got, len(continuationIndent))
		}
		if got := strings.Index(lines[i+2], "go test"); got != len(continuationIndent) {
			t.Errorf("command starts at column %d, want %d", got, len(continuationIndent))
		}
		return
	}
	t.Fatalf("no header row found:\n%s", strings.Join(lines, "\n"))
}

func TestParseRunArgs(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		args                  []string
		resource, label, prio string
		argv                  []string
		wantErr               bool
	}{
		{name: "resource and command", args: []string{"gpu", "--", "tool"},
			resource: "gpu", argv: []string{"tool"}},
		{name: "separate label", args: []string{"gpu", "--label", "Build", "--", "tool"},
			resource: "gpu", label: "Build", argv: []string{"tool"}},
		{name: "separate priority", args: []string{"gpu", "--priority", "1", "--", "tool"},
			resource: "gpu", prio: "1", argv: []string{"tool"}},
		{name: "joined priority", args: []string{"--priority=5", "gpu", "--", "tool"},
			resource: "gpu", prio: "5", argv: []string{"tool"}},
		{name: "label and priority", args: []string{"gpu", "--label", "Build", "--priority", "2", "--", "tool", "-x"},
			resource: "gpu", label: "Build", prio: "2", argv: []string{"tool", "-x"}},
		// The parser is syntactic; the level is checked by the caller, exactly
		// as the resource name is.
		{name: "out of range is not this parser's business",
			args:     []string{"gpu", "--priority", "9", "--", "tool"},
			resource: "gpu", prio: "9", argv: []string{"tool"}},
		// The child owns everything after --, flags of its own included.
		{name: "child keeps its own priority flag",
			args:     []string{"gpu", "--", "tool", "--priority", "9"},
			resource: "gpu", argv: []string{"tool", "--priority", "9"}},
		{name: "priority without a value", args: []string{"gpu", "--priority"}, wantErr: true},
		// --priority takes the next token whatever it is, exactly as --label
		// does, so this swallows the separator and then trips over "tool".
		{name: "priority swallows the separator", args: []string{"gpu", "--priority", "--", "tool"},
			wantErr: true},
		{name: "unknown flag", args: []string{"gpu", "--nope", "--", "tool"}, wantErr: true},
		{name: "no command", args: []string{"gpu", "--"}, wantErr: true},
		{name: "no separator", args: []string{"gpu", "tool"}, wantErr: true},
		{name: "no resource", args: []string{"--", "tool"}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resource, label, prio, argv, err := parseRunArgs(tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseRunArgs(%q) = %q/%q/%q/%q, want an error",
						tc.args, resource, label, prio, argv)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseRunArgs(%q): %v", tc.args, err)
			}
			if resource != tc.resource || label != tc.label || prio != tc.prio {
				t.Errorf("got %q/%q/%q, want %q/%q/%q",
					resource, label, prio, tc.resource, tc.label, tc.prio)
			}
			if strings.Join(argv, " ") != strings.Join(tc.argv, " ") {
				t.Errorf("argv = %q, want %q", argv, tc.argv)
			}
		})
	}
}

func TestParsePriorityArgs(t *testing.T) {
	for _, tc := range []struct {
		name      string
		args      []string
		id, level string
		wantErr   bool
	}{
		{name: "id and level", args: []string{"1eabb7", "1"}, id: "1eabb7", level: "1"},
		// Validation belongs to the caller, so these parse cleanly.
		{name: "unvalidated values", args: []string{"zzz", "9"}, id: "zzz", level: "9"},
		{name: "no arguments", wantErr: true},
		{name: "id only", args: []string{"1eabb7"}, wantErr: true},
		{name: "extra argument", args: []string{"1eabb7", "1", "2"}, wantErr: true},
		{name: "unknown flag", args: []string{"--now", "1eabb7", "1"}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id, level, err := parsePriorityArgs(tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parsePriorityArgs(%q) = %q/%q, want an error", tc.args, id, level)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePriorityArgs(%q): %v", tc.args, err)
			}
			if id != tc.id || level != tc.level {
				t.Errorf("got %q/%q, want %q/%q", id, level, tc.id, tc.level)
			}
		})
	}
}

// The default level is left off the queued notice deliberately: it is what
// almost every workload runs at, and saying so on every line would bury the
// one line where the level is the news.
func TestAtPriority(t *testing.T) {
	if got := atPriority(queue.PriorityDefault); got != "" {
		t.Errorf("atPriority(default) = %q, want empty", got)
	}
	for _, level := range []int{queue.PriorityHighest, queue.PriorityLowest} {
		if got := atPriority(level); got != fmt.Sprintf(" at priority %d", level) {
			t.Errorf("atPriority(%d) = %q", level, got)
		}
	}
}
