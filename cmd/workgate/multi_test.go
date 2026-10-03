package main

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"workgate/internal/queue"
)

// multiRows is one workload holding or waiting for proj and gpu, as List
// returns it: a row per resource, both carrying the workload's id and every
// resource.
func multiRows(id, state string) []queue.Workload {
	var out []queue.Workload
	for _, r := range []string{"proj", "gpu"} {
		w := testWorkload(id, r, "Train and package", state, 4000, 0)
		w.Resources = []string{"proj", "gpu"}
		w.CommandDisplay = "python train.py"
		out = append(out, w)
	}
	return out
}

// entryUnder returns the lines of the first entry for id inside the section
// for resource: its header row and everything stacked under it.
func entryUnder(t *testing.T, lines []string, resource, id string) []string {
	t.Helper()
	in := false
	for i, l := range lines {
		if strings.HasPrefix(l, "RESOURCE: ") {
			in = l == "RESOURCE: "+resource
			continue
		}
		if in && (strings.HasPrefix(l, rowGutter+id) || strings.HasPrefix(l, rowGutterSelected+id)) {
			j := i + 1
			for j < len(lines) && strings.HasPrefix(lines[j], continuationIndent) {
				j++
			}
			return lines[i:j]
		}
	}
	t.Fatalf("no entry %s under %s:\n%s", id, resource, strings.Join(lines, "\n"))
	return nil
}

func TestMultiResourceEntryIsListedUnderEachResource(t *testing.T) {
	for _, tc := range []struct{ state, prefix string }{
		{"running", "also holds: "},
		{"waiting", "also waits for: "},
	} {
		lines := plainTexts(statusLines(multiRows("mmm", tc.state), testNow, false))
		for _, pair := range [][2]string{{"proj", "gpu"}, {"gpu", "proj"}} {
			entry := entryUnder(t, lines, pair[0], "mmm")
			want := []string{
				continuationIndent + `"Train and package"`,
				continuationIndent + tc.prefix + pair[1],
				continuationIndent + "python train.py",
			}
			if !reflect.DeepEqual(entry[1:], want) {
				t.Errorf("%s entry under %s =\n%s\nwant continuations\n%s",
					tc.state, pair[0], strings.Join(entry, "\n"), strings.Join(want, "\n"))
			}
		}
	}
}

// A workload of one resource has nothing to add, and gains no line.
func TestSingleResourceEntryHasNoAlsoLine(t *testing.T) {
	w := testWorkload("aaa", "gpu", "Holder", "running", 5000, 0)
	w.Resources = []string{"gpu"}
	if got := plainText(statusLines([]queue.Workload{w}, testNow, false)); strings.Contains(got, "also") {
		t.Errorf("single-resource entry gained an also line:\n%s", got)
	}
}

func TestAlsoLineNamesEveryOtherResource(t *testing.T) {
	if got := alsoLine("also holds: ", "b", []string{"a", "b", "c"}); got != "also holds: a, c" {
		t.Errorf("alsoLine = %q", got)
	}
	if got := alsoLine("also holds: ", "a", []string{"a"}); got != "" {
		t.Errorf("alsoLine of one = %q, want empty", got)
	}
}

func TestCompletionLinesNameEveryResource(t *testing.T) {
	c := queue.Completion{
		ID: "mmm", Resource: "gpu", Resources: []string{"proj", "gpu"},
		Label: "Train and package", Outcome: queue.OutcomeOK,
		StartedAt: testNow - 5000, FinishedAt: testNow - 1000,
		CommandDisplay: "python train.py",
	}
	// Unscoped, the workload is one entry whose resource column names both,
	// and nothing is left for an also line to add.
	unscoped := plainTexts(completionLines([]queue.Completion{c}, testNow, true))
	if !strings.Contains(unscoped[2], "  proj,gpu") {
		t.Errorf("unscoped header = %q, want it to name proj,gpu", unscoped[2])
	}
	if strings.Contains(strings.Join(unscoped, "\n"), "also") {
		t.Errorf("unscoped completion has an also line:\n%s", strings.Join(unscoped, "\n"))
	}
	// Scoped to gpu, the line says what else it held.
	scoped := plainTexts(completionLines([]queue.Completion{c}, testNow, false))
	want := []string{
		continuationIndent + `"Train and package"`,
		continuationIndent + "also held: proj",
		continuationIndent + "python train.py",
	}
	if !reflect.DeepEqual(scoped[3:], want) {
		t.Errorf("scoped completion =\n%s", strings.Join(scoped, "\n"))
	}
}

// One stop per workload for the highlight, however many queues it is in, and
// the marker on every one of its entries.
func TestAMultiResourceWaiterIsOneStopForTheSelection(t *testing.T) {
	holder := testWorkload("aaa", "gpu", "Holder", "running", 5000, 0)
	first := testWorkload("bbb", "gpu", "First", "waiting", 4500, 0)
	last := testWorkload("zzz", "proj", "Last", "waiting", 1000, 0)
	m := multiRows("mmm", "waiting")
	// List order: by resource, so gpu's queue first, then proj's.
	ws := []queue.Workload{holder, first, m[1], m[0], last}

	sel := selectable(ws)
	var ids []string
	for _, w := range sel {
		ids = append(ids, w.ID)
	}
	if want := []string{"bbb", "mmm", "zzz"}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("selectable = %v, want %v", ids, want)
	}
	if got := moveSelection("mmm", sel, +1); got != "zzz" {
		t.Errorf("down from the multi-resource waiter = %q, want zzz", got)
	}

	lines := plainTexts(selectedStatusLines(ws, testNow, true, "mmm"))
	marked := 0
	for _, l := range lines {
		if strings.HasPrefix(l, rowGutterSelected) {
			marked++
		}
	}
	if marked != 2 {
		t.Errorf("selected workload marked %d times, want once per queue (2):\n%s", marked, strings.Join(lines, "\n"))
	}
}

// Every message about one resource reads exactly as it did before there could
// be several.
func TestSingleResourceMessagesAreUnchanged(t *testing.T) {
	one := []queue.Place{{Resource: "gpu", Position: 3}}
	for _, tc := range []struct{ got, want string }{
		{quoteList([]string{"gpu"}), `"gpu"`},
		{fmtPlaces(one), `"gpu" (position 3)`},
		{positionText(one), "position 3"},
		{stillWaiting(one, 90*time.Second), `Still waiting for "gpu" (position 3, 01:30 elapsed)`},
		{strings.Join(staleNotices([]queue.StaleRemoved{{ID: "fd2b09", Resource: "gpu"}}), "|"),
			`Removed stale workload fd2b09 from "gpu"`},
	} {
		if tc.got != tc.want {
			t.Errorf("got %q, want %q", tc.got, tc.want)
		}
	}
}

func TestMultiResourceMessages(t *testing.T) {
	two := []queue.Place{{Resource: "proj", Position: 2}, {Resource: "gpu", Position: 1}}
	for _, tc := range []struct{ got, want string }{
		{quoteList([]string{"proj", "gpu"}), `"proj" and "gpu"`},
		{quoteList([]string{"a", "b", "c"}), `"a", "b" and "c"`},
		{fmtPlaces(two), `"proj" (position 2) and "gpu" (position 1)`},
		{positionText(two), `position 2 on "proj", 1 on "gpu"`},
		{stillWaiting(two, 90*time.Second), `Still waiting for "proj" (position 2) and "gpu" (position 1); 01:30 elapsed`},
	} {
		if tc.got != tc.want {
			t.Errorf("got %q, want %q", tc.got, tc.want)
		}
	}
}

// A workload reclaimed whole is one workload removed, not one per resource.
func TestStaleNoticesGroupAWorkloadsRows(t *testing.T) {
	rs := []queue.StaleRemoved{
		{ID: "mmm", Resource: "proj"}, {ID: "aaa", Resource: "gpu"}, {ID: "mmm", Resource: "seat"},
	}
	want := []string{
		`Removed stale workload mmm from "proj" and "seat"`,
		`Removed stale workload aaa from "gpu"`,
	}
	if got := staleNotices(rs); !reflect.DeepEqual(got, want) {
		t.Errorf("staleNotices = %q, want %q", got, want)
	}
	one := reclaimedNotice([]queue.StaleRemoved{{ID: "mmm", Resource: "proj"}, {ID: "mmm", Resource: "gpu"}})
	if want := `Removed stale workload mmm from "proj" and "gpu" (its owner has exited)`; one != want {
		t.Errorf("reclaimedNotice = %q, want %q", one, want)
	}
}
