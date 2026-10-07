package main

import (
	"reflect"
	"strings"
	"testing"

	"workgate/internal/queue"
)

func TestParseRunArgsTags(t *testing.T) {
	for _, tc := range []struct {
		name      string
		args      []string
		tags      []string
		blockTags []string
		blockAll  bool
		wantErr   string
	}{
		{name: "separate tag", args: []string{"wt", "--tag", "proj", "--", "tool"},
			tags: []string{"proj"}},
		{name: "joined tag", args: []string{"--tag=proj", "wt", "--", "tool"},
			tags: []string{"proj"}},
		{name: "tags repeat in order", args: []string{"wt", "--tag", "a", "--tag=b", "--", "tool"},
			tags: []string{"a", "b"}},
		{name: "block tag", args: []string{"wt", "--block-tag", "proj", "--block-tag=x", "--", "tool"},
			blockTags: []string{"proj", "x"}},
		{name: "block all", args: []string{"wt", "--block-all", "--", "tool"}, blockAll: true},
		{name: "all three", args: []string{"wt", "--tag", "a", "--block-tag", "b", "--block-all", "--", "tool"},
			tags: []string{"a"}, blockTags: []string{"b"}, blockAll: true},
		// The child owns everything after --.
		{name: "child keeps its own tag flag", args: []string{"wt", "--", "tool", "--tag", "x"}},
		{name: "tag without a value", args: []string{"wt", "--tag"}, wantErr: "--tag requires a value"},
		{name: "block tag without a value", args: []string{"wt", "--block-tag"}, wantErr: "--block-tag requires a value"},
		{name: "block all takes no value", args: []string{"wt", "--block-all=yes", "--", "tool"}, wantErr: "unknown flag"},
		{name: "tags do not replace the resource", args: []string{"--tag", "proj", "--", "tool"},
			wantErr: "missing resource name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := parseRunArgs(tc.args)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("parseRunArgs(%q) error = %v, want one containing %q", tc.args, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseRunArgs(%q): %v", tc.args, err)
			}
			if r.resource != "wt" || !reflect.DeepEqual(r.tags, tc.tags) ||
				!reflect.DeepEqual(r.blockTags, tc.blockTags) || r.blockAll != tc.blockAll {
				t.Errorf("got %q %q %q %v, want wt %q %q %v",
					r.resource, r.tags, r.blockTags, r.blockAll, tc.tags, tc.blockTags, tc.blockAll)
			}
		})
	}
}

func TestRunClaims(t *testing.T) {
	got, err := runClaims(runArgs{resource: "WT,gpu", tags: []string{"Proj"}, blockTags: []string{"bench"}, blockAll: true})
	if err != nil {
		t.Fatal(err)
	}
	want := []queue.Claim{
		{Resource: "wt"}, {Resource: "gpu"},
		{Resource: "tag:proj", Shared: true}, {Resource: "tag:bench"},
		{Resource: queue.AllWorkloads},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("runClaims = %+v, want %+v", got, want)
	}

	for _, tc := range []struct {
		name    string
		r       runArgs
		wantErr string
	}{
		{"tag given twice", runArgs{resource: "wt", tags: []string{"p", "P"}}, "given twice"},
		{"tag given to both flags", runArgs{resource: "wt", tags: []string{"p"}, blockTags: []string{"p"}},
			"both --tag and --block-tag"},
		{"bad tag", runArgs{resource: "wt", tags: []string{"no:pe"}}, "invalid tag"},
		{"tags count toward the limit", runArgs{resource: "a,b,c", tags: []string{"p"}, blockTags: []string{"q"}},
			"at most 4"},
		// A tag and a resource of the same name are different things.
		{"tag named like a resource", runArgs{resource: "proj", tags: []string{"proj"}}, ""},
		// The block-all marker is not one of the four.
		{"block all beyond the limit", runArgs{resource: "a,b,c", tags: []string{"p"}, blockAll: true}, ""},
	} {
		_, err := runClaims(tc.r)
		if tc.wantErr == "" {
			if err != nil {
				t.Errorf("%s: %v", tc.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("%s: error = %v, want one containing %q", tc.name, err, tc.wantErr)
		}
	}
}

// tagRows is a running shared holder of tag:p and a waiting blocker of it,
// each with a worktree of its own, as List returns them.
func tagRows() []queue.Workload {
	var out []queue.Workload
	for _, w := range []struct {
		id, state, wt, mode string
	}{
		{"hhh", "running", "wt-a", queue.ModeShared},
		{"bbb", "waiting", "wt-b", queue.ModeExclusive},
	} {
		for i, r := range []string{w.wt, "tag:p"} {
			row := testWorkload(w.id, r, "", w.state, 4000, 0)
			row.Resources = []string{w.wt, "tag:p"}
			row.Modes = []string{queue.ModeExclusive, w.mode}
			row.Mode = row.Modes[i]
			out = append(out, row)
		}
	}
	return out
}

func TestTagSectionMarksTheBlocker(t *testing.T) {
	lines := plainTexts(statusLines(tagRows(), testNow, false))
	text := strings.Join(lines, "\n")
	if !strings.Contains(text, "TAG: p") {
		t.Fatalf("no tag section:\n%s", text)
	}
	var holder, blocker string
	for _, l := range lines {
		switch {
		case strings.HasPrefix(l, rowGutter+"hhh"):
			holder += l + "\n"
		case strings.HasPrefix(l, rowGutter+"bbb"):
			blocker += l + "\n"
		}
	}
	if strings.Contains(holder, "[BLOCKS TAG]") {
		t.Errorf("a shared holder is marked as blocking:\n%s", text)
	}
	if strings.Count(blocker, "[BLOCKS TAG]") != 1 {
		t.Errorf("the blocker is not marked exactly once, in its tag section:\n%s", text)
	}
	// Under its worktree, the blocker's also line says what it blocks.
	if !strings.Contains(text, "also waits for: tag:p (blocking)") {
		t.Errorf("blocker's also line does not say it blocks:\n%s", text)
	}
	if !strings.Contains(text, "also holds: tag:p\n") {
		t.Errorf("holder's also line is not the plain tag:\n%s", text)
	}
}

func TestBlockAllIsNamedInViewsAndMessages(t *testing.T) {
	var rows []queue.Workload
	for i, r := range []string{"wt", queue.AllWorkloads} {
		w := testWorkload("qqq", r, "", "waiting", 4000, 0)
		w.Resources = []string{"wt", queue.AllWorkloads}
		w.Modes = []string{queue.ModeExclusive, queue.ModeExclusive}
		w.Mode = w.Modes[i]
		rows = append(rows, w)
	}
	text := plainText(statusLines(rows, testNow, false))
	for _, want := range []string{"ALL WORKLOADS (--block-all)", "also waits for: all workloads (blocking)"} {
		if !strings.Contains(text, want) {
			t.Errorf("view lacks %q:\n%s", want, text)
		}
	}
	places := []queue.Place{{Resource: "wt", Position: 1}, {Resource: queue.AllWorkloads, Position: 3}}
	if got, want := fmtPlaces(places), `"wt" (position 1) and "all workloads" (position 3)`; got != want {
		t.Errorf("fmtPlaces = %q, want %q", got, want)
	}
}
