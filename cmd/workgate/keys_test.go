package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"workgate/internal/queue"
)

// feed runs a byte string through a decoder and returns the keys it completed,
// which is how a terminal actually delivers them: one byte at a time.
func feed(s string) []key {
	var d keyDecoder
	var out []key
	for i := 0; i < len(s); i++ {
		if k := d.next(s[i]); k != keyNone {
			out = append(out, k)
		}
	}
	return out
}

func TestKeyDecoderRecognisesBothArrowForms(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want key
	}{
		{"\x1b[A", keyUp}, {"\x1b[B", keyDown}, {"\x1b[C", keyRight}, {"\x1b[D", keyLeft},
		// Application cursor mode, which tmux and some emulators use.
		{"\x1bOA", keyUp}, {"\x1bOB", keyDown}, {"\x1bOC", keyRight}, {"\x1bOD", keyLeft},
	} {
		got := feed(tc.in)
		if len(got) != 1 || got[0] != tc.want {
			t.Errorf("decode(%q) = %v, want [%v]", tc.in, got, tc.want)
		}
	}
}

// The vim keys are the arrows' second names and must map identically.
func TestKeyDecoderReadsVimKeysAndQuit(t *testing.T) {
	got := feed("kjhlqQ")
	want := []key{keyUp, keyDown, keyLeft, keyRight, keyQuit, keyQuit}
	if len(got) != len(want) {
		t.Fatalf("decode = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("decode[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

// A sequence is three bytes, not one packet: a slow link can split it, and the
// key must still arrive — on the final byte and not before.
func TestKeyDecoderCompletesASplitSequence(t *testing.T) {
	var d keyDecoder
	for _, b := range []byte{0x1b, '['} {
		if k := d.next(b); k != keyNone {
			t.Fatalf("byte %q completed %v, want nothing yet", b, k)
		}
	}
	if k := d.next('A'); k != keyUp {
		t.Errorf("final byte = %v, want keyUp", k)
	}
}

// Unbound sequences must do nothing at all rather than something surprising.
func TestKeyDecoderIgnoresUnboundInput(t *testing.T) {
	for _, in := range []string{
		"abc",                 // ordinary typing
		"\x1b[1;5A",           // Ctrl+Up
		"\x1b[<0;1;1M",        // a mouse report
		"\x1b[200~x\x1b[201~", // bracketed paste
		"\x03",                // Ctrl+C, which the signal handler owns
	} {
		if got := feed(in); len(got) != 0 {
			t.Errorf("decode(%q) = %v, want no keys", in, got)
		}
	}
}

// A truncated sequence must not swallow the one that follows it.
func TestKeyDecoderRestartsOnEscape(t *testing.T) {
	got := feed("\x1b\x1b[A")
	if len(got) != 1 || got[0] != keyUp {
		t.Errorf("decode = %v, want [keyUp]", got)
	}
}

// A bare ESC and the start of an arrow key are the same byte; the decoder
// reports the ambiguity and the reader resolves it at a read boundary.
func TestKeyDecoderReportsAPendingEscape(t *testing.T) {
	var d keyDecoder
	d.next(0x1b)
	if !d.pendingEscape() {
		t.Error("an ESC with nothing after it should be pending")
	}
	d.next('[')
	if d.pendingEscape() {
		t.Error("an introduced sequence is no longer a bare ESC")
	}
	d.next('A')
	if d.pendingEscape() {
		t.Error("a completed key should leave nothing pending")
	}
}

// The reader end: a pipe stands in for the terminal, which is as close as a
// test can get without a pty. It covers the two things the decoder alone
// cannot — that keys reach the channel, and that a read ending on a bare ESC
// resolves to a cleared selection rather than sitting there half-decoded.
func TestReadKeysDeliversKeysAndResolvesABareEscape(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ch := make(chan key, keysBuffered)
	go readKeys(r, ch)

	next := func(want key) {
		t.Helper()
		select {
		case got := <-ch:
			if got != want {
				t.Errorf("key = %v, want %v", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %v", want)
		}
	}

	if _, err := w.WriteString("\x1b[C"); err != nil {
		t.Fatal(err)
	}
	next(keyRight)
	if _, err := w.WriteString("\x1b"); err != nil {
		t.Fatal(err)
	}
	next(keyClear)
	w.Close()
}

// waitingQueue is a running holder followed by three waiting rows, which is
// the shape every navigation test wants.
func waitingQueue() []queue.Workload {
	return []queue.Workload{
		testWorkload("aaa", "gpu", "Holder", "running", 5000, 0),
		testWorkload("bbb", "gpu", "First", "waiting", 4000, 0),
		testWorkload("ccc", "gpu", "Second", "waiting", 3000, 0),
		testWorkload("ddd", "gpu", "Third", "waiting", 2000, 0),
	}
}

func TestMoveSelectionEntersFromEitherEnd(t *testing.T) {
	ws := selectable(waitingQueue())
	if got := moveSelection("", ws, +1); got != "bbb" {
		t.Errorf("down from nothing = %q, want the first row", got)
	}
	if got := moveSelection("", ws, -1); got != "ddd" {
		t.Errorf("up from nothing = %q, want the last row", got)
	}
	if got := moveSelection("", nil, +1); got != "" {
		t.Errorf("down over an empty list = %q, want nothing selected", got)
	}
}

// Both ends stop rather than wrap, and the unselected state is a real position
// there: the key that walked off an end walks straight back on.
func TestMoveSelectionStopsAtBothEnds(t *testing.T) {
	ws := selectable(waitingQueue())
	if got := moveSelection("bbb", ws, -1); got != "" {
		t.Errorf("up from the first row = %q, want nothing selected", got)
	}
	if got := moveSelection("ddd", ws, +1); got != "" {
		t.Errorf("down from the last row = %q, want nothing selected", got)
	}
	if got := moveSelection(moveSelection("bbb", ws, -1), ws, +1); got != "bbb" {
		t.Errorf("off the top and back = %q, want the first row again", got)
	}
	if got := moveSelection(moveSelection("ddd", ws, +1), ws, -1); got != "ddd" {
		t.Errorf("off the bottom and back = %q, want the last row again", got)
	}
}

func TestMoveSelectionWalksTheList(t *testing.T) {
	ws := selectable(waitingQueue())
	if got := moveSelection("bbb", ws, +1); got != "ccc" {
		t.Errorf("down = %q, want ccc", got)
	}
	if got := moveSelection("ccc", ws, -1); got != "bbb" {
		t.Errorf("up = %q, want bbb", got)
	}
}

// The running workload holds the resource whatever its level, so it is not a
// row a priority keystroke can mean anything on.
func TestSelectableSkipsTheRunningWorkload(t *testing.T) {
	ws := selectable(waitingQueue())
	for _, w := range ws {
		if w.State == "running" {
			t.Fatalf("the running workload is selectable: %+v", w)
		}
	}
	if len(ws) != 3 {
		t.Errorf("selectable rows = %d, want 3", len(ws))
	}
	if got := moveSelection("aaa", ws, +1); got != "bbb" {
		t.Errorf("an unselectable id should read as nothing selected, got %q", got)
	}
}

// Navigation indexes the list queue.List returns, which is also the order the
// frame renders — including across resource groups.
func TestMoveSelectionCrossesResourceGroups(t *testing.T) {
	ws := selectable([]queue.Workload{
		testWorkload("aaa", "cpu", "One", "waiting", 4000, 0),
		testWorkload("bbb", "gpu", "Two", "waiting", 3000, 0),
	})
	if got := moveSelection("aaa", ws, +1); got != "bbb" {
		t.Errorf("down across resources = %q, want bbb", got)
	}
}

func TestSelectionClearsWhenTheWorkloadDisappears(t *testing.T) {
	m := &monitorState{selected: "ccc"}
	m.reconcile(waitingQueue())
	if m.selected != "ccc" {
		t.Fatalf("a workload still queued must stay selected, got %q", m.selected)
	}
	m.reconcile([]queue.Workload{testWorkload("bbb", "gpu", "First", "waiting", 4000, 0)})
	if m.selected != "" {
		t.Errorf("selection = %q, want nothing selected once the row is gone", m.selected)
	}
}

// The highlight has to read with colour disabled, so the marker is text.
func TestSelectedRowIsMarkedWithoutColour(t *testing.T) {
	lines := plainTexts(selectedStatusLines(waitingQueue(), testNow, true, "ccc"))
	var marked []string
	for _, l := range lines {
		if strings.HasPrefix(l, rowGutterSelected) {
			marked = append(marked, l)
		}
	}
	if len(marked) != 1 {
		t.Fatalf("want exactly one marked row, got %d:\n%s", len(marked), strings.Join(lines, "\n"))
	}
	if !strings.Contains(marked[0], "ccc") {
		t.Errorf("the wrong row is marked: %q", marked[0])
	}
}

// Colour is the second channel, never the only one.
func TestSelectedRowStyleIsBold(t *testing.T) {
	lines := selectedStatusLines(waitingQueue(), testNow, true, "ccc")
	if got, ok := styleOf(lines, rowGutterSelected); !ok || got != styleBold {
		t.Errorf("marker style = %q (found %v), want bold", got, ok)
	}
}

// The marker spends the columns the gutter already had, so nothing shifts —
// and it marks the header row, leaving the label and command under it alone.
func TestSelectionKeepsTheEntryGrid(t *testing.T) {
	ws := waitingQueue()
	plainRows := plainTexts(selectedStatusLines(ws, testNow, true, ""))
	markedRows := plainTexts(selectedStatusLines(ws, testNow, true, "ccc"))
	if len(plainRows) != len(markedRows) {
		t.Fatalf("selection changed the line count: %d vs %d", len(plainRows), len(markedRows))
	}
	marked := 0
	for i := range plainRows {
		if !strings.HasPrefix(plainRows[i], rowGutter+"ccc") {
			if plainRows[i] != markedRows[i] {
				t.Errorf("an unselected line changed:\n %q\n %q", plainRows[i], markedRows[i])
			}
			continue
		}
		marked++
		if !strings.HasPrefix(markedRows[i], rowGutterSelected) {
			t.Fatalf("the selected header row is not marked: %q", markedRows[i])
		}
		if a, b := plainRows[i][len(rowGutter):], markedRows[i][len(rowGutterSelected):]; a != b {
			t.Errorf("the marker shifted the row:\n %q\n %q", a, b)
		}
	}
	if marked != 1 {
		t.Errorf("marked rows = %d, want exactly the selected header row", marked)
	}
}

// status has nothing to select with, and must never print a marker.
func TestStatusLinesNeverMarksARow(t *testing.T) {
	for _, flagStale := range []bool{false, true} {
		for _, l := range plainTexts(statusLines(waitingQueue(), testNow, flagStale)) {
			if strings.HasPrefix(l, rowGutterSelected) {
				t.Errorf("flagStale=%v: status marked a row: %q", flagStale, l)
			}
		}
	}
}

func TestNextPriorityClamps(t *testing.T) {
	for _, tc := range []struct {
		level, step, want int
		changed           bool
	}{
		{queue.PriorityDefault, -1, 2, true},
		{queue.PriorityDefault, +1, 4, true},
		{queue.PriorityHighest, -1, queue.PriorityHighest, false},
		{queue.PriorityLowest, +1, queue.PriorityLowest, false},
	} {
		got, changed := nextPriority(tc.level, tc.step)
		if got != tc.want || changed != tc.changed {
			t.Errorf("nextPriority(%d, %+d) = (%d, %v), want (%d, %v)",
				tc.level, tc.step, got, changed, tc.want, tc.changed)
		}
	}
}

// The hint appears only where the keys do, which is what keeps a redirected
// monitor's frames identical to the ones it produced before there were any.
func TestMonitorFooterOffersKeysOnlyWhenLive(t *testing.T) {
	quiet := monitorFooter(time.Second, false).plain()
	if quiet != "refreshing every 1s - Ctrl+C to stop" {
		t.Errorf("footer without keys = %q, want the original", quiet)
	}
	live := monitorFooter(time.Second, true).plain()
	for _, want := range []string{"up/down select", "right raises priority", "q to stop"} {
		if !strings.Contains(live, want) {
			t.Errorf("footer with keys missing %q: %q", want, live)
		}
	}
	// The frame has a fixed height, and the queue should have it.
	if n := len(monitorFooter(time.Second, true)); n != 1 {
		t.Errorf("footer spans = %d, want one dim line", n)
	}
}

func TestMonitorFrameShowsANotice(t *testing.T) {
	frame := monitorFrame([]line{plainLine("body")}, frameState{
		scope: "gpu", notice: "bbb is already at priority 1",
		keys: true, interval: time.Second, now: time.Now(),
	})
	text := plainText(frame)
	if !strings.Contains(text, "already at priority 1") || !strings.Contains(text, "body") {
		t.Errorf("the notice must join the body, not replace it:\n%s", text)
	}
	if got, _ := styleOf(frame, "already at priority"); got != styleAlert {
		t.Errorf("notice style = %q, want %q", got, styleAlert)
	}
}

func TestNoticeExpires(t *testing.T) {
	now := time.Now()
	m := &monitorState{}
	m.say(now, "something happened")
	if got := m.noticeAt(now.Add(noticeLifetime / 2)); got != "something happened" {
		t.Errorf("notice = %q, want it still showing", got)
	}
	if got := m.noticeAt(now.Add(2 * noticeLifetime)); got != "" {
		t.Errorf("notice = %q, want it expired", got)
	}
}

// The headline case: a promoted row moves, and the highlight moves with it,
// because the selection is an id and not a position.
func TestKeysReorderTheQueueAndKeepTheSelection(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "wg.db")
	d := openTestDB(t, dbFile)
	for _, l := range []string{"First", "Second", "Third"} {
		if _, err := queue.Enqueue(d, "gpu", queue.PriorityDefault, queue.Meta{Label: l}); err != nil {
			t.Fatal(err)
		}
	}
	list := func() []queue.Workload {
		ws, err := queue.List(d, "gpu")
		if err != nil {
			t.Fatal(err)
		}
		return ws
	}

	ws := list()
	last := ws[len(ws)-1]
	m := &monitorState{selected: last.ID}
	now := time.Now()

	// Two presses of right, each computed from a fresh read, as the loop does.
	applyKey(d, m, ws, keyRight, now)
	ws = list()
	applyKey(d, m, ws, keyRight, now)
	ws = list()

	if m.selected != last.ID {
		t.Fatalf("selection = %q, want the row that moved (%s)", m.selected, last.ID)
	}
	if ws[0].ID != last.ID {
		t.Errorf("queue order = %s first, want the promoted row %s:\n%v",
			ws[0].ID, last.ID, plainText(statusLines(ws, testNow, true)))
	}
	w, ok := findWorkload(ws, last.ID)
	if !ok || w.Priority != queue.PriorityHighest {
		t.Errorf("priority = %d, want %d", w.Priority, queue.PriorityHighest)
	}
	// And the frame marks it where it now is.
	rows := plainTexts(selectedStatusLines(ws, testNow, true, m.selected))
	for _, r := range rows {
		if strings.Contains(r, last.ID) && !strings.HasPrefix(r, rowGutterSelected) {
			t.Errorf("the promoted row lost its marker: %q", r)
		}
	}

	// A third press is a clamp: it says so and writes nothing.
	applyKey(d, m, ws, keyRight, now)
	if got := m.noticeAt(now); !strings.Contains(got, "already at priority 1") {
		t.Errorf("notice = %q, want the clamp reported", got)
	}
	if w, _ := findWorkload(list(), last.ID); w.Priority != queue.PriorityHighest {
		t.Errorf("clamped keystroke changed the level to %d", w.Priority)
	}
}

// Left is the way back down, and the level is the one on screen: two rows at
// different levels must not be able to compound each other's readings.
func TestLeftLowersThePriority(t *testing.T) {
	d := openTestDB(t, filepath.Join(t.TempDir(), "wg.db"))
	w, err := queue.Enqueue(d, "gpu", queue.PriorityDefault, queue.Meta{Label: "One"})
	if err != nil {
		t.Fatal(err)
	}
	ws, err := queue.List(d, "gpu")
	if err != nil {
		t.Fatal(err)
	}
	m := &monitorState{selected: w.ID}
	applyKey(d, m, ws, keyLeft, time.Now())

	got, _ := queue.List(d, "gpu")
	if got[0].Priority != queue.PriorityDefault+1 {
		t.Errorf("priority = %d, want %d", got[0].Priority, queue.PriorityDefault+1)
	}
}

// A workload that finished between the read and the keystroke: the selection
// goes, and nothing is written.
func TestKeyOnAVanishedWorkloadClearsTheSelection(t *testing.T) {
	d := openTestDB(t, filepath.Join(t.TempDir(), "wg.db"))
	m := &monitorState{selected: "gone01"}
	applyKey(d, m, waitingQueue(), keyRight, time.Now())
	if m.selected != "" {
		t.Errorf("selection = %q, want it cleared", m.selected)
	}
}

// Esc drops the selection without touching the queue.
func TestClearKeyDropsTheSelection(t *testing.T) {
	m := &monitorState{selected: "ccc", notice: "something"}
	applyKey(nil, m, waitingQueue(), keyClear, time.Now())
	if m.selected != "" || m.notice != "" {
		t.Errorf("state after clear = %+v, want an empty selection and no notice", m)
	}
}
