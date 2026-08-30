package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ttyScreen builds a screen with the terminal path forced on, writing to a
// file. The real path cannot be reached in a test — go test always redirects
// stdout — so the escape-sequence framing is asserted here instead.
func ttyScreen(t *testing.T) (*screen, func() string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "frames")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return &screen{f: f, tty: true, color: true}, func() string {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
}

// The alternate screen must be entered exactly once, no matter how many
// frames are drawn: re-entering it on every frame would stack buffers and
// leave the terminal unrecoverable.
func TestScreenEntersAlternateBufferOnce(t *testing.T) {
	s, read := ttyScreen(t)
	for i := 0; i < 3; i++ {
		if err := s.draw([]line{plainLine("frame")}); err != nil {
			t.Fatal(err)
		}
	}
	out := read()
	if n := strings.Count(out, escAltScreenOn); n != 1 {
		t.Errorf("entered the alternate screen %d times, want 1", n)
	}
	if n := strings.Count(out, escCursorHide); n != 1 {
		t.Errorf("hid the cursor %d times, want 1", n)
	}
	if n := strings.Count(out, escCursorHome); n != 3 {
		t.Errorf("homed the cursor %d times, want one per frame (3)", n)
	}
}

// Each frame erases to the end of every line it writes and to the end of the
// screen below itself, so a shorter frame cannot leave a taller predecessor's
// text behind.
func TestScreenFrameErasesStaleContent(t *testing.T) {
	s, read := ttyScreen(t)
	if err := s.draw([]line{plainLine("one"), plainLine("two"), plainLine("three")}); err != nil {
		t.Fatal(err)
	}
	if err := s.draw([]line{plainLine("one")}); err != nil {
		t.Fatal(err)
	}
	out := read()
	if n := strings.Count(out, escEraseLine); n != 4 {
		t.Errorf("erased %d lines, want one per written line (4)", n)
	}
	if n := strings.Count(out, escEraseBelow); n != 2 {
		t.Errorf("erased below %d times, want one per frame (2)", n)
	}
}

func TestScreenCloseRestoresTerminal(t *testing.T) {
	s, read := ttyScreen(t)
	restored := false
	s.restore = func() { restored = true }
	if err := s.draw([]line{plainLine("frame")}); err != nil {
		t.Fatal(err)
	}
	s.close()
	out := read()
	if !strings.Contains(out, escCursorShow) || !strings.Contains(out, escAltScreenOff) {
		t.Errorf("close did not restore the cursor and screen buffer:\n%q", out)
	}
	if strings.Index(out, escCursorShow) > strings.Index(out, escAltScreenOff) {
		t.Error("the cursor must be shown before leaving the alternate screen")
	}
	if !restored {
		t.Error("close did not run the platform restore hook")
	}
	// close must be safe to call again: cmdMonitor defers it, and an early
	// return path may already have run it.
	s.close()
}

// Nothing was drawn, so there is no alternate screen to leave; close must not
// emit sequences that would disturb the user's terminal.
func TestScreenCloseWithoutDrawEmitsNothing(t *testing.T) {
	s, read := ttyScreen(t)
	s.close()
	if out := read(); out != "" {
		t.Errorf("close before any frame wrote %q, want nothing", out)
	}
}

// Every styled span resets immediately after its text, so the erase that
// follows a line always runs with default attributes — otherwise a coloured
// line would paint its style across the rest of the row.
func TestScreenResetsStyleBeforeErasing(t *testing.T) {
	s, read := ttyScreen(t)
	if err := s.draw([]line{{{text: "RUNNING", style: styleRunning}}}); err != nil {
		t.Fatal(err)
	}
	out := read()
	want := styleRunning + "RUNNING" + styleReset + escEraseLine
	if !strings.Contains(out, want) {
		t.Errorf("styled line rendered as %q, want it to contain %q", out, want)
	}
}

// Mixed lines keep unstyled spans free of escape sequences.
func TestScreenRendersOnlyStyledSpans(t *testing.T) {
	s, read := ttyScreen(t)
	l := line{{text: "  "}, {text: "aaa", style: styleDim}, {text: ` "Holder"`}}
	if err := s.draw([]line{l}); err != nil {
		t.Fatal(err)
	}
	want := "  " + styleDim + "aaa" + styleReset + ` "Holder"`
	if out := read(); !strings.Contains(out, want) {
		t.Errorf("rendered %q, want it to contain %q", out, want)
	}
}

// Colour is suppressed independently of the alternate-screen machinery, so
// NO_COLOR still gets the in-place redraw — just without styling.
func TestScreenWithoutColorEmitsNoStyles(t *testing.T) {
	s, read := ttyScreen(t)
	s.color = false
	if err := s.draw([]line{{{text: "RUNNING", style: styleRunning}}}); err != nil {
		t.Fatal(err)
	}
	out := read()
	if strings.Contains(out, styleRunning) || strings.Contains(out, styleReset) {
		t.Errorf("colour was emitted with styling disabled: %q", out)
	}
	if !strings.Contains(out, "RUNNING") {
		t.Errorf("text was lost with styling disabled: %q", out)
	}
	// The frame itself is still drawn in place.
	if !strings.Contains(out, escCursorHome) {
		t.Errorf("disabling colour should not disable the redraw: %q", out)
	}
}

func TestNewScreenHonoursNoColor(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	// go test redirects stdout, so this exercises the redirected path; the
	// point is that NO_COLOR is read at all.
	if s := newScreen(os.Stdout); s.color {
		t.Error("NO_COLOR was set but colour is enabled")
	}
}

// A redirected stdout must stay free of escape sequences so the output can be
// piped, teed, or read by a test.
func TestScreenNonTTYAppendsPlainFrames(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frames")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	s := &screen{f: f, tty: false}
	if err := s.draw([]line{plainLine("first")}); err != nil {
		t.Fatal(err)
	}
	if err := s.draw([]line{{{text: "second", style: styleRunning}}}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(b), "first\n\nsecond\n"; got != want {
		t.Errorf("non-TTY output = %q, want %q", got, want)
	}
}

// The non-TTY path must not truncate: a pipe has no width, and clipping
// piped output to an assumed 80 columns would silently lose information.
func TestScreenNonTTYDoesNotTruncate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "frames")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	long := strings.Repeat("x", 300)
	s := &screen{f: f, tty: false}
	if err := s.draw([]line{plainLine(long)}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimRight(string(b), "\n"); got != long {
		t.Errorf("piped line was truncated to %d characters", len(got))
	}
}

// A closed pipe (`workgate monitor | head`) surfaces as a write error so the
// loop can stop rather than spinning forever.
func TestScreenDrawReportsWriteError(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "frames"))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	s := &screen{f: f, tty: false}
	if err := s.draw([]line{plainLine("frame")}); err == nil {
		t.Error("draw to a closed file returned no error")
	}
}
