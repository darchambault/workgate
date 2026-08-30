// Terminal handling for the monitor's live view.
//
// Only the handful of escape sequences a full-screen redraw needs are used,
// so no terminal library is pulled in. Platform specifics — enabling virtual
// terminal processing on Windows, reading the window size — live in
// term_windows.go and term_unix.go, following the same split as signals_*.go.
package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"unicode/utf8"
)

const (
	escAltScreenOn  = "\x1b[?1049h" // switch to the alternate screen buffer
	escAltScreenOff = "\x1b[?1049l" // restore the original screen and scrollback
	escCursorHide   = "\x1b[?25l"
	escCursorShow   = "\x1b[?25h"
	escCursorHome   = "\x1b[H"
	escEraseLine    = "\x1b[K" // erase from the cursor to the end of the line
	escEraseBelow   = "\x1b[J" // erase from the cursor to the end of the screen
)

// Styles for the monitor's view. These stay deliberately restrained: dim and
// bold plus the three basic colours, which every terminal maps into its own
// palette. Nothing here assumes a light or dark background, and no style
// carries meaning on its own — the view reads correctly with colour disabled.
const (
	styleDim     = "\x1b[2m"
	styleBold    = "\x1b[1m"
	styleRunning = "\x1b[1;32m" // bold green
	styleWaiting = "\x1b[1;33m" // bold yellow
	styleAlert   = "\x1b[31m"   // red: stale entries and refresh failures
	styleReset   = "\x1b[0m"
)

// Dimensions assumed when the real terminal size cannot be determined.
const (
	fallbackWidth  = 80
	fallbackHeight = 24
)

// span is a run of text sharing one style; a line is a sequence of spans.
//
// Keeping style separate from text is what makes fitting a frame to the
// terminal safe. Every width calculation runs on plain text, and escape
// sequences are produced only when a frame is written. Measuring an
// already-coloured string would count escape bytes as columns and could
// truncate a line mid-sequence, smearing that style across the screen.
type span struct {
	text  string
	style string
}

// line is one display line.
type line []span

// plainLine builds an unstyled line.
func plainLine(s string) line { return line{{text: s}} }

// styledLine builds a line rendered entirely in one style.
func styledLine(style, s string) line { return line{{text: s, style: style}} }

// plain returns the line's text with no styling — what a redirected stdout
// and `workgate status` both want.
func (l line) plain() string {
	if len(l) == 1 {
		return l[0].text
	}
	var b strings.Builder
	for _, s := range l {
		b.WriteString(s.text)
	}
	return b.String()
}

// width is the line's length in characters, ignoring style. Runes are counted
// rather than bytes so a multi-byte label measures as what it occupies.
func (l line) width() int {
	n := 0
	for _, s := range l {
		n += utf8.RuneCountInString(s.text)
	}
	return n
}

// render writes the line, wrapping each styled span in its escape sequence
// and resetting immediately after it. Resetting per span (rather than once
// per line) means the erase that follows always runs with default attributes.
func (l line) render(b *bytes.Buffer, color bool) {
	for _, s := range l {
		if !color || s.style == "" {
			b.WriteString(s.text)
			continue
		}
		b.WriteString(s.style)
		b.WriteString(s.text)
		b.WriteString(styleReset)
	}
}

// screen draws successive frames of the monitor's view.
//
// On a terminal it takes over the alternate screen buffer and rewrites each
// frame in place, so the view never scrolls and the user's scrollback is
// untouched once it exits. When stdout is redirected — a pipe, a file, CI —
// it emits no escape sequences at all and simply appends each frame, which
// keeps `workgate monitor gpu | tee watch.log` readable.
type screen struct {
	f       *os.File
	tty     bool
	color   bool
	restore func()
	entered bool
	frames  int
	buf     bytes.Buffer
}

func newScreen(f *os.File) *screen {
	tty, restore := prepareTerminal(f)
	// NO_COLOR is the de facto convention for "styling off, output still
	// on"; any non-empty value disables it. See https://no-color.org.
	return &screen{f: f, tty: tty, color: tty && os.Getenv("NO_COLOR") == "", restore: restore}
}

// draw writes one frame. The whole frame is composed in a buffer and written
// in a single call, so a slow terminal never shows a half-drawn view. A write
// error (a closed pipe, typically) is returned so the caller can stop.
func (s *screen) draw(lines []line) error {
	s.buf.Reset()
	if !s.tty {
		if s.frames > 0 {
			s.buf.WriteString("\n")
		}
		for _, l := range lines {
			s.buf.WriteString(l.plain())
			s.buf.WriteString("\n")
		}
	} else {
		if !s.entered {
			s.entered = true
			s.buf.WriteString(escAltScreenOn)
			s.buf.WriteString(escCursorHide)
		}
		// The size is re-read every frame so resizing the window is picked
		// up without restarting the monitor.
		width, height := fallbackWidth, fallbackHeight
		if w, h, ok := terminalSize(s.f); ok {
			width, height = w, h
		}
		s.buf.WriteString(escCursorHome)
		for i, l := range fitFrame(lines, width, height) {
			if i > 0 {
				s.buf.WriteString("\r\n")
			}
			l.render(&s.buf, s.color)
			s.buf.WriteString(escEraseLine)
		}
		// Clear anything a taller previous frame left below this one.
		s.buf.WriteString(escEraseBelow)
	}
	s.frames++
	_, err := s.f.Write(s.buf.Bytes())
	return err
}

// close restores the terminal: the original screen buffer, the cursor, and
// any console mode that had to be changed to get here.
func (s *screen) close() {
	if s.entered {
		fmt.Fprint(s.f, escCursorShow+escAltScreenOff)
		s.entered = false
	}
	if s.restore != nil {
		s.restore()
	}
}

// fitFrame trims lines to the terminal's dimensions. A frame must never wrap
// or scroll: wrapping desynchronises the in-place redraw, and scrolling
// defeats the fixed full-screen view. Content that does not fit collapses
// into a trailing count rather than being dropped silently.
func fitFrame(lines []line, width, height int) []line {
	if width < 1 {
		width = fallbackWidth
	}
	if height < 1 {
		height = fallbackHeight
	}
	if len(lines) > height {
		hidden := len(lines) - (height - 1)
		kept := append([]line(nil), lines[:height-1]...)
		lines = append(kept, styledLine(styleDim,
			fmt.Sprintf("... %d more (enlarge the window to see them)", hidden)))
	}
	out := make([]line, len(lines))
	for i, l := range lines {
		limit := width
		// Writing the very last cell of the screen makes some terminals
		// scroll, so a frame filling the height stops one column short.
		if len(lines) == height && i == len(lines)-1 {
			limit = width - 1
		}
		out[i] = truncateLine(l, limit)
	}
	return out
}

// truncateLine shortens l to at most n columns, marking the cut with an
// ellipsis that inherits the style of the span it interrupted.
func truncateLine(l line, n int) line {
	if n <= 0 {
		return nil
	}
	if l.width() <= n {
		return l
	}
	if n <= 3 {
		return plainLine(strings.Repeat(".", n))
	}
	budget := n - 3
	var out line
	for _, s := range l {
		w := utf8.RuneCountInString(s.text)
		if w <= budget {
			out = append(out, s)
			budget -= w
			continue
		}
		if budget > 0 {
			out = append(out, span{text: string([]rune(s.text)[:budget]), style: s.style})
		}
		return append(out, span{text: "...", style: s.style})
	}
	return out
}
