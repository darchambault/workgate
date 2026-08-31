//go:build windows

package main

import (
	"os"
	"unicode/utf16"
	"unicode/utf8"
	"unsafe"

	"golang.org/x/sys/windows"
)

// prepareTerminal reports whether f is a console that can render the escape
// sequences the monitor uses, and returns a function undoing any console
// state it had to change.
//
// GetConsoleMode failing is how a redirected stdout is detected: a pipe or a
// file has no console mode. Virtual terminal processing is off by default in
// conhost, so it is enabled here and restored on exit; if that fails — a
// pre-1703 console — the monitor falls back to plain appended frames rather
// than spraying raw escape sequences at the user.
func prepareTerminal(f *os.File) (bool, func()) {
	h := windows.Handle(f.Fd())
	var mode uint32
	if err := windows.GetConsoleMode(h, &mode); err != nil {
		return false, nil
	}
	if mode&windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING != 0 {
		return true, nil
	}
	if err := windows.SetConsoleMode(h, mode|windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING); err != nil {
		return false, nil
	}
	return true, func() { windows.SetConsoleMode(h, mode) }
}

// terminalSize returns the console window's character dimensions. The window
// rectangle is used rather than the screen buffer size, which on Windows is
// commonly much taller than the visible window.
func terminalSize(f *os.File) (int, int, bool) {
	var info windows.ConsoleScreenBufferInfo
	if err := windows.GetConsoleScreenBufferInfo(windows.Handle(f.Fd()), &info); err != nil {
		return 0, 0, false
	}
	w := int(info.Window.Right-info.Window.Left) + 1
	h := int(info.Window.Bottom-info.Window.Top) + 1
	if w < 1 || h < 1 {
		return 0, 0, false
	}
	return w, h, true
}

// enableKeys switches the console input handle to the mode the monitor needs:
// keystrokes delivered as they are typed, without echo. It reports whether
// that succeeded and returns a function restoring the mode it found. A
// redirected stdin has no console mode, which is how it is detected.
//
// Virtual terminal input is the important part. With it the console reports
// arrow keys as the same escape sequences a Unix terminal sends, so one
// decoder serves both platforms and there is no second, record-based input
// path to keep working. It needs Windows 10 1703 or later; where SetConsoleMode
// refuses, the monitor falls back to the read-only view rather than to a
// half-working one, exactly as prepareTerminal does.
//
// ENABLE_PROCESSED_INPUT is deliberately kept, so Ctrl+C still raises the
// interrupt the monitor already handles. The mode is changed from what the
// console had rather than built from zero: everything else set there is the
// user's choice and none of the monitor's business.
func enableKeys(f *os.File) (bool, func()) {
	h := windows.Handle(f.Fd())
	var mode uint32
	if err := windows.GetConsoleMode(h, &mode); err != nil {
		return false, nil
	}
	next := mode &^ (windows.ENABLE_LINE_INPUT | windows.ENABLE_ECHO_INPUT)
	next |= windows.ENABLE_VIRTUAL_TERMINAL_INPUT | windows.ENABLE_PROCESSED_INPUT
	if err := windows.SetConsoleMode(h, next); err != nil {
		return false, nil
	}
	return true, func() { windows.SetConsoleMode(h, mode) }
}

// Reading a console. ReadConsoleInput is not in x/sys/windows, so it is bound
// here rather than pulling in a dependency for one call.
var procReadConsoleInput = windows.NewLazySystemDLL("kernel32.dll").NewProc("ReadConsoleInputW")

// keyEvent is the only INPUT_RECORD event type worth reading here.
const keyEvent = 0x0001

// inputRecord is INPUT_RECORD with KEY_EVENT_RECORD inlined — twenty bytes,
// the same size the union gives it. The other event types are shorter and are
// skipped by EventType, so nothing else needs describing.
type inputRecord struct {
	eventType       uint16
	_               uint16
	keyDown         int32
	repeatCount     uint16
	virtualKeyCode  uint16
	virtualScanCode uint16
	unicodeChar     uint16
	controlKeyState uint32
}

// readInput reads the keystrokes waiting on f.
//
// A console cannot be read as a file here. os.File reads one through
// ReadConsoleW, which in this mode does not return a partial buffer: keys are
// held until enough characters arrive to fill whatever was asked for, so a
// monitor asking for 64 bytes sees nothing at all from a few arrow presses.
// Reading the input records instead returns exactly what is queued, which is
// also what every console-mode program ends up doing.
//
// Only the characters are taken, and ENABLE_VIRTUAL_TERMINAL_INPUT has already
// turned the arrow keys into the escape sequences the shared decoder reads —
// so this is a different way of reading the same bytes, not a second input
// language. Key releases, mouse movement and window events carry no character
// and fall away here.
func readInput(f *os.File, buf []byte) (int, error) {
	h := windows.Handle(f.Fd())
	var mode uint32
	if err := windows.GetConsoleMode(h, &mode); err != nil {
		// Not a console — a pipe or a file — and an ordinary read is right.
		return f.Read(buf)
	}
	var recs [16]inputRecord
	for {
		var got uint32
		r, _, err := procReadConsoleInput.Call(uintptr(h),
			uintptr(unsafe.Pointer(&recs[0])), uintptr(len(recs)),
			uintptr(unsafe.Pointer(&got)))
		if r == 0 {
			return 0, err
		}
		n := 0
		for _, rec := range recs[:got] {
			if rec.eventType != keyEvent || rec.keyDown == 0 || rec.unicodeChar == 0 {
				continue
			}
			c := rune(rec.unicodeChar)
			// Half of a surrogate pair is never one of the monitor's keys, and
			// encoding one alone would only put nonsense in front of the decoder.
			if utf16.IsSurrogate(c) || n+utf8.RuneLen(c) > len(buf) {
				continue
			}
			n += utf8.EncodeRune(buf[n:], c)
		}
		if n > 0 {
			return n, nil
		}
		// A batch of releases or mouse movement. Waiting for the next one is
		// right: returning zero would spin the caller instead.
	}
}
