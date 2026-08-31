//go:build windows

package main

import (
	"os"

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
