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
