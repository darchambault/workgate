//go:build !windows

package main

import (
	"os"

	"golang.org/x/sys/unix"
)

// prepareTerminal reports whether f is a terminal. Nothing has to be changed
// to use escape sequences here — unlike Windows, terminal emulators interpret
// them by default — so there is no state to restore.
func prepareTerminal(f *os.File) (bool, func()) {
	fi, err := f.Stat()
	if err != nil {
		return false, nil
	}
	return fi.Mode()&os.ModeCharDevice != 0, nil
}

// terminalSize returns the terminal's character dimensions.
func terminalSize(f *os.File) (int, int, bool) {
	ws, err := unix.IoctlGetWinsize(int(f.Fd()), unix.TIOCGWINSZ)
	if err != nil || ws.Col < 1 || ws.Row < 1 {
		return 0, 0, false
	}
	return int(ws.Col), int(ws.Row), true
}

// enableKeys puts f into cbreak mode: keystrokes arrive one at a time,
// unbuffered and unechoed. It reports whether that succeeded and returns a
// function restoring the mode it found. A pipe or a file has no terminal
// attributes, which is how a redirected stdin is detected.
//
// This is cbreak, not raw. ISIG is deliberately left on, so Ctrl+C still
// raises SIGINT and the monitor still ends through the signal path it already
// has; clearing it would leave a full-screen view that only SIGKILL could
// close. Output processing is left alone for the same kind of reason — the
// screen writes its own line endings and has no quarrel with the terminal.
func enableKeys(f *os.File) (bool, func()) {
	fd := int(f.Fd())
	prev, err := unix.IoctlGetTermios(fd, ioctlReadTermios)
	if err != nil {
		return false, nil
	}
	cbreak := *prev
	cbreak.Lflag &^= unix.ICANON | unix.ECHO
	// With ICANON off these two decide when a read returns: one byte, no timer.
	cbreak.Cc[unix.VMIN] = 1
	cbreak.Cc[unix.VTIME] = 0
	if err := unix.IoctlSetTermios(fd, ioctlWriteTermios, &cbreak); err != nil {
		return false, nil
	}
	return true, func() { unix.IoctlSetTermios(fd, ioctlWriteTermios, prev) }
}
