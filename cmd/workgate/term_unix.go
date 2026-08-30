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
