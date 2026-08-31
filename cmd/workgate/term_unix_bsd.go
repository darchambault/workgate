//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package main

import "golang.org/x/sys/unix"

// The ioctl requests that read and write terminal attributes have no portable
// name: x/sys/unix exposes TIOCGETA only where it exists, and TCGETS only
// where that one does, so the pair is selected by build tag. See
// term_unix_other.go for the other half; between them every non-Windows
// target is covered exactly once.
//
// They are left untyped deliberately. IoctlGetTermios takes an unsigned
// request on most systems and a signed one on AIX and Solaris; an untyped
// constant converts to whichever the platform declares, so one call site in
// term_unix.go serves both.
const (
	ioctlReadTermios  = unix.TIOCGETA
	ioctlWriteTermios = unix.TIOCSETA
)
