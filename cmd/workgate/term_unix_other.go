//go:build !windows && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd

package main

import "golang.org/x/sys/unix"

// The System V spelling of the requests term_unix_bsd.go declares; see there
// for why they are split and why they are untyped.
const (
	ioctlReadTermios  = unix.TCGETS
	ioctlWriteTermios = unix.TCSETS
)
