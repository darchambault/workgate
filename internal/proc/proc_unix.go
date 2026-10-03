//go:build !windows

package proc

import (
	"errors"
	"syscall"
	"time"
)

// Exited reports whether no process has pid any more.
//
// startedBy is not consulted here. Signal 0 says whether a pid is in use, not
// who is using it, and there is no portable way to ask a process when it
// started. A pid that has been reused therefore reads as alive, which is the
// safe way to be wrong: that workload is simply left for the next run or
// status to reclaim. Unix hands pids out in sequence, so a number comes back
// round far less readily than on Windows.
func Exited(pid int, startedBy time.Time) bool {
	// Kill treats 0 and negative pids as process groups, and -1 as every
	// process there is: never a question about one pid.
	if pid <= 0 {
		return false
	}
	// EPERM means the process exists and belongs to someone else.
	return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
}
