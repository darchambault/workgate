//go:build windows

package proc

import (
	"errors"
	"time"

	"golang.org/x/sys/windows"
)

// stillActive is what GetExitCodeProcess reports for a process that has not
// exited (STILL_ACTIVE, which x/sys/windows does not export).
const stillActive = 259

// Exited reports whether the process that held pid at startedBy has certainly
// exited: no process has that pid now, or the one that does was started after
// startedBy and is therefore a newcomer that inherited the number.
//
// The second half matters on Windows, which hands a freed pid back out
// readily. Without it a workload whose owner died while nobody was watching
// could look alive for as long as some unrelated process happened to hold its
// number.
func Exited(pid int, startedBy time.Time) bool {
	if pid <= 0 {
		return false
	}
	// The limited right is enough for both queries below, and unlike the full
	// query right it is granted across integrity levels, so an elevated owner
	// can still be judged from an ordinary terminal.
	h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		// ERROR_INVALID_PARAMETER is how OpenProcess says no such process.
		// Anything else, ERROR_ACCESS_DENIED above all, means one exists that
		// may not be looked at, which is not an answer.
		return errors.Is(err, windows.ERROR_INVALID_PARAMETER)
	}
	defer windows.CloseHandle(h)

	var code uint32
	if err := windows.GetExitCodeProcess(h, &code); err == nil && code != stillActive {
		// Exited, and still openable only because some handle keeps its
		// number reserved. (A process that exited with 259 reads as running
		// here, and falls through to the start-time check: a missed answer,
		// never a wrong one.)
		return true
	}
	var created, exited, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(h, &created, &exited, &kernel, &user); err != nil {
		return false
	}
	return time.Unix(0, created.Nanoseconds()).After(startedBy)
}
