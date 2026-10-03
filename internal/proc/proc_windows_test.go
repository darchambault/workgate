//go:build windows

package proc

import (
	"os"
	"testing"
	"time"
)

// TestReusedPidHasExited covers the case start times exist for. This process
// started moments ago, so asking about whoever held its pid a day ago is
// asking about a process that must have exited for this one to get the number.
func TestReusedPidHasExited(t *testing.T) {
	if !Exited(os.Getpid(), time.Now().Add(-24*time.Hour)) {
		t.Fatal("a process started after startedBy should be taken as a newcomer to the pid")
	}
}
