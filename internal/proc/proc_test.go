package proc

import (
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestRunningProcessHasNotExited(t *testing.T) {
	if Exited(os.Getpid(), time.Now()) {
		t.Fatal("this test process reports itself as exited")
	}
}

func TestFinishedProcessHasExited(t *testing.T) {
	// The test binary itself, told to run nothing: a child that exits at once
	// on every platform without depending on anything installed.
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	started := time.Now()
	if err := cmd.Run(); err != nil {
		t.Fatalf("running child: %v", err)
	}
	if !Exited(cmd.Process.Pid, started) {
		t.Fatalf("child pid %d has exited but was not reported as such", cmd.Process.Pid)
	}
}

// A pid is never a question about one process unless it is positive; on Unix
// the signal-0 probe would otherwise address a whole process group.
func TestNonPositivePidsAreNeverExited(t *testing.T) {
	for _, pid := range []int{0, -1} {
		if Exited(pid, time.Now()) {
			t.Errorf("Exited(%d) = true, want false", pid)
		}
	}
}
