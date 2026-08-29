//go:build !windows

package runner

import (
	"os/exec"
	"syscall"
)

// childGuard tracks the child's process group. The child is started in its
// own group (see setupChild) so that interrupt/terminate can signal the
// whole process tree, mirroring the Windows Job Object semantics. Unlike a
// kill-on-close job object, a hard kill of workgate itself does not take
// the group down (Linux mitigates for the direct child via PR_SET_PDEATHSIG,
// see pdeathsig_linux.go); heartbeat staleness recovers the queue state.
type childGuard struct {
	pgid int
}

// setupChild places the child in a new process group so signals can be
// delivered to its whole tree. It must run before cmd.Start().
func setupChild(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	setPdeathsig(cmd.SysProcAttr)
}

func newChildGuard(pid int) (*childGuard, error) {
	// With Setpgid the child's process group ID equals its PID.
	return &childGuard{pgid: pid}, nil
}

// interrupt forwards SIGINT to the child's process group. Required on Unix:
// the child is in its own group, so the terminal's Ctrl+C never reaches it.
func (g *childGuard) interrupt() {
	syscall.Kill(-g.pgid, syscall.SIGINT)
}

func (g *childGuard) terminate() {
	syscall.Kill(-g.pgid, syscall.SIGKILL)
}

func (g *childGuard) close() {}
