//go:build linux

package runner

import "syscall"

// setPdeathsig arms PR_SET_PDEATHSIG so the kernel kills the direct child
// if workgate dies hard without running any cleanup. It covers only the
// direct child (not its descendants) and fires on death of the thread that
// forked it — a best-effort orphan mitigation, not a job-object equivalent.
func setPdeathsig(attr *syscall.SysProcAttr) {
	attr.Pdeathsig = syscall.SIGKILL
}
