//go:build !windows && !linux

package runner

import "syscall"

// setPdeathsig is a no-op: PR_SET_PDEATHSIG is Linux-only. On macOS a hard
// kill of workgate can orphan the child; heartbeat staleness recovers the
// queue state.
func setPdeathsig(attr *syscall.SysProcAttr) {}
