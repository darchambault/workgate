//go:build !windows

package main

import (
	"os"
	"syscall"
)

// shutdownSignals lists the signals that should release the workload and
// stop workgate. SIGTERM is what shells and supervisors send by default;
// SIGHUP matters because the child runs in its own process group and never
// sees the terminal's hangup — workgate must react and propagate.
func shutdownSignals() []os.Signal {
	return []os.Signal{os.Interrupt, syscall.SIGTERM, syscall.SIGHUP}
}
