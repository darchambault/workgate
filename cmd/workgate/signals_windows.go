//go:build windows

package main

import "os"

// shutdownSignals lists the signals that should release the workload and
// stop workgate. On Windows only Ctrl+C (and Ctrl+Break, both surfaced as
// os.Interrupt) is meaningfully deliverable.
func shutdownSignals() []os.Signal {
	return []os.Signal{os.Interrupt}
}
