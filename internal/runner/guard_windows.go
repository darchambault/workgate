//go:build windows

package runner

import (
	"os/exec"
	"unsafe"

	"golang.org/x/sys/windows"
)

// childGuard wraps a Windows Job Object configured with
// JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE: when the last handle to the job
// closes — which the OS does automatically when this process dies, even on
// a hard kill — every process in the job is terminated. This is the
// reliable Windows mechanism for preventing orphaned child process trees.
type childGuard struct {
	handle windows.Handle
}

// setupChild needs no pre-start configuration on Windows; the child is
// assigned to the job object after it starts.
func setupChild(cmd *exec.Cmd) {}

func newChildGuard(pid int) (*childGuard, error) {
	h, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, err
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{
		BasicLimitInformation: windows.JOBOBJECT_BASIC_LIMIT_INFORMATION{
			LimitFlags: windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
		},
	}
	if _, err := windows.SetInformationJobObject(
		h, windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		windows.CloseHandle(h)
		return nil, err
	}
	g := &childGuard{handle: h}
	if err := g.assign(pid); err != nil {
		g.close()
		return nil, err
	}
	return g, nil
}

func (g *childGuard) assign(pid int) error {
	p, err := windows.OpenProcess(
		windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(pid))
	if err != nil {
		return err
	}
	defer windows.CloseHandle(p)
	return windows.AssignProcessToJobObject(g.handle, p)
}

// interrupt is a no-op: the child shares the console, so Ctrl+C already
// reaches it directly.
func (g *childGuard) interrupt() {}

func (g *childGuard) terminate() {
	windows.TerminateJobObject(g.handle, uint32(ExitInterrupted))
}

func (g *childGuard) close() {
	windows.CloseHandle(g.handle)
}
