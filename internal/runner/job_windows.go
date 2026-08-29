//go:build windows

package runner

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

// job wraps a Windows Job Object configured with
// JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE: when the last handle to the job
// closes — which the OS does automatically when this process dies, even on
// a hard kill — every process in the job is terminated. This is the
// reliable Windows mechanism for preventing orphaned child process trees.
type job struct {
	handle windows.Handle
}

func newJob() (*job, error) {
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
	return &job{handle: h}, nil
}

func (j *job) assign(pid int) error {
	p, err := windows.OpenProcess(
		windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(pid))
	if err != nil {
		return err
	}
	defer windows.CloseHandle(p)
	return windows.AssignProcessToJobObject(j.handle, p)
}

func (j *job) terminate() {
	windows.TerminateJobObject(j.handle, uint32(ExitInterrupted))
}

func (j *job) close() {
	windows.CloseHandle(j.handle)
}
