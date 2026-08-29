// Package runner launches the wrapped child command, ties its lifetime to
// the workgate process (via a Windows Job Object), and maps outcomes to
// exit codes.
package runner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"time"
)

// Exit codes for workgate's own failure modes (GNU-style conventions).
// A successfully launched child's exit code is propagated verbatim.
const (
	ExitInternalError = 125 // workgate coordination/database failure
	ExitCannotLaunch  = 126 // command found but could not be started
	ExitNotFound      = 127 // command not found
	ExitInterrupted   = 130 // workgate interrupted while child was active
)

// interruptGrace is how long an interrupted runner waits for the child to
// exit on its own (it shares the console, so Ctrl+C reaches it too) before
// forcibly terminating the job.
const interruptGrace = 5 * time.Second

// Run executes argv as a direct child process (no shell interpretation),
// forwarding stdio. The child is placed in a kill-on-close Job Object so
// that if the workgate process dies for any reason — including a hard kill
// where no cleanup code runs — the OS terminates the child's process tree
// rather than leaving orphans.
//
// If ctx is canceled while the child runs, Run waits briefly for the child
// to exit, then terminates the job, and returns ExitInterrupted.
//
// The returned error is nil whenever the returned code is the child's own
// exit status; warn (may be nil) receives non-fatal diagnostics.
func Run(ctx context.Context, argv []string, warn func(string)) (int, error) {
	if len(argv) == 0 {
		return ExitInternalError, errors.New("no command given")
	}
	path, err := exec.LookPath(argv[0])
	if err != nil {
		return ExitNotFound, fmt.Errorf("command not found: %s", argv[0])
	}
	cmd := exec.Command(path, argv[1:]...)
	cmd.Args = argv // preserve the caller's argv[0] spelling
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return ExitCannotLaunch, fmt.Errorf("starting %s: %w", argv[0], err)
	}

	j, jerr := newJob()
	if jerr == nil {
		if aerr := j.assign(cmd.Process.Pid); aerr != nil {
			j.close()
			j = nil
			jerr = aerr
		}
	}
	if jerr != nil && warn != nil {
		warn(fmt.Sprintf("could not attach child to job object (%v); relying on heartbeat recovery", jerr))
	}
	defer func() {
		if j != nil {
			j.close()
		}
	}()

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case werr := <-done:
		return childExitCode(werr), nil
	case <-ctx.Done():
		select {
		case <-done:
		case <-time.After(interruptGrace):
			if j != nil {
				j.terminate()
			} else {
				cmd.Process.Kill()
			}
			<-done
		}
		return ExitInterrupted, ctx.Err()
	}
}

func childExitCode(err error) int {
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	return ExitCannotLaunch
}
