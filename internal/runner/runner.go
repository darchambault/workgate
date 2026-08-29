// Package runner launches the wrapped child command, ties its lifetime to
// the workgate process (via a Windows Job Object, or a dedicated process
// group on macOS/Linux), and maps outcomes to exit codes.
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
// exit on its own — after nudging it via the guard's interrupt() — before
// forcibly terminating the child's process tree.
const interruptGrace = 5 * time.Second

// Run executes argv as a direct child process (no shell interpretation),
// forwarding stdio. The child's process tree is tied to workgate through a
// platform childGuard: on Windows a kill-on-close Job Object (the OS
// terminates the tree even if workgate is hard-killed); on macOS/Linux a
// dedicated process group that workgate signals on its own termination
// paths (Linux additionally arms PR_SET_PDEATHSIG for the direct child).
//
// If ctx is canceled while the child runs, Run forwards an interrupt to
// the child, waits briefly for it to exit, then terminates its process
// tree, and returns ExitInterrupted.
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

	setupChild(cmd)

	if err := cmd.Start(); err != nil {
		return ExitCannotLaunch, fmt.Errorf("starting %s: %w", argv[0], err)
	}

	g, gerr := newChildGuard(cmd.Process.Pid)
	if gerr != nil {
		g = nil
		if warn != nil {
			warn(fmt.Sprintf("could not set up child process guard (%v); relying on heartbeat recovery", gerr))
		}
	}
	defer func() {
		if g != nil {
			g.close()
		}
	}()

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case werr := <-done:
		return childExitCode(werr), nil
	case <-ctx.Done():
		if g != nil {
			// On Unix the child's own process group never sees the
			// terminal's Ctrl+C, so forward the interrupt (no-op on
			// Windows, where the shared console already delivered it).
			g.interrupt()
		}
		select {
		case <-done:
		case <-time.After(interruptGrace):
			if g != nil {
				g.terminate()
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
