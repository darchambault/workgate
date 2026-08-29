// Command workgate provides machine-global, named, FIFO-exclusive execution
// of locally launched workloads:
//
//	workgate run <resource> [--label "<text>"] -- <command> [args...]
//	workgate status [<resource>]
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"workgate/internal/db"
	"workgate/internal/gitmeta"
	"workgate/internal/queue"
	"workgate/internal/runner"
)

const usage = `workgate - machine-global FIFO-exclusive execution of local workloads

Usage:
  workgate run <resource> [--label "<description>"] -- <command> [args...]
  workgate status [<resource>]

Workloads targeting the same resource execute one at a time, in strict
arrival order, across all projects and terminals on this machine. The
resource is released automatically when the wrapped command exits.

Resource names: [a-zA-Z0-9][a-zA-Z0-9._-]* (case-insensitive, max 64 chars).
`

func main() {
	queue.LoadEnvOverrides()
	os.Exit(realMain(os.Args[1:]))
}

func realMain(args []string) int {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
	switch args[0] {
	case "run":
		return cmdRun(args[1:])
	case "status":
		return cmdStatus(args[1:])
	case "help", "-h", "--help":
		fmt.Print(usage)
		return 0
	default:
		fmt.Fprintf(os.Stderr, "workgate: unknown command %q\n\n%s", args[0], usage)
		return 2
	}
}

// note writes workgate's own diagnostics to stderr, keeping the child's
// stdout clean for piping.
func note(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "[workgate] "+format+"\n", a...)
}

func fail(err error) int {
	fmt.Fprintf(os.Stderr, "workgate: error: %v\n", err)
	return runner.ExitInternalError
}

func cmdRun(args []string) int {
	resource, label, argv, err := parseRunArgs(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "workgate: %v\n\n%s", err, usage)
		return 2
	}
	resource, err = queue.ValidateResource(resource)
	if err != nil {
		fmt.Fprintf(os.Stderr, "workgate: %v\n", err)
		return 2
	}

	path, err := db.Path()
	if err != nil {
		return fail(err)
	}
	d, err := db.Open(path)
	if err != nil {
		return fail(err)
	}
	defer d.Close()

	cwd, _ := os.Getwd()
	info := gitmeta.Collect(cwd)
	if label == "" {
		label = deriveLabel(argv)
	}
	meta := queue.Meta{
		Label:            label,
		PID:              info.PID,
		WorkingDirectory: info.WorkingDirectory,
		RepositoryRoot:   info.RepositoryRoot,
		GitCommonDir:     info.GitCommonDir,
		GitBranch:        info.GitBranch,
		CommandDisplay:   truncate(strings.Join(argv, " "), 200),
		Hostname:         info.Hostname,
	}

	w, err := queue.Enqueue(d, resource, meta)
	if err != nil {
		return fail(err)
	}

	// The heartbeat runs for the row's entire life — while waiting and while
	// the child runs — on its own context so cleanup controls when it stops.
	hbCtx, hbCancel := context.WithCancel(context.Background())
	queue.StartHeartbeat(hbCtx, d, w, func(err error) {
		if errors.Is(err, queue.ErrGone) {
			note("warning: this workload was removed as stale by another process")
		}
	})
	released := false
	release := func() {
		hbCancel()
		if released {
			return
		}
		released = true
		wasRunning := w.State == "running"
		if err := queue.Release(d, w); err != nil {
			note("warning: releasing workload: %v", err)
		} else if wasRunning {
			note("Released %q", resource)
		}
	}
	defer release()

	// Interrupt (Ctrl+C or termination request) cancels this context; the
	// child shares the console so it receives Ctrl+C as well.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	onStale := func(r queue.StaleRemoved) {
		note("Removed stale workload %s from %q", r.ID, r.Resource)
	}

	acquired, removed, err := queue.TryAcquire(d, w)
	for _, r := range removed {
		onStale(r)
	}
	if err != nil {
		return fail(err)
	}
	if !acquired {
		pos, perr := queue.Position(d, w)
		if perr != nil {
			pos = 0
		}
		if pos > 0 {
			note("Queued for %q (position %d): %s", resource, pos, label)
		} else {
			note("Queued for %q: %s", resource, label)
		}
		err = queue.Await(ctx, d, w, queue.AwaitEvents{
			OnStaleRemoved: onStale,
			OnLongWait: func(pos int, waited time.Duration) {
				note("Still waiting for %q (position %d, %s elapsed)", resource, pos, fmtElapsed(waited))
			},
		})
		switch {
		case errors.Is(err, context.Canceled):
			note("Interrupted while waiting for %q", resource)
			return runner.ExitInterrupted
		case errors.Is(err, queue.ErrGone):
			return fail(errors.New("workload was removed as stale by another process " +
				"(machine sleep or a long stall?); please retry"))
		case err != nil:
			return fail(err)
		}
	}
	note("Acquired %q", resource)

	code, err := runner.Run(ctx, argv, func(msg string) { note("warning: %s", msg) })
	if err != nil {
		if errors.Is(err, context.Canceled) {
			note("Interrupted; terminated child command")
		} else {
			fmt.Fprintf(os.Stderr, "workgate: %v\n", err)
		}
	}
	release()
	return code
}

func cmdStatus(args []string) int {
	resource := ""
	if len(args) > 0 {
		var err error
		resource, err = queue.ValidateResource(args[0])
		if err != nil {
			fmt.Fprintf(os.Stderr, "workgate: %v\n", err)
			return 2
		}
	}
	path, err := db.Path()
	if err != nil {
		return fail(err)
	}
	d, err := db.Open(path)
	if err != nil {
		return fail(err)
	}
	defer d.Close()

	removed, err := queue.CleanupStale(d, resource)
	if err != nil {
		return fail(err)
	}
	for _, r := range removed {
		note("Removed stale workload %s from %q", r.ID, r.Resource)
	}

	workloads, err := queue.List(d, resource)
	if err != nil {
		return fail(err)
	}
	if len(workloads) == 0 {
		fmt.Println("No active workgate workloads.")
		return 0
	}
	printStatus(workloads)
	return 0
}

func printStatus(workloads []queue.Workload) {
	now := time.Now().UnixMilli()
	byResource := map[string][]queue.Workload{}
	var order []string
	for _, w := range workloads {
		if _, seen := byResource[w.Resource]; !seen {
			order = append(order, w.Resource)
		}
		byResource[w.Resource] = append(byResource[w.Resource], w)
	}
	for i, res := range order {
		if i > 0 {
			fmt.Println()
		}
		fmt.Printf("RESOURCE: %s\n", res)
		section := ""
		for _, w := range byResource[res] {
			header := "WAITING"
			since := w.CreatedAt
			if w.State == "running" {
				header = "RUNNING"
				since = w.AcquiredAt
			}
			if header != section {
				section = header
				fmt.Printf("\n%s\n", header)
			}
			label := w.Label
			if label == "" {
				label = "(no label)"
			}
			fmt.Printf("  %-8s %-32s %s\n", w.ID, fmt.Sprintf("%q", label),
				fmtElapsed(time.Duration(now-since)*time.Millisecond))
			if p := displayProject(w); p != "" {
				fmt.Printf("           project: %s\n", p)
			}
			if w.PID != 0 {
				fmt.Printf("           pid: %d\n", w.PID)
			}
		}
	}
}

// displayProject prefers the Git-derived repository name and falls back to
// the working directory's basename.
func displayProject(w queue.Workload) string {
	if w.GitCommonDir != "" && filepath.Base(w.GitCommonDir) == ".git" {
		return filepath.Base(filepath.Dir(w.GitCommonDir))
	}
	if w.RepositoryRoot != "" {
		return filepath.Base(w.RepositoryRoot)
	}
	if w.WorkingDirectory != "" {
		return filepath.Base(w.WorkingDirectory)
	}
	return ""
}

func fmtElapsed(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	s := int(d.Seconds())
	if s >= 3600 {
		return fmt.Sprintf("%02d:%02d:%02d", s/3600, (s%3600)/60, s%60)
	}
	return fmt.Sprintf("%02d:%02d", s/60, s%60)
}

// parseRunArgs parses: <resource> [--label <text>] -- <command> [args...]
// Everything after "--" is the child's argv, preserved exactly.
func parseRunArgs(args []string) (resource, label string, argv []string, err error) {
	i := 0
	for i < len(args) {
		a := args[i]
		switch {
		case a == "--":
			argv = args[i+1:]
			if len(argv) == 0 {
				return "", "", nil, errors.New("no command given after --")
			}
			if resource == "" {
				return "", "", nil, errors.New("missing resource name")
			}
			return resource, label, argv, nil
		case a == "--label":
			if i+1 >= len(args) {
				return "", "", nil, errors.New("--label requires a value")
			}
			label = args[i+1]
			i += 2
		case strings.HasPrefix(a, "--label="):
			label = strings.TrimPrefix(a, "--label=")
			i++
		case strings.HasPrefix(a, "-"):
			return "", "", nil, fmt.Errorf("unknown flag %q", a)
		case resource == "":
			resource = a
			i++
		default:
			return "", "", nil, fmt.Errorf("unexpected argument %q (child command must follow --)", a)
		}
	}
	return "", "", nil, errors.New("missing -- before the command to run")
}

// deriveLabel builds a concise display label from the child command when no
// --label was supplied.
func deriveLabel(argv []string) string {
	parts := []string{filepath.Base(argv[0])}
	if len(argv) > 1 {
		parts = append(parts, argv[1])
	}
	if len(argv) > 2 {
		parts = append(parts, "...")
	}
	return truncate(strings.Join(parts, " "), 60)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}
