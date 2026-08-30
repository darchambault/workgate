package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"workgate/internal/db"
	"workgate/internal/queue"
)

const (
	defaultMonitorInterval = time.Second
	// A floor on the refresh rate. Faster than this buys nothing — elapsed
	// times are shown to the second — and only adds database traffic.
	minMonitorInterval = 100 * time.Millisecond
)

// cmdMonitor renders a live view of the queue until interrupted.
//
// Unlike status, monitor never mutates the queue: it does not remove stale
// workloads, it labels them. Watching a resource should not change who owns
// it, and abandoned rows are already cleaned up by any run that needs the
// resource — a monitor left open overnight would otherwise be quietly
// making decisions about other sessions' workloads.
func cmdMonitor(args []string) int {
	resource, interval, err := parseMonitorArgs(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "workgate: %v\n\n%s", err, usage)
		return 2
	}
	if resource != "" {
		if resource, err = queue.ValidateResource(resource); err != nil {
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

	// Ctrl+C is the documented way to stop a monitor, so an interrupt is a
	// normal exit here rather than the failure code cmdRun reports.
	ctx, stop := signal.NotifyContext(context.Background(), shutdownSignals()...)
	defer stop()

	sc := newScreen(os.Stdout)
	defer sc.close()

	scope := "all resources"
	if resource != "" {
		scope = resource
	}

	t := time.NewTicker(interval)
	defer t.Stop()

	var body []line
	for {
		workloads, listErr := queue.List(d, resource)
		if listErr == nil {
			body = monitorBody(workloads, resource, time.Now().UnixMilli())
		}
		if err := sc.draw(monitorFrame(scope, body, listErr, interval, time.Now())); err != nil {
			// Almost always a closed pipe (`| head`); nothing to report.
			return 0
		}
		select {
		case <-ctx.Done():
			return 0
		case <-t.C:
		}
	}
}

// monitorBody renders the workload block, or the empty-queue message.
func monitorBody(workloads []queue.Workload, resource string, now int64) []line {
	if len(workloads) == 0 {
		if resource != "" {
			return []line{plainLine(fmt.Sprintf("No active workloads for %q.", resource))}
		}
		return []line{plainLine("No active workgate workloads.")}
	}
	return statusLines(workloads, now, true)
}

// monitorFrame assembles one frame from the most recent successful read. A
// failed read keeps the previous body on screen and adds a warning line
// instead of blanking the view or ending the session: a monitor is expected
// to stay up for hours, and brief database contention is not fatal to it.
//
// The header and footer are dimmed so the eye lands on the queue itself,
// which is the only part that changes between frames.
func monitorFrame(scope string, body []line, readErr error, interval time.Duration, now time.Time) []line {
	frame := []line{
		styledLine(styleDim, fmt.Sprintf("workgate monitor - %s - %s", scope, now.Format("15:04:05"))),
		plainLine(""),
	}
	if len(body) == 0 {
		body = []line{styledLine(styleDim, "Reading the queue...")}
	}
	frame = append(frame, body...)
	frame = append(frame, plainLine(""))
	if readErr != nil {
		frame = append(frame,
			styledLine(styleAlert, fmt.Sprintf("warning: last refresh failed: %v", readErr)),
			plainLine(""))
	}
	return append(frame, styledLine(styleDim,
		fmt.Sprintf("refreshing every %s - Ctrl+C to stop", interval)))
}

// parseMonitorArgs parses: [<resource>] [--interval <duration>]
// The resource is returned unvalidated so the caller can report a bad name
// the same way status does.
func parseMonitorArgs(args []string) (resource string, interval time.Duration, err error) {
	interval = defaultMonitorInterval
	seenResource := false
	for i := 0; i < len(args); {
		a := args[i]
		switch {
		case a == "--interval":
			if i+1 >= len(args) {
				return "", 0, errors.New("--interval requires a value")
			}
			if interval, err = parseInterval(args[i+1]); err != nil {
				return "", 0, err
			}
			i += 2
		case strings.HasPrefix(a, "--interval="):
			if interval, err = parseInterval(strings.TrimPrefix(a, "--interval=")); err != nil {
				return "", 0, err
			}
			i++
		case strings.HasPrefix(a, "-"):
			return "", 0, fmt.Errorf("unknown flag %q", a)
		case !seenResource:
			resource, seenResource = a, true
			i++
		default:
			return "", 0, fmt.Errorf("unexpected argument %q", a)
		}
	}
	return resource, interval, nil
}

func parseInterval(s string) (time.Duration, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid --interval %q (try 1s, 500ms, 2s)", s)
	}
	if d < minMonitorInterval {
		return 0, fmt.Errorf("--interval %s is too short (minimum %s)", d, minMonitorInterval)
	}
	return d, nil
}
