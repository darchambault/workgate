package main

import (
	"context"
	"database/sql"
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
	// How many recent completions the monitor shows. Fixed rather than a
	// flag: the frame has a fixed height, so a larger number would push the
	// live queue — the thing the monitor exists for — into the overflow
	// counter. --interval is a flag because there is no right refresh rate;
	// three is a fine answer here.
	monitorRecentCount = 3
	// How long a notice about a keystroke stays on screen. Long enough to
	// read, short enough that a monitor left alone goes back to being only the
	// queue. A monitor on a long --interval keeps it until its next frame:
	// nothing wakes the loop just to clear a line of text.
	noticeLifetime = 3 * time.Second
)

// cmdMonitor renders a live view of the queue until stopped, and lets a
// waiting workload be re-prioritized from the view that shows why it needs to
// be.
//
// Unlike status, monitor never removes a workload: it does not reclaim stale
// rows, it labels them. Watching a resource should not change who owns it, and
// abandoned rows are already cleaned up by any run that needs the resource — a
// monitor left open overnight would otherwise be quietly making decisions
// about other sessions' workloads.
//
// The one thing it does write is a priority, for the row the user selected and
// only on the keystroke asking for it, through the same single transaction
// `workgate priority` runs. Keys are live only when both ends are a terminal,
// so a redirected monitor is still exactly the read-only view it was.
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

	// Ctrl+C is a documented way to stop a monitor — q is the other — so an
	// interrupt is a normal exit here rather than the failure code cmdRun
	// reports.
	ctx, stop := signal.NotifyContext(context.Background(), shutdownSignals()...)
	defer stop()

	sc := newScreen(os.Stdout)
	defer sc.close()

	// Deferred after the screen, so it runs before it: the keyboard is handed
	// back first, and the shell the user returns to is already echoing.
	kr := newKeyReader(os.Stdin, sc.tty)
	defer kr.close()

	scope := "all resources"
	if resource != "" {
		scope = resource
	}

	t := time.NewTicker(interval)
	defer t.Stop()

	st := &monitorState{}
	var body []line
	var workloads []queue.Workload
	for {
		read, listErr := queue.List(d, resource)
		var done []queue.Completion
		if listErr == nil {
			done, listErr = queue.RecentCompletions(d, resource, monitorRecentCount)
		}
		if listErr == nil {
			// Held over a failed refresh along with the body it rendered, so
			// brief database contention does not silently drop the selection
			// out from under the next keystroke.
			workloads = read
			st.reconcile(workloads)
			body = monitorBody(workloads, done, resource, time.Now().UnixMilli(), st.selected)
		}
		now := time.Now()
		if err := sc.draw(monitorFrame(body, frameState{
			scope:    scope,
			readErr:  listErr,
			notice:   st.noticeAt(now),
			keys:     kr.enabled,
			interval: interval,
			now:      now,
		})); err != nil {
			// Almost always a closed pipe (`| head`); nothing to report.
			return 0
		}
		select {
		case <-ctx.Done():
			return 0
		case <-t.C:
		case k := <-kr.keys():
			// Falling out of this arm re-reads and redraws at the top of the
			// loop, so a row moves the moment its priority changes rather than
			// at the next tick. The ticker is deliberately not reset: a
			// keystroke should not shift the refresh cadence.
			if k == keyQuit {
				return 0
			}
			applyKey(d, st, workloads, k, time.Now())
		}
	}
}

// monitorState is the monitor's view state: what survives a refresh without
// being in the database.
//
// The selection is an id, never an index. A priority change is meant to move
// the row it acts on, so an index would end up highlighting whatever slid into
// the vacated position — the one row the user is certainly not looking at.
type monitorState struct {
	selected string    // workload id; "" is the deliberate unselected state
	notice   string    // what the last keystroke did
	expires  time.Time // when the notice stops being shown
}

// reconcile drops a selection whose workload is no longer there to act on.
// Losing the highlight is the honest response to a workload that finished; the
// alternative is the next keystroke landing on whichever row inherited its
// place.
func (m *monitorState) reconcile(ws []queue.Workload) {
	if m.selected == "" {
		return
	}
	if _, ok := findWorkload(selectable(ws), m.selected); !ok {
		m.selected = ""
	}
}

// move walks the selection delta rows through the list.
func (m *monitorState) move(ws []queue.Workload, delta int) {
	m.selected, m.notice = moveSelection(m.selected, selectable(ws), delta), ""
}

// say sets the notice shown until noticeLifetime has passed.
func (m *monitorState) say(now time.Time, format string, args ...any) {
	m.notice, m.expires = fmt.Sprintf(format, args...), now.Add(noticeLifetime)
}

// noticeAt returns the notice, or "" once it has expired.
func (m *monitorState) noticeAt(now time.Time) string {
	if m.notice == "" || now.After(m.expires) {
		return ""
	}
	return m.notice
}

// applyPriority moves the selected workload one level and says what happened.
//
// step is the change to the level number, so right — which raises a priority,
// and level 1 is the highest — passes -1. The level is absolute and taken from
// the row this frame read, so two keystrokes cannot compound a stale reading:
// the second is computed from the refresh the first one triggered.
func (m *monitorState) applyPriority(d *sql.DB, ws []queue.Workload, step int, now time.Time) {
	if m.selected == "" {
		return
	}
	w, ok := findWorkload(selectable(ws), m.selected)
	if !ok {
		m.selected = ""
		return
	}
	level, changed := nextPriority(w.Priority, step)
	if !changed {
		// No write at all. This is also the autorepeat case — a held arrow key
		// at the end of the range must not become a stream of transactions
		// against a database other sessions are waiting on.
		m.say(now, "%s is already at priority %d", w.ID, w.Priority)
		return
	}
	ch, err := queue.SetPriority(d, m.selected, level)
	switch {
	case errors.Is(err, queue.ErrNoSuchWorkload):
		m.selected = ""
		m.say(now, "%s finished before it could be re-prioritized", w.ID)
	case err != nil:
		// stderr would tear a hole in the alternate screen, so the frame is
		// the only place this can be said.
		m.say(now, "could not change priority: %v", err)
	case ch.State == "running":
		// It acquired the resource between the read and the write. Reported
		// rather than hidden: the level really did change, and it really will
		// not matter.
		m.say(now, "%s started running; priority %d no longer affects scheduling", ch.ID, ch.To)
	default:
		m.say(now, "%s: priority %d -> %d (now position %d)", ch.ID, ch.From, ch.To, ch.Position)
	}
}

// applyKey folds one keystroke into the view state. It is a function of the
// last read rather than of the database, so the whole interaction can be
// tested without a terminal.
func applyKey(d *sql.DB, m *monitorState, ws []queue.Workload, k key, now time.Time) {
	switch k {
	case keyUp:
		m.move(ws, -1)
	case keyDown:
		m.move(ws, +1)
	case keyRight:
		m.applyPriority(d, ws, -1, now)
	case keyLeft:
		m.applyPriority(d, ws, +1, now)
	case keyClear:
		m.selected, m.notice = "", ""
	}
}

// selectable returns the rows the highlight can land on, in display order.
//
// The running workload is not one of them. It already holds the resource, and
// workgate never preempts, so re-prioritizing it would be a keystroke with
// nothing behind it — worse than one that does nothing at all, because the P
// column would move and the queue would not.
func selectable(ws []queue.Workload) []queue.Workload {
	var out []queue.Workload
	for _, w := range ws {
		if w.State != "running" {
			out = append(out, w)
		}
	}
	return out
}

// findWorkload returns the workload with this id.
func findWorkload(ws []queue.Workload, id string) (queue.Workload, bool) {
	for _, w := range ws {
		if w.ID == id {
			return w, true
		}
	}
	return queue.Workload{}, false
}

// moveSelection returns the id to highlight after moving delta rows over ws,
// which must be in display order — the order queue.List already returns, and
// the order statusLines renders without re-sorting.
//
// Both ends stop rather than wrap: stepping off the first row, or past the
// last, leaves nothing highlighted. That unselected state is a real position at
// each end rather than an error, so the key that walked off an end walks back
// on — down from nothing enters at the top, up from nothing enters at the
// bottom.
func moveSelection(selected string, ws []queue.Workload, delta int) string {
	if len(ws) == 0 {
		return ""
	}
	i := -1
	for n, w := range ws {
		if w.ID == selected {
			i = n
			break
		}
	}
	if i < 0 {
		// Nothing selected, or a selection that has since left the queue.
		if delta > 0 {
			return ws[0].ID
		}
		return ws[len(ws)-1].ID
	}
	if next := i + delta; next >= 0 && next < len(ws) {
		return ws[next].ID
	}
	return ""
}

// nextPriority applies step to a level and reports whether anything changed.
// Clamping without a write is what keeps a held-down arrow key at the end of
// the range off the database.
func nextPriority(level, step int) (int, bool) {
	next := level + step
	if next < queue.PriorityHighest || next > queue.PriorityLowest {
		return level, false
	}
	return next, true
}

// monitorBody renders the workload block, followed by the recent-completions
// section. The completions are appended even when the queue is empty: "there
// is nothing running, and here is what just finished" is the question an
// empty monitor is most often being asked.
func monitorBody(workloads []queue.Workload, done []queue.Completion, resource string, now int64, selected string) []line {
	var out []line
	switch {
	case len(workloads) > 0:
		out = selectedStatusLines(workloads, now, true, selected)
	case resource != "":
		out = []line{plainLine(fmt.Sprintf("No active workloads for %q.", resource))}
	default:
		out = []line{plainLine("No active workgate workloads.")}
	}
	// Unlike status --recent, the monitor was not asked for this section by
	// name, so an empty ring shows nothing rather than "(none recorded)".
	if len(done) > 0 {
		out = append(out, completionLines(done, now, true, resource == "")...)
	}
	return out
}

// frameState is everything a frame needs that is not the queue itself.
// A struct rather than more parameters: they are all chrome, and a caller
// passing seven positional arguments would be the harder thing to read.
type frameState struct {
	scope    string
	readErr  error
	notice   string
	keys     bool // key input is live, so the footer says what the keys do
	interval time.Duration
	now      time.Time
}

// monitorFrame assembles one frame from the most recent successful read. A
// failed read keeps the previous body on screen and adds a warning line
// instead of blanking the view or ending the session: a monitor is expected
// to stay up for hours, and brief database contention is not fatal to it.
//
// The header and footer are dimmed so the eye lands on the queue itself,
// which is the only part that changes between frames.
func monitorFrame(body []line, st frameState) []line {
	frame := []line{
		styledLine(styleDim, fmt.Sprintf("workgate monitor - %s - %s", st.scope, st.now.Format("15:04:05"))),
		plainLine(""),
	}
	if len(body) == 0 {
		body = []line{styledLine(styleDim, "Reading the queue...")}
	}
	frame = append(frame, body...)
	frame = append(frame, plainLine(""))
	if st.readErr != nil {
		frame = append(frame,
			styledLine(styleAlert, fmt.Sprintf("warning: last refresh failed: %v", st.readErr)),
			plainLine(""))
	}
	// What a keystroke did, in the frame rather than on stderr, which would
	// tear a hole in the alternate screen. It sits with the refresh warning:
	// both are things that happened to this view, not to the queue.
	if st.notice != "" {
		frame = append(frame, styledLine(styleAlert, st.notice), plainLine(""))
	}
	return append(frame, monitorFooter(st.interval, st.keys))
}

// monitorFooter names the refresh rate and how to stop, plus what the keys do
// where there are keys. One line, because the frame has a fixed height and the
// queue should have it.
//
// The hint appears only when key input is actually live, which is what keeps a
// redirected monitor byte-identical to the one that had no keys at all: the
// flag comes from whether the terminal could be put into cbreak mode, not from
// an assumption about it. Ctrl+C still stops a monitor with keys, and is
// documented; the line has room for one way out, and q is the shorter.
func monitorFooter(interval time.Duration, keys bool) line {
	hint := "Ctrl+C to stop"
	if keys {
		hint = "up/down select - right raises priority - q to stop"
	}
	return styledLine(styleDim, fmt.Sprintf("refreshing every %s - %s", interval, hint))
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
