// Command workgate provides machine-global, named, FIFO-exclusive execution
// of locally launched workloads:
//
//	workgate run <resource> [--label "<text>"] -- <command> [args...]
//	workgate status [<resource>]
//	workgate monitor [<resource>] [--interval <duration>]
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
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
  workgate status [<resource>] [--recent[=<count>]]
  workgate monitor [<resource>] [--interval <duration>]

Workloads targeting the same resource execute one at a time, in strict
arrival order, across all projects and terminals on this machine. The
resource is released automatically when the wrapped command exits.

"monitor" is "status" as a live, full-screen view: it redraws once per
second until interrupted with Ctrl+C, and never modifies the queue.

"--recent" appends the last few workloads that finished, with how each one
ended; the monitor always shows them.

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
	case "monitor":
		return cmdMonitor(args[1:])
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
	// The outcome the release will record. It is assigned from the runner's
	// result just before the explicit release below; the deferred paths that
	// reach a running workload are, by construction, abnormal early exits, so
	// "canceled" is the honest default. Note the closure reads this variable
	// at call time — assigning with := at the call site would shadow it and
	// silently record the default.
	outcome := queue.Outcome{Kind: queue.OutcomeCanceled}
	release := func() {
		hbCancel()
		if released {
			return
		}
		released = true
		wasRunning := w.State == "running"
		if err := queue.Release(d, w, outcome); err != nil {
			note("warning: releasing workload: %v", err)
		} else if wasRunning {
			note("Released %q", resource)
		}
	}
	defer release()

	// Interrupt (Ctrl+C or a termination signal) cancels this context; the
	// runner then forwards the interrupt to the child where the platform
	// does not deliver it directly.
	ctx, stop := signal.NotifyContext(context.Background(), shutdownSignals()...)
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
	outcome = outcomeFor(code, err) // '=', not ':=': see the closure above
	release()
	return code
}

// outcomeFor classifies a finished run for the completions ring. The
// vocabulary is deliberately narrow: it is rendered in a fixed-width column,
// and a run that could not be launched at all is honestly described by the
// 126/127 the shell sees.
func outcomeFor(code int, err error) queue.Outcome {
	switch {
	case errors.Is(err, context.Canceled):
		return queue.Outcome{Kind: queue.OutcomeCanceled}
	case code < 0 || code > 255:
		// A signalled child reports -1 on Unix; a crashing Windows child
		// reports its NTSTATUS (3221225477 for an access violation, say).
		// Neither belongs in an "exit N" column.
		return queue.Outcome{Kind: queue.OutcomeKilled}
	case code == 0:
		return queue.Outcome{Kind: queue.OutcomeOK}
	default:
		return queue.Outcome{Kind: queue.OutcomeExit, ExitCode: code}
	}
}

func cmdStatus(args []string) int {
	resource, recent, err := parseStatusArgs(args)
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
	now := time.Now().UnixMilli()
	var out []line
	if len(workloads) == 0 {
		out = []line{plainLine("No active workgate workloads.")}
	} else {
		out = statusLines(workloads, now, false)
	}
	// The recent section is appended even to an empty queue: "nothing is
	// running, and here is what just finished" is the whole question.
	if recent > 0 {
		done, err := queue.RecentCompletions(d, resource, recent)
		if err != nil {
			return fail(err)
		}
		out = append(out, completionLines(done, now, false, resource == "")...)
	}
	// status is plain text: the styles the renderers attach are only ever
	// rendered by the monitor.
	for _, l := range out {
		fmt.Println(l.plain())
	}
	return 0
}

// parseStatusArgs parses: [<resource>] [--recent[=<count>]]
// The resource is returned unvalidated so the caller can report a bad name
// the same way monitor does. A recent of 0 means the section is off, which
// is the default and keeps plain `status` output exactly as it was.
//
// Unlike --interval, --recent is a bare flag with an optional joined value.
// A duration can never be confused with a resource name, but a count can:
// under separate-value semantics `status --recent gpu` is ambiguous, and
// under bare semantics `status --recent 3` would quietly treat 3 as a
// resource name, since it is a valid one.
func parseStatusArgs(args []string) (resource string, recent int, err error) {
	seenResource := false
	for i := 0; i < len(args); {
		a := args[i]
		switch {
		case a == "--recent":
			if i+1 < len(args) && isDigits(args[i+1]) {
				return "", 0, fmt.Errorf("--recent takes no separate value; use --recent=%s", args[i+1])
			}
			recent = defaultRecentCount
			i++
		case strings.HasPrefix(a, "--recent="):
			if recent, err = parseRecent(strings.TrimPrefix(a, "--recent=")); err != nil {
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
	return resource, recent, nil
}

func parseRecent(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > queue.CompletionsPerResource {
		return 0, fmt.Errorf("invalid --recent %q (must be a count between 1 and %d)",
			s, queue.CompletionsPerResource)
	}
	return n, nil
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// statusLines renders workloads as display lines, grouped by resource and
// split into RUNNING/WAITING sections. Both `status` and `monitor` format
// through here so the two views cannot drift apart.
//
// compact folds the worktree onto the entry line and flags entries whose
// heartbeat has gone stale; the monitor needs one line per workload to fit a
// fixed-height screen. Non-compact is the `status` layout, which gives the
// worktree a continuation line of its own.
//
// Styles are attached to every line in both modes. They cost nothing where
// they are unwanted — status prints line.plain(), and the monitor drops them
// when stdout is redirected or NO_COLOR is set.
func statusLines(workloads []queue.Workload, now int64, compact bool) []line {
	byResource := map[string][]queue.Workload{}
	var order []string
	for _, w := range workloads {
		if _, seen := byResource[w.Resource]; !seen {
			order = append(order, w.Resource)
		}
		byResource[w.Resource] = append(byResource[w.Resource], w)
	}
	var out []line
	for i, res := range order {
		if i > 0 {
			out = append(out, plainLine(""))
		}
		out = append(out, styledLine(styleBold, fmt.Sprintf("RESOURCE: %s", res)))
		section := ""
		for _, w := range byResource[res] {
			header, style := "WAITING", styleWaiting
			since := w.CreatedAt
			if w.State == "running" {
				header, style = "RUNNING", styleRunning
				since = w.AcquiredAt
			}
			if header != section {
				section = header
				out = append(out, plainLine(""), styledLine(style, header))
			}
			// Fixed-width columns so a row reads down as well as across:
			// identity first (id, then the pid or, for a finished
			// workload, the outcome), then the timer, then the label. The
			// timer is what the eye is usually after, and it should not
			// have to scan past a variable-length label to reach it.
			entry := entryLine(w.ID, pidSpan(w.PID),
				fmtElapsed(time.Duration(now-since)*time.Millisecond), w.Label, compact)
			if !compact {
				out = append(out, entry)
				if p := displayContext(w); p != "" {
					out = append(out, plainLine(entryIndent+"project: "+p))
				}
				continue
			}
			// The monitor never removes stale rows, so it labels them instead:
			// this owner has stopped heartbeating, and the next run or status
			// will clear the entry.
			//
			// The marker goes before the worktree deliberately. A narrow
			// terminal truncates the end of the line, and losing "[STALE]"
			// would hide the very thing the row is trying to say; losing the
			// worktree, or the branch at its tail, costs far less.
			if now-w.HeartbeatAt > queue.StaleThreshold.Milliseconds() {
				entry = append(entry, span{text: "  [STALE]", style: styleAlert})
			}
			if p := displayContext(w); p != "" {
				entry = append(entry, span{text: "  " + p})
			}
			out = append(out, entry)
		}
	}
	return out
}

// completionsHeading names the recent-completions section in both views.
const completionsHeading = "LAST COMPLETED"

// completionLines renders the recent-completions section: a heading and one
// row per completion, newest first. It is a sibling of statusLines rather
// than a mode of it — statusLines exists to group by resource and split
// RUNNING from WAITING, and completions are a flat list — but both build
// their rows with entryLine, so the two blocks stay on one grid.
//
// showResource adds the resource to each row. It is driven by the caller's
// scope rather than by whether these rows happen to share a resource:
// deriving it would make the column appear and disappear between monitor
// frames as the ring turns over.
//
// An empty list still renders the heading. Callers that show the section
// unconditionally check for themselves and skip it; a caller that was asked
// for the section by name deserves an answer.
func completionLines(cs []queue.Completion, now int64, compact, showResource bool) []line {
	out := []line{plainLine(""), styledLine(styleDim, completionsHeading)}
	if len(cs) == 0 {
		return append(out, styledLine(styleDim, "  (none recorded)"))
	}
	for _, c := range cs {
		// The timer column holds how long the workload ran, not how long
		// ago it finished: the list is already newest-first, so recency is
		// carried by the ordering, and the duration is the number that is
		// not otherwise on screen. The label is padded in both modes here,
		// unlike a live entry: a completion always has columns after it.
		entry := entryLine(c.ID, outcomeSpan(c),
			fmtElapsed(time.Duration(c.FinishedAt-c.StartedAt)*time.Millisecond),
			c.Label, true)
		// Suffix order is truncation order, most important first. Without
		// the resource an unscoped row is unattributable; the age is next;
		// the worktree costs the least to lose.
		if showResource {
			entry = append(entry, span{text: "  " + c.Resource})
		}
		context := displayContextOf(c.RepositoryRoot, c.WorkingDirectory, c.GitBranch)
		agoText := fmtAgo(time.Duration(now-c.FinishedAt) * time.Millisecond)
		// Padded only where the worktree follows and would otherwise sit at
		// a different column on every row; where the age ends the line,
		// padding it would just leave trailing whitespace.
		if compact && context != "" {
			agoText = fmt.Sprintf("%-*s", agoWidth, agoText)
		}
		ago := span{text: "  " + agoText, style: styleDim}
		if !compact {
			out = append(out, append(entry, ago))
			if context != "" {
				out = append(out, plainLine(entryIndent+"project: "+context))
			}
			continue
		}
		entry = append(entry, ago)
		if context != "" {
			entry = append(entry, span{text: "  " + context})
		}
		out = append(out, entry)
	}
	return out
}

// entryLine builds the entry grid shared by live and finished workloads:
// "  <id> <col2> <timer> <label>". col2 is the pid for a live workload and
// the outcome for a finished one — the pid is not merely useless once the
// process is gone, it is misleading, because pids are recycled. Keeping both
// row types on one grid is what lets a frame read as a single table.
//
// Each column is its own span so the id and col2 can carry their own style:
// they identify a row but are rarely what is wanted. padLabel is for rows
// with more columns after the label; where the label ends the line, padding
// it would only leave trailing whitespace.
func entryLine(id string, col2 span, timer, label string, padLabel bool) line {
	if label == "" {
		label = "(no label)"
	}
	labelText := fmt.Sprintf("%q", label)
	if padLabel {
		labelText = fmt.Sprintf("%-*s", labelWidth, labelText)
	}
	return line{
		{text: "  "},
		{text: fmt.Sprintf("%-*s", idWidth, id), style: styleDim},
		{text: " "},
		col2,
		{text: " " + fmt.Sprintf("%-*s", elapsedWidth, timer)},
		{text: " " + labelText},
	}
}

// outcomeSpan renders the outcome in the column a live row spends on its pid.
// It is the one part of a finished row that carries meaning, so it is never
// dimmed away: the outcomes worth noticing are red, matching [STALE]. Colour
// is never the only signal — the word says it too.
//
// The vocabulary is chosen to fit pidWidth ("interrupted" would not, which is
// why the kind is "canceled"), and outcomeFor keeps a signalled or crashed
// child out of "exit N", where an NTSTATUS would run to fifteen digits. The
// truncation here is a backstop for both, since %-*s pads but never trims and
// one long word would shift every later column on the row.
func outcomeSpan(c queue.Completion) span {
	text, style := c.Outcome, styleAlert
	switch c.Outcome {
	case queue.OutcomeExit:
		text = fmt.Sprintf("exit %d", c.ExitCode)
	case queue.OutcomeOK, queue.OutcomeCanceled:
		style = styleDim
	}
	if len(text) > pidWidth {
		text = text[:pidWidth]
	}
	return span{text: fmt.Sprintf("%-*s", pidWidth, text), style: style}
}

// fmtAgo renders how long ago something finished. fmtElapsed cannot serve:
// it has no day rollover, so a completion from yesterday would read 25:03:11.
func fmtAgo(d time.Duration) string {
	switch {
	case d < time.Minute:
		return "(just now)"
	case d < time.Hour:
		return fmt.Sprintf("(%dm ago)", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("(%dh ago)", int(d.Hours()))
	default:
		return fmt.Sprintf("(%dd ago)", int(d.Hours())/24)
	}
}

// Column widths for an entry line: "  <id> <pid-or-outcome> <timer> <label>".
const (
	idWidth      = 8
	pidWidth     = 10 // "pid " plus a six-digit pid
	elapsedWidth = 8  // "HH:MM:SS"
	labelWidth   = 32
	agoWidth     = 10 // "(just now)", and every age the retention allows
)

// defaultRecentCount is how many completions `status --recent` shows when no
// count is given, matching the monitor.
const defaultRecentCount = 3

// entryIndent aligns status's project continuation line under the label.
var entryIndent = strings.Repeat(" ", 2+idWidth+1+pidWidth+1+elapsedWidth+1)

// pidSpan renders the pid column, dimmed like the id. A workload that has no
// pid yet still occupies the column, so the columns after it stay aligned.
func pidSpan(pid int64) span {
	if pid == 0 {
		return span{text: strings.Repeat(" ", pidWidth)}
	}
	return span{text: fmt.Sprintf("%-*s", pidWidth, fmt.Sprintf("pid %d", pid)), style: styleDim}
}

// displayContext names the checkout a workload runs in: the worktree
// directory, then the branch it has checked out. Agents that name every new
// worktree after the main one leave the directory alone unable to tell two
// workloads apart, and the branch is what distinguishes them.
func displayContext(w queue.Workload) string {
	return displayContextOf(w.RepositoryRoot, w.WorkingDirectory, w.GitBranch)
}

// displayContextOf is the field-level form, shared with the completions
// renderer, which reads from a Completion rather than a Workload.
func displayContextOf(repositoryRoot, workingDirectory, gitBranch string) string {
	name := displayWorktree(repositoryRoot, workingDirectory)
	// "HEAD" is what `git rev-parse --abbrev-ref` reports for a detached
	// head: a name that says nothing, so it is left off.
	if name == "" || gitBranch == "" || gitBranch == "HEAD" {
		return name
	}
	return name + " [" + gitBranch + "]"
}

// displayWorktree prefers the worktree checkout's basename and falls back to
// the working directory's. It deliberately does not resolve a linked worktree
// back to its parent repository: several worktrees of one repo is exactly the
// case these views have to tell apart.
func displayWorktree(repositoryRoot, workingDirectory string) string {
	if repositoryRoot != "" {
		return filepath.Base(repositoryRoot)
	}
	if workingDirectory != "" {
		return filepath.Base(workingDirectory)
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
