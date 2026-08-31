// Command workgate provides machine-global, named, exclusive execution of
// locally launched workloads, ordered by priority and then by arrival:
//
//	workgate run <resource> [--label "<text>"] [--priority <1-5>] -- <command> [args...]
//	workgate status [<resource>]
//	workgate monitor [<resource>] [--interval <duration>]
//	workgate priority <id> <1-5>
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

const usage = `workgate - machine-global exclusive execution of local workloads

Usage:
  workgate run <resource> [--label "<description>"] [--priority <1-5>] -- <command> [args...]
  workgate status [<resource>] [--recent[=<count>]]
  workgate monitor [<resource>] [--interval <duration>]
  workgate priority <id> <1-5>

Workloads targeting the same resource execute one at a time, across all
projects and terminals on this machine: the highest priority first, and in
strict arrival order within one priority level. The resource is released
automatically when the wrapped command exits.

Priority runs from 1 (highest) to 5 (lowest) and defaults to 3. A higher
priority overtakes workloads that are still waiting, but never interrupts one
that is already running. "priority" re-prioritizes a workload that is already
queued, whichever session started it; take the id from "workgate status".

"monitor" is "status" as a live, full-screen view: it redraws once per
second until stopped with q or Ctrl+C. On a terminal, up/down select a
waiting workload and right/left raise and lower its priority.

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
	case "priority":
		return cmdPriority(args[1:])
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
	resource, label, priorityArg, argv, err := parseRunArgs(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "workgate: %v\n\n%s", err, usage)
		return 2
	}
	resource, err = queue.ValidateResource(resource)
	if err != nil {
		fmt.Fprintf(os.Stderr, "workgate: %v\n", err)
		return 2
	}
	priority := queue.PriorityDefault
	if priorityArg != "" {
		if priority, err = queue.ValidatePriority(priorityArg); err != nil {
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

	cwd, _ := os.Getwd()
	info := gitmeta.Collect(cwd)
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

	w, err := queue.Enqueue(d, resource, priority, meta)
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
		// The label is what tells one queued workload from another; with none
		// there is nothing to name it by, and the colon would dangle.
		named := ""
		if label != "" {
			named = ": " + label
		}
		if pos > 0 {
			note("Queued for %q (position %d)%s%s", resource, pos, atPriority(priority), named)
		} else {
			note("Queued for %q%s%s", resource, atPriority(priority), named)
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
		out = append(out, completionLines(done, now, resource == "")...)
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
// An entry is a header row of fixed-width columns, followed by its label and
// its command on lines of their own. Stacking them is what lets both be shown
// whole: sharing one line, they competed with the worktree and the [STALE]
// marker for the right-hand side of the screen, so the label had to be clamped
// and the command had nowhere to go at all.
//
// flagStale is the only thing the two views still disagree about. The monitor
// labels a workload whose owner has stopped heartbeating; status never has one
// to label, because it reclaims stale rows before it lists.
//
// Styles are attached to every line in both modes. They cost nothing where
// they are unwanted — status prints line.plain(), and the monitor drops them
// when stdout is redirected or NO_COLOR is set.
func statusLines(workloads []queue.Workload, now int64, flagStale bool) []line {
	return selectedStatusLines(workloads, now, flagStale, "")
}

// selectedStatusLines is statusLines with the monitor's highlight: the entry
// whose id is selected opens with a marker instead of the usual blank gutter.
// An empty selection renders exactly what statusLines renders, which is what
// `status` wants — it prints one frame and has nothing to select with.
func selectedStatusLines(workloads []queue.Workload, now int64, flagStale bool, selected string) []line {
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
			// workload, the outcome), then the timer, then the priority.
			// The timer is what the eye is usually after, and every entry
			// puts it in the same place.
			entry := entryLine(w.ID, pidSpan(w.PID),
				fmtElapsed(time.Duration(now-since)*time.Millisecond), w.Priority)
			if selected != "" && w.ID == selected {
				entry[0] = span{text: rowGutterSelected, style: styleBold}
			}
			// The monitor never removes stale rows, so it labels them instead:
			// this owner has stopped heartbeating, and the next run or status
			// will clear the entry.
			//
			// The marker goes before the worktree deliberately. A narrow
			// terminal truncates the end of the line, and losing "[STALE]"
			// would hide the very thing the row is trying to say; losing the
			// worktree, or the branch at its tail, costs far less.
			if flagStale && now-w.HeartbeatAt > queue.StaleThreshold.Milliseconds() {
				entry = append(entry, span{text: "  [STALE]", style: styleAlert})
			}
			if p := displayContext(w); p != "" {
				entry = append(entry, span{text: "  " + p})
			}
			out = append(out, entry)
			out = append(out, continuationLines(w.Label, w.CommandDisplay)...)
		}
	}
	return out
}

// completionsHeading names the recent-completions section in both views.
const completionsHeading = "LAST COMPLETED"

// completionLines renders the recent-completions section: a heading and one
// entry per completion, newest first. It is a sibling of statusLines rather
// than a mode of it — statusLines exists to group by resource and split
// RUNNING from WAITING, and completions are a flat list — but both build their
// entries the same way, so the two blocks stay on one grid.
//
// showResource adds the resource to each header row. It is driven by the
// caller's scope rather than by whether these rows happen to share a resource:
// deriving it would make the column appear and disappear between monitor
// frames as the ring turns over.
//
// An empty list still renders the heading. Callers that show the section
// unconditionally check for themselves and skip it; a caller that was asked
// for the section by name deserves an answer.
func completionLines(cs []queue.Completion, now int64, showResource bool) []line {
	out := []line{plainLine(""), styledLine(styleDim, completionsHeading)}
	if len(cs) == 0 {
		return append(out, styledLine(styleDim, "  (none recorded)"))
	}
	for _, c := range cs {
		// The timer column holds how long the workload ran, not how long
		// ago it finished: the list is already newest-first, so recency is
		// carried by the ordering, and the duration is the number that is
		// not otherwise on screen.
		entry := entryLine(c.ID, outcomeSpan(c),
			fmtElapsed(time.Duration(c.FinishedAt-c.StartedAt)*time.Millisecond), 0)
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
		if context != "" {
			agoText = fmt.Sprintf("%-*s", agoWidth, agoText)
		}
		entry = append(entry, span{text: "  " + agoText, style: styleDim})
		if context != "" {
			entry = append(entry, span{text: "  " + context})
		}
		out = append(out, entry)
		// A completion recorded before the column existed has no command, and
		// simply renders the one line it always did.
		out = append(out, continuationLines(c.Label, c.CommandDisplay)...)
	}
	return out
}

// entryLine builds the header row shared by live and finished workloads:
// "  <id> <col2> <timer> <priority>". col2 is the pid for a live workload and
// the outcome for a finished one — the pid is not merely useless once the
// process is gone, it is misleading, because pids are recycled. Keeping both
// row types on one grid is what lets a frame read as a single table. priority
// is 0 for a finished workload, whose level was not recorded and would say
// nothing about a queue it has already left; the column is still spent, so the
// grid holds.
//
// Priority is last because it is a fact about the queue, like the timer, and
// because a narrow terminal truncates from the right: the column that explains
// why the rows are in this order should come before the worktree and the
// markers a caller appends behind it.
//
// Each column is its own span so the id and col2 can carry their own style:
// they identify a row but are rarely what is wanted. The label and the command
// are not here — they are lines of their own, from continuationLines.
func entryLine(id string, col2 span, timer string, priority int) line {
	return line{
		{text: rowGutter},
		{text: fmt.Sprintf("%-*s", idWidth, id), style: styleDim},
		{text: " "},
		col2,
		{text: " " + fmt.Sprintf("%-*s", elapsedWidth, timer)},
		{text: " "},
		prioritySpan(priority),
	}
}

// continuationLines returns the rows that sit under an entry's header row: the
// label, and then the command that was run. Each is skipped when there is none
// — a workload with no label gets no label line, rather than a placeholder
// standing in for one — and only a live workload has a command to show.
//
// Neither is clamped. Ending its own line is exactly what lets a long label be
// read whole, and the terminal width, applied by fitFrame, is the only limit
// either one needs. The command is dimmed: it is the least-consulted fact on
// an entry, and it should not compete with the label above it.
func continuationLines(label, command string) []line {
	var out []line
	if label != "" {
		out = append(out, plainLine(continuationIndent+fmt.Sprintf("%q", label)))
	}
	if command != "" {
		out = append(out, styledLine(styleDim, continuationIndent+command))
	}
	return out
}

// prioritySpan renders the priority column. It always occupies priorityWidth,
// so a row without a level — a finished workload, or one written by a binary
// that predates priorities — keeps the columns after it aligned.
//
// The default level is dimmed: it is the answer for most rows and should not
// compete with the label. The top level is bold rather than red, because red
// is this palette's word for trouble ([STALE], a failed refresh) and urgent
// work is not a fault. Colour is a second channel either way — the level is
// written out.
func prioritySpan(p int) span {
	if p < queue.PriorityHighest || p > queue.PriorityLowest {
		return span{text: strings.Repeat(" ", priorityWidth)}
	}
	text := fmt.Sprintf("%-*s", priorityWidth, fmt.Sprintf("P%d", p))
	switch p {
	case queue.PriorityHighest:
		return span{text: text, style: styleBold}
	case queue.PriorityDefault:
		return span{text: text, style: styleDim}
	}
	return span{text: text}
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

// The two columns every entry line opens with. The selected row spends them on
// a marker instead of blank space: the highlight has to read on a terminal
// with colour disabled, so the text carries it and the style is only ever a
// second channel. Two columns either way, so the grid holds and a selected
// row's label sits exactly where an unselected one's does.
const (
	rowGutter         = "  "
	rowGutterSelected = "> "
)

// Column widths for an entry's header row:
// "  <id> <pid-or-outcome> <timer> <priority>".
const (
	idWidth       = 8
	pidWidth      = 10 // "pid " plus a six-digit pid
	elapsedWidth  = 8  // "HH:MM:SS"
	priorityWidth = 2  // "P" plus one level
	agoWidth      = 10 // "(just now)", and every age the retention allows
)

// defaultRecentCount is how many completions `status --recent` shows when no
// count is given, matching the monitor.
const defaultRecentCount = 3

// continuationIndent starts an entry's label and command under the pid column:
// far enough in that a continuation cannot be misread as a header row, not so
// far that a long command loses its tail to the right edge of the terminal.
var continuationIndent = strings.Repeat(" ", len(rowGutter)+idWidth+1)

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

// parseRunArgs parses:
// <resource> [--label <text>] [--priority <1-5>] -- <command> [args...]
//
// The priority is returned as written and validated by the caller, exactly as
// the resource is: this function decides what is a flag, not what is a legal
// value. Everything after "--" is the child's argv, including a --priority the
// child itself takes.
func parseRunArgs(args []string) (resource, label, priority string, argv []string, err error) {
	// Named results make a five-value error return unreadable at each site.
	bad := func(err error) (string, string, string, []string, error) {
		return "", "", "", nil, err
	}
	i := 0
	for i < len(args) {
		a := args[i]
		switch {
		case a == "--":
			argv = args[i+1:]
			if len(argv) == 0 {
				return bad(errors.New("no command given after --"))
			}
			if resource == "" {
				return bad(errors.New("missing resource name"))
			}
			return resource, label, priority, argv, nil
		case a == "--label":
			if i+1 >= len(args) {
				return bad(errors.New("--label requires a value"))
			}
			label = args[i+1]
			i += 2
		case strings.HasPrefix(a, "--label="):
			label = strings.TrimPrefix(a, "--label=")
			i++
		case a == "--priority":
			if i+1 >= len(args) {
				return bad(errors.New("--priority requires a value"))
			}
			priority = args[i+1]
			i += 2
		case strings.HasPrefix(a, "--priority="):
			priority = strings.TrimPrefix(a, "--priority=")
			i++
		case strings.HasPrefix(a, "-"):
			return bad(fmt.Errorf("unknown flag %q", a))
		case resource == "":
			resource = a
			i++
		default:
			return bad(fmt.Errorf("unexpected argument %q (child command must follow --)", a))
		}
	}
	return bad(errors.New("missing -- before the command to run"))
}

// parsePriorityArgs parses: <id> <1-5>
// Both are returned as written; cmdPriority validates them, so a bad id and a
// bad level produce their own messages rather than one about arity.
func parsePriorityArgs(args []string) (id, level string, err error) {
	var positional []string
	for _, a := range args {
		if strings.HasPrefix(a, "-") {
			return "", "", fmt.Errorf("unknown flag %q", a)
		}
		positional = append(positional, a)
	}
	switch len(positional) {
	case 0:
		return "", "", errors.New("missing workload id and priority level")
	case 1:
		return "", "", errors.New("missing priority level")
	case 2:
		return positional[0], positional[1], nil
	default:
		return "", "", fmt.Errorf("unexpected argument %q", positional[2])
	}
}

// atPriority names a non-default level for the queued notice. The default is
// left unsaid: it is what almost every workload runs at, and repeating it on
// every line would make the one line that matters harder to spot.
func atPriority(level int) string {
	if level == queue.PriorityDefault {
		return ""
	}
	return fmt.Sprintf(" at priority %d", level)
}

// cmdPriority re-prioritizes a workload that is already queued. It is the only
// command that writes a row another session owns, which is the point: the
// process that would otherwise change its own mind is blocked inside its own
// `workgate run`, so someone else has to speak for it.
//
// Nothing is signalled. The waiting process re-reads its priority on its next
// poll, so the change lands within a poll interval on its own.
func cmdPriority(args []string) int {
	idArg, levelArg, err := parsePriorityArgs(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "workgate: %v\n\n%s", err, usage)
		return 2
	}
	id, err := queue.ValidateID(idArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "workgate: %v\n", err)
		return 2
	}
	level, err := queue.ValidatePriority(levelArg)
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

	// Like status, and for the same reason: the position reported below would
	// otherwise count workloads whose owner is already gone.
	removed, err := queue.CleanupStale(d, "")
	if err != nil {
		return fail(err)
	}
	for _, r := range removed {
		note("Removed stale workload %s from %q", r.ID, r.Resource)
	}

	ch, err := queue.SetPriority(d, id, level)
	if errors.Is(err, queue.ErrNoSuchWorkload) {
		// Not an internal failure: the workload finished, or the id was
		// mistyped. Both are things the user can see and fix.
		fmt.Fprintf(os.Stderr,
			"workgate: no queued workload %s (it may have finished; see `workgate status`)\n", id)
		return 2
	}
	if err != nil {
		return fail(err)
	}

	label := ch.Label
	if label == "" {
		label = "(no label)"
	}
	switch {
	case ch.State == "running":
		note("%s %q: priority %d -> %d (already running; priority no longer affects scheduling)",
			ch.ID, label, ch.From, ch.To)
	case ch.From == ch.To:
		note("%s %q: priority already %d (position %d)", ch.ID, label, ch.To, ch.Position)
	default:
		note("%s %q: priority %d -> %d (now position %d)",
			ch.ID, label, ch.From, ch.To, ch.Position)
	}
	return 0
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}
