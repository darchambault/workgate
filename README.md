# workgate

Machine-global, named, exclusive execution of locally launched workloads,
ordered by priority and then by arrival.

`workgate` lets multiple coding-agent sessions (Codex, Claude Code, plain
terminals) — across different projects, Git repositories, and worktrees on the
same machine (Windows, macOS, or Linux) — serialize workloads that need
exclusive access to a shared machine-level resource (e.g. a GPU, a hardware
test rig, a licensed toolchain seat). It is a small local coordination
primitive, not a job scheduler: no daemon, no server, no configuration.

```sh
workgate run gpu --label "Run integration tests" -- test-runner --suite integration
```

Workloads targeting the same resource run strictly one at a time: the highest
priority first, and in arrival order within one priority level. Workloads
targeting different resources run concurrently. The resource is released
automatically when the wrapped command exits — or, if the process is killed
outright, recovered automatically via heartbeat staleness.

## Usage

```text
workgate run <resource> [--label "<description>"] [--priority <1-5>] -- <command> [args...]
workgate status [<resource>] [--recent[=<count>]]
workgate monitor [<resource>] [--interval <duration>]
workgate priority <id> <1-5>
```

- Everything after `--` is the child command, passed through verbatim
  (no shell interpretation; quote arguments for your own shell as usual).
- Resource names: `[a-zA-Z0-9][a-zA-Z0-9._-]*`, max 64 chars, case-insensitive
  (`GPU`, `Gpu`, and `gpu` share one queue).
- `--label` is diagnostic only; without it a label is derived from the command.
- `--priority` runs from `1` (highest) to `5` (lowest) and defaults to `3`;
  see [Priority](#priority) below. Unlike `--label` it is not diagnostic — it
  decides who runs next.
- `priority` re-prioritizes a workload that is already queued, whichever
  session started it. The id is the first column of `status`.
- `monitor` is `status` as a live view; see [Monitoring](#monitoring) below.
- `--recent` appends the last few workloads that finished, and how each one
  ended; see [Recent completions](#recent-completions). `monitor` always shows
  them. Without the flag, `status` output is unchanged.
- The child's exit code is propagated. Workgate's own failures use distinct
  codes: `2` usage error, `125` internal error, `126` cannot launch, `127`
  command not found, `130` interrupted.
- During `run`, workgate's own messages go to **stderr**, so the child's
  stdout stays clean for piping. `priority` confirms itself on stderr for the
  same reason. `status` and `monitor` are output in their own right and write
  to **stdout**.

Typical output:

```text
[workgate] Queued for "gpu" (position 3): Run integration tests
[workgate] Acquired "gpu"
...child output...
[workgate] Released "gpu"
```

```text
> workgate status gpu
RESOURCE: gpu

RUNNING
  49ce3e   pid 77900  00:04    P3 "Workload-A"
                                  project: MyApp [main]

WAITING
  1eabb7   pid 64732  00:02    P1 "Urgent hotfix run"
                                  project: MyApp [hotfix]
  70ce62   pid 51204  00:31    P3 "Workload-B"
                                  project: MyApp [fix-42]
```

The level-1 workload arrived last and still sorts above the waiter that has
been queued for half a minute. The running workload keeps the resource
regardless: priority decides who goes next, never who stops.

### Priority

Every workload has a level from `1` (highest) to `5` (lowest). `3` is the
default and means "ordinary": it is what a workload gets when nobody has said
otherwise, which is almost all of them.

```sh
workgate run gpu --priority 1 --label "Urgent hotfix run" -- test-runner
```

A workload that is already queued can be re-prioritized from anywhere — the
session that started it is blocked inside its own `workgate run`, so somebody
else has to speak for it:

```text
> workgate priority 1eabb7 1
[workgate] 1eabb7 "Urgent hotfix run": priority 3 -> 1 (now position 2)
```

- **Nothing is signalled.** Each waiting workload re-reads its own level on its
  next poll, so a change takes effect within about a second without sockets,
  signals, or a daemon.
- **No preemption.** A higher-priority workload waits for the running one to
  finish. Re-prioritizing a running workload is accepted and says so, but
  changes nothing — it is allowed because a workload can start between your
  reading `status` and your typing its id, and failing on that race would be
  worse than doing nothing.
- **No aging.** A level-5 workload behind a steady supply of level-1 work waits
  indefinitely. That is deliberate: the alternative makes "who runs next"
  depend on the clock, and `workgate priority` is a better answer than a
  heuristic. If something is starved, promote it.
- A waiting workload's position can therefore go **up** as well as down. Higher
  priority work arriving behind you is normal, not a stall.

### Monitoring

`workgate monitor` is the same information as a live view: it takes over the
terminal, redraws once per second, and runs until you stop it with Ctrl+C.
Use it to watch a contended resource instead of re-running `status` in a loop.

```text
workgate monitor [<resource>] [--interval <duration>]
```

```text
workgate monitor - gpu - 19:42:07

RESOURCE: gpu

RUNNING
  49ce3e   pid 77900  00:04    P3 "Workload-A"                      MyApp [main]

WAITING
  1eabb7   pid 64732  00:02    P1 "Urgent hotfix run"               MyApp [hotfix]
  70ce62   pid 51204  00:31    P3 "Workload-B"                      MyApp [fix-42]

LAST COMPLETED
  b0d41c   ok         00:03       "Workload-Z"                      (just now)  MyApp [main]
  7f2a90   exit 1     00:00       "Workload-Y"                      (2m ago)    MyApp [fix-42]
  3c4d5e   stale      00:31       "Workload-X"                      (18m ago)   Other [main]

refreshing every 1s - Ctrl+C to stop
```

- Without a resource it watches every resource, grouped, exactly as `status`
  does. `--interval` accepts any Go duration (`2s`, `500ms`); the default is
  `1s` and the minimum is `100ms`.
- The view uses the terminal's alternate screen buffer, so your scrollback is
  untouched: on exit the terminal is restored and nothing is left behind.
  Resizing the window mid-run is fine — each frame is re-fitted, and a window
  too short for the whole queue ends with a count of what did not fit rather
  than dropping it silently.
- **Monitoring never modifies the queue.** Unlike `status`, it does not remove
  abandoned workloads; it labels them `[STALE]` and leaves them to the next
  `run` or `status`. Watching a resource should not change who owns it. (The
  database is still opened normally, which applies the idempotent
  `CREATE TABLE IF NOT EXISTS` schema step and, once on a database that
  predates priorities, the `ALTER TABLE` that adds the column — "read-only"
  means no queue mutation, not zero writes.)
- A finished workload leaves the priority column blank: its level described a
  queue it has already left. The column is still spent, so live and finished
  rows stay on one grid.
- Restrained colour picks out the structure: section headings (green for
  RUNNING, yellow for WAITING), a red `[STALE]` and a red failing outcome,
  and dimmed chrome so the eye lands on the workloads. Colour is never the
  only signal — every state it distinguishes is also written out — and setting
  `NO_COLOR` to any value turns it off while keeping the live redraw.
- Redirected output (`workgate monitor gpu | tee watch.log`) emits no escape
  sequences at all: frames are simply appended, one per interval, unstyled
  and untruncated.
- Ctrl+C is how a monitor is meant to end, so it exits `0`. The `130`
  interrupted code listed above belongs to `run`, where an interrupt cuts a
  child command short.

### Recent completions

A workload that finishes disappears from the queue, which leaves the question
a monitor is most often being asked — *did my build finish, and how?* — with
nowhere to look. `monitor` therefore ends with a `LAST COMPLETED` section, and
`status --recent` shows the same thing on demand:

```text
> workgate status gpu --recent
No active workgate workloads.

LAST COMPLETED
  b0d41c   ok         00:03    "Workload-Z"                      (just now)
                               project: MyApp [main]
  7f2a90   exit 1     00:00    "Workload-Y"                      (2m ago)
                               project: MyApp [fix-42]
```

- A finished row sits on the same columns as a live one, so a frame reads as
  one table. The column a live workload spends on its pid carries the
  **outcome** instead — a pid is not merely useless once the process is gone,
  it is misleading, because pids get recycled. The timer column carries how
  long the workload **ran**; the list is already newest-first, so the age in
  brackets is what says when.
- Outcomes are `ok`, `exit <code>`, `killed` (signalled, or crashed),
  `canceled` (interrupted mid-run), and `stale` (the owner stopped
  heartbeating and was reclaimed). Everything that held the resource is
  recorded, so a hard-killed workload leaves a trace rather than vanishing —
  though, since `monitor` never modifies the queue, such a workload reads
  `[STALE]` in the live section until the next `run` or `status` reclaims it
  and moves it down here.
- `monitor` always shows three. `status` shows none unless asked: `--recent`
  for three, `--recent=<count>` for up to ten.
- The section is last on screen, so a window too short for everything drops it
  before the live queue — which is the right way round, the queue being what
  the tool is for.
- This is a bounded ring, not history: at most ten completions per resource,
  expiring after a day, with nothing to query them beyond the last few. See
  [Scope](#scope).

## Scope

Coordination state lives in one machine-user-global SQLite database, in the
platform's per-user cache directory (created on demand):

```text
Windows   %LOCALAPPDATA%\Workgate\workgate.db
macOS     ~/Library/Caches/Workgate/workgate.db
Linux     $XDG_CACHE_HOME/Workgate/workgate.db   (default ~/.cache/...)
```

The database holds live coordination rows plus a small bounded ring of recent
completions (at most ten per resource, expiring after a day — see
[Recent completions](#recent-completions)). Nothing in it is durable history,
and nothing reads the ring to make a coordination decision, so deleting the
file merely resets an idle queue and forgets what finished recently. Every
`workgate` process for the current OS user shares it, so `gpu` means the same resource
no matter which project, repository, worktree, or non-Git directory invoked
it. Git information (repo root, common dir, branch)
is recorded as diagnostic metadata only — it never affects locking. If a
project-specific resource is needed, encode it in the name
(e.g. `myproject-build`).

## Build

Requires Go 1.25+ (no CGO; SQLite via pure-Go `modernc.org/sqlite`).

```sh
go build -o workgate ./cmd/workgate
```

(On Windows, use `-o workgate.exe`.) The result is a single self-contained
binary — no runtime, no shared libraries.

## Install (Windows)

From the repository root:

```powershell
.\install.ps1
```

This runs the tests, builds `workgate.exe`, copies it to
`%LOCALAPPDATA%\Programs\workgate`, and adds that directory to the user
`PATH` if it isn't there yet. Re-run it after any source change to deploy the
new build (`-SkipTests` skips the test run). Already-open terminals keep
their old `PATH`; new ones see `workgate` immediately. If the copy fails
because the exe is in use, an active workload is still running — check
`workgate status` and re-run once it finishes.

Alternatively, install by hand: build with `go build -o workgate.exe
./cmd/workgate` and copy the exe into any directory already on `PATH`.

## Install (macOS / Linux)

From the repository root:

```bash
./install.sh
```

This runs the tests, builds `workgate`, and installs it to `~/.local/bin`
(created if needed). If that directory is not on your `PATH`, the script
prints the line to add to your shell profile. Re-run it after any source
change to deploy the new build (`--skip-tests` skips the test run).

Alternatively, install by hand: build with `go build -o workgate
./cmd/workgate` and copy the binary into any directory already on `PATH`.

## Instructing AI agents to use workgate

workgate is designed to be driven by coding agents (Codex, Claude Code)
through ordinary shell commands. Agents follow instructions best when the
rules are concrete: name the exact resources, map the exact operations to
them, and spell out what waiting looks like so the agent doesn't "fix" it.
Add a section like this to each project's `AGENTS.md` / `CLAUDE.md`
(workgate itself does not depend on these files):

```markdown
### Shared exclusive resources

Some operations must not execute concurrently with workloads from other
coding-agent sessions or projects on this machine.

Run any such operation through workgate:

    workgate run <resource> --label "<short description>" -- <command>

Defined shared resources:

- `gpu` — ANY command that requires exclusive access to the GPU
  (rendering, ML training, hardware-accelerated tests).

Rules:

1. Wrap the exclusive command itself, exactly as you would otherwise run
   it, after the `--`. Everything after `--` is passed through verbatim.
2. Always pass `--label` with a short description of what you are doing;
   other sessions see it in `workgate status`.
3. If workgate prints `Queued for "<resource>" (position N)`, that is
   normal: it is waiting for other sessions. Let it wait — do not kill
   the command, do not retry, and do not run the underlying operation
   directly to bypass the queue. Queued waits can take many minutes, so
   run the command with a generous (or no) timeout. Your position can go
   up as well as down, because higher-priority work may arrive behind
   you; that is also normal, and not a stall.
4. While waiting, workgate is intentionally quiet. Silence does not mean
   it is hung. To see who holds the resource, run `workgate status
   <resource>` in a separate command.
5. Never kill another session's workload to free the queue. Abandoned
   entries are removed automatically (~60 s after their owner dies).
6. The resource is released automatically when the wrapped command exits;
   there is no lock to clean up and no release command to call.
7. The wrapped command's exit code is passed through, so interpret
   failures exactly as if you had run the command directly. Exit codes
   125/126/127 with a `workgate:` message on stderr are workgate's own
   errors, not the command's.
8. Do not wrap commands that need no exclusive access — that only
   serializes work that could run in parallel.
9. Do not pass `--priority` unless a rule above tells you to; the default
   is correct for ordinary work. Never raise your own priority to get out
   of a queue, and never run `workgate priority` on a workload you did
   not start — that reorders another session's work.
```

Tips for adapting the snippet:

- **Enumerate resources concretely.** "Use workgate for exclusive things" is
  too vague for an agent to apply; "any command that runs `render-tool`
  uses the `gpu` resource" is followed reliably. One bullet per resource,
  with the trigger commands named.
- **Warn about the wait explicitly.** The most common agent failure mode is
  treating a queued (and deliberately quiet) workgate as a hung command —
  killing it, retrying, or bypassing the queue. Rules 3–4 above exist for
  that; keep them even if you trim everything else. If your agent harness
  enforces a per-command timeout, tell the agent to raise it or run the
  wrapped command in the background for potentially long queues.
- **Require labels.** Labels are how a human (or another agent) looking at
  `workgate status` understands who is blocking whom. Session/task context
  ("Claude: run integration tests for PR 42") beats a generic "tests".
- **Say who may use priority, or say nothing.** Enumerate the cases that
  justify a level, the same way you enumerate resources ("a hotfix build uses
  `--priority 1`"). An agent given a vague permission will reach for the flag
  to get out of a queue, which is exactly what rules 3 and 9 are for; silence
  is safer than a general licence. `workgate priority` is best left a human's
  tool.
- **Keep the wrapped span tight.** One `workgate run` per exclusive
  operation — not one per shell command inside it, and not a whole
  multi-step task that only briefly needs the resource.
- **Point agents at `status`, not `monitor`.** `monitor` runs until
  interrupted, so an agent that starts one never gets its command back.
  It is a human's window onto the queue; `workgate status <resource>` is
  the one-shot form an agent should use.

## How it works

One SQLite table holds the entire coordination state:

```sql
CREATE TABLE workloads (
  seq               INTEGER PRIMARY KEY AUTOINCREMENT,  -- arrival order
  id                TEXT UNIQUE NOT NULL,
  resource          TEXT NOT NULL,
  label             TEXT,
  state             TEXT NOT NULL CHECK (state IN ('waiting','running')),
  pid               INTEGER,
  created_at        INTEGER NOT NULL,
  acquired_at       INTEGER,
  heartbeat_at      INTEGER NOT NULL,
  working_directory TEXT, repository_root TEXT, git_common_dir TEXT,
  git_branch        TEXT, command_display TEXT, hostname TEXT,
  priority          INTEGER NOT NULL DEFAULT 3 CHECK (priority BETWEEN 1 AND 5)
);
CREATE INDEX idx_workloads_resource_seq ON workloads(resource, seq);
CREATE INDEX idx_workloads_resource_priority_seq ON workloads(resource, priority, seq);
CREATE UNIQUE INDEX idx_one_running ON workloads(resource) WHERE state = 'running';
```

`priority` is the one column that needs a real migration: `CREATE TABLE IF NOT
EXISTS` cannot add a column to a table that already exists, so opening a
database that predates priorities runs an `ALTER TABLE` first, inside the same
immediate transaction that every other write uses. `NOT NULL DEFAULT 3` is what
keeps the two binary versions compatible in both directions — rows written
before the column existed read as the neutral level, and an older binary, whose
`INSERT` never mentions the column, still writes a valid row.

A second table holds the recent-completions ring. Nothing reads it to make a
coordination decision; it exists only so `monitor` and `status --recent` can
say what just finished:

```sql
CREATE TABLE completions (
  seq         INTEGER PRIMARY KEY AUTOINCREMENT,  -- completion order
  id          TEXT NOT NULL,                      -- deliberately not unique
  resource    TEXT NOT NULL,
  label       TEXT,
  outcome     TEXT NOT NULL,                      -- ok|exit|killed|canceled|stale
  exit_code   INTEGER NOT NULL DEFAULT 0,
  started_at  INTEGER NOT NULL, finished_at INTEGER NOT NULL,
  working_directory TEXT, repository_root TEXT, git_branch TEXT
);
CREATE INDEX idx_completions_resource_seq ON completions(resource, seq);
```

- **Order** is `(priority, seq)`: the lower priority number first, then the
  `seq` autoincrement within a level. `seq` is never timestamps — in both
  tables, so that a clock that jumps cannot reorder either the queue or the
  ring.
- `completions.id` is deliberately not unique: a workload id is three random
  bytes, unique only among live rows, and a cosmetic collision must never be
  able to fail a release and strand a resource.
- The partial unique index makes a second `running` row per resource
  impossible at the database level, independent of application logic.
- Non-default pragmas (chosen deliberately): `journal_mode=WAL` (readers never
  block the short write transactions), `busy_timeout=5000`,
  `synchronous=NORMAL` (safe with WAL; this is live coordination state, not an
  audit log), and `_txlock=immediate` (every transaction takes the write lock
  up front, avoiding upgrade deadlocks).

The lifecycle:

1. **Enqueue** — one transaction inserts the row with `state='waiting'` and
   `heartbeat_at` already set (no window where a fresh row looks stale).
2. **Wait** — conservative polling (~750 ms); no transaction is held while
   waiting. A heartbeat goroutine refreshes `heartbeat_at` every 5 s for the
   row's whole life. Each poll re-reads the row's own priority, which is what
   lets `workgate priority` from another terminal take effect within one poll
   interval without any signalling. Occasional "still waiting" notices;
   otherwise quiet.
3. **Acquire** — one short `BEGIN IMMEDIATE` transaction: delete stale rows
   for the resource (heartbeat older than 60 s), verify no owner exists and
   that no waiting row for the resource outranks this one on `(priority,
   seq)`, then flip it to `running`. Atomicity guarantees two processes can
   never both win and cleanup can never race acquisition; `seq` is unique, so
   `(priority, seq)` is a strict total order and exactly one waiter can pass
   the rank check. A newer workload can overtake a healthy older waiter only
   by having a higher priority — within a level arrival order is absolute, and
   a running workload is never preempted.
4. **Run** — the child runs with stdio forwarded; the database is untouched
   except for heartbeats. The child's process tree is tied to workgate
   per platform: on Windows it is placed in a Job Object with
   *kill-on-close*, so if workgate dies for any reason — including a hard
   kill — the OS terminates the child's process tree. On macOS/Linux the
   child runs in its own process group; on interrupt workgate signals the
   whole group with SIGINT, then SIGKILL after the grace period, and Linux
   additionally arms `PR_SET_PDEATHSIG` so a hard-killed workgate takes the
   direct child with it.
5. **Release** — one transaction deletes the row and, if the workload actually
   held the resource, records how it ended in the completions ring
   (deferred-path, automatic; also on Ctrl+C after terminating the child).
   Deleting and recording together is what stops a crash between the two from
   leaving a resource held. Completed workloads are removed from the queue and
   summarised, not archived.

**Crash recovery:** if a workgate process is killed so hard that no cleanup
runs, its row simply stops heartbeating; the next acquisition attempt (or
`workgate status`) removes it after the 60-second stale threshold and reports:

```text
[workgate] Removed stale workload fd2b09 from "gpu"
```

The reclaimed workload is recorded as `stale` in the completions ring at the
same moment, so a hard kill leaves a trace rather than vanishing.

The threshold is 12× the heartbeat interval, deliberately conservative against
machine sleep, debugger pauses, and scheduling stalls. A healthy 30-minute
child is never at risk: heartbeats continue regardless of child output.

## Development

```sh
go test ./...
```

Tests include multi-process end-to-end coverage (ordering across real
processes, priority overtaking and live re-prioritization,
hard-kill recovery, exit-code propagation, and completions surviving both a
clean exit and a hard kill). `monitor` is covered through its
redirected-output path, and its escape sequences are asserted directly; the
alternate-screen view itself needs a real console, so changes to it are worth
running by eye. Environment variables
`WORKGATE_DB`, `WORKGATE_HEARTBEAT_INTERVAL_MS`, `WORKGATE_STALE_THRESHOLD_MS`
and `WORKGATE_POLL_INTERVAL_MS` exist solely so tests can isolate state and
shorten timings; they are not user-facing configuration.

## Intentional limitations

- Scope is per OS user account (the DB lives under that user's cache
  directory, see [Scope](#scope)); different users on one machine do not
  contend.
- Recovery after a hard kill takes up to the stale threshold (~60 s) — the
  price of being conservative about false-positive stale detection.
- On macOS/Linux there is no exact equivalent of the Windows kill-on-close
  Job Object: if workgate itself is killed with SIGKILL, the running child
  can be orphaned (on Linux the direct child is still killed via
  `PR_SET_PDEATHSIG`; its descendants, and everything on macOS, keep
  running). The queue itself always recovers via heartbeat staleness.
- On macOS/Linux the child runs in its own process group, so a wrapped
  command that reads from the terminal is stopped by `SIGTTIN`. Wrap
  non-interactive workloads only — which matches the tool's agent-driven
  purpose.
- Waiting uses sub-second polling rather than event-driven wakeup; the
  database traffic involved is negligible.
- Killing `monitor` outright (`SIGKILL`, `taskkill /F`) skips its cleanup and
  leaves the terminal on the alternate screen with the cursor hidden. Ctrl+C
  and, on macOS/Linux, `SIGTERM`/`SIGHUP` are all handled and restore it; a
  terminal stranded by a hard kill is recovered with `reset` on macOS/Linux,
  or by opening a new tab on Windows.
- Priorities are strict and small: five levels, arrival order within a level,
  and no aging. A low-priority workload behind a steady supply of
  high-priority work can wait indefinitely; `workgate priority <id> <level>`
  is the manual remedy, deliberately a human decision rather than a scheduler
  heuristic. There is no preemption, and no per-resource or per-project
  default level.
- Deliberately excluded: explicit acquire/release commands, multi-resource
  acquisition, preemption, priority aging, retries, daemons, networking, and
  per-project scopes.
- There is no history. The recent-completions ring is a display aid, bounded
  at ten per resource and expiring after a day, with no command to query it
  beyond the last few — not a record you can go back to.

## License

MIT — see [LICENSE](LICENSE). Use it freely, including in commercial and
closed-source work; the only condition is that the copyright notice travels
with copies of the software.

workgate wraps child commands across a process boundary (separate address
space, communication limited to argv, stdio, and an exit code). Wrapping a
command with workgate has no effect whatsoever on that command's licensing.

All dependencies are permissive (BSD-3-Clause and MIT); none impose copyleft
or source-disclosure obligations. Their license texts are reproduced in
[THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES), which should be included
alongside any binary distribution of workgate.
