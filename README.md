# workgate

Machine-global, named, exclusive execution of locally launched workloads,
ordered by priority and then by arrival.

`workgate` lets multiple coding-agent sessions (Codex, Claude Code, plain
terminals) — across different projects, Git repositories, and worktrees on the
same machine (Windows, macOS, or Linux) — serialize workloads that need
exclusive access to a shared machine-level resource (e.g. a GPU, a hardware
test rig, a licensed toolchain seat). It is a small local coordination
primitive, not a job scheduler: no daemon, no server, and nothing to
configure before it works.

```sh
workgate run gpu --label "Run integration tests" -- test-runner --suite integration
```

Workloads targeting the same resource run strictly one at a time: the highest
priority first, and in arrival order within one priority level. Workloads
targeting different resources run concurrently, and a workload that needs
several — a project *and* the GPU — names them all and takes them all at once.
The resource is released automatically when the wrapped command exits — or,
if the process is killed outright, recovered automatically via heartbeat
staleness.

## Usage

```text
workgate run <resource>[,<resource>...] [--label "<description>"] [--priority <1-5>] -- <command> [args...]
workgate status [<resource>] [--recent[=<count>]]
workgate monitor [<resource>] [--interval <duration>]
workgate priority <id> <1-5>
```

- Everything after `--` is the child command, passed through verbatim
  (no shell interpretation; quote arguments for your own shell as usual).
- Resource names: `[a-zA-Z0-9][a-zA-Z0-9._-]*`, max 64 chars, case-insensitive
  (`GPU`, `Gpu`, and `gpu` share one queue).
- `run` takes up to four resources as a comma-separated list
  (`myproject,gpu`), for a command that needs all of them at once; see
  [Multiple resources](#multiple-resources) below.
- `--label` is diagnostic only. Without it an entry simply has no label: the
  views show the command on a line of its own regardless, and no placeholder
  stands in for the description you did not write.
- `--priority` runs from `1` (highest) to `5` (lowest) and defaults to `3`;
  see [Priority](#priority) below. Unlike `--label` it is not diagnostic — it
  decides who runs next.
- `priority` re-prioritizes a workload that is already queued, whichever
  session started it. The id is the first column of `status`.
- `monitor` is `status` as a live view, and the one place a queue can be
  re-ordered by hand; see [Monitoring](#monitoring) below.
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
- There is nothing to configure to use any of this. An optional file can
  shorten the long install roots those two views print; see
  [Configuration](#configuration).

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
  49ce3e   pid 77900  00:04    P3  MyApp [main]
           "Workload-A"
           test-runner --suite integration

WAITING
  1eabb7   pid 64732  00:02    P1  MyApp [hotfix]
           "Urgent hotfix run"
           test-runner --suite hotfix
  70ce62   pid 51204  00:31    P3  MyApp [fix-42]
           "Workload-B"
           test-runner --suite regression
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

### Multiple resources

Some commands need more than one thing to themselves: a build that must not
overlap another build of the same project, and that also needs the GPU. Name
every resource in one `run`:

```sh
workgate run myproject,gpu --label "Train and package" -- python train.py
```

```text
[workgate] Queued for "myproject" (position 2) and "gpu" (position 1): Train and package
[workgate] Acquired "myproject" and "gpu"
...child output...
[workgate] Released "myproject" and "gpu"
```

- **All at once, or not at all.** The workload waits holding *none* of its
  resources, and takes every one of them in the single transaction that
  acquires one. It never holds `myproject` while it waits for `gpu`.
- **That is why it cannot deadlock.** The alternative — nesting
  `workgate run myproject -- workgate run gpu -- …` — holds the first resource
  idle while waiting for the second, and two sessions that nest in opposite
  orders wait for each other forever. Don't nest runs; name both.
- **One order across every queue.** A workload ranks by priority and then
  arrival on every resource it names, so two waiters can never each be ahead
  of the other. The order you list the resources in changes nothing but how
  they are displayed.
- **A waiter keeps its place everywhere, even on an idle resource.** While the
  workload above waits for `myproject`, a later `gpu`-only command waits behind
  it, even though the GPU is free. Letting it through would let a steady supply
  of GPU-only work starve the workload that needs both, indefinitely. The cost
  is an idle resource for a while, and the answer to that, as for any queue
  that matters to you, is `workgate priority`.
- **A position per queue.** It is next once it is first in every queue and
  nothing is running on any of them. `workgate priority` re-prioritizes the
  whole workload, and reports where that leaves it in each queue:
  `(now position 2 on "myproject", 1 on "gpu")`.
- **Listed under each resource.** `status` and `monitor` show the workload in
  every queue it is in, with a line saying what else it holds or waits for:

  ```text
  RESOURCE: gpu

  RUNNING
    49ce3e   pid 77900  00:04    P3  MyApp [main]
             "Train and package"
             also holds: myproject
             python train.py
  ```

  Finished, it is one entry: under `LAST COMPLETED` for one resource it says
  `also held: …`, and in the unscoped view its resource column reads
  `myproject,gpu`. In the monitor it is one stop for the selection, however
  many queues it is in, and the `>` marks it in all of them.
- At most **four** resources, each named once. `a,,b`, `gpu,GPU` and a fifth
  name are usage errors (exit `2`).
- A workload whose owner dies is reclaimed whole: the next `run` on *any* of
  its resources frees all of them, and reports it once.

### Monitoring

`workgate monitor` is the same information as a live view: it takes over the
terminal, redraws once per second, and runs until you stop it with `q` or
Ctrl+C. Use it to watch a contended resource instead of re-running `status` in
a loop — and, on a terminal, to re-prioritize a waiting workload with the arrow
keys, without leaving the view that shows why it needs it.

```text
workgate monitor [<resource>] [--interval <duration>]
```

```text
workgate monitor - gpu - 19:42:07

RESOURCE: gpu

RUNNING
  49ce3e   pid 77900  00:04    P3  MyApp [main]
           "Workload-A"
           test-runner --suite integration

WAITING
> 1eabb7   pid 64732  00:02    P1  MyApp [hotfix]
           "Urgent hotfix run"
           test-runner --suite hotfix
  70ce62   pid 51204  00:31    P3  MyApp [fix-42]
           "Workload-B"
           test-runner --suite regression

LAST COMPLETED
  b0d41c   ok         00:03        (just now)  MyApp [main]
           "Workload-Z"
           test-runner --suite smoke
  7f2a90   exit 1     00:00        (2m ago)    MyApp [fix-42]
           "Workload-Y"
           test-runner --suite regression
  3c4d5e   stale      00:31        (18m ago)   Other [main]
           "Workload-X"
           build.sh --release

refreshing every 1s - up/down select - right raises priority - q to stop
```

- Without a resource it watches every resource, grouped, exactly as `status`
  does. `--interval` accepts any Go duration (`2s`, `500ms`); the default is
  `1s` and the minimum is `100ms`.
- The view uses the terminal's alternate screen buffer, so your scrollback is
  untouched: on exit the terminal is restored and nothing is left behind.
  Resizing the window mid-run is fine — each frame is re-fitted, and a window
  too short for the whole queue ends with a count of what did not fit rather
  than dropping it silently.
- **Monitoring removes a workload only once its owner is dead.** A workload
  whose heartbeat has gone stale is labelled `[STALE]`. It is removed — and
  moves to `LAST COMPLETED` as `stale` — only when the monitor can also see
  that the `workgate` process that owns it has exited. That is a stricter rule
  than `status` applies. `status` reclaims on a stale heartbeat alone. A
  monitor is left open for hours, across machine sleeps, and after a sleep every
  heartbeat is stale at once: reclaiming on that alone would take the resource
  from an owner that is only late, and let a second workload start while the
  first one's command still runs. A dead process cannot come back, so its row
  goes on the next refresh.
  - Only processes on this machine are judged, and anything the monitor cannot
    be sure of counts as alive. An owner it may not inspect, or a pid it cannot
    tell apart from a newcomer, keeps its `[STALE]` label until a `run` on that
    resource or any `status` clears it. On Windows, where pids are reused
    readily, a pid now held by a process that started after the workload was
    queued is recognised as a newcomer. Elsewhere a reused pid is simply taken as alive.
  - This matters most for a resource named after one project or worktree. A
    `run` only reclaims its own resource, so an abandoned row on a resource
    nobody runs again would otherwise stay under RUNNING until somebody typed
    `workgate status`.
  - Apart from that, the only thing a monitor writes is a priority, for the row
    you selected and only on the keystroke that asks for it. (The database is
    still opened normally, which applies the idempotent `CREATE TABLE IF NOT
    EXISTS` schema step and, once on a database that predates priorities, the
    `ALTER TABLE` that adds the column — so even an untouched monitor is not
    literally zero writes.)
- **The keys are four arrows and `q`.** Up and down move the highlight through
  the waiting workloads; right raises the selected one's priority and left
  lowers it, clamped at 1 and 5. `j`/`k`/`h`/`l` do the same, Esc drops the
  selection, and `q` stops the monitor. Each keystroke redraws at once, so a row
  moves as you promote it.
  - The selection follows the **workload**, not the position: the row you
    promoted stays highlighted where it lands. Moving past the top or the bottom
    stops there and leaves nothing selected, and the same key steps back on.
  - The running workload is not selectable. It already holds the resource, and
    workgate never preempts, so its level has nothing left to decide. Use
    `workgate priority` if you want to set one anyway.
  - A selected workload that finishes takes the highlight with it, rather than
    leaving it on whichever row inherited the place.
  - Keys need a terminal at **both** ends. With stdin or stdout redirected, or
    on a console too old for virtual terminal input, the monitor silently offers
    no keys and cannot re-prioritize anything — the footer only offers keys that
    work. (It still clears workloads whose owner is dead, as above.)
- **An entry is up to four lines**: a header row of fixed-width columns —
  id, pid, elapsed, priority, then the worktree and its branch — followed by
  the label, the other resources of a [multi-resource](#multiple-resources)
  workload, and the command, each on a line of its own and indented under the
  id. Each continuation is dropped when there is nothing to put on it, so an
  unlabelled single-resource workload costs two lines rather than spending
  them on placeholders. Stacking is what lets a long label and a long command be read
  whole: sharing one row, they competed with the worktree and the `[STALE]`
  marker for the right-hand side of the screen. `status` prints exactly the
  same entry, so the two views cannot drift apart.
- A finished workload leaves the priority column blank: its level described a
  queue it has already left. The column is still spent, so live and finished
  rows stay on one grid — and a finished entry stacks exactly like a live one,
  label and command included.
- Restrained colour picks out the structure: section headings (green for
  RUNNING, yellow for WAITING), a red `[STALE]` and a red failing outcome,
  and dimmed chrome so the eye lands on the workloads. Colour is never the
  only signal: every state it distinguishes is also written out, and the
  selected row is marked with a literal `>` in the two columns every row
  already spends on its indent. Setting `NO_COLOR` to any value turns colour
  off while keeping the live redraw.
- Redirected output (`workgate monitor gpu | tee watch.log`) emits no escape
  sequences at all: frames are simply appended, one per interval, unstyled
  and untruncated.
- `q` and Ctrl+C are both how a monitor is meant to end, so it exits `0`. The
  `130` interrupted code listed above belongs to `run`, where an interrupt cuts
  a child command short. Ctrl+C keeps working in the interactive view because
  the monitor takes the keyboard in cbreak mode rather than raw mode: it reads
  keys unbuffered and unechoed, and deliberately leaves the interrupt alone.

### Recent completions

A workload that finishes disappears from the queue, which leaves the question
a monitor is most often being asked — *did my build finish, and how?* — with
nowhere to look. `monitor` therefore ends with a `LAST COMPLETED` section, and
`status --recent` shows the same thing on demand:

```text
> workgate status gpu --recent
No active workgate workloads.

LAST COMPLETED
  b0d41c   ok         00:03        (just now)  MyApp [main]
           "Workload-Z"
           test-runner --suite smoke
  7f2a90   exit 1     00:00        (2m ago)    MyApp [fix-42]
           "Workload-Y"
           test-runner --suite regression
```

- A finished entry's header row sits on the same columns as a live one, so a
  frame reads as one table. The column a live workload spends on its pid
  carries the **outcome** instead — a pid is not merely useless once the process is gone,
  it is misleading, because pids get recycled. The timer column carries how
  long the workload **ran**; the list is already newest-first, so the age in
  brackets is what says when.
- Outcomes are `ok`, `exit <code>`, `killed` (signalled, or crashed),
  `canceled` (interrupted mid-run), and `stale` (the owner stopped
  heartbeating and was reclaimed). Everything that held the resource is
  recorded, so a hard-killed workload leaves a trace rather than vanishing.
  A monitor moves one down here about a minute after its owner dies, once the
  heartbeat is stale and the process is gone; until then it reads `[STALE]` in
  the live section.
- The command is copied off the workload row in the same transaction that
  deletes it, so it survives however the workload ended — including the
  reclaim path, where nobody was around to release it. A completion recorded
  by a version that predates the column simply has no command line; there is
  none to invent for it.
- `monitor` always shows three. `status` shows none unless asked: `--recent`
  for three, `--recent=<count>` for up to ten.
- The section is last on screen, so a window too short for everything drops it
  before the live queue — which is the right way round, the queue being what
  the tool is for.
- This is a bounded ring, not history: at most ten completions per resource,
  expiring after a day, with nothing to query them beyond the last few. See
  [Scope](#scope).

## Configuration

Workgate needs no configuration, and has none until you write a file. That
file is read only by `status` and `monitor`, and only to decide how commands
are printed — nothing in it reaches the queue, so two sessions with different
files still contend for a resource identically. It lives in the platform's
per-user config directory, which you create yourself:

```text
Windows   %APPDATA%\Workgate\config.yaml
macOS     ~/Library/Application Support/Workgate/config.yaml
Linux     $XDG_CONFIG_HOME/Workgate/config.yaml   (default ~/.config/...)
```

Deliberately not the cache directory the database lives in: a cache is
disposable and may be swept at any time, and this is a file you wrote.

### Shortening displayed commands

An entry shows the command on a line of its own, and on a real machine most of
that line is the same leading noise over and over — a toolchain directory, a
virtualenv, the parent every agent worktree hangs off, the launcher every one
of them is started through. `strip-prefixes` names that text, and the views
drop it:

```yaml
display:
  strip-prefixes:
    - C:\Users\you\AppData\Local\Programs\Python\Python312\
    - D:\Projects\
    - 'powershell -NoProfile -ExecutionPolicy Bypass -File '
```

```text
> workgate status gpu
RESOURCE: gpu

RUNNING
  b0d41c   pid 24196  04:12    P2   ml-service [train-v3]
           "Fine-tune the reranker"
           python.exe train.py --config configs/rerank.yaml
```

instead of the same entry running out to
`C:\Users\you\AppData\Local\Programs\Python\Python312\python.exe`, whose
interesting half a narrow terminal never reaches.

- An entry is **literal text, matched exactly as written.** Nothing is added
  to it, so write the trailing separator yourself: `D:\Projects` strips less
  than `D:\Projects\`, and leaves the line starting on a bare `\`.
- It does not have to be a path. A launcher invocation is the same problem in
  different clothes, and ends at a space rather than a separator — which is
  why nothing is completed for you. Workgate would have to guess which of the
  two a prefix ends at, and guessing `-File\` builds a needle no command can
  contain: a setting that fails silently. A prefix written short fails
  visibly instead, by leaving the separator on screen.
- A prefix is removed **everywhere it appears**, not only at the front. A
  command routinely names one root twice — once for the program, once for the
  file it is given — and shortening only the first leaves the line as long as
  it was.
- Where two prefixes overlap, the longer one wins on each occurrence, whatever
  order the file lists them in.
- On Windows, matching follows Windows' own path rules: ASCII case is ignored,
  and `/` and `\` are the same separator. Elsewhere a path matches only
  itself. What is *kept* is always verbatim — matching is case-insensitive,
  printing is not.
- Only the command is shortened. The label is prose you wrote, and a path in
  one is there because you put it there.
- Nothing is stored shortened. This is a view: widening a prefix or deleting
  the file brings the full command straight back, for workloads already
  queued as much as for new ones. What *is* stored is capped at 200
  characters, and shortening happens after that — so a command that was
  already clipped to `...` stays clipped, however much of it a prefix
  removes.

Two things about YAML itself are worth knowing here, because both fail
quietly:

- **A trailing space only survives inside quotes.** YAML strips trailing
  whitespace from an unquoted scalar, so the launcher prefix above must be
  written `'powershell -NoProfile -ExecutionPolicy Bypass -File '`, quotes
  included. Leading and trailing whitespace you did *not* mean is removed by
  the parser the same way, and workgate adds no trimming of its own.
- **Write Windows paths unquoted, or in `'single quotes'`.** In a
  *double*-quoted scalar `\t` is a tab and `\b` a backspace, so
  `"C:\tools\bin\"` is not the path it looks like.

Unknown keys are an error rather than being quietly ignored: `strip_prefixes`
for `strip-prefixes` would otherwise be a setting that appears to do nothing.
A file that cannot be read or parsed is reported — on stderr for `status`, and
in the frame for `monitor`, which has no stderr to use — and the view falls
back to full commands rather than withholding the queue over a display
setting.

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

An operation that needs several of the resources below names them all in
one run, separated by commas:

    workgate run <resource>,<resource> --label "<short description>" -- <command>

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
   If a command needs several resources, name them all in one `workgate
   run`; never put one `workgate run` inside another, which can deadlock.
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
  interrupted, so an agent that starts one never gets its command back, and
  its keys are for a pair of hands. It is a human's window onto the queue;
  `workgate status <resource>` is the one-shot form an agent should use.

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
  priority          INTEGER NOT NULL DEFAULT 3 CHECK (priority BETWEEN 1 AND 5),
  group_id          TEXT  -- the workload's id on every one of its rows
);
CREATE INDEX idx_workloads_resource_seq ON workloads(resource, seq);
CREATE INDEX idx_workloads_resource_priority_seq ON workloads(resource, priority, seq);
CREATE UNIQUE INDEX idx_one_running ON workloads(resource) WHERE state = 'running';
```

`priority` and `group_id` were added after the first release, and need a real
migration: `CREATE TABLE IF NOT EXISTS` cannot add a column to a table that
already exists, so opening an older database runs an `ALTER TABLE` first,
inside the same immediate transaction that every other write uses. `NOT NULL
DEFAULT 3` is what keeps the binary versions compatible in both directions —
rows written before the column existed read as the neutral level, and an older
binary, whose `INSERT` never mentions the column, still writes a valid row.

A workload with several resources is **one row per resource**. The first row's
`id` is the workload's, and every row carries it in `group_id`; the other rows'
own ids are never shown. A row with no `group_id` — every row an older binary
writes — is a workload of its own, so the workload key throughout is
`IFNULL(group_id, id)`. Keeping a row per resource is what keeps
`idx_one_running` meaningful for multi-resource workloads, and keeps an older
binary safe on a shared database: it sees each row as a workload of its own,
and so waits for a resource a multi-resource workload holds. It does list
those rows as separate workloads, and its stale cleanup can remove one of
them; the workload that loses a row then stops, reporting it was removed as
stale, rather than go on holding the rest.

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
- A workload's rows are inserted in one transaction, so their `seq`s are
  consecutive and no other workload's can fall between them. Comparing two
  rows on `(priority, seq)` is then the same as comparing their workloads,
  which is what makes the rank one total order across every resource.
- Non-default pragmas (chosen deliberately): `journal_mode=WAL` (readers never
  block the short write transactions), `busy_timeout=5000`,
  `synchronous=NORMAL` (safe with WAL; this is live coordination state, not an
  audit log), and `_txlock=immediate` (every transaction takes the write lock
  up front, avoiding upgrade deadlocks).

The lifecycle:

1. **Enqueue** — one transaction inserts a row per resource with
   `state='waiting'` and `heartbeat_at` already set (no window where a fresh
   row looks stale).
2. **Wait** — conservative polling (~750 ms); no transaction is held while
   waiting. A heartbeat goroutine refreshes `heartbeat_at` every 5 s for the
   workload's whole life, on all of its rows in one statement. Each poll re-reads the row's own priority, which is what
   lets `workgate priority` from another terminal take effect within one poll
   interval without any signalling. Occasional "still waiting" notices;
   otherwise quiet.
3. **Acquire** — one short `BEGIN IMMEDIATE` transaction: delete stale
   workloads on its resources (heartbeat older than 60 s), verify that on
   every one of them no owner exists and no waiting row outranks this one on
   `(priority, seq)`, then flip all of its rows to `running`. Atomicity guarantees two processes can
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
5. **Release** — one transaction deletes the workload's rows and, if it
   actually held its resources, records how it ended in the completions ring,
   once per resource
   (deferred-path, automatic; also on Ctrl+C after terminating the child).
   Deleting and recording together is what stops a crash between the two from
   leaving a resource held. Completed workloads are removed from the queue and
   summarised, not archived.

**Crash recovery:** if a workgate process is killed so hard that no cleanup
runs, its rows simply stop heartbeating; the next acquisition attempt on any
of its resources (or any `workgate status`) removes the whole workload after
the 60-second stale threshold and reports:

```text
[workgate] Removed stale workload fd2b09 from "gpu"
```

The reclaimed workload is recorded as `stale` in the completions ring at the
same moment, so a hard kill leaves a trace rather than vanishing. A running
`workgate monitor` removes such a row as well, but only once it can also see
that the owning process has exited (see [Monitoring](#monitoring)).

The threshold is 12× the heartbeat interval, deliberately conservative against
machine sleep, debugger pauses, and scheduling stalls. A healthy 30-minute
child is never at risk: heartbeats continue regardless of child output.

## Development

```sh
go test ./...
```

Tests include multi-process end-to-end coverage (ordering across real
processes, priority overtaking and live re-prioritization, multi-resource
workloads waiting, blocking and crossing without deadlock,
hard-kill recovery by both the next `run` and a watching `monitor`, exit-code
propagation, and completions surviving both a clean exit and a hard kill).
`monitor` is covered through its
redirected-output path, and its escape sequences are asserted directly; key
decoding, selection movement and the priority keystroke are unit-tested, the
last against a real database. The alternate-screen view itself needs a real
console, and so does putting the keyboard into cbreak mode, so changes to
either are worth running by eye. Environment variables
`WORKGATE_DB`, `WORKGATE_CONFIG`, `WORKGATE_HEARTBEAT_INTERVAL_MS`,
`WORKGATE_STALE_THRESHOLD_MS` and `WORKGATE_POLL_INTERVAL_MS` exist so tests
can isolate state and shorten timings; they are not the intended way to
configure workgate. The end-to-end tests set `WORKGATE_CONFIG` to a file that
is not there, so that a developer with `strip-prefixes` set for their own
machine does not see commands shortened out from under an assertion about
them.

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
  leaves the terminal on the alternate screen with the cursor hidden and, if it
  had taken the keyboard, without echo or line editing. `q`, Ctrl+C and, on
  macOS/Linux, `SIGTERM`/`SIGHUP` are all handled and restore every part of it;
  a terminal stranded by a hard kill is recovered with `reset` on macOS/Linux,
  or by opening a new tab on Windows.
- Priorities are strict and small: five levels, arrival order within a level,
  and no aging. A low-priority workload behind a steady supply of
  high-priority work can wait indefinitely; `workgate priority <id> <level>`
  is the manual remedy, deliberately a human decision rather than a scheduler
  heuristic. There is no preemption, and no per-resource or per-project
  default level.
- Configuration is display-only, and per OS user. There is no way to set a
  default priority, a resource, or anything else that would change what runs
  next: a queue two sessions share must not depend on a file only one of them
  has. It is also not per project — the file lives with the user, like the
  database, and a per-project one would make the same command print
  differently depending on where it was started.
- A multi-resource workload holds its place on every resource it names, even
  one that sits idle while it waits for another. That idle time is the price
  of never starving it; see [Multiple resources](#multiple-resources). It may
  name at most four.
- Deliberately excluded: explicit acquire/release commands, preemption,
  priority aging, retries, daemons, networking, and per-project scopes.
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
