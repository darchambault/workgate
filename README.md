# workgate

Machine-global, named, FIFO-exclusive execution of locally launched workloads.

`workgate` lets multiple coding-agent sessions (Codex, Claude Code, plain
terminals) — across different projects, Git repositories, and worktrees on the
same Windows workstation — serialize workloads that need exclusive access to a
shared machine-level resource (e.g. a Unity editor/project, a Steam upload
slot). It is a small local coordination primitive, not a job scheduler: no
daemon, no server, no configuration.

```powershell
workgate run unity --label "Run EditMode tests" -- Unity.exe -batchmode ...
```

Workloads targeting the same resource run strictly one at a time, in arrival
order. Workloads targeting different resources run concurrently. The resource
is released automatically when the wrapped command exits — or, if the process
is killed outright, recovered automatically via heartbeat staleness.

## Usage

```text
workgate run <resource> [--label "<description>"] -- <command> [args...]
workgate status [<resource>]
```

- Everything after `--` is the child command, passed through verbatim
  (no shell interpretation; quote arguments for your own shell as usual).
- Resource names: `[a-zA-Z0-9][a-zA-Z0-9._-]*`, max 64 chars, case-insensitive
  (`Unity`, `UNITY`, and `unity` share one queue).
- `--label` is diagnostic only; without it a label is derived from the command.
- The child's exit code is propagated. Workgate's own failures use distinct
  codes: `2` usage error, `125` internal error, `126` cannot launch, `127`
  command not found, `130` interrupted.
- Workgate's own messages go to **stderr**; the child's stdout stays clean.

Typical output:

```text
[workgate] Queued for "unity" (position 3): Run EditMode tests
[workgate] Acquired "unity"
...child output...
[workgate] Released "unity"
```

```text
> workgate status unity
RESOURCE: unity

RUNNING
  49ce3e   "Workload-A"                     00:04
           project: AirportTycoon
           pid: 77900

WAITING
  1eabb7   "Workload-B"                     00:02
           project: OtherUnityGame
           pid: 64732
```

## Scope

Coordination state lives in one machine-user-global SQLite database:

```text
%LOCALAPPDATA%\Workgate\workgate.db
```

(resolved via the platform's Local AppData mechanism, created on demand).
Every `workgate` process for the current Windows user shares it, so `unity`
means the same resource no matter which project, repository, worktree, or
non-Git directory invoked it. Git information (repo root, common dir, branch)
is recorded as diagnostic metadata only — it never affects locking. If a
project-specific resource is needed, encode it in the name
(e.g. `airport-tycoon-build`).

## Build

Requires Go 1.24+ (no CGO; SQLite via pure-Go `modernc.org/sqlite`).

```powershell
go build -o workgate.exe ./cmd/workgate
```

The result is a single self-contained `workgate.exe` — no runtime, no DLLs.

## Install (Windows)

Put `workgate.exe` on `PATH`. For example:

```powershell
New-Item -ItemType Directory -Force "$env:LOCALAPPDATA\Programs\workgate" | Out-Null
Copy-Item workgate.exe "$env:LOCALAPPDATA\Programs\workgate\"
[Environment]::SetEnvironmentVariable("Path",
  [Environment]::GetEnvironmentVariable("Path","User") + ";$env:LOCALAPPDATA\Programs\workgate",
  "User")
```

(Open a new terminal afterwards.) Alternatively copy it into any directory
already on `PATH`.

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

- `unity` — ANY command that opens or drives the Unity editor or runs
  Unity in batch mode (tests, builds, asset imports, project generation).

Rules:

1. Wrap the exclusive command itself, exactly as you would otherwise run
   it, after the `--`. Everything after `--` is passed through verbatim.
2. Always pass `--label` with a short description of what you are doing;
   other sessions see it in `workgate status`.
3. If workgate prints `Queued for "<resource>" (position N)`, that is
   normal: it is waiting for other sessions. Let it wait — do not kill
   the command, do not retry, and do not run the underlying operation
   directly to bypass the queue. Queued waits can take many minutes, so
   run the command with a generous (or no) timeout.
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
```

Tips for adapting the snippet:

- **Enumerate resources concretely.** "Use workgate for exclusive things" is
  too vague for an agent to apply; "any command that runs `Unity.exe` uses
  the `unity` resource" is followed reliably. One bullet per resource, with
  the trigger commands named.
- **Warn about the wait explicitly.** The most common agent failure mode is
  treating a queued (and deliberately quiet) workgate as a hung command —
  killing it, retrying, or bypassing the queue. Rules 3–4 above exist for
  that; keep them even if you trim everything else. If your agent harness
  enforces a per-command timeout, tell the agent to raise it or run the
  wrapped command in the background for potentially long queues.
- **Require labels.** Labels are how a human (or another agent) looking at
  `workgate status` understands who is blocking whom. Session/task context
  ("Claude: run EditMode tests for PR 42") beats a generic "tests".
- **Keep the wrapped span tight.** One `workgate run` per exclusive
  operation — not one per shell command inside it, and not a whole
  multi-step task that only briefly needs the resource.

## How it works

One SQLite table is the entire coordination state:

```sql
CREATE TABLE workloads (
  seq               INTEGER PRIMARY KEY AUTOINCREMENT,  -- FIFO order
  id                TEXT UNIQUE NOT NULL,
  resource          TEXT NOT NULL,
  label             TEXT,
  state             TEXT NOT NULL CHECK (state IN ('waiting','running')),
  pid               INTEGER,
  created_at        INTEGER NOT NULL,
  acquired_at       INTEGER,
  heartbeat_at      INTEGER NOT NULL,
  working_directory TEXT, repository_root TEXT, git_common_dir TEXT,
  git_branch        TEXT, command_display TEXT, hostname TEXT
);
CREATE INDEX idx_workloads_resource_seq ON workloads(resource, seq);
CREATE UNIQUE INDEX idx_one_running ON workloads(resource) WHERE state = 'running';
```

- **FIFO** is the `seq` autoincrement, never timestamps.
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
   row's whole life. Occasional "still waiting" notices; otherwise quiet.
3. **Acquire** — one short `BEGIN IMMEDIATE` transaction: delete stale rows
   for the resource (heartbeat older than 60 s), verify no owner exists and
   that this row has the lowest waiting `seq`, then flip it to `running`.
   Atomicity guarantees two processes can never both win, cleanup can never
   race acquisition, and a newer workload can never overtake a healthy older
   waiter.
4. **Run** — the child runs with stdio forwarded; the database is untouched
   except for heartbeats. The child is placed in a Windows Job Object with
   *kill-on-close*, so if workgate dies for any reason — including a hard
   kill — the OS terminates the child's process tree.
5. **Release** — one transaction deletes the row (deferred-path, automatic;
   also on Ctrl+C after terminating the child). Completed workloads are
   deleted, not archived.

**Crash recovery:** if a workgate process is killed so hard that no cleanup
runs, its row simply stops heartbeating; the next acquisition attempt (or
`workgate status`) removes it after the 60-second stale threshold and reports:

```text
[workgate] Removed stale workload fd2b09 from "unity"
```

The threshold is 12× the heartbeat interval, deliberately conservative against
machine sleep, debugger pauses, and scheduling stalls. A healthy 30-minute
child is never at risk: heartbeats continue regardless of child output.

## Development

```powershell
go test ./...
```

Tests include multi-process end-to-end coverage (FIFO across real processes,
hard-kill recovery, exit-code propagation). Environment variables
`WORKGATE_DB`, `WORKGATE_HEARTBEAT_INTERVAL_MS`, `WORKGATE_STALE_THRESHOLD_MS`
and `WORKGATE_POLL_INTERVAL_MS` exist solely so tests can isolate state and
shorten timings; they are not user-facing configuration.

## Intentional limitations

- Scope is per Windows user account (the DB lives under that user's
  `%LOCALAPPDATA%`); different Windows users on one machine do not contend.
- Recovery after a hard kill takes up to the stale threshold (~60 s) — the
  price of being conservative about false-positive stale detection.
- Waiting uses sub-second polling rather than event-driven wakeup; the
  database traffic involved is negligible.
- Deliberately excluded: explicit acquire/release commands, multi-resource
  acquisition, priorities, retries, history, daemons, networking, and
  per-project scopes.
