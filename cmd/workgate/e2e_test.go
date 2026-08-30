package main

// End-to-end tests: build the real workgate binary plus a small helper child
// process, then exercise coordination across genuinely separate OS processes.
// Timing assertions use generous margins; ordering is asserted via marker
// files, not wall-clock measurements.

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"workgate/internal/db"
	"workgate/internal/queue"
)

var (
	workgateExe string
	helperExe   string
)

func TestMain(m *testing.M) {
	tmp, err := os.MkdirTemp("", "workgate-e2e")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	suffix := ""
	if runtime.GOOS == "windows" {
		suffix = ".exe"
	}
	workgateExe = filepath.Join(tmp, "workgate"+suffix)
	helperExe = filepath.Join(tmp, "helper"+suffix)
	for _, b := range [][2]string{
		{workgateExe, "workgate/cmd/workgate"},
		{helperExe, "workgate/internal/testutil/helper"},
	} {
		cmd := exec.Command("go", "build", "-o", b[0], b[1])
		if out, err := cmd.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "building %s: %v\n%s", b[1], err, out)
			os.Exit(1)
		}
	}
	code := m.Run()
	os.RemoveAll(tmp)
	os.Exit(code)
}

// fastEnv configures a per-test database and quick polling/heartbeat, with a
// stale threshold long enough that healthy processes are never at risk.
func fastEnv(dbPath string) []string {
	return []string{
		"WORKGATE_DB=" + dbPath,
		"WORKGATE_POLL_INTERVAL_MS=100",
		"WORKGATE_HEARTBEAT_INTERVAL_MS=200",
		"WORKGATE_STALE_THRESHOLD_MS=30000",
	}
}

type wgProc struct {
	cmd  *exec.Cmd
	out  bytes.Buffer
	mu   sync.Mutex
	done chan error
}

// syncWriter serializes writes into the shared buffer.
type syncWriter struct {
	p *wgProc
}

func (w syncWriter) Write(b []byte) (int, error) {
	w.p.mu.Lock()
	defer w.p.mu.Unlock()
	return w.p.out.Write(b)
}

func (p *wgProc) output() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.out.String()
}

func startWG(t *testing.T, dir string, env []string, args ...string) *wgProc {
	t.Helper()
	p := &wgProc{done: make(chan error, 1)}
	p.cmd = exec.Command(workgateExe, args...)
	p.cmd.Dir = dir
	p.cmd.Env = append(os.Environ(), env...)
	p.cmd.Stdout = syncWriter{p}
	p.cmd.Stderr = syncWriter{p}
	// Hard-kill tests leave workgate's helper child alive holding the
	// stdio pipes; on Unix (without Linux's pdeathsig) Wait would then
	// block until the orphan exits on its own. WaitDelay bounds that.
	p.cmd.WaitDelay = 10 * time.Second
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("starting workgate: %v", err)
	}
	go func() { p.done <- p.cmd.Wait() }()
	t.Cleanup(func() {
		if p.cmd.ProcessState == nil {
			p.cmd.Process.Kill()
			<-p.done
		}
	})
	return p
}

// waitExit waits for the process and returns its exit code.
func (p *wgProc) waitExit(t *testing.T, timeout time.Duration) int {
	t.Helper()
	select {
	case err := <-p.done:
		if err == nil || errors.Is(err, exec.ErrWaitDelay) {
			// ErrWaitDelay only replaces a nil Wait error: the process
			// exited cleanly but an orphaned child still holds its pipes.
			return 0
		}
		var ee *exec.ExitError
		if ok := isExitError(err, &ee); ok {
			return ee.ExitCode()
		}
		t.Fatalf("workgate wait: %v\noutput:\n%s", err, p.output())
		return -1
	case <-time.After(timeout):
		t.Fatalf("workgate did not exit within %s\noutput:\n%s", timeout, p.output())
		return -1
	}
}

func isExitError(err error, out **exec.ExitError) bool {
	ee, ok := err.(*exec.ExitError)
	if ok {
		*out = ee
	}
	return ok
}

func openTestDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	d, err := db.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for: %s", timeout, desc)
}

func listState(t *testing.T, d *sql.DB, resource string) []queue.Workload {
	t.Helper()
	ws, err := queue.List(d, resource)
	if err != nil {
		t.Fatal(err)
	}
	return ws
}

func readMarkers(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Fields(strings.ReplaceAll(strings.TrimSpace(string(b)), "\r\n", "\n"))
}

func TestChildExitCodeIsPropagated(t *testing.T) {
	env := fastEnv(filepath.Join(t.TempDir(), "wg.db"))
	p := startWG(t, t.TempDir(), env, "run", "exit-res", "--", helperExe, "-exit", "7")
	if code := p.waitExit(t, 30*time.Second); code != 7 {
		t.Fatalf("exit code = %d, want 7\noutput:\n%s", code, p.output())
	}
}

func TestLaunchFailureReleasesResource(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "wg.db")
	env := fastEnv(dbPath)
	p := startWG(t, t.TempDir(), env, "run", "launch-res", "--", "definitely-not-a-command-xyz-12345")
	if code := p.waitExit(t, 30*time.Second); code != 127 {
		t.Fatalf("exit code = %d, want 127\noutput:\n%s", code, p.output())
	}
	d := openTestDB(t, dbPath)
	if ws := listState(t, d, "launch-res"); len(ws) != 0 {
		t.Fatalf("resource not released after launch failure: %+v", ws)
	}
	// The next workload must acquire promptly.
	p2 := startWG(t, t.TempDir(), env, "run", "launch-res", "--", helperExe, "-exit", "0")
	if code := p2.waitExit(t, 30*time.Second); code != 0 {
		t.Fatalf("follow-up workload exit = %d, want 0\noutput:\n%s", code, p2.output())
	}
}

// TestStrictFIFOAcrossProcesses starts three real workgate processes on one
// resource, confirming each is registered before starting the next, and
// asserts the children executed strictly serially in arrival order.
func TestStrictFIFOAcrossProcesses(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wg.db")
	marker := filepath.Join(dir, "markers.txt")
	env := fastEnv(dbPath)
	d := openTestDB(t, dbPath)

	run := func(name, sleep string) *wgProc {
		return startWG(t, dir, env, "run", "fifo-res", "--label", name, "--",
			helperExe, "-marker", marker, "-name", name, "-sleep", sleep)
	}

	a := run("A", "1200ms")
	waitFor(t, 15*time.Second, "A running", func() bool {
		ws := listState(t, d, "fifo-res")
		return len(ws) == 1 && ws[0].State == "running"
	})
	b := run("B", "600ms")
	waitFor(t, 15*time.Second, "B enqueued", func() bool {
		return len(listState(t, d, "fifo-res")) == 2
	})
	c := run("C", "300ms")
	waitFor(t, 15*time.Second, "C enqueued", func() bool {
		return len(listState(t, d, "fifo-res")) == 3
	})

	for name, p := range map[string]*wgProc{"A": a, "B": b, "C": c} {
		if code := p.waitExit(t, 60*time.Second); code != 0 {
			t.Fatalf("%s exit = %d, want 0\noutput:\n%s", name, code, p.output())
		}
	}

	want := []string{"start", "A", "end", "A", "start", "B", "end", "B", "start", "C", "end", "C"}
	got := readMarkers(t, marker)
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("execution order = %v, want %v", got, want)
	}
	if ws := listState(t, d, "fifo-res"); len(ws) != 0 {
		t.Fatalf("workloads remain after completion: %+v", ws)
	}
}

// TestDifferentResourcesRunConcurrently proves a workload on resource B
// completes while resource A is still held.
func TestDifferentResourcesRunConcurrently(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wg.db")
	env := fastEnv(dbPath)
	d := openTestDB(t, dbPath)

	a := startWG(t, dir, env, "run", "res-one", "--", helperExe, "-sleep", "8s")
	waitFor(t, 15*time.Second, "A running", func() bool {
		ws := listState(t, d, "res-one")
		return len(ws) == 1 && ws[0].State == "running"
	})
	b := startWG(t, dir, env, "run", "res-two", "--", helperExe, "-exit", "0")
	// B must finish while A still holds res-one (well before A's 8s sleep ends).
	if code := b.waitExit(t, 6*time.Second); code != 0 {
		t.Fatalf("B exit = %d, want 0\noutput:\n%s", code, b.output())
	}
	if ws := listState(t, d, "res-one"); len(ws) != 1 || ws[0].State != "running" {
		t.Fatalf("A should still be running res-one, got %+v", ws)
	}
	a.cmd.Process.Kill()
}

func TestStatusReportsRunningAndWaiting(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wg.db")
	env := fastEnv(dbPath)
	d := openTestDB(t, dbPath)

	a := startWG(t, dir, env, "run", "status-res", "--label", "First workload", "--",
		helperExe, "-sleep", "20s")
	waitFor(t, 15*time.Second, "A running", func() bool {
		ws := listState(t, d, "status-res")
		return len(ws) == 1 && ws[0].State == "running"
	})
	b := startWG(t, dir, env, "run", "status-res", "--label", "Second workload", "--",
		helperExe, "-exit", "0")
	waitFor(t, 15*time.Second, "B waiting", func() bool {
		return len(listState(t, d, "status-res")) == 2
	})

	cmd := exec.Command(workgateExe, "status", "status-res")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("status: %v\n%s", err, out)
	}
	text := string(out)
	for _, want := range []string{"RESOURCE: status-res", "RUNNING", "WAITING",
		"First workload", "Second workload", "pid:"} {
		if !strings.Contains(text, want) {
			t.Errorf("status output missing %q:\n%s", want, text)
		}
	}
	a.cmd.Process.Kill()
	b.cmd.Process.Kill()
}

func TestStatusEmpty(t *testing.T) {
	env := fastEnv(filepath.Join(t.TempDir(), "wg.db"))
	cmd := exec.Command(workgateExe, "status")
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("status: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "No active workgate workloads.") {
		t.Fatalf("unexpected empty-status output:\n%s", out)
	}
}

// TestStaleWorkloadRecoveryAfterHardKill hard-kills the workgate process
// holding a resource (no cleanup code runs) and verifies the next workload
// takes over once the heartbeat goes stale.
func TestStaleWorkloadRecoveryAfterHardKill(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wg.db")
	env := []string{
		"WORKGATE_DB=" + dbPath,
		"WORKGATE_POLL_INTERVAL_MS=100",
		"WORKGATE_HEARTBEAT_INTERVAL_MS=300",
		"WORKGATE_STALE_THRESHOLD_MS=2500",
	}
	d := openTestDB(t, dbPath)

	a := startWG(t, dir, env, "run", "stale-res", "--label", "doomed", "--",
		helperExe, "-sleep", "120s")
	waitFor(t, 15*time.Second, "A running", func() bool {
		ws := listState(t, d, "stale-res")
		return len(ws) == 1 && ws[0].State == "running"
	})
	if err := a.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-a.done

	b := startWG(t, dir, env, "run", "stale-res", "--", helperExe, "-exit", "0")
	if code := b.waitExit(t, 30*time.Second); code != 0 {
		t.Fatalf("B exit = %d, want 0\noutput:\n%s", code, b.output())
	}
	if !strings.Contains(b.output(), "Removed stale workload") {
		t.Errorf("B did not report stale removal:\n%s", b.output())
	}
	if ws := listState(t, d, "stale-res"); len(ws) != 0 {
		t.Fatalf("workloads remain: %+v", ws)
	}
}

func TestInvalidResourceRejected(t *testing.T) {
	env := fastEnv(filepath.Join(t.TempDir(), "wg.db"))
	p := startWG(t, t.TempDir(), env, "run", "bad!name", "--", helperExe)
	if code := p.waitExit(t, 15*time.Second); code != 2 {
		t.Fatalf("exit = %d, want 2\noutput:\n%s", code, p.output())
	}
}

// TestContentionAcrossIndependentRepositories runs two workloads from two
// unrelated Git repositories; they must contend for the same machine-global
// queue.
func TestContentionAcrossIndependentRepositories(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	base := t.TempDir()
	repo1 := filepath.Join(base, "RepoOne")
	repo2 := filepath.Join(base, "RepoTwo")
	for _, r := range []string{repo1, repo2} {
		if err := os.Mkdir(r, 0o755); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command("git", "init", "-b", "main")
		cmd.Dir = r
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git init: %v\n%s", err, out)
		}
	}
	dbPath := filepath.Join(base, "wg.db")
	env := fastEnv(dbPath)
	d := openTestDB(t, dbPath)

	a := startWG(t, repo1, env, "run", "unity", "--", helperExe, "-sleep", "20s")
	waitFor(t, 15*time.Second, "repo1 workload running", func() bool {
		ws := listState(t, d, "unity")
		return len(ws) == 1 && ws[0].State == "running"
	})
	b := startWG(t, repo2, env, "run", "unity", "--", helperExe, "-exit", "0")
	waitFor(t, 15*time.Second, "repo2 workload waiting behind repo1", func() bool {
		ws := listState(t, d, "unity")
		return len(ws) == 2 && ws[0].State == "running" && ws[1].State == "waiting"
	})
	if !strings.Contains(b.output(), "Queued for \"unity\"") {
		t.Errorf("repo2 workload did not report queueing:\n%s", b.output())
	}
	a.cmd.Process.Kill()
	b.cmd.Process.Kill()
}

// TestMonitorRendersLiveQueue drives the monitor with stdout on a pipe, which
// selects its non-TTY path: no escape sequences, one appended frame per
// interval. The alternate-screen path cannot be exercised without a real
// console, so it is verified by hand.
func TestMonitorRendersLiveQueue(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "wg.db")
	env := fastEnv(dbPath)
	d := openTestDB(t, dbPath)

	a := startWG(t, dir, env, "run", "monitor-res", "--label", "Held workload", "--",
		helperExe, "-sleep", "20s")
	waitFor(t, 15*time.Second, "A running", func() bool {
		ws := listState(t, d, "monitor-res")
		return len(ws) == 1 && ws[0].State == "running"
	})
	b := startWG(t, dir, env, "run", "monitor-res", "--label", "Queued workload", "--",
		helperExe, "-exit", "0")
	waitFor(t, 15*time.Second, "B waiting", func() bool {
		return len(listState(t, d, "monitor-res")) == 2
	})

	m := startWG(t, dir, env, "monitor", "monitor-res", "--interval", "200ms")
	waitFor(t, 15*time.Second, "two monitor frames", func() bool {
		return strings.Count(m.output(), "workgate monitor - monitor-res") >= 2
	})

	text := m.output()
	for _, want := range []string{"RESOURCE: monitor-res", "RUNNING", "WAITING",
		"Held workload", "Queued workload", "refreshing every 200ms"} {
		if !strings.Contains(text, want) {
			t.Errorf("monitor output missing %q:\n%s", want, text)
		}
	}
	// The non-TTY path must stay pipe-friendly.
	if strings.Contains(text, "\x1b[") {
		t.Errorf("monitor emitted escape sequences to a pipe:\n%q", text)
	}
	// Monitoring is read-only: both workloads must still be queued.
	if ws := listState(t, d, "monitor-res"); len(ws) != 2 {
		t.Errorf("monitor changed the queue: %d workloads remain, want 2", len(ws))
	}

	m.cmd.Process.Kill()
	a.cmd.Process.Kill()
	b.cmd.Process.Kill()
}

// TestMonitorRejectsBadArguments covers the usage paths, which must fail fast
// rather than opening a full-screen view.
func TestMonitorRejectsBadArguments(t *testing.T) {
	env := fastEnv(filepath.Join(t.TempDir(), "wg.db"))
	for _, args := range [][]string{
		{"monitor", "not a resource"},
		{"monitor", "--interval", "10ms"},
		{"monitor", "--nope"},
	} {
		cmd := exec.Command(workgateExe, args...)
		cmd.Env = append(os.Environ(), env...)
		out, err := cmd.CombinedOutput()
		var ee *exec.ExitError
		if !isExitError(err, &ee) || ee.ExitCode() != 2 {
			t.Errorf("%v: exit = %v, want usage error 2\n%s", args, err, out)
		}
	}
}
