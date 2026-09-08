package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"workgate/internal/config"
	"workgate/internal/queue"
)

// withConfig points the process at a configuration file holding contents, for
// the duration of one test. displayConfig is process state — one file, read
// once — so it is restored rather than passed around, and every test that
// touches it goes through here.
func withConfig(t *testing.T, contents string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), config.FileName)
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WORKGATE_CONFIG", path)
	before := displayConfig
	t.Cleanup(func() { displayConfig = before })
	if err := loadDisplayConfig(); err != nil {
		t.Fatalf("loadDisplayConfig: %v", err)
	}
}

// stripping is a configuration file that removes one prefix, written with the
// separator of the platform the test runs on.
func stripping(prefix string) string {
	if runtime.GOOS == "windows" {
		prefix = strings.ReplaceAll(prefix, "/", `\`)
	}
	return "display:\n  strip-prefixes:\n    - " + prefix + "\n"
}

// osPath renders a path with this platform's separator, so that a fixture
// exercises the matching rules the platform actually has.
func osPath(p string) string {
	if runtime.GOOS == "windows" {
		return strings.ReplaceAll(p, "/", `\`)
	}
	return p
}

// commandLine returns the dimmed continuation line, which is the command.
func commandLine(t *testing.T, lines []line) string {
	t.Helper()
	for _, l := range lines {
		if text := strings.TrimSpace(l.plain()); strings.HasPrefix(l.plain(), continuationIndent) &&
			len(l) == 1 && l[0].style == styleDim {
			return text
		}
	}
	t.Fatalf("no command line in:\n%s", plainText(lines))
	return ""
}

func configuredWorkload(command string) queue.Workload {
	w := testWorkload("aaa", "gpu", "Train the model", "running", 5000, 0)
	w.CommandDisplay = command
	return w
}

// The default is the machine with no configuration file: every command shown
// exactly as it was run.
func TestNoConfigurationLeavesCommandsWhole(t *testing.T) {
	withConfig(t, "")
	cmd := osPath("C:/tools/python/python.exe") + " train.py"
	got := statusLines([]queue.Workload{configuredWorkload(cmd)}, testNow, false)
	if commandLine(t, got) != cmd {
		t.Errorf("command line = %q, want %q", commandLine(t, got), cmd)
	}
}

func TestStatusShortensTheCommand(t *testing.T) {
	withConfig(t, stripping("C:/tools/python/"))
	got := statusLines([]queue.Workload{
		configuredWorkload(osPath("C:/tools/python/python.exe") + " train.py --epochs 5"),
	}, testNow, false)
	if want := "python.exe train.py --epochs 5"; commandLine(t, got) != want {
		t.Errorf("command line = %q, want %q", commandLine(t, got), want)
	}
}

// The recent-completions section renders through the same continuation lines,
// and a completion that scrolled off the live queue should not suddenly be
// written differently from the entry it used to be.
func TestCompletionsShortenTheCommandTheSameWay(t *testing.T) {
	withConfig(t, stripping("C:/tools/python/"))
	cmd := osPath("C:/tools/python/python.exe") + " train.py"
	live := statusLines([]queue.Workload{configuredWorkload(cmd)}, testNow, false)
	done := completionLines([]queue.Completion{{
		ID: "aaa", Resource: "gpu", Label: "Train the model",
		Outcome: queue.OutcomeOK, StartedAt: testNow - 5000, FinishedAt: testNow,
		CommandDisplay: cmd,
	}}, testNow, false)
	if commandLine(t, live) != commandLine(t, done) {
		t.Errorf("live command %q and completed command %q differ",
			commandLine(t, live), commandLine(t, done))
	}
	if want := "python.exe train.py"; commandLine(t, done) != want {
		t.Errorf("command line = %q, want %q", commandLine(t, done), want)
	}
}

// The monitor and status format through one renderer, and configuration must
// not become the thing that finally splits them.
func TestMonitorAndStatusShortenAlike(t *testing.T) {
	withConfig(t, stripping("D:/Projects/"))
	ws := []queue.Workload{configuredWorkload(osPath("D:/Projects/app/build.exe") + " --release")}
	monitor := plainText(selectedStatusLines(ws, testNow, true, ""))
	status := plainText(statusLines(ws, testNow, false))
	if !strings.Contains(monitor, osPath("app/build.exe")) {
		t.Errorf("monitor did not shorten the command:\n%s", monitor)
	}
	if !strings.Contains(status, osPath("app/build.exe")) {
		t.Errorf("status did not shorten the command:\n%s", status)
	}
	if strings.Contains(monitor+status, osPath("D:/Projects/")) {
		t.Errorf("the stripped prefix survived:\n%s\n%s", monitor, status)
	}
}

// A label is prose a person wrote. A path inside one is there because they put
// it there, and shortening it would be editing what they said.
func TestTheLabelIsNeverShortened(t *testing.T) {
	withConfig(t, stripping("D:/Projects/"))
	w := configuredWorkload(osPath("D:/Projects/app/build.exe"))
	w.Label = "Rebuild " + osPath("D:/Projects/app")
	got := plainText(statusLines([]queue.Workload{w}, testNow, false))
	// Quoted, because that is how the label line renders it — and on Windows
	// %q is also what turns each separator into an escaped pair.
	if want := fmt.Sprintf("%q", w.Label); !strings.Contains(got, want) {
		t.Errorf("the label was shortened: want %s in\n%s", want, got)
	}
}

// Shortening changes what is displayed and nothing else: the entry keeps its
// header row, its label, and its command line, in that order.
func TestShorteningDoesNotChangeTheShapeOfAnEntry(t *testing.T) {
	w := configuredWorkload(osPath("C:/tools/python/python.exe") + " train.py")
	withConfig(t, "")
	before := statusLines([]queue.Workload{w}, testNow, false)
	withConfig(t, stripping("C:/tools/python/"))
	after := statusLines([]queue.Workload{w}, testNow, false)
	if len(before) != len(after) {
		t.Fatalf("entry went from %d lines to %d:\n%s\n---\n%s",
			len(before), len(after), plainText(before), plainText(after))
	}
}

// A workload with no command still has none, rather than an empty dimmed line
// where the shortener passed through.
func TestAnEntryWithNoCommandGainsNoLine(t *testing.T) {
	withConfig(t, stripping("C:/tools/"))
	w := configuredWorkload("")
	for _, l := range statusLines([]queue.Workload{w}, testNow, false) {
		if len(l) == 1 && l[0].style == styleDim && strings.TrimSpace(l[0].text) == "" {
			t.Errorf("an empty command produced a line:\n%s",
				plainText(statusLines([]queue.Workload{w}, testNow, false)))
		}
	}
}

// A broken file is reported, and the view carries on unconfigured rather than
// refusing to show the queue over a display setting.
func TestABrokenConfigurationIsReportedAndSurvived(t *testing.T) {
	path := filepath.Join(t.TempDir(), config.FileName)
	if err := os.WriteFile(path, []byte("display:\n  strip_prefixes:\n    - /tmp/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WORKGATE_CONFIG", path)
	before := displayConfig
	t.Cleanup(func() { displayConfig = before })

	err := loadDisplayConfig()
	if err == nil {
		t.Fatal("loadDisplayConfig accepted an unknown key, want an error")
	}
	cmd := osPath("/tmp/tool") + " --run"
	got := statusLines([]queue.Workload{configuredWorkload(cmd)}, testNow, false)
	if commandLine(t, got) != cmd {
		t.Errorf("command line = %q, want it unshortened: %q", commandLine(t, got), cmd)
	}
}

// The monitor cannot report this on stderr — the alternate screen would take
// the message with it — so the frame has to carry it.
func TestTheMonitorFrameCarriesAConfigurationWarning(t *testing.T) {
	frame := plainText(monitorFrame([]line{plainLine("body")}, frameState{
		scope:     "gpu",
		configErr: errors.New("parsing config.yaml: field strip_prefixes not found"),
		interval:  time.Second,
		now:       time.Unix(0, 0),
	}))
	if !strings.Contains(frame, "strip_prefixes not found") {
		t.Errorf("frame does not carry the configuration warning:\n%s", frame)
	}
	// And a frame with a healthy configuration is the frame it always was.
	clean := plainText(monitorFrame([]line{plainLine("body")}, frameState{
		scope: "gpu", interval: time.Second, now: time.Unix(0, 0),
	}))
	if strings.Contains(clean, "warning") {
		t.Errorf("a frame with no configuration error still warns:\n%s", clean)
	}
}
