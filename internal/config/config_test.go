package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// write puts contents in a temporary config file and returns its path.
func write(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), FileName)
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// load reads a config written from contents, failing the test on any error.
func load(t *testing.T, contents string) *Config {
	t.Helper()
	c, err := loadFile(write(t, contents))
	if err != nil {
		t.Fatalf("loadFile: %v", err)
	}
	return c
}

// sep writes a path with the separator this platform's tests should use.
// The stripping rules are the platform's own path rules, so the fixtures
// have to be the platform's own paths.
func sep(p string) string {
	if runtime.GOOS == "windows" {
		return strings.ReplaceAll(p, "/", `\`)
	}
	return p
}

func TestPathHonorsEnvOverride(t *testing.T) {
	t.Setenv("WORKGATE_CONFIG", sep("/tmp/elsewhere.yaml"))
	got, err := Path()
	if err != nil {
		t.Fatal(err)
	}
	if want := sep("/tmp/elsewhere.yaml"); got != want {
		t.Errorf("Path() = %q, want %q", got, want)
	}
}

func TestPathDefaultsUnderTheUserConfigDirectory(t *testing.T) {
	t.Setenv("WORKGATE_CONFIG", "")
	got, err := Path()
	if err != nil {
		t.Skipf("no user config directory here: %v", err)
	}
	// The directory itself is the OS's business; what this pins is that
	// workgate claims a directory of its own inside it, and that the file is
	// the one the documentation names.
	if want := filepath.Join("Workgate", FileName); !strings.HasSuffix(got, want) {
		t.Errorf("Path() = %q, want it to end in %q", got, want)
	}
	base, err := os.UserConfigDir()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, base) {
		t.Errorf("Path() = %q, want it under the user config directory %q", got, base)
	}
	// The config file must not land in the cache directory, which is
	// disposable and may be swept at any time.
	if cache, err := os.UserCacheDir(); err == nil && base != cache && strings.HasPrefix(got, cache) {
		t.Errorf("Path() = %q is under the cache directory %q", got, cache)
	}
}

// A machine with no configuration file is the ordinary case, and every command
// has to work on it.
func TestMissingFileIsNotAnError(t *testing.T) {
	c, err := loadFile(filepath.Join(t.TempDir(), "absent.yaml"))
	if err != nil {
		t.Fatalf("loadFile on a missing file: %v", err)
	}
	cmd := sep("C:/tools/go/bin/go.exe test ./...")
	if got := c.ShortenCommand(cmd); got != cmd {
		t.Errorf("ShortenCommand = %q, want it unchanged", got)
	}
}

// The zero value is what a caller holds before Load runs and after Load fails,
// so it has to be usable rather than merely non-nil.
func TestZeroAndNilConfigsShortenNothing(t *testing.T) {
	cmd := sep("C:/tools/go/bin/go.exe test")
	if got := (&Config{}).ShortenCommand(cmd); got != cmd {
		t.Errorf("zero Config: ShortenCommand = %q, want it unchanged", got)
	}
	var nilCfg *Config
	if got := nilCfg.ShortenCommand(cmd); got != cmd {
		t.Errorf("nil Config: ShortenCommand = %q, want it unchanged", got)
	}
}

func TestEmptyFileConfiguresNothing(t *testing.T) {
	for name, contents := range map[string]string{
		"empty":             "",
		"comment only":      "# nothing set yet\n",
		"empty display":     "display:\n",
		"empty prefix list": "display:\n  strip-prefixes: []\n",
	} {
		t.Run(name, func(t *testing.T) {
			cmd := sep("C:/tools/go/bin/go.exe test")
			if got := load(t, contents).ShortenCommand(cmd); got != cmd {
				t.Errorf("ShortenCommand = %q, want it unchanged", got)
			}
		})
	}
}

func TestStripsAConfiguredPrefix(t *testing.T) {
	c := load(t, "display:\n  strip-prefixes:\n    - "+sep("C:/tools/go/bin/")+"\n")
	got := c.ShortenCommand(sep("C:/tools/go/bin/go.exe") + " test ./...")
	if want := "go.exe test ./..."; got != want {
		t.Errorf("ShortenCommand = %q, want %q", got, want)
	}
}

// A prefix that appears again in the arguments goes too. Shortening only the
// program would leave the line the length it was, which is the thing the
// setting exists to fix.
func TestStripsEveryOccurrence(t *testing.T) {
	c := load(t, "display:\n  strip-prefixes:\n    - "+sep("D:/Projects/")+"\n")
	got := c.ShortenCommand(sep("D:/Projects/tools/build.exe") + " --out " + sep("D:/Projects/app/dist"))
	if want := sep("tools/build.exe") + " --out " + sep("app/dist"); got != want {
		t.Errorf("ShortenCommand = %q, want %q", got, want)
	}
}

// Written without one, a prefix still means "up to and including the
// separator": leaving it would produce "\build\tool.exe", which reads as a
// path from the root of the drive and is not one.
func TestPrefixWithoutTrailingSeparator(t *testing.T) {
	c := load(t, "display:\n  strip-prefixes:\n    - "+sep("D:/Projects/workgate")+"\n")
	got := c.ShortenCommand(sep("D:/Projects/workgate/build/tool.exe") + " --fast")
	if want := sep("build/tool.exe") + " --fast"; got != want {
		t.Errorf("ShortenCommand = %q, want %q", got, want)
	}
}

// The more specific of two overlapping prefixes has to claim an occurrence
// first, whatever order the file lists them in. The shorter one matching first
// would leave "workgate\" behind.
func TestLongestOverlappingPrefixWins(t *testing.T) {
	both := "display:\n  strip-prefixes:\n    - " + sep("D:/Projects/") +
		"\n    - " + sep("D:/Projects/workgate/build/") + "\n"
	got := load(t, both).ShortenCommand(sep("D:/Projects/workgate/build/tool.exe"))
	if want := "tool.exe"; got != want {
		t.Errorf("ShortenCommand = %q, want %q", got, want)
	}
	// And the shorter prefix still applies where the longer one does not.
	got = load(t, both).ShortenCommand(sep("D:/Projects/other/tool.exe"))
	if want := sep("other/tool.exe"); got != want {
		t.Errorf("ShortenCommand = %q, want %q", got, want)
	}
}

// Blank entries are what a half-edited list leaves behind. An empty prefix
// matches everywhere, so taking one seriously would shred every command.
func TestBlankPrefixesAreIgnored(t *testing.T) {
	c := load(t, "display:\n  strip-prefixes:\n    - \"\"\n    - \"   \"\n")
	cmd := sep("C:/tools/go.exe") + " test"
	if got := c.ShortenCommand(cmd); got != cmd {
		t.Errorf("ShortenCommand = %q, want it unchanged", got)
	}
}

// Surrounding whitespace in the file is not part of the path the user meant.
//
// Single-quoted, because a Windows path in a *double*-quoted YAML scalar is
// not the path it looks like: there, \t is a tab and \b a backspace. Users
// meet this too, which is why the README writes prefixes unquoted.
func TestPrefixesAreTrimmed(t *testing.T) {
	c := load(t, "display:\n  strip-prefixes:\n    - '  "+sep("C:/tools/")+"  '\n")
	if got := c.ShortenCommand(sep("C:/tools/go.exe")); got != "go.exe" {
		t.Errorf("ShortenCommand = %q, want %q", got, "go.exe")
	}
}

// A command that is nothing but a prefix has no shorter form worth showing,
// and the entry would quietly lose the line it had.
func TestAPrefixNeverSwallowsTheWholeCommand(t *testing.T) {
	c := load(t, "display:\n  strip-prefixes:\n    - "+sep("C:/tools/")+"\n")
	cmd := sep("C:/tools/")
	if got := c.ShortenCommand(cmd); got != cmd {
		t.Errorf("ShortenCommand = %q, want the command back whole", got)
	}
}

func TestEmptyCommandStaysEmpty(t *testing.T) {
	c := load(t, "display:\n  strip-prefixes:\n    - "+sep("C:/tools/")+"\n")
	if got := c.ShortenCommand(""); got != "" {
		t.Errorf("ShortenCommand(\"\") = %q, want \"\"", got)
	}
}

// A prefix that is nowhere in the command leaves it exactly as it was, bytes
// and all — the fast path must not quietly rewrite separators or case.
func TestNonMatchingPrefixLeavesTheCommandAlone(t *testing.T) {
	c := load(t, "display:\n  strip-prefixes:\n    - "+sep("C:/tools/")+"\n")
	cmd := sep("D:/Projects/App/Run.EXE") + " --Flag"
	if got := c.ShortenCommand(cmd); got != cmd {
		t.Errorf("ShortenCommand = %q, want %q", got, cmd)
	}
}

// What survives a strip is the text the command actually had, never the
// folded form matching used. This is the property that makes the ASCII fold
// safe, and it is worth pinning on every platform.
func TestTheKeptTextIsVerbatim(t *testing.T) {
	c := load(t, "display:\n  strip-prefixes:\n    - "+sep("C:/tools/")+"\n")
	got := c.ShortenCommand(sep("C:/tools/Run-IT.EXE") + " --Mixed/Case")
	if want := "Run-IT.EXE --Mixed/Case"; got != want {
		t.Errorf("ShortenCommand = %q, want %q", got, want)
	}
}

// Windows path comparison ignores ASCII case and treats the two separators as
// one; everywhere else a path matches only itself.
func TestPlatformPathEquivalence(t *testing.T) {
	c := load(t, "display:\n  strip-prefixes:\n    - "+`C:\Program Files\Tools\`+"\n")
	for _, cmd := range []string{
		`c:\program files\tools\run.exe --go`,
		`C:/Program Files/Tools/run.exe --go`,
		`C:\PROGRAM FILES\tools/run.exe --go`,
	} {
		got := c.ShortenCommand(cmd)
		shortened := got != cmd
		if shortened != (runtime.GOOS == "windows") {
			t.Errorf("ShortenCommand(%q) = %q; shortened=%v, want %v",
				cmd, got, shortened, runtime.GOOS == "windows")
		}
		if runtime.GOOS == "windows" && got != "run.exe --go" {
			t.Errorf("ShortenCommand(%q) = %q, want %q", cmd, got, "run.exe --go")
		}
	}
}

// Non-ASCII is left to match itself, which is all the ASCII-only fold
// promises — and it must not corrupt what it passes through.
func TestNonASCIIPathsMatchThemselves(t *testing.T) {
	c := load(t, "display:\n  strip-prefixes:\n    - "+sep("C:/Users/josé/Projets/Été/")+"\n")
	got := c.ShortenCommand(sep("C:/Users/josé/Projets/Été/exécuter.exe") + " --naïve")
	if want := "exécuter.exe --naïve"; got != want {
		t.Errorf("ShortenCommand = %q, want %q", got, want)
	}
}

// The mistake this catches has no other symptom: an unrecognized key would
// simply be dropped, and the setting would appear to do nothing at all.
func TestUnknownKeysAreRejected(t *testing.T) {
	for name, contents := range map[string]string{
		"misspelled setting": "display:\n  strip_prefixes:\n    - /tmp/\n",
		"misspelled section": "displays:\n  strip-prefixes:\n    - /tmp/\n",
		"invented setting":   "display:\n  strip-suffixes:\n    - /tmp/\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := write(t, contents)
			c, err := loadFile(path)
			if err == nil {
				t.Fatal("loadFile accepted an unknown key, want an error")
			}
			if !strings.Contains(err.Error(), path) {
				t.Errorf("error %q does not name the file %q", err, path)
			}
			// A caller that reports the error and carries on must still get
			// a configuration it can render through.
			if got := c.ShortenCommand("go test"); got != "go test" {
				t.Errorf("ShortenCommand after a failed load = %q, want it unchanged", got)
			}
		})
	}
}

func TestMalformedYAMLIsReported(t *testing.T) {
	path := write(t, "display:\n  strip-prefixes:\n  - [unclosed\n")
	if _, err := loadFile(path); err == nil {
		t.Fatal("loadFile accepted malformed YAML, want an error")
	} else if !strings.Contains(err.Error(), path) {
		t.Errorf("error %q does not name the file %q", err, path)
	}
}

// A scalar where a list belongs is a plausible slip, and it is a mistake
// rather than a shorthand: reporting it is what tells the user why nothing
// changed.
func TestAScalarWhereAListBelongsIsReported(t *testing.T) {
	if _, err := loadFile(write(t, "display:\n  strip-prefixes: "+sep("C:/tools/")+"\n")); err == nil {
		t.Fatal("loadFile accepted a scalar for strip-prefixes, want an error")
	}
}

func TestLoadReadsThePathFromTheEnvironment(t *testing.T) {
	path := write(t, "display:\n  strip-prefixes:\n    - "+sep("C:/tools/")+"\n")
	t.Setenv("WORKGATE_CONFIG", path)
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := c.ShortenCommand(sep("C:/tools/go.exe") + " test"); got != "go.exe test" {
		t.Errorf("ShortenCommand = %q, want %q", got, "go.exe test")
	}
}
