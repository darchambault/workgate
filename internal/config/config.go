// Package config resolves and reads workgate's optional user configuration:
// the one file that changes how the views read, and nothing about how
// workloads are coordinated.
//
// Configuration is a display concern here by design. Nothing in this package
// is consulted while acquiring, releasing, or ordering a resource, so two
// sessions with different files still queue against each other identically —
// only what each one sees on screen differs.
package config

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	yaml "go.yaml.in/yaml/v3"
)

// FileName is the configuration file's name inside the workgate directory.
const FileName = "config.yaml"

// Path returns the configuration file's path for the current OS user,
// <user config dir>/Workgate/config.yaml, resolved via os.UserConfigDir:
// %APPDATA%\Workgate\config.yaml on Windows,
// ~/Library/Application Support/Workgate/config.yaml on macOS, and
// $XDG_CONFIG_HOME/Workgate/config.yaml (default ~/.config/...) on Linux.
//
// This is the user's config directory, not the cache directory the database
// lives in. The two are deliberately apart: the database is disposable
// coordination state that a cache sweep may take at any time, and the config
// file is something a person wrote and expects to still be there.
//
// The WORKGATE_CONFIG environment variable overrides the path, mirroring
// WORKGATE_DB. It exists for tests and for trying a file out; it is not the
// intended way to configure workgate.
func Path() (string, error) {
	if p := os.Getenv("WORKGATE_CONFIG"); p != "" {
		return p, nil
	}
	base, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolving user config directory: %w", err)
	}
	return filepath.Join(base, "Workgate", FileName), nil
}

// Config is the whole of workgate's user configuration.
//
// The compiled form of the settings is kept alongside the decoded YAML rather
// than derived per call: Load normalizes once, and the views then call
// ShortenCommand for every line of every frame.
type Config struct {
	Display Display `yaml:"display"`

	// stripPrefixes is Display.StripPrefixes normalized and ordered by
	// descending length, so that where two configured prefixes overlap the
	// more specific one claims an occurrence first.
	stripPrefixes []string
}

// Display holds the settings that only affect how `status` and `monitor`
// render. A section rather than top-level keys, so that a later setting which
// does change behaviour cannot be mistaken for one of these.
type Display struct {
	// StripPrefixes are leading paths removed from the command shown under
	// an entry. Machines accumulate long, uninteresting install roots —
	// toolchain directories, virtualenvs, an agent's worktree parent — and
	// repeating one on every line pushes the part that differs off the
	// right edge of a narrow terminal.
	StripPrefixes []string `yaml:"strip-prefixes"`
}

// Load reads the configuration file, or returns an empty configuration if
// there is none. A missing file is the overwhelmingly common case and is not
// an error: workgate is usable with no configuration at all, which is why
// there is no command to create one.
//
// A file that exists but cannot be read or parsed *is* an error. Ignoring it
// would leave a person staring at a view that quietly disregards what they
// wrote, with nothing on screen to say so; callers report it and carry on
// unconfigured, which is the same view they had before the file existed.
//
// Decoding is strict: a key this version does not know is rejected rather
// than dropped. "strip_prefixes" for "strip-prefixes" is the mistake this
// catches, and it is exactly the mistake whose only other symptom would be a
// setting that appears to do nothing.
func Load() (*Config, error) {
	path, err := Path()
	if err != nil {
		return &Config{}, err
	}
	return loadFile(path)
}

func loadFile(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return &Config{}, nil
		}
		return &Config{}, fmt.Errorf("reading %s: %w", path, err)
	}
	defer f.Close()

	var c Config
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		// An empty file reports io.EOF rather than decoding to an empty
		// document. A file with nothing in it configures nothing, which is
		// what the zero Config already says.
		if errors.Is(err, io.EOF) {
			return &Config{}, nil
		}
		return &Config{}, fmt.Errorf("parsing %s: %w", path, err)
	}
	c.compile()
	return &c, nil
}

// compile turns the decoded settings into the form the views use.
func (c *Config) compile() {
	c.stripPrefixes = nil
	for _, p := range c.Display.StripPrefixes {
		if p = normalizePrefix(p); p != "" {
			c.stripPrefixes = append(c.stripPrefixes, p)
		}
	}
	// Longest first. Given both "D:\Projects\" and "D:\Projects\workgate\",
	// an occurrence of the latter should lose the whole of it, not be left
	// holding "workgate\" because the shorter prefix matched first.
	sort.SliceStable(c.stripPrefixes, func(i, j int) bool {
		return len(c.stripPrefixes[i]) > len(c.stripPrefixes[j])
	})
}

// normalizePrefix trims a configured prefix and gives it a trailing separator,
// returning "" for one that holds nothing.
//
// The trailing separator is supplied rather than required, so that
// "D:\Projects\workgate" and "D:\Projects\workgate\" mean the same thing.
// Without it the first would leave the command starting on a bare separator —
// "\build\tool.exe" — which reads like a path from the root of the drive and
// is not one.
func normalizePrefix(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return ""
	}
	if !isSeparator(p[len(p)-1]) {
		p += string(os.PathSeparator)
	}
	return p
}

// ShortenCommand returns command with every configured prefix removed, and is
// the only thing the views call. The zero Config, which is what a machine with
// no configuration file has, returns command unchanged.
//
// Every occurrence goes, not only the one at the front: a command commonly
// names the same root twice — once for the program and once for the file it
// is given — and shortening only the first would leave the line nearly as
// long as it was, for no gain in what can be read from it.
//
// A prefix that swallows the command whole is ignored. There is no useful
// line left to show, and the entry would silently lose the row it had.
func (c *Config) ShortenCommand(command string) string {
	if c == nil || command == "" {
		return command
	}
	short := command
	for _, p := range c.stripPrefixes {
		short = stripEvery(short, p)
	}
	if short == "" {
		return command
	}
	return short
}

// stripEvery removes every occurrence of prefix from s, comparing under the
// platform's own idea of when two paths are the same one.
func stripEvery(s, prefix string) string {
	hay, needle := matchKey(s), matchKey(prefix)
	if !strings.Contains(hay, needle) {
		return s
	}
	// matchKey is byte-for-byte length preserving, so an index into the
	// folded string is the same index into the original, and what is written
	// out is always the text the command actually had.
	var b strings.Builder
	for {
		i := strings.Index(hay, needle)
		if i < 0 {
			b.WriteString(s)
			return b.String()
		}
		b.WriteString(s[:i])
		s, hay = s[i+len(needle):], hay[i+len(needle):]
	}
}

// matchKey folds a string into the form paths are compared in, without
// changing its length: on Windows ASCII case is ignored and the two
// separators are treated as one, and everywhere else comparison is exact.
//
// Folding only ASCII is a deliberate limit. Unicode case folding does not
// preserve length — Turkish dotted capital I lowercases to two runes — and an
// index into the folded string would stop being an index into the original,
// which is the one property this function exists to have. Install roots and
// drive letters are ASCII; a directory named in another script still matches
// itself.
func matchKey(s string) string {
	if runtime.GOOS != "windows" {
		return s
	}
	b := []byte(s)
	for i, c := range b {
		switch {
		case c == '/':
			b[i] = '\\'
		case c >= 'A' && c <= 'Z':
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}

// isSeparator reports whether c ends a path element. Windows accepts both
// separators, and a configured prefix written with either should be taken as
// already terminated.
func isSeparator(c byte) bool {
	return c == '/' || (runtime.GOOS == "windows" && c == '\\')
}
