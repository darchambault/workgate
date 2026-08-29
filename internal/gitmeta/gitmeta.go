// Package gitmeta collects best-effort diagnostic context about the invoking
// environment. Nothing here affects locking semantics: failure to determine
// Git information never prevents Workgate from operating.
package gitmeta

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Info is the diagnostic context recorded with a workload.
type Info struct {
	WorkingDirectory string
	RepositoryRoot   string // worktree top-level (empty outside Git)
	GitCommonDir     string // shared .git directory (empty outside Git)
	GitBranch        string
	Project          string // human-facing repo name derived from the common dir
	Hostname         string
	PID              int
}

const gitTimeout = 2 * time.Second

// Collect gathers environment metadata for dir. It never fails: outside a
// Git repository (or without git on PATH) the Git fields are simply empty.
func Collect(dir string) Info {
	info := Info{WorkingDirectory: dir, PID: os.Getpid()}
	if h, err := os.Hostname(); err == nil {
		info.Hostname = h
	}

	ctx, cancel := context.WithTimeout(context.Background(), gitTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", "rev-parse",
		"--show-toplevel", "--git-common-dir", "--abbrev-ref", "HEAD")
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		return info
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) < 3 {
		return info
	}
	info.RepositoryRoot = filepath.Clean(strings.TrimSpace(lines[0]))
	commonDir := strings.TrimSpace(lines[1])
	// --git-common-dir may be relative (e.g. ".git"); resolve against the
	// worktree top-level so the stored path is unambiguous.
	if !filepath.IsAbs(commonDir) {
		commonDir = filepath.Join(info.RepositoryRoot, commonDir)
	}
	info.GitCommonDir = filepath.Clean(commonDir)
	info.GitBranch = strings.TrimSpace(lines[2])
	info.Project = projectName(info.GitCommonDir, info.RepositoryRoot)
	return info
}

// projectName derives a display name for the underlying repository. For a
// normal repo or any of its linked worktrees the common dir is
// <repo>\.git, so the parent directory's basename identifies the repo even
// when invoked from a differently-named worktree checkout.
func projectName(commonDir, root string) string {
	if filepath.Base(commonDir) == ".git" {
		return filepath.Base(filepath.Dir(commonDir))
	}
	if root != "" {
		return filepath.Base(root) // bare/unusual layouts: fall back to worktree name
	}
	return ""
}
