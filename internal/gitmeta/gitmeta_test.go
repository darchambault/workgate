package gitmeta

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestCollectOutsideGitWorksNormally(t *testing.T) {
	dir := t.TempDir()
	info := Collect(dir)
	if info.WorkingDirectory != dir {
		t.Errorf("WorkingDirectory = %q, want %q", info.WorkingDirectory, dir)
	}
	if info.RepositoryRoot != "" || info.GitCommonDir != "" || info.GitBranch != "" {
		t.Errorf("expected empty git fields outside a repo, got %+v", info)
	}
	if info.PID != os.Getpid() {
		t.Errorf("PID = %d, want %d", info.PID, os.Getpid())
	}
}

func git(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func TestWorktreeResolvesToSameProject(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	base := t.TempDir()
	repo := filepath.Join(base, "AirportTycoon")
	if err := os.Mkdir(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	git(t, repo, "init", "-b", "main")
	if err := os.WriteFile(filepath.Join(repo, "f.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, repo, "add", ".")
	git(t, repo, "commit", "-m", "init")
	worktree := filepath.Join(base, "AirportTycoon-codex-42")
	git(t, repo, "worktree", "add", "-b", "agent", worktree)

	main := Collect(repo)
	wt := Collect(worktree)

	if main.Project != "AirportTycoon" {
		t.Errorf("main repo Project = %q, want AirportTycoon", main.Project)
	}
	if wt.Project != "AirportTycoon" {
		t.Errorf("worktree Project = %q, want AirportTycoon (same underlying repo)", wt.Project)
	}
	if wt.RepositoryRoot == main.RepositoryRoot {
		t.Errorf("worktree RepositoryRoot should differ from main repo's; both %q", wt.RepositoryRoot)
	}
	if wt.GitBranch != "agent" {
		t.Errorf("worktree branch = %q, want agent", wt.GitBranch)
	}
}
