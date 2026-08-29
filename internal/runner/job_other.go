//go:build !windows

package runner

import "errors"

// Non-Windows builds have no job object; runner falls back to killing the
// direct child on interrupt, and heartbeat staleness covers hard kills.
type job struct{}

func newJob() (*job, error)     { return nil, errors.New("job objects unsupported on this platform") }
func (j *job) assign(int) error { return nil }
func (j *job) terminate()       {}
func (j *job) close()           {}
