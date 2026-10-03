// Package proc answers one question about another process on this machine:
// has it certainly exited? It exists so that a workload whose owner is known
// to be dead can be reclaimed by a view that otherwise only reads the queue.
//
// Every answer is one-sided. Exited returns true only when it is sure, and
// false both for "running" and for "cannot tell" — an access check that fails,
// a pid it has no way to judge. Callers act on true and leave everything else
// alone, so a wrong answer can only ever be a missed reclaim.
package proc
