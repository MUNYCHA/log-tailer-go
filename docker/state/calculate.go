package state

import "time"

// LastExit is how a stopped container last exited, as published.
type LastExit struct {
	ExitCode   int
	FinishedAt string // RFC 3339, UTC
}

// Exit reports the container's last exit. It reports false, so the caller
// omits both fields, when there is nothing true to say: the container has
// never stopped (created but never started, where Docker's exit code 0 would
// read as a clean exit), or it started again after the container list was
// read, so the exit code belongs to a run that is already over.
func Exit(s Sample) (LastExit, bool) {
	if s.Running || s.FinishedAt.IsZero() {
		return LastExit{}, false
	}
	return LastExit{
		ExitCode:   s.ExitCode,
		FinishedAt: s.FinishedAt.UTC().Format(time.RFC3339),
	}, true
}
