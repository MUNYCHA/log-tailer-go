// Package mounts reads the mount table, from /proc/self/mounts, and answers
// which filesystem a path lives on and whether it is a local disk.
package mounts

import "os"

// Path is the mount table as seen by this process. "self" rather than a pid,
// so a systemd-sandboxed agent sees its own mount namespace.
const Path = "/proc/self/mounts"

// Read returns the raw content of /proc/self/mounts. It is the only function
// in this package that touches the server.
func Read() ([]byte, error) {
	return os.ReadFile(Path)
}
