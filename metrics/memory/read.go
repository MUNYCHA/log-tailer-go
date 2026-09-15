// Package memory reports RAM and swap in bytes, from /proc/meminfo.
package memory

import "os"

// Path is where the kernel publishes memory statistics.
const Path = "/proc/meminfo"

// Read returns the raw content of /proc/meminfo. It is the only function in
// this package that touches the server.
func Read() ([]byte, error) {
	return os.ReadFile(Path)
}
