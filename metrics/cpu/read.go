// Package cpu reports the mean CPU busy percentage between two ticks, from
// /proc/stat.
package cpu

import "os"

// Path is where the kernel publishes cumulative CPU time counters.
const Path = "/proc/stat"

// Read returns the raw content of /proc/stat. It is the only function in this
// package that touches the server.
func Read() ([]byte, error) {
	return os.ReadFile(Path)
}
