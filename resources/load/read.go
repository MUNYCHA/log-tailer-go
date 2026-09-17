// Package load reports the 1, 5 and 15 minute load averages, from
// /proc/loadavg.
package load

import "os"

// Path is where the kernel publishes the load averages.
const Path = "/proc/loadavg"

// Read returns the raw content of /proc/loadavg. It is the only function in
// this package that touches the server.
func Read() ([]byte, error) {
	return os.ReadFile(Path)
}
