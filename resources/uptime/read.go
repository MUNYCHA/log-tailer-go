// Package uptime reports how long the server has been up, from /proc/uptime.
package uptime

import "os"

// Path is where the kernel publishes seconds since boot.
const Path = "/proc/uptime"

// Read returns the raw content of /proc/uptime. It is the only function in
// this package that touches the server.
func Read() ([]byte, error) {
	return os.ReadFile(Path)
}
