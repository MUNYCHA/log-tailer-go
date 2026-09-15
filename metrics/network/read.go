// Package network reports download (rx) and upload (tx) bytes per second
// between two ticks, from /proc/net/dev.
package network

import (
	"os"
	"path/filepath"
)

// Path is where the kernel publishes cumulative per-interface counters.
const Path = "/proc/net/dev"

const sysNetPath = "/sys/class/net"

// Read returns the raw content of /proc/net/dev. It and KeepPhysical are the
// only functions in this package that touch the server.
func Read() ([]byte, error) {
	return os.ReadFile(Path)
}

// KeepPhysical removes every interface not backed by a device. Loopback,
// bridges, veths and tunnels have no device link, and counting them would
// double count traffic that also crosses the NIC (docker0 plus eth0).
func KeepPhysical(s Sample) {
	for iface := range s {
		if _, err := os.Stat(filepath.Join(sysNetPath, iface, "device")); err != nil {
			delete(s, iface)
		}
	}
}
