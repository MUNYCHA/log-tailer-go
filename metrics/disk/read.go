// Package disk reports usage of a mounted filesystem, from the statfs system
// call.
package disk

import "syscall"

// Read runs statfs on a mount path. It is the only function in this package
// that touches the server.
func Read(path string) (syscall.Statfs_t, error) {
	var stat syscall.Statfs_t
	err := syscall.Statfs(path, &stat)
	return stat, err
}
