package mounts

import (
	"path/filepath"
	"sort"
	"strings"
)

// localTypes are filesystems on a disk in this server.
var localTypes = map[string]bool{
	"ext2": true, "ext3": true, "ext4": true, "xfs": true, "btrfs": true, "vfat": true,
}

// networkTypes are filesystems served by another machine. statfs on one can
// block for as long as the remote server is unreachable, so they are never
// statted.
var networkTypes = map[string]bool{
	"nfs": true, "nfs4": true, "cifs": true, "smb3": true, "ceph": true,
	"glusterfs": true, "fuse.sshfs": true, "9p": true,
}

// IsLocal reports whether fsType is a local disk filesystem. It is an
// allowlist: an unknown type is not local.
func IsLocal(fsType string) bool {
	return localTypes[fsType]
}

// IsNetwork reports whether fsType is served over the network.
func IsNetwork(fsType string) bool {
	return networkTypes[fsType]
}

// Find returns the filesystem a path lives on: the entry whose mount point is
// the longest match for the path, compared by whole path components ("/var"
// holds "/var/log" but not "/variable"). When the same mount point appears
// more than once, the later entry wins, since it is mounted on top.
//
// The path is matched as written, without resolving symlinks: resolving
// would stat every component and could block on a dead network mount, which
// is exactly what the lookup exists to avoid.
func Find(entries []Entry, path string) (Entry, bool) {
	path = filepath.Clean(path)

	var best Entry
	found := false
	for _, e := range entries {
		if !contains(e.Path, path) {
			continue
		}
		if !found || len(e.Path) >= len(best.Path) {
			best = e
			found = true
		}
	}
	return best, found
}

// contains reports whether mount point mnt holds path.
func contains(mnt, path string) bool {
	mnt = filepath.Clean(mnt)
	if mnt == "/" {
		return strings.HasPrefix(path, "/")
	}
	return path == mnt || strings.HasPrefix(path, mnt+"/")
}

// LocalFilesystems picks the filesystems that make up the server's own
// storage, each counted once:
//
//   - local types only (tmpfs, overlay, network and unknown types are skipped)
//   - /dev/loop* devices skipped: snaps and images, not server disks
//   - one entry per device, keeping the shortest mount point, so bind mounts
//     and btrfs subvolumes of one disk don't count it twice and "/" is the
//     entry that represents the root disk on every tick
func LocalFilesystems(entries []Entry) []Entry {
	var local []Entry
	for _, e := range entries {
		if !IsLocal(e.FSType) || strings.HasPrefix(e.Device, "/dev/loop") {
			continue
		}
		local = append(local, e)
	}

	sort.SliceStable(local, func(i, j int) bool {
		return len(local[i].Path) < len(local[j].Path)
	})

	seen := make(map[string]bool, len(local))
	unique := local[:0]
	for _, e := range local {
		if seen[e.Device] {
			continue
		}
		seen[e.Device] = true
		unique = append(unique, e)
	}
	return unique
}
