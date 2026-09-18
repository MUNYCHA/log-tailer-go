package mounts

import "testing"

// Trimmed from a real WSL host: duplicates of one disk, RAM and kernel
// filesystems, Windows drives over 9p, and escaped spaces and backslashes
const sampleMounts = `none /usr/lib/modules/6.6.114.1-microsoft-standard-WSL2 overlay rw,nosuid,nodev,noatime 0 0
none /mnt/wsl tmpfs rw,relatime 0 0
/dev/sdd / ext4 rw,relatime,discard,errors=remount-ro,data=ordered 0 0
/dev/sdd /mnt/wslg/distro ext4 ro,relatime,discard,errors=remount-ro,data=ordered 0 0
proc /proc proc rw,nosuid,nodev,noexec,noatime 0 0
tmpfs /tmp tmpfs rw,nosuid,nodev 0 0
C:\134 /mnt/c 9p rw,noatime,dirsync,aname=drvfs 0 0
C:\134Program\040Files\134Docker /Docker/host 9p rw,noatime 0 0
/dev/sdf /mnt/wsl/docker-desktop/docker-desktop-user-distro ext4 rw,relatime 0 0
/dev/loop0 /mnt/wsl/docker-desktop/cli-tools iso9660 ro,relatime 0 0
/dev/sdb1 /mnt/my\040disk xfs rw,relatime 0 0
broken line
`

func TestParse(t *testing.T) {
	got := Parse([]byte(sampleMounts))
	if len(got) != 11 {
		t.Fatalf("expected 11 entries (broken line skipped), got %d", len(got))
	}
	want := Entry{Device: "/dev/sdd", Path: "/", FSType: "ext4"}
	if got[2] != want {
		t.Fatalf("expected %+v, got %+v", want, got[2])
	}
}

func TestParse_DecodesOctalEscapes(t *testing.T) {
	got := Parse([]byte(sampleMounts))
	if got[6].Device != `C:\` {
		t.Fatalf(`expected device C:\, got %q`, got[6].Device)
	}
	if got[7].Device != `C:\Program Files\Docker` {
		t.Fatalf("expected escaped spaces and backslashes decoded, got %q", got[7].Device)
	}
	if got[10].Path != "/mnt/my disk" {
		t.Fatalf("expected path with a space, got %q", got[10].Path)
	}
}

func TestUnescape_KeepsIncompleteEscapes(t *testing.T) {
	for in, want := range map[string]string{
		`a\04`:   `a\04`,
		`a\089b`: `a\089b`,
		`end\`:   `end\`,
		`plain`:  `plain`,
	} {
		if got := unescape(in); got != want {
			t.Fatalf("unescape(%q): expected %q, got %q", in, want, got)
		}
	}
}

func TestFind_LongestMountPointWins(t *testing.T) {
	entries := Parse([]byte(sampleMounts))
	cases := map[string]string{
		"/":                 "/",
		"/var/log":          "/",
		"/tmp":              "/tmp",
		"/tmp/sub/dir":      "/tmp",
		"/mnt/c":            "/mnt/c",
		"/mnt/c/Users":      "/mnt/c",
		"/mnt/my disk/data": "/mnt/my disk",
		"/var/log/":         "/",
	}
	for path, wantMount := range cases {
		got, ok := Find(entries, path)
		if !ok {
			t.Fatalf("%s: expected a match", path)
		}
		if got.Path != wantMount {
			t.Fatalf("%s: expected mount %q, got %q", path, wantMount, got.Path)
		}
	}
}

func TestFind_MatchesWholeComponentsOnly(t *testing.T) {
	entries := []Entry{
		{Device: "/dev/sda1", Path: "/", FSType: "ext4"},
		{Device: "/dev/sdb1", Path: "/var", FSType: "xfs"},
	}
	got, _ := Find(entries, "/variable")
	if got.Path != "/" {
		t.Fatalf("expected /variable to live on /, got %q", got.Path)
	}
	got, _ = Find(entries, "/var/log")
	if got.Path != "/var" {
		t.Fatalf("expected /var/log to live on /var, got %q", got.Path)
	}
}

func TestFind_LaterMountOnSamePathWins(t *testing.T) {
	entries := []Entry{
		{Device: "/dev/sda1", Path: "/", FSType: "ext4"},
		{Device: "/dev/sdb1", Path: "/data", FSType: "ext4"},
		{Device: "server:/export", Path: "/data", FSType: "nfs4"},
	}
	got, _ := Find(entries, "/data/x")
	if got.FSType != "nfs4" {
		t.Fatalf("expected the top-most mount (nfs4), got %+v", got)
	}
}

func TestFind_NoEntries(t *testing.T) {
	if _, ok := Find(nil, "/"); ok {
		t.Fatal("expected no match on an empty table")
	}
}

func TestIsLocalAndIsNetwork(t *testing.T) {
	for _, fs := range []string{"ext2", "ext3", "ext4", "xfs", "btrfs", "vfat"} {
		if !IsLocal(fs) || IsNetwork(fs) {
			t.Fatalf("expected %s local only", fs)
		}
	}
	for _, fs := range []string{"nfs", "nfs4", "cifs", "smb3", "ceph", "glusterfs", "fuse.sshfs", "9p"} {
		if !IsNetwork(fs) || IsLocal(fs) {
			t.Fatalf("expected %s network only", fs)
		}
	}
	for _, fs := range []string{"tmpfs", "proc", "overlay", "squashfs", "iso9660", "zfs", ""} {
		if IsLocal(fs) || IsNetwork(fs) {
			t.Fatalf("expected %s to be neither local nor network", fs)
		}
	}
}

func TestRead_RealMountTable(t *testing.T) {
	data, err := Read()
	if err != nil {
		t.Fatalf("unexpected error reading %s: %v", Path, err)
	}
	if _, ok := Find(Parse(data), "/"); !ok {
		t.Fatal("expected the real mount table to hold /")
	}
}

func TestLocalFilesystems_RealWSLTable(t *testing.T) {
	got := LocalFilesystems(Parse([]byte(sampleMounts)))
	want := []Entry{
		{Device: "/dev/sdd", Path: "/", FSType: "ext4"},
		{Device: "/dev/sdb1", Path: "/mnt/my disk", FSType: "xfs"},
		{Device: "/dev/sdf", Path: "/mnt/wsl/docker-desktop/docker-desktop-user-distro", FSType: "ext4"},
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d local filesystems, got %d: %+v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("entry %d: expected %+v, got %+v", i, want[i], got[i])
		}
	}
}

func TestLocalFilesystems_ShortestPathOwnsTheDevice(t *testing.T) {
	// The bind mount is listed first, but "/" must be the entry kept
	got := LocalFilesystems([]Entry{
		{Device: "/dev/sda1", Path: "/srv/bind", FSType: "ext4"},
		{Device: "/dev/sda1", Path: "/", FSType: "ext4"},
		{Device: "/dev/sda1", Path: "/home", FSType: "ext4"},
	})
	if len(got) != 1 || got[0].Path != "/" {
		t.Fatalf("expected only / for /dev/sda1, got %+v", got)
	}
}

func TestLocalFilesystems_SkipsLoopAndNonLocal(t *testing.T) {
	got := LocalFilesystems([]Entry{
		{Device: "/dev/loop3", Path: "/snap/core/1", FSType: "ext4"},
		{Device: "nas:/export", Path: "/mnt/nas", FSType: "nfs4"},
		{Device: "tmpfs", Path: "/run", FSType: "tmpfs"},
		{Device: "tank/data", Path: "/tank", FSType: "zfs"},
		{Device: "/dev/sda2", Path: "/boot/efi", FSType: "vfat"},
	})
	if len(got) != 1 || got[0].Path != "/boot/efi" {
		t.Fatalf("expected only /boot/efi, got %+v", got)
	}
}

func TestLocalFilesystems_Empty(t *testing.T) {
	if got := LocalFilesystems(nil); len(got) != 0 {
		t.Fatalf("expected none, got %+v", got)
	}
}
