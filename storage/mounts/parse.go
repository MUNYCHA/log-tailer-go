package mounts

import (
	"bytes"
	"strings"
)

// Entry is one mounted filesystem.
type Entry struct {
	Device string // e.g. /dev/sda1, tmpfs, server:/export
	Path   string // mount point
	FSType string // e.g. ext4, tmpfs, nfs4
}

// Parse reads the mount table's "device path fstype options dump pass" lines,
// in file order. Lines with fewer than three fields are skipped. The kernel
// escapes space, tab, newline and backslash in device and path as octal
// (\040, \011, \012, \134); those are decoded so a path matches what the
// config says.
func Parse(data []byte) []Entry {
	var entries []Entry
	for _, line := range bytes.Split(data, []byte("\n")) {
		fields := bytes.Fields(line)
		if len(fields) < 3 {
			continue
		}
		entries = append(entries, Entry{
			Device: unescape(string(fields[0])),
			Path:   unescape(string(fields[1])),
			FSType: string(fields[2]),
		})
	}
	return entries
}

// unescape decodes \NNN octal escapes. A backslash not followed by three octal
// digits is kept as is.
func unescape(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) && isOctal(s[i+1]) && isOctal(s[i+2]) && isOctal(s[i+3]) {
			b.WriteByte((s[i+1]-'0')<<6 | (s[i+2]-'0')<<3 | (s[i+3] - '0'))
			i += 3
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func isOctal(c byte) bool {
	return c >= '0' && c <= '7'
}
