package cpu

import (
	"bytes"
	"os"
	"strconv"
)

// Sample is one reading of /proc/stat: the aggregate "cpu" line's counters,
// in jiffies since boot, and how many per-CPU lines the file lists.
type Sample struct {
	Total uint64
	Idle  uint64
	CPUs  int // "cpu0" … "cpuN" lines: online logical CPUs
}

// Parse sums the aggregate "cpu" line of /proc/stat and counts the per-CPU
// lines. Idle counts both the idle and iowait columns: a server blocked on a
// dead mount is waiting, not burning CPU, and load average is what surfaces
// that instead.
//
// The guest and guest_nice columns are left out of Total: the kernel already
// counts guest time inside user and nice, so adding them again would inflate
// Total and busy time by the same amount, overstating the busy percentage on a
// host running VMs.
func Parse(data []byte) (Sample, error) {
	var s Sample
	found := false
	for _, line := range bytes.Split(data, []byte("\n")) {
		fields := bytes.Fields(line)
		if len(fields) < 5 || !bytes.HasPrefix(fields[0], []byte("cpu")) {
			continue
		}
		if name := fields[0]; len(name) > 3 {
			if name[3] >= '0' && name[3] <= '9' {
				s.CPUs++
			}
			continue
		}
		if found {
			continue
		}

		for i, field := range fields[1:] {
			v, err := strconv.ParseUint(string(field), 10, 64)
			if err != nil {
				return Sample{}, err
			}
			// Columns are user, nice, system, idle, iowait, irq, softirq,
			// steal, guest, guest_nice
			if i == 8 || i == 9 {
				continue
			}
			s.Total += v
			if i == 3 || i == 4 {
				s.Idle += v
			}
		}
		found = true
	}
	if !found {
		return Sample{}, os.ErrInvalid
	}
	return s, nil
}
