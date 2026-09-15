package cpu

import (
	"bytes"
	"os"
	"strconv"
)

// Sample is one reading of the aggregate "cpu" line's counters, in jiffies
// since boot.
type Sample struct {
	Total uint64
	Idle  uint64
}

// Parse sums the aggregate "cpu" line of /proc/stat. Idle counts both the
// idle and iowait columns: a server blocked on a dead mount is waiting, not
// burning CPU, and load average is what surfaces that instead.
//
// The guest and guest_nice columns are left out of Total: the kernel already
// counts guest time inside user and nice, so adding them again would inflate
// Total and busy time by the same amount, overstating cpuPercent on a host
// running VMs.
func Parse(data []byte) (Sample, error) {
	for _, line := range bytes.Split(data, []byte("\n")) {
		fields := bytes.Fields(line)
		if len(fields) < 5 || string(fields[0]) != "cpu" {
			continue
		}

		var s Sample
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
		return s, nil
	}
	return Sample{}, os.ErrInvalid
}
