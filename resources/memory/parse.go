package memory

import (
	"bytes"
	"fmt"
	"strconv"
)

// Sample is the /proc/meminfo lines we use, in kB as the file has them. Every
// line is read on every tick; the Has flags record which ones the kernel
// published, so memory and swap can each be reported or omitted on their own.
type Sample struct {
	MemTotalKB     uint64
	MemAvailableKB uint64

	// Only used to estimate available memory on kernels without MemAvailable
	MemFreeKB      uint64
	BuffersKB      uint64
	CachedKB       uint64
	ShmemKB        uint64
	SReclaimableKB uint64

	SwapTotalKB uint64
	SwapFreeKB  uint64

	HasMemTotal     bool
	HasMemAvailable bool
	HasEstimate     bool // MemFree, Buffers, Cached, Shmem and SReclaimable all present
	HasSwap         bool // SwapTotal and SwapFree both present
}

// Parse pulls the lines we use from /proc/meminfo in one pass. A missing line
// is not an error here — it only clears the matching Has flag — but a line
// whose value isn't a number fails the whole sample, since the file can't be
// trusted.
func Parse(data []byte) (Sample, error) {
	var s Sample
	targets := map[string]*uint64{
		"MemTotal:":     &s.MemTotalKB,
		"MemAvailable:": &s.MemAvailableKB,
		"MemFree:":      &s.MemFreeKB,
		"Buffers:":      &s.BuffersKB,
		"Cached:":       &s.CachedKB,
		"Shmem:":        &s.ShmemKB,
		"SReclaimable:": &s.SReclaimableKB,
		"SwapTotal:":    &s.SwapTotalKB,
		"SwapFree:":     &s.SwapFreeKB,
	}

	seen := make(map[string]bool, len(targets))
	for _, line := range bytes.Split(data, []byte("\n")) {
		fields := bytes.Fields(line)
		if len(fields) < 2 {
			continue
		}
		key := string(fields[0])
		target, wanted := targets[key]
		if !wanted {
			continue
		}
		kb, err := strconv.ParseUint(string(fields[1]), 10, 64)
		if err != nil {
			return Sample{}, fmt.Errorf("%s: %w", key, err)
		}
		*target = kb
		seen[key] = true
	}

	s.HasMemTotal = seen["MemTotal:"]
	s.HasMemAvailable = seen["MemAvailable:"]
	s.HasEstimate = seen["MemFree:"] && seen["Buffers:"] && seen["Cached:"] && seen["Shmem:"] && seen["SReclaimable:"]
	s.HasSwap = seen["SwapTotal:"] && seen["SwapFree:"]
	return s, nil
}
