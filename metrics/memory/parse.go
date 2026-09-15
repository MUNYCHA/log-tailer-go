package memory

import (
	"bytes"
	"fmt"
	"strconv"
)

// Sample is the four /proc/meminfo lines we report, in kB as the file has them.
type Sample struct {
	MemTotalKB     uint64
	MemAvailableKB uint64
	SwapTotalKB    uint64
	SwapFreeKB     uint64
}

// Parse pulls MemTotal, MemAvailable, SwapTotal and SwapFree from
// /proc/meminfo. MemAvailable (not MemFree) is the figure that accounts for
// reclaimable cache; a kernel too old to publish it fails the whole sample.
func Parse(data []byte) (Sample, error) {
	var s Sample
	targets := map[string]*uint64{
		"MemTotal:":     &s.MemTotalKB,
		"MemAvailable:": &s.MemAvailableKB,
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

	for key := range targets {
		if !seen[key] {
			return Sample{}, fmt.Errorf("missing %s", key)
		}
	}
	return s, nil
}
