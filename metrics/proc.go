package metrics

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Parsers for the /proc files sampled each tick. Each takes raw bytes so it
// can be tested without a live /proc, and each fails as a group: a partial
// parse returns an error rather than a struct with some fields guessed.

type loadAvg struct {
	one     float64
	five    float64
	fifteen float64
}

// parseLoadavg reads the three load figures from /proc/loadavg's
// "0.02 0.04 0.05 1/791 12163" line, as-is.
func parseLoadavg(data []byte) (loadAvg, error) {
	fields := bytes.Fields(data)
	if len(fields) < 3 {
		return loadAvg{}, fmt.Errorf("expected at least 3 fields, got %d", len(fields))
	}

	var vals [3]float64
	for i := range vals {
		v, err := strconv.ParseFloat(string(fields[i]), 64)
		if err != nil {
			return loadAvg{}, err
		}
		vals[i] = v
	}
	return loadAvg{one: vals[0], five: vals[1], fifteen: vals[2]}, nil
}

type memInfo struct {
	totalBytes     uint64
	availableBytes uint64
	swapTotalBytes uint64
	swapUsedBytes  uint64
}

// parseMeminfo pulls the four lines we report from /proc/meminfo and converts
// them to bytes. MemAvailable (not MemFree) is the figure that accounts for
// reclaimable cache; a kernel too old to publish it fails the whole group.
func parseMeminfo(data []byte) (memInfo, error) {
	var memTotal, memAvailable, swapTotal, swapFree uint64
	targets := map[string]*uint64{
		"MemTotal:":     &memTotal,
		"MemAvailable:": &memAvailable,
		"SwapTotal:":    &swapTotal,
		"SwapFree:":     &swapFree,
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
			return memInfo{}, fmt.Errorf("%s: %w", key, err)
		}
		*target = kb
		seen[key] = true
	}

	for key := range targets {
		if !seen[key] {
			return memInfo{}, fmt.Errorf("missing %s", key)
		}
	}

	// Defensive: the two swap lines are read from one snapshot, so free
	// should never exceed total, but underflowing uint64 would report
	// petabytes of swap in use
	if swapFree > swapTotal {
		swapFree = swapTotal
	}

	const kb = 1024
	return memInfo{
		totalBytes:     memTotal * kb,
		availableBytes: memAvailable * kb,
		swapTotalBytes: swapTotal * kb,
		swapUsedBytes:  (swapTotal - swapFree) * kb,
	}, nil
}

// cpuSample is one reading of the aggregate "cpu" line's counters, in jiffies.
type cpuSample struct {
	total uint64
	idle  uint64
}

// parseStat sums the aggregate "cpu" line of /proc/stat. idle counts both the
// idle and iowait columns: a server blocked on a dead mount is waiting, not
// burning CPU, and load average is what surfaces that instead.
func parseStat(data []byte) (cpuSample, error) {
	for _, line := range bytes.Split(data, []byte("\n")) {
		fields := bytes.Fields(line)
		if len(fields) < 5 || string(fields[0]) != "cpu" {
			continue
		}

		var sample cpuSample
		for i, field := range fields[1:] {
			v, err := strconv.ParseUint(string(field), 10, 64)
			if err != nil {
				return cpuSample{}, err
			}
			sample.total += v
			// Columns are user, nice, system, idle, iowait, irq, ...
			if i == 3 || i == 4 {
				sample.idle += v
			}
		}
		return sample, nil
	}
	return cpuSample{}, os.ErrInvalid
}

// cpuPercent is the mean busy percentage across the window between two
// samples. It reports false when the window is unusable — no elapsed jiffies,
// or counters that moved backwards (a reboot between ticks) — so the caller
// omits the field instead of publishing a number it cannot stand behind.
func cpuPercent(prev, now cpuSample) (float64, bool) {
	if now.total <= prev.total || now.idle < prev.idle {
		return 0, false
	}

	totalDelta := now.total - prev.total
	idleDelta := now.idle - prev.idle
	if idleDelta > totalDelta {
		return 0, false
	}

	return 100 * float64(totalDelta-idleDelta) / float64(totalDelta), true
}

// netCounters is one interface's cumulative byte counters.
type netCounters struct {
	rxBytes uint64
	txBytes uint64
}

// parseNetDev reads per-interface byte counters from /proc/net/dev. Past the
// two header lines each row is "iface: <8 receive columns> <8 transmit
// columns>", with bytes first in each group, so rx is column 0 and tx column 8.
func parseNetDev(data []byte) (map[string]netCounters, error) {
	counters := make(map[string]netCounters)
	for _, line := range bytes.Split(data, []byte("\n")) {
		// Header lines have no colon; a busy counter can abut it ("eth0:1234")
		name, rest, found := bytes.Cut(line, []byte(":"))
		if !found {
			continue
		}
		iface := string(bytes.TrimSpace(name))

		fields := bytes.Fields(rest)
		if len(fields) < 16 {
			return nil, fmt.Errorf("%s: expected 16 fields, got %d", iface, len(fields))
		}
		rx, err := strconv.ParseUint(string(fields[0]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", iface, err)
		}
		tx, err := strconv.ParseUint(string(fields[8]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", iface, err)
		}
		counters[iface] = netCounters{rxBytes: rx, txBytes: tx}
	}
	if len(counters) == 0 {
		return nil, os.ErrInvalid
	}
	return counters, nil
}

// netRates is the mean receive and transmit rate in bytes per second across
// the window between two samples, summed over interfaces present in both — one
// that appeared mid-window has no baseline and is left out. It reports false
// when the window is unusable — no elapsed time, no interface common to both,
// or a counter that moved backwards (a driver reload) — so the caller omits
// the fields instead of publishing a number it cannot stand behind.
func netRates(prev, now map[string]netCounters, elapsed time.Duration) (rx, tx float64, ok bool) {
	if elapsed <= 0 {
		return 0, 0, false
	}

	var rxDelta, txDelta uint64
	shared := 0
	for iface, cur := range now {
		old, seen := prev[iface]
		if !seen {
			continue
		}
		if cur.rxBytes < old.rxBytes || cur.txBytes < old.txBytes {
			return 0, 0, false
		}
		rxDelta += cur.rxBytes - old.rxBytes
		txDelta += cur.txBytes - old.txBytes
		shared++
	}
	if shared == 0 {
		return 0, 0, false
	}

	secs := elapsed.Seconds()
	return float64(rxDelta) / secs, float64(txDelta) / secs, true
}
