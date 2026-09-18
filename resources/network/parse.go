package network

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
)

// Counters is one interface's cumulative byte counters since boot.
type Counters struct {
	RxBytes uint64
	TxBytes uint64
}

// Sample is one reading of /proc/net/dev, keyed by interface name.
type Sample map[string]Counters

// Parse reads per-interface byte counters from /proc/net/dev. Past the two
// header lines each row is "iface: <8 receive columns> <8 transmit columns>",
// with bytes first in each group, so rx is column 0 and tx column 8.
func Parse(data []byte) (Sample, error) {
	s := make(Sample)
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
		s[iface] = Counters{RxBytes: rx, TxBytes: tx}
	}
	if len(s) == 0 {
		return nil, os.ErrInvalid
	}
	return s, nil
}
