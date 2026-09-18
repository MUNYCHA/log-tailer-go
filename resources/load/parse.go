package load

import (
	"bytes"
	"fmt"
	"strconv"
)

// Sample is the three load figures from /proc/loadavg.
type Sample struct {
	One     float64
	Five    float64
	Fifteen float64
}

// Parse reads the first three fields of /proc/loadavg's
// "0.02 0.04 0.05 1/791 12163" line. Any bad field fails the whole sample.
func Parse(data []byte) (Sample, error) {
	fields := bytes.Fields(data)
	if len(fields) < 3 {
		return Sample{}, fmt.Errorf("expected at least 3 fields, got %d", len(fields))
	}

	var vals [3]float64
	for i := range vals {
		v, err := strconv.ParseFloat(string(fields[i]), 64)
		if err != nil {
			return Sample{}, err
		}
		vals[i] = v
	}
	return Sample{One: vals[0], Five: vals[1], Fifteen: vals[2]}, nil
}
