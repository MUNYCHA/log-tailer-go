package uptime

import (
	"bytes"
	"os"
	"strconv"
)

// Sample is /proc/uptime's seconds-since-boot figure as the kernel wrote it.
type Sample struct {
	Seconds float64
}

// Parse extracts the first field of /proc/uptime's "<uptime> <idle>" line.
func Parse(data []byte) (Sample, error) {
	fields := bytes.Fields(data)
	if len(fields) == 0 {
		return Sample{}, os.ErrInvalid
	}
	seconds, err := strconv.ParseFloat(string(fields[0]), 64)
	if err != nil {
		return Sample{}, err
	}
	return Sample{Seconds: seconds}, nil
}
