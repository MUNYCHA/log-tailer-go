package memory

// Usage is memory as published: bytes, with swap reported as used, not free.
type Usage struct {
	TotalBytes     uint64
	AvailableBytes uint64
	SwapTotalBytes uint64
	SwapUsedBytes  uint64
}

// ToBytes converts a kB sample to bytes and derives swap used as
// SwapTotal - SwapFree.
func ToBytes(s Sample) Usage {
	// Defensive: the two swap lines are read from one snapshot, so free
	// should never exceed total, but underflowing uint64 would report
	// petabytes of swap in use
	swapFree := s.SwapFreeKB
	if swapFree > s.SwapTotalKB {
		swapFree = s.SwapTotalKB
	}

	const kb = 1024
	return Usage{
		TotalBytes:     s.MemTotalKB * kb,
		AvailableBytes: s.MemAvailableKB * kb,
		SwapTotalBytes: s.SwapTotalKB * kb,
		SwapUsedBytes:  (s.SwapTotalKB - swapFree) * kb,
	}
}
