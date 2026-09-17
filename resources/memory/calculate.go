package memory

const kb = 1024

// MemoryUsage is RAM as published, in bytes.
type MemoryUsage struct {
	TotalBytes     uint64
	UsedBytes      uint64
	AvailableBytes uint64
	UsedPercent    float64
	Estimated      bool // available came from our formula, not MemAvailable
}

// SwapUsage is swap as published, in bytes.
type SwapUsage struct {
	TotalBytes  uint64
	UsedBytes   uint64
	UsedPercent float64
}

// Memory derives used memory from available memory. Available is the kernel's
// MemAvailable when published (3.14+, or backported); otherwise it is
// estimated as reclaimable memory:
//
//	MemFree + Buffers + Cached + SReclaimable - Shmem
//
// Shmem (tmpfs, shared memory) sits inside Cached but can't be dropped, so it
// is taken back out. The estimate ignores the kernel's low watermark, so it
// reads slightly more available than MemAvailable would.
//
// It reports false when MemTotal is missing or zero, or when neither
// MemAvailable nor every estimate line is present, so the caller omits the
// group instead of publishing a number it cannot stand behind.
func Memory(s Sample) (MemoryUsage, bool) {
	if !s.HasMemTotal || s.MemTotalKB == 0 {
		return MemoryUsage{}, false
	}

	var availableKB uint64
	estimated := false
	switch {
	case s.HasMemAvailable:
		availableKB = s.MemAvailableKB
	case s.HasEstimate:
		reclaimable := s.MemFreeKB + s.BuffersKB + s.CachedKB + s.SReclaimableKB
		if s.ShmemKB < reclaimable {
			availableKB = reclaimable - s.ShmemKB
		}
		estimated = true
	default:
		return MemoryUsage{}, false
	}

	// Defensive: available above total would underflow used into petabytes
	if availableKB > s.MemTotalKB {
		availableKB = s.MemTotalKB
	}
	usedKB := s.MemTotalKB - availableKB

	return MemoryUsage{
		TotalBytes:     s.MemTotalKB * kb,
		UsedBytes:      usedKB * kb,
		AvailableBytes: availableKB * kb,
		UsedPercent:    float64(usedKB) / float64(s.MemTotalKB) * 100,
		Estimated:      estimated,
	}, true
}

// Swap derives used swap as SwapTotal - SwapFree. A server with no swap
// reports all zeros. It reports false when either line is missing.
func Swap(s Sample) (SwapUsage, bool) {
	if !s.HasSwap {
		return SwapUsage{}, false
	}

	// Defensive: the two lines are read from one snapshot, so free should
	// never exceed total, but underflowing uint64 would report petabytes of
	// swap in use
	freeKB := s.SwapFreeKB
	if freeKB > s.SwapTotalKB {
		freeKB = s.SwapTotalKB
	}
	usedKB := s.SwapTotalKB - freeKB

	var usedPercent float64
	if s.SwapTotalKB > 0 {
		usedPercent = float64(usedKB) / float64(s.SwapTotalKB) * 100
	}

	return SwapUsage{
		TotalBytes:  s.SwapTotalKB * kb,
		UsedBytes:   usedKB * kb,
		UsedPercent: usedPercent,
	}, true
}
