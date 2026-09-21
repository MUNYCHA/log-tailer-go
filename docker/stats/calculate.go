package stats

// Memory is one container's memory as published, in bytes.
type Memory struct {
	UsedBytes   uint64
	LimitBytes  uint64
	UsedPercent float64
}

// CPUPercent is the container's CPU use across the window between two
// samples, computed the way docker stats does: its share of all host CPU
// time, times the number of CPUs. One full core is 100, so a container busy
// on two cores reads 200. It reports false when the window is unusable — no
// previous sample, no host CPU time, or counters that moved backwards (the
// container restarted between ticks) — so the caller omits the field.
func CPUPercent(prev, now Sample) (float64, bool) {
	if !prev.HasSystem || !now.HasSystem || now.CPUs == 0 {
		return 0, false
	}
	if now.SystemTotal <= prev.SystemTotal || now.CPUTotal < prev.CPUTotal {
		return 0, false
	}
	cpuDelta := float64(now.CPUTotal - prev.CPUTotal)
	systemDelta := float64(now.SystemTotal - prev.SystemTotal)
	return cpuDelta / systemDelta * float64(now.CPUs) * 100, true
}

// MemoryUsage is used memory the way docker stats shows it: usage minus the
// inactive file cache, which the kernel can drop at any time. Usage below the
// cache (never seen, but the counters are read at slightly different times)
// falls back to raw usage rather than underflowing. UsedPercent is 0 only
// when Docker reports no limit at all.
func MemoryUsage(s Sample) (Memory, bool) {
	if !s.HasMemory {
		return Memory{}, false
	}
	used := s.MemUsage
	if s.MemCache < used {
		used -= s.MemCache
	}
	var pct float64
	if s.MemLimit > 0 {
		pct = float64(used) / float64(s.MemLimit) * 100
	}
	return Memory{UsedBytes: used, LimitBytes: s.MemLimit, UsedPercent: pct}, true
}
