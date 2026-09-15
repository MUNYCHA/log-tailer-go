package cpu

// Percent is the mean busy percentage across the window between two samples.
// It reports false when the window is unusable — no elapsed jiffies, or
// counters that moved backwards (a reboot between ticks) — so the caller
// omits the field instead of publishing a number it cannot stand behind.
func Percent(prev, now Sample) (float64, bool) {
	if now.Total <= prev.Total || now.Idle < prev.Idle {
		return 0, false
	}

	totalDelta := now.Total - prev.Total
	idleDelta := now.Idle - prev.Idle
	if idleDelta > totalDelta {
		return 0, false
	}

	return 100 * float64(totalDelta-idleDelta) / float64(totalDelta), true
}
