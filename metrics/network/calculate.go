package network

import "time"

// Rates is the mean receive and transmit rate in bytes per second across the
// window between two samples, summed over interfaces present in both — one
// that appeared mid-window has no baseline and is left out. It reports false
// when the window is unusable — no elapsed time, no interface common to both,
// or a counter that moved backwards (a driver reload) — so the caller omits
// the fields instead of publishing a number it cannot stand behind.
func Rates(prev, now Sample, elapsed time.Duration) (rx, tx float64, ok bool) {
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
		if cur.RxBytes < old.RxBytes || cur.TxBytes < old.TxBytes {
			return 0, 0, false
		}
		rxDelta += cur.RxBytes - old.RxBytes
		txDelta += cur.TxBytes - old.TxBytes
		shared++
	}
	if shared == 0 {
		return 0, 0, false
	}

	secs := elapsed.Seconds()
	return float64(rxDelta) / secs, float64(txDelta) / secs, true
}
