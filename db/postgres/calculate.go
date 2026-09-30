package postgres

import "time"

// Sample is one database's counters at the moment they were read, with the
// postmaster start time they belong to.
type Sample struct {
	Counters  Counters
	At        time.Time
	StartedAt float64
}

// Activity is what happened in one database between two samples.
type Activity struct {
	Seconds   float64
	Commits   int64
	Rollbacks int64
	Deadlocks int64
	TempFiles int64
	TempBytes int64
}

// Diff returns what happened between prev and cur, and false when the two
// cannot be differenced: Postgres restarted between them, the database's stats
// were reset, or any counter went backwards (both of those end up there too).
func Diff(prev, cur Sample) (Activity, bool) {
	p, c := prev.Counters, cur.Counters
	seconds := cur.At.Sub(prev.At).Seconds()
	if seconds <= 0 || prev.StartedAt != cur.StartedAt || p.StatsReset != c.StatsReset {
		return Activity{}, false
	}
	if c.Commits < p.Commits || c.Rollbacks < p.Rollbacks || c.Deadlocks < p.Deadlocks ||
		c.TempFiles < p.TempFiles || c.TempBytes < p.TempBytes ||
		c.BlocksHit < p.BlocksHit || c.BlocksRead < p.BlocksRead {
		return Activity{}, false
	}
	return Activity{
		Seconds:   seconds,
		Commits:   c.Commits - p.Commits,
		Rollbacks: c.Rollbacks - p.Rollbacks,
		Deadlocks: c.Deadlocks - p.Deadlocks,
		TempFiles: c.TempFiles - p.TempFiles,
		TempBytes: c.TempBytes - p.TempBytes,
	}, true
}

// CacheHitPercent is the share of block reads between two samples served
// from Postgres's shared buffers rather than asked of the OS, and false when
// no block was read at all. Call it only for samples Diff accepts.
func CacheHitPercent(prev, cur Counters) (float64, bool) {
	hit := cur.BlocksHit - prev.BlocksHit
	read := cur.BlocksRead - prev.BlocksRead
	if hit+read <= 0 {
		return 0, false
	}
	return float64(hit) / float64(hit+read) * 100, true
}

// UptimeSeconds is whole seconds since the postmaster started, never negative.
func UptimeSeconds(startedEpoch float64, now time.Time) int64 {
	up := now.Sub(EpochToTime(startedEpoch))
	if up < 0 {
		return 0
	}
	return int64(up / time.Second)
}
