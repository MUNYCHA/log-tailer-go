package postgres

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// errTimedOut is published for anything that ran past the round's deadline.
const errTimedOut = "timed out"

// SQLSTATEs the agent tells apart when connecting.
const (
	// The server is starting up, shutting down or in recovery that does not
	// accept connections
	codeCannotConnectNow = "57P03"
	// max_connections is full, or the agent's role is at its CONNECTION LIMIT
	codeTooManyConnections = "53300"
)

// ConnectFailure turns a failed connect into the published up flag and error.
// Postgres answering with an error means it is running (up) but refused the
// agent — a rejected login, a missing role — except for starting up or
// shutting down, when it is not serving anyone. No answer at all (no socket,
// refused, timed out) means it is down.
func ConnectFailure(err error) (up bool, text string) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code != codeCannotConnectNow, pgErr.Error()
	}
	return false, ErrorText(err)
}

// IsTooManyConnections reports whether a connect was refused only because
// every connection slot, or the role's CONNECTION LIMIT, was in use.
func IsTooManyConnections(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == codeTooManyConnections
}

// ErrorText is the published error: a plain "timed out" for the round's
// deadline, otherwise the driver's or Postgres's own message.
func ErrorText(err error) string {
	if errors.Is(err, context.DeadlineExceeded) || pgconn.Timeout(err) {
		return errTimedOut
	}
	return err.Error()
}

// Counters are one database's cumulative pg_stat_database counters.
// StatsReset is 0 when the stats were never reset.
type Counters struct {
	Commits    int64
	Rollbacks  int64
	Deadlocks  int64
	TempFiles  int64
	TempBytes  int64
	BlocksHit  int64
	BlocksRead int64
	StatsReset float64
}

// ParseCounters returns a database's counters, and false when it has no stats
// entry yet (every counter NULL).
func ParseCounters(r DatabaseRow) (Counters, bool) {
	for _, v := range []*int64{r.Commits, r.Rollbacks, r.Deadlocks, r.TempFiles, r.TempBytes, r.BlocksHit, r.BlocksRead} {
		if v == nil {
			return Counters{}, false
		}
	}
	c := Counters{
		Commits:    *r.Commits,
		Rollbacks:  *r.Rollbacks,
		Deadlocks:  *r.Deadlocks,
		TempFiles:  *r.TempFiles,
		TempBytes:  *r.TempBytes,
		BlocksHit:  *r.BlocksHit,
		BlocksRead: *r.BlocksRead,
	}
	if r.StatsReset != nil {
		c.StatsReset = *r.StatsReset
	}
	return c, true
}

// EpochToTime converts epoch seconds with a fraction to a UTC time.
func EpochToTime(epoch float64) time.Time {
	sec, frac := math.Modf(epoch)
	return time.Unix(int64(sec), int64(frac*1e9)).UTC()
}

// FormatEpoch converts epoch seconds to RFC 3339 UTC, the format of every
// timestamp the agent publishes.
func FormatEpoch(epoch float64) string {
	return EpochToTime(epoch).Format(time.RFC3339)
}

// Pids widens the int4[] pg_blocking_pids returns.
func Pids(pids []int32) []int {
	out := make([]int, len(pids))
	for i, p := range pids {
		out[i] = int(p)
	}
	return out
}
