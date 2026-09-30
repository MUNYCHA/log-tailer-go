// Package postgres reads the health of the Postgres running on this server,
// for the db collector to publish.
//
// Engine only orchestrates, like the other collectors: read.go is the only
// file that talks to Postgres, parse.go turns what it returns into values,
// and calculate.go turns values into what is published.
//
// The agent may fail, but must never slow the database down or change it.
// Every session is read-only with short statement and lock timeouts (see
// read.go); at most one session is open at a time and each is closed before
// the next opens; and every query reads statistics views and catalogs only,
// never a user table. Nothing published contains SQL text or row data.
package postgres

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"log-tailer-go/model"
)

// maintenanceDB is the database the server-wide stats are read from. Every
// Postgres install creates it.
const maintenanceDB = "postgres"

// errNoConnections is published for a database that has datallowconn off, so
// its tables are never read.
const errNoConnections = "database does not allow connections"

// A new session can be refused for the role's CONNECTION LIMIT while the
// previous one is still exiting on the server, a few milliseconds after the
// agent closed it, so that refusal is retried briefly.
const (
	connectAttempts   = 3
	connectRetryDelay = 100 * time.Millisecond
)

type Engine struct {
	serverID  string
	socketDir string

	// Only ever touched from the collector's goroutine, one round at a time,
	// so they need no lock.
	//
	// prev is each database's counters from the last round they were read,
	// keyed by name, differenced against the next round for activity.
	// failing holds what failed last round, so a failure is logged when it
	// starts and when it clears rather than on every tick.
	prev    map[string]Sample
	failing map[string]bool
}

func New(serverID, socketDir string) *Engine {
	return &Engine{
		serverID:  serverID,
		socketDir: socketDir,
		prev:      make(map[string]Sample),
		failing:   make(map[string]bool),
	}
}

// Collect reads the server-wide stats over one session, closes it, then
// reads each database's tables over a session to that database, one after
// another. A part that fails is left out and named in the event's errors;
// only failing to connect at all leaves the event empty.
func (e *Engine) Collect(ctx context.Context) any {
	r := &round{ctx: ctx, engine: e, seen: make(map[string]bool)}
	defer r.prune()

	event := &model.PostgresEvent{
		ServerID:    e.serverID,
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
		LongRunning: []model.PostgresSession{},
		Blocked:     []model.PostgresBlocked{},
		Replicas:    []model.PostgresReplica{},
		Databases:   []model.PostgresDatabase{},
		Errors:      make(map[string]string),
	}

	conn, err := e.connect(ctx, maintenanceDB)
	if !r.ok(nil, "connect", "connect", err) {
		event.Up, event.Error = ConnectFailure(err)
		return event
	}
	event.Up = true

	server, err := ReadServer(ctx, conn)
	serverOK := r.ok(event.Errors, "server", "server", err)
	if serverOK {
		event.StartedAt = FormatEpoch(server.StartedEpoch)
		uptime := UptimeSeconds(server.StartedEpoch, time.Now())
		event.UptimeSeconds = &uptime
		event.Role = "primary"
		if server.InRecovery {
			event.Role = "replica"
		}
	}

	if c, err := readConnections(ctx, conn); r.ok(event.Errors, "connections", "connections", err) {
		event.Connections = c
	}
	if rows, err := ReadLongRunning(ctx, conn); r.ok(event.Errors, "longRunning", "longRunning", err) {
		event.LongRunning = sessions(rows)
	}
	if rows, err := ReadBlocked(ctx, conn); r.ok(event.Errors, "blocked", "blocked", err) {
		event.Blocked = blocked(rows)
	}
	e.readReplication(ctx, conn, r, event, serverOK && server.InRecovery)

	readAt := time.Now()
	dbRows, err := ReadDatabases(ctx, conn)
	dbOK := r.ok(event.Errors, "databases", "databases", err)
	sizeRows, err := ReadSizes(ctx, conn)
	sizes := make(map[string]*int64)
	if r.ok(event.Errors, "sizes", "sizes", err) {
		for _, s := range sizeRows {
			sizes[s.Name] = s.Bytes
		}
	}

	// Closed before any per-database session opens, so the agent never holds
	// more than one
	conn.Close(ctx)

	if dbOK {
		var started *float64
		if serverOK {
			started = &server.StartedEpoch
		}
		event.Databases = e.databases(ctx, r, dbRows, sizes, started, readAt)
	}
	return event
}

// connect opens a session, retrying only a refusal for too many connections.
func (e *Engine) connect(ctx context.Context, database string) (*pgx.Conn, error) {
	for attempt := 1; ; attempt++ {
		conn, err := Connect(ctx, e.socketDir, database)
		if err == nil || attempt == connectAttempts || !IsTooManyConnections(err) {
			return conn, err
		}
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(connectRetryDelay):
		}
	}
}

func readConnections(ctx context.Context, conn *pgx.Conn) (*model.PostgresConnections, error) {
	maxConns, err := ReadMaxConnections(ctx, conn)
	if err != nil {
		return nil, err
	}
	rows, err := ReadConnections(ctx, conn)
	if err != nil {
		return nil, err
	}
	c := &model.PostgresConnections{Max: maxConns, ByState: make(map[string]int)}
	for _, row := range rows {
		c.ByState[row.State] = row.Count
		c.Used += row.Count
	}
	return c, nil
}

// readReplication fills the servers streaming from this one and, on a
// replica, how far its own replay is behind. Both are one part: when either
// read fails the error is published once under "replication".
func (e *Engine) readReplication(ctx context.Context, conn *pgx.Conn, r *round, event *model.PostgresEvent, replica bool) {
	rows, err := ReadReplicas(ctx, conn)
	var replay ReplayRow
	if err == nil && replica {
		replay, err = ReadReplay(ctx, conn)
	}
	if !r.ok(event.Errors, "replication", "replication", err) {
		return
	}
	for _, row := range rows {
		rep := model.PostgresReplica{
			Application:      row.Application,
			State:            row.State,
			ReplayLagSeconds: row.ReplayLagSeconds,
			ReplayLagBytes:   row.ReplayLagBytes,
		}
		if row.ClientAddr != nil {
			rep.ClientAddr = *row.ClientAddr
		}
		event.Replicas = append(event.Replicas, rep)
	}
	event.ReplayDelaySeconds = replay.DelaySeconds
	event.UnreplayedBytes = replay.UnreplayedBytes
}

// databases builds every database's entry, with its activity since the
// previous round, and reads its tables over a session to it. started is nil
// when the server's start time could not be read this round: a restart then
// can't be ruled out, so no activity is published and the previous counters
// are kept for the next round.
func (e *Engine) databases(ctx context.Context, r *round, rows []DatabaseRow, sizes map[string]*int64, started *float64, readAt time.Time) []model.PostgresDatabase {
	next := make(map[string]Sample)
	out := make([]model.PostgresDatabase, len(rows))
	for i, row := range rows {
		d := model.PostgresDatabase{
			Name:          row.Name,
			SizeBytes:     sizes[row.Name],
			XIDAge:        row.XIDAge,
			LargestTables: []model.PostgresTableSize{},
			DeadTuples:    []model.PostgresDeadTuples{},
			Errors:        make(map[string]string),
		}

		if counters, ok := ParseCounters(row); ok && started != nil {
			cur := Sample{Counters: counters, At: readAt, StartedAt: *started}
			if prev, ok := e.prev[row.Name]; ok {
				if a, ok := Diff(prev, cur); ok {
					d.Activity = &model.PostgresActivity{
						Seconds:   a.Seconds,
						Commits:   a.Commits,
						Rollbacks: a.Rollbacks,
						Deadlocks: a.Deadlocks,
						TempFiles: a.TempFiles,
						TempBytes: a.TempBytes,
					}
					if pct, ok := CacheHitPercent(prev.Counters, counters); ok {
						d.CacheHitPercent = &pct
					}
				}
			}
			next[row.Name] = cur
		}

		e.readTables(ctx, r, row, &d)
		out[i] = d
	}
	if started != nil {
		e.prev = next
	}
	return out
}

// readTables reads one database's largest tables and dead tuples over a
// session to that database, closed before the next database's opens.
func (e *Engine) readTables(ctx context.Context, r *round, row DatabaseRow, d *model.PostgresDatabase) {
	if !row.AllowConn {
		d.Errors["connect"] = errNoConnections
		return
	}
	key := "db/" + row.Name + "/"

	conn, err := e.connect(ctx, row.Name)
	if !r.ok(d.Errors, "connect", key+"connect", err) {
		return
	}
	defer conn.Close(ctx)

	if rows, err := ReadLargestTables(ctx, conn); r.ok(d.Errors, "largestTables", key+"largestTables", err) {
		for _, t := range rows {
			d.LargestTables = append(d.LargestTables, model.PostgresTableSize{Schema: t.Schema, Table: t.Table, TotalBytes: t.Bytes})
		}
	}
	if rows, err := ReadDeadTuples(ctx, conn); r.ok(d.Errors, "deadTuples", key+"deadTuples", err) {
		for _, t := range rows {
			dt := model.PostgresDeadTuples{Schema: t.Schema, Table: t.Table, LiveTuples: t.Live, DeadTuples: t.Dead}
			if t.LastAutovacuumEpoch != nil {
				dt.LastAutovacuum = FormatEpoch(*t.LastAutovacuumEpoch)
			}
			d.DeadTuples = append(d.DeadTuples, dt)
		}
	}
}

func sessions(rows []SessionRow) []model.PostgresSession {
	out := make([]model.PostgresSession, len(rows))
	for i, s := range rows {
		out[i] = model.PostgresSession{
			PID:                s.PID,
			Database:           s.Database,
			User:               s.User,
			Application:        s.Application,
			State:              s.State,
			QuerySeconds:       s.QuerySeconds,
			TransactionSeconds: s.TransactionSeconds,
		}
	}
	return out
}

func blocked(rows []BlockedRow) []model.PostgresBlocked {
	out := make([]model.PostgresBlocked, len(rows))
	for i, b := range rows {
		out[i] = model.PostgresBlocked{
			PID:          b.PID,
			Database:     b.Database,
			User:         b.User,
			Application:  b.Application,
			QuerySeconds: b.QuerySeconds,
			BlockedBy:    Pids(b.BlockedBy),
		}
	}
	return out
}

// round tracks one Collect's failures against the previous round's, so each
// is logged once when it starts and once when it clears.
type round struct {
	ctx    context.Context
	engine *Engine
	seen   map[string]bool
}

// ok reports whether err is nil. A failure is published in errs under part
// (errs may be nil when the caller publishes it itself). logKey names the
// part uniquely across databases for the once-only logging. Once the round's
// deadline has passed every failure reads "timed out": the driver closes the
// session at the deadline, and the parts after it fail with "conn closed".
func (r *round) ok(errs map[string]string, part, logKey string, err error) bool {
	r.seen[logKey] = true
	if err != nil {
		if errs != nil {
			errs[part] = ErrorText(err)
			if r.ctx.Err() != nil {
				errs[part] = errTimedOut
			}
		}
		if !r.engine.failing[logKey] {
			slog.Warn("Failed to read postgres, publishing the error", "part", logKey, "error", err)
			r.engine.failing[logKey] = true
		}
		return false
	}
	if r.engine.failing[logKey] {
		slog.Info("Postgres readable again", "part", logKey)
		delete(r.engine.failing, logKey)
	}
	return true
}

// prune forgets failures of parts not attempted this round — a dropped
// database, or everything after a failed connect — so a later failure there
// is logged afresh.
func (r *round) prune() {
	for key := range r.engine.failing {
		if !r.seen[key] {
			delete(r.engine.failing, key)
		}
	}
}
