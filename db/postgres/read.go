package postgres

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// SocketDir is where the Postgres packages for Debian, Ubuntu and RHEL put
// the server's Unix socket. The driver adds the file name from the port:
// /var/run/postgresql/.s.PGSQL.5432.
const SocketDir = "/var/run/postgresql"

// Every session the agent opens is read-only and short-lived, so it can fail
// but never slow the database down: a query is cancelled after
// statementTimeout, a lock is never waited on for more than lockTimeout, and
// a DBA can see the session by its application_name.
const (
	statementTimeout = "5s"
	lockTimeout      = "1s"
	connectTimeout   = 5 * time.Second
	applicationName  = "log-tailer-go"
)

// Connect opens a read-only session to one database over the Unix socket in
// socketDir, logging in with peer authentication as the OS user the agent
// runs as. No password is ever sent or stored.
func Connect(ctx context.Context, socketDir, database string) (*pgx.Conn, error) {
	// A host that is a path makes the driver use the socket, never TCP or TLS
	cfg, err := pgx.ParseConfig("host=" + quote(socketDir) + " port=5432 sslmode=disable")
	if err != nil {
		return nil, err
	}
	cfg.Database = database
	cfg.ConnectTimeout = connectTimeout
	cfg.Fallbacks = nil
	cfg.RuntimeParams["application_name"] = applicationName
	cfg.RuntimeParams["statement_timeout"] = statementTimeout
	cfg.RuntimeParams["lock_timeout"] = lockTimeout
	cfg.RuntimeParams["default_transaction_read_only"] = "on"
	// One round trip per query and nothing prepared, since every session
	// runs each query once and closes
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	return pgx.ConnectConfig(ctx, cfg)
}

// quote makes a value safe inside a keyword/value connection string.
func quote(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

// Every query reads statistics views and system catalogs only, never a user
// table, and returns names and numbers only, never SQL text. Times come back
// as epoch seconds so parsing never depends on the server's DateStyle.

const qServer = `
SELECT extract(epoch FROM pg_postmaster_start_time())::float8,
       pg_is_in_recovery()`

// ServerRow is when the postmaster started and whether this is a replica.
type ServerRow struct {
	StartedEpoch float64
	InRecovery   bool
}

func ReadServer(ctx context.Context, conn *pgx.Conn) (ServerRow, error) {
	var r ServerRow
	err := conn.QueryRow(ctx, qServer).Scan(&r.StartedEpoch, &r.InRecovery)
	return r, err
}

const qMaxConnections = `SELECT current_setting('max_connections')::int`

func ReadMaxConnections(ctx context.Context, conn *pgx.Conn) (int, error) {
	var n int
	err := conn.QueryRow(ctx, qMaxConnections).Scan(&n)
	return n, err
}

// Client sessions only, so background workers and replication senders are
// not counted against max_connections, and never the agent's own session.
const qConnections = `
SELECT coalesce(state, 'unknown'), count(*)
FROM pg_stat_activity
WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()
GROUP BY 1`

type StateCountRow struct {
	State string
	Count int
}

func ReadConnections(ctx context.Context, conn *pgx.Conn) ([]StateCountRow, error) {
	return collect[StateCountRow](ctx, conn, qConnections)
}

// A session counts as long-running when its active query, or its open
// transaction, is over a minute old.
const qLongRunning = `
SELECT pid,
       coalesce(datname, ''),
       coalesce(usename, ''),
       coalesce(application_name, ''),
       coalesce(state, 'unknown'),
       CASE WHEN state = 'active' THEN extract(epoch FROM now() - query_start)::float8 END,
       extract(epoch FROM now() - xact_start)::float8
FROM pg_stat_activity
WHERE backend_type = 'client backend'
  AND pid <> pg_backend_pid()
  AND ((state = 'active' AND query_start < now() - interval '1 minute')
       OR xact_start < now() - interval '1 minute')
ORDER BY least(CASE WHEN state = 'active' THEN query_start END, xact_start)
LIMIT 10`

type SessionRow struct {
	PID                int
	Database           string
	User               string
	Application        string
	State              string
	QuerySeconds       *float64
	TransactionSeconds *float64
}

func ReadLongRunning(ctx context.Context, conn *pgx.Conn) ([]SessionRow, error) {
	return collect[SessionRow](ctx, conn, qLongRunning)
}

// pg_blocking_pids is only called for sessions already waiting on a lock, so
// the lock manager is asked about a handful of pids, not every session.
const qBlocked = `
SELECT pid, datname, usename, application_name, query_seconds, blocked_by
FROM (
  SELECT pid,
         coalesce(datname, '') AS datname,
         coalesce(usename, '') AS usename,
         coalesce(application_name, '') AS application_name,
         extract(epoch FROM now() - query_start)::float8 AS query_seconds,
         pg_blocking_pids(pid) AS blocked_by
  FROM pg_stat_activity
  WHERE wait_event_type = 'Lock' AND pid <> pg_backend_pid()
) waiting
WHERE cardinality(blocked_by) > 0
ORDER BY query_seconds DESC NULLS LAST
LIMIT 10`

type BlockedRow struct {
	PID          int
	Database     string
	User         string
	Application  string
	QuerySeconds *float64
	BlockedBy    []int32
}

func ReadBlocked(ctx context.Context, conn *pgx.Conn) ([]BlockedRow, error) {
	return collect[BlockedRow](ctx, conn, qBlocked)
}

// On a replica that feeds others, its own position is the last WAL it
// replayed; pg_current_wal_lsn() is only allowed on a primary.
const qReplicas = `
SELECT coalesce(application_name, ''),
       host(client_addr),
       coalesce(state, 'unknown'),
       extract(epoch FROM replay_lag)::float8,
       pg_wal_lsn_diff(
         CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_lsn() END,
         replay_lsn)::bigint
FROM pg_stat_replication
ORDER BY application_name, client_addr`

type ReplicaRow struct {
	Application      string
	ClientAddr       *string
	State            string
	ReplayLagSeconds *float64
	ReplayLagBytes   *int64
}

func ReadReplicas(ctx context.Context, conn *pgx.Conn) ([]ReplicaRow, error) {
	return collect[ReplicaRow](ctx, conn, qReplicas)
}

// Replica only: both are NULL until something has been received or replayed.
const qReplay = `
SELECT extract(epoch FROM now() - pg_last_xact_replay_timestamp())::float8,
       pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())::bigint`

type ReplayRow struct {
	DelaySeconds    *float64
	UnreplayedBytes *int64
}

func ReadReplay(ctx context.Context, conn *pgx.Conn) (ReplayRow, error) {
	var r ReplayRow
	err := conn.QueryRow(ctx, qReplay).Scan(&r.DelaySeconds, &r.UnreplayedBytes)
	return r, err
}

// The stats columns are NULL for a database with no stats entry yet.
const qDatabases = `
SELECT d.datname,
       age(d.datfrozenxid)::bigint,
       d.datallowconn,
       s.xact_commit,
       s.xact_rollback,
       s.deadlocks,
       s.temp_files,
       s.temp_bytes,
       s.blks_hit,
       s.blks_read,
       extract(epoch FROM s.stats_reset)::float8
FROM pg_database d
LEFT JOIN pg_stat_database s ON s.datid = d.oid
WHERE NOT d.datistemplate
ORDER BY d.datname`

type DatabaseRow struct {
	Name       string
	XIDAge     int64
	AllowConn  bool
	Commits    *int64
	Rollbacks  *int64
	Deadlocks  *int64
	TempFiles  *int64
	TempBytes  *int64
	BlocksHit  *int64
	BlocksRead *int64
	StatsReset *float64
}

func ReadDatabases(ctx context.Context, conn *pgx.Conn) ([]DatabaseRow, error) {
	return collect[DatabaseRow](ctx, conn, qDatabases)
}

// Kept apart from qDatabases because it walks each database's files on disk:
// if it times out, only the sizes are lost.
const qSizes = `
SELECT datname, pg_database_size(oid)
FROM pg_database
WHERE NOT datistemplate`

type SizeRow struct {
	Name  string
	Bytes *int64
}

func ReadSizes(ctx context.Context, conn *pgx.Conn) ([]SizeRow, error) {
	return collect[SizeRow](ctx, conn, qSizes)
}

// Needs a connection to the database itself. Each table is opened briefly
// to size it, so a table locked exclusively (ALTER TABLE, VACUUM FULL) makes
// the query give up at lock_timeout rather than queue behind it.
const qLargestTables = `
SELECT nspname, relname, total_bytes
FROM (
  SELECT n.nspname, c.relname, pg_total_relation_size(c.oid) AS total_bytes
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind IN ('r', 'm')
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    AND n.nspname NOT LIKE 'pg\_toast%'
    AND n.nspname NOT LIKE 'pg\_temp%'
) sized
WHERE total_bytes IS NOT NULL
ORDER BY total_bytes DESC
LIMIT 10`

type TableSizeRow struct {
	Schema string
	Table  string
	Bytes  int64
}

func ReadLargestTables(ctx context.Context, conn *pgx.Conn) ([]TableSizeRow, error) {
	return collect[TableSizeRow](ctx, conn, qLargestTables)
}

// Needs a connection to the database itself; reads counters only.
const qDeadTuples = `
SELECT schemaname, relname, n_live_tup, n_dead_tup,
       extract(epoch FROM last_autovacuum)::float8
FROM pg_stat_user_tables
WHERE n_dead_tup > 0
ORDER BY n_dead_tup DESC
LIMIT 10`

type DeadTuplesRow struct {
	Schema              string
	Table               string
	Live                int64
	Dead                int64
	LastAutovacuumEpoch *float64
}

func ReadDeadTuples(ctx context.Context, conn *pgx.Conn) ([]DeadTuplesRow, error) {
	return collect[DeadTuplesRow](ctx, conn, qDeadTuples)
}

// collect runs sql and scans every row into a T, column by column in order.
func collect[T any](ctx context.Context, conn *pgx.Conn, sql string) ([]T, error) {
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[T])
}
