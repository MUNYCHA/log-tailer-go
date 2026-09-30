package model

type LogEvent struct {
	ServerID  string `json:"serverId"`
	Path      string `json:"path"`
	Channel   string `json:"channel"`
	Timestamp string `json:"timestamp"`
	Message   string `json:"message"`
}

// ResourcesEvent is one snapshot of a server's uptime, cpu, memory, swap and
// network. Each group is a pointer so a group that cannot be read is omitted
// from the JSON as a whole rather than sent as zeros — an absent group is
// "unknown", where 0 would read as a real measurement.
type ResourcesEvent struct {
	ServerID      string `json:"serverId"`
	Timestamp     string `json:"timestamp"`
	UptimeSeconds *int64 `json:"uptimeSeconds,omitempty"`

	CPU     *CPUGroup     `json:"cpu,omitempty"`
	Memory  *MemoryGroup  `json:"memory,omitempty"`
	Swap    *SwapGroup    `json:"swap,omitempty"`
	Network *NetworkGroup `json:"network,omitempty"`
}

// CPUGroup comes from two files that fail independently, so each part is a
// pointer: the group is omitted only when neither can be read.
type CPUGroup struct {
	// Online logical CPUs, from the per-CPU lines of /proc/stat
	Count *int `json:"count,omitempty"`

	// Mean busy percentage over the interval since the previous tick, from
	// /proc/stat. Omitted on the first tick after start (and after a
	// supervised restart), when there is no previous sample to difference.
	UsedPercent *float64 `json:"usedPercent,omitempty"`

	// /proc/loadavg — omitted as a set if the file cannot be read or parsed
	Load1  *float64 `json:"load1,omitempty"`
	Load5  *float64 `json:"load5,omitempty"`
	Load15 *float64 `json:"load15,omitempty"`
}

// MemoryGroup is RAM from /proc/meminfo, in bytes (the file is kB). Used is
// total - available. Estimated is true when the kernel has no MemAvailable and
// available was calculated from reclaimable memory instead, so used,
// available and the percentage are close but not exact; total is always exact.
type MemoryGroup struct {
	TotalBytes     uint64  `json:"totalBytes"`
	UsedBytes      uint64  `json:"usedBytes"`
	AvailableBytes uint64  `json:"availableBytes"`
	UsedPercent    float64 `json:"usedPercent"`
	Estimated      bool    `json:"estimated"`
}

// SwapGroup is swap from /proc/meminfo, in bytes. Used is SwapTotal - SwapFree.
// A server with no swap reports all zeros.
type SwapGroup struct {
	TotalBytes  uint64  `json:"totalBytes"`
	UsedBytes   uint64  `json:"usedBytes"`
	UsedPercent float64 `json:"usedPercent"`
}

// NetworkGroup is the mean download (rx) and upload (tx) rate since the
// previous tick, summed over physical interfaces from /proc/net/dev. Omitted
// on the first tick for the same reason as CPUGroup.UsedPercent.
type NetworkGroup struct {
	RxBytesPerSec float64 `json:"rxBytesPerSec"`
	TxBytesPerSec float64 `json:"txBytesPerSec"`
}

// StorageEvent is one snapshot of disk usage for the configured mounts, in
// config order.
type StorageEvent struct {
	ServerID  string `json:"serverId"`
	Timestamp string `json:"timestamp"`

	// Nil when no local filesystem could be read
	Server *ServerStorage `json:"server,omitempty"`

	Mounts []MountUsage `json:"mounts"`
}

// ServerStorage is the server's mounted local storage summed over every local
// filesystem, each counted once, independent of the configured mounts. Partial
// is true when at least one local filesystem could not be read, so the totals
// are smaller than the server really has.
type ServerStorage struct {
	DiskUsage
	Partial bool `json:"partial"`

	// Mount points left out of the totals because they could not be read,
	// sorted, so a partial event says which filesystems are missing instead
	// of leaving that only in the agent's log. Empty when Partial is false.
	MissingPaths []string `json:"missingPaths,omitempty"`
}

// MountUsage is one configured path: the filesystem it lives on and its usage.
type MountUsage struct {
	Path   string `json:"path"`
	Device string `json:"device,omitempty"`
	FSType string `json:"fsType,omitempty"`

	// Nil when the path could not be read, so a failed mount publishes only
	// its path (and fsType when known) with Error, never zero sizes that
	// would read as an empty disk
	*DiskUsage

	Error string `json:"error,omitempty"`
}

// DiskUsage is one filesystem's size in bytes, matching df -B1.
// UsedBytes + FreeBytes + ReservedBytes = TotalBytes.
type DiskUsage struct {
	TotalBytes    uint64  `json:"totalBytes"`
	UsedBytes     uint64  `json:"usedBytes"`
	FreeBytes     uint64  `json:"freeBytes"`
	ReservedBytes uint64  `json:"reservedBytes"`
	UsedPercent   float64 `json:"usedPercent"`
}

// HeartbeatEvent is the entire heartbeat payload: the same identity pair the
// live metrics key is built from, so a beat maps to exactly one server.
type HeartbeatEvent struct {
	ServerID string `json:"serverId"`
}

// DockerEvent is one snapshot of every container on the server, running or
// not, sorted by name.
type DockerEvent struct {
	ServerID  string `json:"serverId"`
	Timestamp string `json:"timestamp"`

	// The kernel cgroup version behind Docker's per-container counters, 1 or
	// 2. Omitted until Docker has answered once. On 1, block I/O write bytes
	// miss buffered writes, so compare them only between servers on the
	// same version.
	CgroupVersion int `json:"cgroupVersion,omitempty"`

	// Always present; empty when Error is set, so check Error before reading
	// an empty list as "no containers"
	Containers []DockerContainer `json:"containers"`

	// Set when Docker could not be reached or its reply could not be read
	Error string `json:"error,omitempty"`
}

// DockerContainer is one container. State is Docker's own: running, paused,
// restarting, exited, created, removing or dead. Running and paused
// containers also carry stats, the same figures docker stats shows; every
// other state carries how it last exited instead.
type DockerContainer struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Image string `json:"image"`
	State string `json:"state"`

	// Share of one CPU core, like docker stats: two busy cores read 200.
	// Averaged since the previous tick, so omitted the round after a
	// container starts or restarts. On a 3s or longer interval a pre-read
	// covers that, except for a start in the last second before the tick.
	CPUPercent *float64       `json:"cpuPercent,omitempty"`
	Memory     *DockerMemory  `json:"memory,omitempty"`
	Network    *DockerNetwork `json:"network,omitempty"`
	BlockIO    *DockerBlockIO `json:"blockIO,omitempty"`
	PIDs       *uint64        `json:"pids,omitempty"`

	// How a stopped container last exited. Omitted for running containers,
	// for containers never started, and for one that started again while the
	// round was being read.
	ExitCode   *int   `json:"exitCode,omitempty"`
	FinishedAt string `json:"finishedAt,omitempty"` // RFC 3339, UTC

	// Set when this container's stats could not be read; its metadata is
	// still published
	Error string `json:"error,omitempty"`
}

// DockerMemory is used memory without the page cache, as docker stats
// shows it. With no memory limit set, LimitBytes is the host's memory.
type DockerMemory struct {
	UsedBytes   uint64  `json:"usedBytes"`
	LimitBytes  uint64  `json:"limitBytes"`
	UsedPercent float64 `json:"usedPercent"`
}

// DockerNetwork is bytes received and sent since the container started,
// summed over its networks. Omitted for host or none networking.
type DockerNetwork struct {
	RxBytes uint64 `json:"rxBytes"`
	TxBytes uint64 `json:"txBytes"`
}

// DockerBlockIO is bytes read from and written to disk since the container
// started. On cgroupVersion 1, buffered writes are not counted, so
// WriteBytes often reads 0.
type DockerBlockIO struct {
	ReadBytes  uint64 `json:"readBytes"`
	WriteBytes uint64 `json:"writeBytes"`
}

// PostgresEvent is one snapshot of the local Postgres: whether it is up, its
// sessions and replication, and per-database stats. It carries numbers and
// names only — never SQL text or row data, which can hold secrets.
type PostgresEvent struct {
	ServerID  string `json:"serverId"`
	Timestamp string `json:"timestamp"`

	// False when the agent could not reach Postgres at all: no socket, the
	// connection refused or timed out, or the server starting up or shutting
	// down. A login Postgres answers but rejects reads true with Error set:
	// the server is running, it just refused the agent.
	Up bool `json:"up"`

	// Set when the agent could not connect; every other field is then absent
	// and every list empty
	Error string `json:"error,omitempty"`

	StartedAt     string `json:"startedAt,omitempty"` // RFC 3339, UTC
	UptimeSeconds *int64 `json:"uptimeSeconds,omitempty"`
	Role          string `json:"role,omitempty"` // primary or replica

	Connections *PostgresConnections `json:"connections,omitempty"`

	// Always present; capped at the 10 longest, longest first
	LongRunning []PostgresSession `json:"longRunning"`
	Blocked     []PostgresBlocked `json:"blocked"`

	// Always present: servers streaming from this one, empty when none
	Replicas []PostgresReplica `json:"replicas"`

	// Replica only. Delay is the time since the last replayed transaction,
	// so it also grows while the primary is idle. Unreplayed is WAL received
	// but not replayed yet.
	ReplayDelaySeconds *float64 `json:"replayDelaySeconds,omitempty"`
	UnreplayedBytes    *int64   `json:"unreplayedBytes,omitempty"`

	// Always present: every non-template database, sorted by name
	Databases []PostgresDatabase `json:"databases"`

	// What could not be read this round, keyed by the part it belongs to
	// (server, connections, longRunning, blocked, replication, databases,
	// sizes). Everything else in the event is still published.
	Errors map[string]string `json:"errors,omitempty"`
}

// PostgresConnections counts client sessions, not counting the agent's own,
// against the max_connections setting.
type PostgresConnections struct {
	Max     int            `json:"max"`
	Used    int            `json:"used"`
	ByState map[string]int `json:"byState"`
}

// PostgresSession is a client session running one query, or holding one
// transaction open, for over a minute. QuerySeconds is set while a query is
// active; TransactionSeconds while a transaction is open, which catches
// sessions left idle in transaction.
type PostgresSession struct {
	PID                int      `json:"pid"`
	Database           string   `json:"database,omitempty"`
	User               string   `json:"user,omitempty"`
	Application        string   `json:"application,omitempty"`
	State              string   `json:"state"`
	QuerySeconds       *float64 `json:"querySeconds,omitempty"`
	TransactionSeconds *float64 `json:"transactionSeconds,omitempty"`
}

// PostgresBlocked is a session waiting on a lock, and the pids holding it up.
// QuerySeconds is how long its current query has run, which includes the wait.
type PostgresBlocked struct {
	PID          int      `json:"pid"`
	Database     string   `json:"database,omitempty"`
	User         string   `json:"user,omitempty"`
	Application  string   `json:"application,omitempty"`
	QuerySeconds *float64 `json:"querySeconds,omitempty"`
	BlockedBy    []int    `json:"blockedBy"`
}

// PostgresReplica is one server streaming WAL from this one. The lags are
// absent until the replica has replayed something, and ReplayLagSeconds also
// while nothing is being written.
type PostgresReplica struct {
	Application      string   `json:"application,omitempty"`
	ClientAddr       string   `json:"clientAddr,omitempty"`
	State            string   `json:"state"`
	ReplayLagSeconds *float64 `json:"replayLagSeconds,omitempty"`
	ReplayLagBytes   *int64   `json:"replayLagBytes,omitempty"`
}

// PostgresDatabase is one database. Its tables are read over a connection to
// that database; when the connection fails, Errors["connect"] says why and
// both table lists are empty.
type PostgresDatabase struct {
	Name      string `json:"name"`
	SizeBytes *int64 `json:"sizeBytes,omitempty"`

	// Transactions since the database was last frozen. Postgres stops
	// accepting writes near 2 billion to prevent wraparound.
	XIDAge int64 `json:"xidAge"`

	// Share of block reads served from memory since the previous event.
	// Absent when nothing was read, or when Activity is absent.
	CacheHitPercent *float64 `json:"cacheHitPercent,omitempty"`

	Activity *PostgresActivity `json:"activity,omitempty"`

	// Always present; top 10 each
	LargestTables []PostgresTableSize  `json:"largestTables"`
	DeadTuples    []PostgresDeadTuples `json:"deadTuples"`

	// Keyed by connect, largestTables or deadTuples
	Errors map[string]string `json:"errors,omitempty"`
}

// PostgresActivity is what happened in a database since the previous event,
// over Seconds. Absent on the first event of a database and after Postgres
// restarts or its stats are reset, when there is nothing to difference.
type PostgresActivity struct {
	Seconds   float64 `json:"seconds"`
	Commits   int64   `json:"commits"`
	Rollbacks int64   `json:"rollbacks"`
	Deadlocks int64   `json:"deadlocks"`
	TempFiles int64   `json:"tempFiles"`
	TempBytes int64   `json:"tempBytes"`
}

// PostgresTableSize is a table with its indexes and TOAST, in bytes.
type PostgresTableSize struct {
	Schema     string `json:"schema"`
	Table      string `json:"table"`
	TotalBytes int64  `json:"totalBytes"`
}

// PostgresDeadTuples is a table's dead rows waiting for vacuum.
// LastAutovacuum is absent when autovacuum has never run on it.
type PostgresDeadTuples struct {
	Schema         string `json:"schema"`
	Table          string `json:"table"`
	LiveTuples     int64  `json:"liveTuples"`
	DeadTuples     int64  `json:"deadTuples"`
	LastAutovacuum string `json:"lastAutovacuum,omitempty"` // RFC 3339, UTC
}
