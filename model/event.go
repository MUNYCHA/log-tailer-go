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
