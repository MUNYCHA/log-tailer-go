package model

type LogEvent struct {
	SystemID   string `json:"systemId"`
	SystemName string `json:"systemName"`
	ServerName string `json:"serverName"`
	ServerIP   string `json:"serverIp"`
	Path       string `json:"path"`
	Channel    string `json:"channel"`
	Timestamp  string `json:"timestamp"`
	Message    string `json:"message"`
}

// ResourcesEvent is one snapshot of a server's uptime, cpu, memory, swap and
// network. Each group is a pointer so a group that cannot be read is omitted
// from the JSON as a whole rather than sent as zeros — an absent group is
// "unknown", where 0 would read as a real measurement.
type ResourcesEvent struct {
	SystemID      string `json:"systemId"`
	SystemName    string `json:"systemName"`
	ServerName    string `json:"serverName"`
	ServerIP      string `json:"serverIp"`
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

// StorageEvent is one snapshot of disk usage for the configured mounts.
type StorageEvent struct {
	SystemID   string `json:"systemId"`
	SystemName string `json:"systemName"`
	ServerName string `json:"serverName"`
	ServerIP   string `json:"serverIp"`
	Timestamp  string `json:"timestamp"`

	Mounts []MountUsage `json:"mounts"`
}

type MountUsage struct {
	Path          string  `json:"path"`
	TotalBytes    uint64  `json:"totalBytes"`
	UsedBytes     uint64  `json:"usedBytes"`
	FreeBytes     uint64  `json:"freeBytes"`
	ReservedBytes uint64  `json:"reservedBytes"`
	UsedPercent   float64 `json:"usedPercent"`
	Error         string  `json:"error,omitempty"`
}

// HeartbeatEvent is the entire heartbeat payload: the same identity pair the
// live metrics key is built from, so a beat maps to exactly one server.
type HeartbeatEvent struct {
	SystemID   string `json:"systemId"`
	ServerName string `json:"serverName"`
}
