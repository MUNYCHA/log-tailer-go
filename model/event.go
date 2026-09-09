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

// MetricsEvent is one snapshot of a server. Every field sourced from /proc is
// a pointer so a failed read is omitted from the JSON rather than sent as a
// zero — an absent field is "unknown", where 0 would read as a real measurement.
type MetricsEvent struct {
	SystemID      string `json:"systemId"`
	SystemName    string `json:"systemName"`
	ServerName    string `json:"serverName"`
	ServerIP      string `json:"serverIp"`
	Timestamp     string `json:"timestamp"`
	UptimeSeconds int64  `json:"uptimeSeconds"`

	// /proc/loadavg — omitted as a group if the file cannot be read or parsed
	Load1  *float64 `json:"load1,omitempty"`
	Load5  *float64 `json:"load5,omitempty"`
	Load15 *float64 `json:"load15,omitempty"`

	// /proc/meminfo — omitted as a group. Values are bytes; the file is kB.
	MemTotalBytes     *uint64 `json:"memTotalBytes,omitempty"`
	MemAvailableBytes *uint64 `json:"memAvailableBytes,omitempty"`
	SwapTotalBytes    *uint64 `json:"swapTotalBytes,omitempty"`
	SwapUsedBytes     *uint64 `json:"swapUsedBytes,omitempty"`

	// Mean CPU busy percentage over the interval since the previous tick.
	// Omitted on the first tick after start (and after a supervised restart),
	// when there is no previous /proc/stat sample to difference against.
	CPUPercent *float64 `json:"cpuPercent,omitempty"`

	Mounts []MountUsage `json:"mounts"`
}

type MountUsage struct {
	Path        string  `json:"path"`
	TotalBytes  uint64  `json:"totalBytes"`
	UsedBytes   uint64  `json:"usedBytes"`
	FreeBytes   uint64  `json:"freeBytes"`
	UsedPercent float64 `json:"usedPercent"`
	Error       string  `json:"error,omitempty"`
}

// HeartbeatEvent is the entire heartbeat payload: the same identity pair the
// live metrics key is built from, so a beat maps to exactly one server.
type HeartbeatEvent struct {
	SystemID   string `json:"systemId"`
	ServerName string `json:"serverName"`
}
