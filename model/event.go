package model

type LogEvent struct {
	ServerName string `json:"serverName"`
	Path       string `json:"path"`
	Channel    string `json:"channel"`
	Timestamp  string `json:"timestamp"`
	Message    string `json:"message"`
}

type MetricsEvent struct {
	SystemID      string       `json:"systemId"`
	SystemName    string       `json:"systemName"`
	ServerName    string       `json:"serverName"`
	ServerIP      string       `json:"serverIp"`
	Timestamp     string       `json:"timestamp"`
	UptimeSeconds int64        `json:"uptimeSeconds"`
	Mounts        []MountUsage `json:"mounts"`
}

type MountUsage struct {
	Path        string  `json:"path"`
	TotalBytes  uint64  `json:"totalBytes"`
	UsedBytes   uint64  `json:"usedBytes"`
	FreeBytes   uint64  `json:"freeBytes"`
	UsedPercent float64 `json:"usedPercent"`
	Error       string  `json:"error,omitempty"`
}
