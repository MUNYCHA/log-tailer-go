package stats

import (
	"encoding/json"
	"strings"
	"time"
)

// Sample is one stats reply reduced to the numbers the agent uses. Has* flags
// mark figures Docker actually sent, so a missing one is omitted rather than
// published as 0.
type Sample struct {
	// False when Docker answered for a container that is no longer running:
	// it sends a zero read time and empty groups instead of an error
	Running bool

	// Cumulative CPU time in nanoseconds: the container's, and the whole
	// host's, the pair CPU % is differenced from
	CPUTotal    uint64
	SystemTotal uint64
	HasSystem   bool
	CPUs        int // online_cpus, or the per-CPU list length on old Docker

	HasMemory  bool
	MemUsage   uint64
	MemLimit   uint64
	MemCache   uint64 // inactive file pages, left out of used like docker stats does
	HasNetwork bool   // false for host or none networking, where Docker sends no networks
	RxBytes    uint64
	TxBytes    uint64
	ReadBytes  uint64
	WriteBytes uint64
	HasPIDs    bool
	PIDs       uint64
}

type rawStats struct {
	Read     time.Time `json:"read"`
	CPUStats struct {
		CPUUsage struct {
			TotalUsage  uint64   `json:"total_usage"`
			PercpuUsage []uint64 `json:"percpu_usage"`
		} `json:"cpu_usage"`
		SystemCPUUsage *uint64 `json:"system_cpu_usage"`
		OnlineCPUs     int     `json:"online_cpus"`
	} `json:"cpu_stats"`
	MemoryStats struct {
		Usage *uint64           `json:"usage"`
		Limit uint64            `json:"limit"`
		Stats map[string]uint64 `json:"stats"`
	} `json:"memory_stats"`
	Networks map[string]struct {
		RxBytes uint64 `json:"rx_bytes"`
		TxBytes uint64 `json:"tx_bytes"`
	} `json:"networks"`
	PIDsStats struct {
		Current *uint64 `json:"current"`
	} `json:"pids_stats"`
	BlkioStats struct {
		IOServiceBytesRecursive []struct {
			Op    string `json:"op"`
			Value uint64 `json:"value"`
		} `json:"io_service_bytes_recursive"`
	} `json:"blkio_stats"`
}

// Parse decodes one stats reply from either cgroup version. The two differ
// only in names: v1 calls the memory cache total_inactive_file and block I/O
// ops "Read"/"Write"; v2 calls them inactive_file and "read"/"write".
func Parse(data []byte) (Sample, error) {
	var raw rawStats
	if err := json.Unmarshal(data, &raw); err != nil {
		return Sample{}, err
	}

	s := Sample{Running: !raw.Read.IsZero()}
	if !s.Running {
		return s, nil
	}

	s.CPUTotal = raw.CPUStats.CPUUsage.TotalUsage
	if raw.CPUStats.SystemCPUUsage != nil {
		s.SystemTotal = *raw.CPUStats.SystemCPUUsage
		s.HasSystem = true
	}
	s.CPUs = raw.CPUStats.OnlineCPUs
	if s.CPUs == 0 {
		s.CPUs = len(raw.CPUStats.CPUUsage.PercpuUsage)
	}

	if raw.MemoryStats.Usage != nil {
		s.HasMemory = true
		s.MemUsage = *raw.MemoryStats.Usage
		s.MemLimit = raw.MemoryStats.Limit
		if v, ok := raw.MemoryStats.Stats["total_inactive_file"]; ok {
			s.MemCache = v
		} else {
			s.MemCache = raw.MemoryStats.Stats["inactive_file"]
		}
	}

	if raw.Networks != nil {
		s.HasNetwork = true
		for _, n := range raw.Networks {
			s.RxBytes += n.RxBytes
			s.TxBytes += n.TxBytes
		}
	}

	for _, e := range raw.BlkioStats.IOServiceBytesRecursive {
		switch strings.ToLower(e.Op) {
		case "read":
			s.ReadBytes += e.Value
		case "write":
			s.WriteBytes += e.Value
		}
	}

	if raw.PIDsStats.Current != nil {
		s.HasPIDs = true
		s.PIDs = *raw.PIDsStats.Current
	}

	return s, nil
}
