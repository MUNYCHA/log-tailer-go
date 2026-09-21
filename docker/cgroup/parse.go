package cgroup

import (
	"encoding/json"
	"fmt"
)

// Parse returns the cgroup version from Docker's info reply. Docker before
// 20.10 (API 1.40) has no CgroupVersion field and only ever ran on cgroup v1,
// so a missing field means 1.
func Parse(data []byte) (int, error) {
	var info struct {
		CgroupVersion string `json:"CgroupVersion"`
	}
	if err := json.Unmarshal(data, &info); err != nil {
		return 0, err
	}
	switch info.CgroupVersion {
	case "", "1":
		return 1, nil
	case "2":
		return 2, nil
	default:
		return 0, fmt.Errorf("unknown cgroup version %q", info.CgroupVersion)
	}
}
