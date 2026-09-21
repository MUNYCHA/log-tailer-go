package state

import (
	"encoding/json"
	"time"
)

// Sample is the part of a container's details the agent uses. The details
// also hold the container's environment, command and mounts, often with
// secrets in them; none of it is decoded.
type Sample struct {
	Running    bool
	ExitCode   int
	FinishedAt time.Time // zero when the container has never stopped
}

// Parse decodes the State block of Docker's container details.
func Parse(data []byte) (Sample, error) {
	var raw struct {
		State struct {
			Running    bool      `json:"Running"`
			ExitCode   int       `json:"ExitCode"`
			FinishedAt time.Time `json:"FinishedAt"`
		} `json:"State"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return Sample{}, err
	}
	return Sample{
		Running:    raw.State.Running,
		ExitCode:   raw.State.ExitCode,
		FinishedAt: raw.State.FinishedAt,
	}, nil
}
