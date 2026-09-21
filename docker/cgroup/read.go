// Package cgroup reports which kernel cgroup version Docker's per-container
// counters come from (docker info). It matters to the consumer because
// cgroup v1 misses buffered disk writes, so block I/O write bytes are only
// comparable between servers on the same version.
package cgroup

import (
	"context"
	"net/http"

	"log-tailer-go/docker/client"
)

// Path is the Docker API request for daemon-wide info.
const Path = "/info"

// Read returns Docker's raw info reply. It is the only function in this
// package that touches the server.
func Read(ctx context.Context, c *http.Client) ([]byte, error) {
	return client.Get(ctx, c, Path)
}
