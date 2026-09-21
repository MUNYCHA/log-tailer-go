// Package stats reports one running container's CPU, memory, network, block
// I/O and process count, the same figures docker stats shows.
package stats

import (
	"context"
	"net/http"

	"log-tailer-go/docker/client"
)

// Path is the Docker API request for one container's stats. one-shot asks
// Docker 20.10+ to answer at once without its own second sample; older
// daemons ignore it and take about a second. Either way CPU is differenced
// against the agent's previous reading, not Docker's.
func Path(id string) string {
	return "/containers/" + id + "/stats?stream=false&one-shot=true"
}

// Read returns Docker's raw stats reply for one container. It is the only
// function in this package that touches the server.
func Read(ctx context.Context, c *http.Client, id string) ([]byte, error) {
	return client.Get(ctx, c, Path(id))
}
