// Package containers lists every container on the server, running or not,
// from Docker's container list (docker ps -a).
package containers

import (
	"context"
	"net/http"

	"log-tailer-go/docker/client"
)

// Path is the Docker API request for every container, stopped ones included.
const Path = "/containers/json?all=true"

// Read returns Docker's raw container list. It is the only function in this
// package that touches the server.
func Read(ctx context.Context, c *http.Client) ([]byte, error) {
	return client.Get(ctx, c, Path)
}
