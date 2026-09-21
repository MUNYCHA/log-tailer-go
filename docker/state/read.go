// Package state reports how a stopped container last exited — its exit code
// and when — from Docker's container details (docker inspect).
package state

import (
	"context"
	"net/http"

	"log-tailer-go/docker/client"
)

// Path is the Docker API request for one container's details.
func Path(id string) string {
	return "/containers/" + id + "/json"
}

// Read returns Docker's raw details for one container. It is the only
// function in this package that touches the server.
func Read(ctx context.Context, c *http.Client, id string) ([]byte, error) {
	return client.Get(ctx, c, Path(id))
}
