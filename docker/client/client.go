// Package client talks to the Docker daemon over its unix socket, read-only.
// Every request passes the allow-list guard in guard.go before it reaches the
// socket, so the agent can never stop, remove, exec into or otherwise change a
// container, even though it runs as root.
package client

import (
	"context"
	"net"
	"net/http"
	"time"
)

// SocketPath is where the Docker daemon listens.
const SocketPath = "/var/run/docker.sock"

const dialTimeout = 5 * time.Second

// New returns an HTTP client that dials socketPath and refuses every request
// the guard does not allow. Request URLs use the placeholder host "docker",
// e.g. http://docker/containers/json?all=true. Callers bound each request with
// a context deadline.
func New(socketPath string) *http.Client {
	dialer := &net.Dialer{Timeout: dialTimeout}
	return &http.Client{
		Transport: readOnlyTransport{next: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return dialer.DialContext(ctx, "unix", socketPath)
			},
		}},
		// Docker never redirects; following one could only lead off the list
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
