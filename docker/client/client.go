// Package client talks to the Docker daemon over its unix socket, read-only.
// Every request passes the allow-list guard in guard.go before it reaches the
// socket, so the agent can never stop, remove, exec into or otherwise change a
// container, even though it runs as root.
package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
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

// maxBody caps how much of one reply is read, so a runaway response can't
// push the agent into its systemd memory cap and take every collector down.
const maxBody = 8 << 20

// Get sends GET path (e.g. "/containers/json?all=true") and returns the body
// of a 200 reply. Any other status is an error carrying Docker's message.
func Get(ctx context.Context, c *http.Client, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker"+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxBody {
		return nil, fmt.Errorf("docker: reply to %s larger than %d bytes", path, maxBody)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("docker: %s returned %s: %s", path, resp.Status, bytes.TrimSpace(body))
	}
	return body, nil
}
