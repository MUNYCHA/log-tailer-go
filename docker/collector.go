// Package docker publishes every container on the server, running or not,
// per interval.
//
// The collector here only orchestrates. The containers and cgroup
// subpackages have the same steps as every metric — read.go touches the
// server, parse.go turns the raw reply into values, calculate.go turns values
// into what is published — so this file never talks to Docker or reshapes
// data itself. Every request goes through docker/client, which refuses
// anything but the read-only requests on its allow-list.
package docker

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"log-tailer-go/config"
	"log-tailer-go/docker/cgroup"
	"log-tailer-go/docker/client"
	"log-tailer-go/docker/containers"
	"log-tailer-go/model"
)

// requestTimeout bounds each request, so a hung daemon costs one late event
// rather than a collector stuck forever.
const requestTimeout = 5 * time.Second

// Publisher ships a batch of serialized events to a pub/sub channel in one
// pipelined round trip, returning how many were accepted.
type Publisher interface {
	PublishBatch(ctx context.Context, channel string, payloads [][]byte) int
}

type Collector struct {
	socketPath string
	channel    string
	identity   config.IdentityConfig
	interval   time.Duration
	publisher  Publisher
	client     *http.Client

	// Only ever touched from Run's goroutine, so they need no lock.
	//
	// cgroupVersion is 0 until Docker has answered /info once; it can't change
	// without a reboot, so it is never asked again after that. failing is set
	// while Docker can't be read, so the failure is logged when it starts and
	// when it clears rather than on every tick.
	cgroupVersion int
	failing       bool
}

func New(socketPath, channel string, identity config.IdentityConfig, interval time.Duration, publisher Publisher) *Collector {
	return &Collector{
		socketPath: socketPath,
		channel:    channel,
		identity:   identity,
		interval:   interval,
		publisher:  publisher,
		client:     client.New(socketPath),
	}
}

// Run lists every container each interval and publishes one event per tick.
// Returns when ctx is cancelled (graceful shutdown).
func (c *Collector) Run(ctx context.Context) {
	slog.Info("Starting docker collector", "channel", c.channel, "interval", c.interval, "socket", c.socketPath)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	// Publish once after the startup delay, then on the interval, so a
	// restarted agent reports at once instead of after a whole interval of
	// silence, and does it in step with the other collectors.
	if !config.WaitForStartup(ctx, c.interval) {
		return
	}
	c.collectAndPublish(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.collectAndPublish(ctx)
		}
	}
}

func (c *Collector) collectAndPublish(ctx context.Context) {
	event := model.DockerEvent{
		ServerID:   c.identity.ServerID,
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		Containers: []model.DockerContainer{},
	}

	list, err := c.readContainers(ctx)
	if err != nil {
		// Shutdown cancels the request mid-flight; that is not Docker failing
		if ctx.Err() != nil {
			return
		}
		if !c.failing {
			slog.Warn("Failed to read docker containers, publishing the error", "socket", c.socketPath, "error", err)
			c.failing = true
		}
		event.Error = err.Error()
	} else {
		if c.failing {
			slog.Info("Docker readable again", "socket", c.socketPath)
			c.failing = false
		}
		c.readCgroupVersion(ctx)
		for _, ct := range list {
			event.Containers = append(event.Containers, model.DockerContainer{
				ID:    ct.ID,
				Name:  ct.Name,
				Image: ct.Image,
				State: ct.State,
			})
		}
	}
	event.CgroupVersion = c.cgroupVersion

	payload, err := json.Marshal(event)
	if err != nil {
		slog.Error("Failed to serialize docker event", "error", err)
		return
	}

	c.publisher.PublishBatch(ctx, c.channel, [][]byte{payload})
}

func (c *Collector) readContainers(ctx context.Context) ([]containers.Container, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	data, err := containers.Read(ctx, c.client)
	if err != nil {
		return nil, err
	}
	entries, err := containers.Parse(data)
	if err != nil {
		return nil, err
	}
	return containers.ToContainers(entries), nil
}

// readCgroupVersion asks Docker once. A failure leaves the version unknown
// (omitted from the event) and is retried next tick; it is logged at debug
// only, since the container list failing is what reports Docker being down.
func (c *Collector) readCgroupVersion(ctx context.Context) {
	if c.cgroupVersion != 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	data, err := cgroup.Read(ctx, c.client)
	if err != nil {
		slog.Debug("Failed to read docker info, cgroupVersion omitted", "error", err)
		return
	}
	version, err := cgroup.Parse(data)
	if err != nil {
		slog.Warn("Failed to parse docker cgroup version, cgroupVersion omitted", "error", err)
		return
	}
	c.cgroupVersion = version
}
