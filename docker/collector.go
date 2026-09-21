// Package docker publishes every container on the server per interval:
// metadata for all of them, and docker stats figures for the running ones.
//
// The collector here only orchestrates. The containers, stats and cgroup
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
	"sync"
	"time"

	"log-tailer-go/config"
	"log-tailer-go/docker/cgroup"
	"log-tailer-go/docker/client"
	"log-tailer-go/docker/containers"
	"log-tailer-go/docker/stats"
	"log-tailer-go/model"
)

// requestTimeout bounds each request, so a hung daemon costs one late event
// rather than a collector stuck forever.
const requestTimeout = 5 * time.Second

// maxParallel bounds concurrent stats requests, so a server with hundreds of
// containers doesn't open hundreds of connections to the daemon at once.
const maxParallel = 8

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

	// prevStats is each running container's previous sample, differenced
	// against the next tick for CPU %. statsFailing holds containers whose
	// stats failed last tick, logged once when it starts and once when it
	// clears. Both are keyed by container id and pruned to the containers
	// still running.
	prevStats    map[string]stats.Sample
	statsFailing map[string]bool
}

func New(socketPath, channel string, identity config.IdentityConfig, interval time.Duration, publisher Publisher) *Collector {
	return &Collector{
		socketPath:   socketPath,
		channel:      channel,
		identity:     identity,
		interval:     interval,
		publisher:    publisher,
		client:       client.New(socketPath),
		prevStats:    make(map[string]stats.Sample),
		statsFailing: make(map[string]bool),
	}
}

// Run lists every container each interval and publishes one event per tick.
// Returns when ctx is cancelled (graceful shutdown).
func (c *Collector) Run(ctx context.Context) {
	slog.Info("Starting docker collector", "channel", c.channel, "interval", c.interval, "socket", c.socketPath)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	// Read stats first, so the startup delay doubles as the window CPU % is
	// measured over and the first event is complete rather than missing
	// cpuPercent until the next tick. Then publish once after the delay and
	// on the interval, in step with the other collectors.
	c.primeCPU(ctx)
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

// primeCPU takes the first stats reading, so the event published after the
// startup delay has something to difference CPU against. Nothing is
// published here, and the reading is stored by the same code the ticker
// uses. Docker being down is left for the first tick to report.
func (c *Collector) primeCPU(ctx context.Context) {
	list, err := c.readContainers(ctx)
	if err != nil {
		return
	}
	c.withStats(ctx, list)
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
		event.Containers = c.withStats(ctx, list)
		// Shutdown mid-round leaves stats half read; publish nothing rather
		// than containers wrongly marked as failing
		if ctx.Err() != nil {
			return
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

// hasStats reports whether Docker keeps stats for a container in this state.
// A paused container still has memory and processes, so it is included.
func hasStats(state string) bool {
	return state == "running" || state == "paused"
}

// withStats turns the container list into published containers, reading
// stats for the running ones in parallel.
func (c *Collector) withStats(ctx context.Context, list []containers.Container) []model.DockerContainer {
	type result struct {
		sample stats.Sample
		err    error
	}
	results := make([]result, len(list))

	var wg sync.WaitGroup
	sem := make(chan struct{}, maxParallel)
	for i, ct := range list {
		if !hasStats(ct.State) {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[i].sample, results[i].err = c.readStats(ctx, ct.ID)
		}()
	}
	wg.Wait()

	out := make([]model.DockerContainer, len(list))
	seen := make(map[string]bool)
	for i, ct := range list {
		out[i] = model.DockerContainer{ID: ct.ID, Name: ct.Name, Image: ct.Image, State: ct.State}
		if !hasStats(ct.State) {
			continue
		}
		seen[ct.ID] = true

		r := results[i]
		if r.err != nil {
			if !c.statsFailing[ct.ID] {
				slog.Warn("Failed to read container stats, publishing metadata only", "container", ct.Name, "error", r.err)
				c.statsFailing[ct.ID] = true
			}
			out[i].Error = r.err.Error()
			delete(c.prevStats, ct.ID)
			continue
		}
		if c.statsFailing[ct.ID] {
			slog.Info("Container stats readable again", "container", ct.Name)
			delete(c.statsFailing, ct.ID)
		}
		// Stopped between the list and the stats request; the next tick
		// lists it with its new state
		if !r.sample.Running {
			delete(c.prevStats, ct.ID)
			continue
		}
		c.fillStats(&out[i], r.sample)
	}

	for id := range c.prevStats {
		if !seen[id] {
			delete(c.prevStats, id)
		}
	}
	for id := range c.statsFailing {
		if !seen[id] {
			delete(c.statsFailing, id)
		}
	}
	return out
}

// fillStats copies one sample onto a published container, differencing CPU
// against the container's previous sample.
func (c *Collector) fillStats(out *model.DockerContainer, s stats.Sample) {
	if prev, ok := c.prevStats[out.ID]; ok {
		if pct, ok := stats.CPUPercent(prev, s); ok {
			out.CPUPercent = &pct
		}
	}
	c.prevStats[out.ID] = s

	if m, ok := stats.MemoryUsage(s); ok {
		out.Memory = &model.DockerMemory{UsedBytes: m.UsedBytes, LimitBytes: m.LimitBytes, UsedPercent: m.UsedPercent}
	}
	if s.HasNetwork {
		out.Network = &model.DockerNetwork{RxBytes: s.RxBytes, TxBytes: s.TxBytes}
	}
	out.BlockIO = &model.DockerBlockIO{ReadBytes: s.ReadBytes, WriteBytes: s.WriteBytes}
	if s.HasPIDs {
		pids := s.PIDs
		out.PIDs = &pids
	}
}

func (c *Collector) readStats(ctx context.Context, id string) (stats.Sample, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	data, err := stats.Read(ctx, c.client, id)
	if err != nil {
		return stats.Sample{}, err
	}
	return stats.Parse(data)
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
