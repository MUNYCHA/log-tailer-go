// Package docker publishes every container on the server per interval:
// metadata for all of them, docker stats figures for the running ones, and
// the last exit code and time for the stopped ones.
//
// The collector here only orchestrates. The containers, stats, state and
// cgroup subpackages have the same steps as every metric — read.go touches the
// server, parse.go turns the raw reply into values, calculate.go turns values
// into what is published — so this file never talks to Docker or reshapes
// data itself. Every request goes through docker/client, which refuses
// anything but the read-only requests on its allow-list.
package docker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"log-tailer-go/config"
	"log-tailer-go/docker/cgroup"
	"log-tailer-go/docker/client"
	"log-tailer-go/docker/containers"
	"log-tailer-go/docker/state"
	"log-tailer-go/docker/stats"
	"log-tailer-go/model"
)

// requestTimeout bounds each request, so a hung daemon costs one late event
// rather than a collector stuck forever.
const requestTimeout = 5 * time.Second

// maxParallel bounds concurrent requests of each kind — stats for running
// containers, details for stopped ones — so a server with hundreds of
// containers doesn't open hundreds of connections to the daemon at once. The
// two kinds get separate slots, so stopped containers never queue ahead of
// running containers' stats.
const maxParallel = 8

// A whole round is bounded too: a daemon that answers the list but hangs on
// stats (a wedged containerd does this) would otherwise cost requestTimeout
// per batch of containers. When the budget runs out, the containers still
// waiting are published with an error and the event goes out on time. The
// budget is the interval, capped at maxRound, and never under minRound so a
// very short interval can't starve Docker of time to answer at all.
const (
	maxRound = 10 * time.Second
	minRound = time.Second
)

// errTimedOut is published for a container whose request ran past its
// timeout or the round budget.
const errTimedOut = "timed out"

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
	roundBudget   time.Duration

	// prevStats is each running container's previous sample, differenced
	// against the next tick for CPU %, pruned to the containers still
	// running. detailsFailing holds containers whose stats or details failed
	// last tick, logged once when it starts and once when it clears, pruned
	// to the containers still listed. Both are keyed by container id.
	prevStats      map[string]stats.Sample
	detailsFailing map[string]bool
}

func New(socketPath, channel string, identity config.IdentityConfig, interval time.Duration, publisher Publisher) *Collector {
	return &Collector{
		socketPath:     socketPath,
		channel:        channel,
		identity:       identity,
		interval:       interval,
		publisher:      publisher,
		client:         client.New(socketPath),
		roundBudget:    roundBudget(interval),
		prevStats:      make(map[string]stats.Sample),
		detailsFailing: make(map[string]bool),
	}
}

func roundBudget(interval time.Duration) time.Duration {
	return max(minRound, min(interval, maxRound))
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
	round, cancel := context.WithTimeout(ctx, c.roundBudget)
	defer cancel()

	list, err := c.readContainers(round)
	if err != nil {
		return
	}
	c.withDetails(round, list)
}

func (c *Collector) collectAndPublish(ctx context.Context) {
	event := model.DockerEvent{
		ServerID:   c.identity.ServerID,
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		Containers: []model.DockerContainer{},
	}

	round, cancel := context.WithTimeout(ctx, c.roundBudget)
	defer cancel()

	list, err := c.readContainers(round)
	if err != nil {
		// Shutdown cancels the request mid-flight; that is not Docker failing
		if ctx.Err() != nil {
			return
		}
		if !c.failing {
			slog.Warn("Failed to read docker containers, publishing the error", "socket", c.socketPath, "error", err)
			c.failing = true
		}
		event.Error = errorText(err)
	} else {
		if c.failing {
			slog.Info("Docker readable again", "socket", c.socketPath)
			c.failing = false
		}
		c.readCgroupVersion(round)
		event.Containers = c.withDetails(round, list)
		// Shutdown mid-round leaves details half read; publish nothing rather
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

// withDetails turns the container list into published containers, reading
// stats for the running ones and last-exit details for the rest, in
// parallel. A stopped container's details are read every round rather than
// cached: one that starts and stops again between two rounds keeps its id
// and state, and only a fresh read shows its new exit code.
func (c *Collector) withDetails(ctx context.Context, list []containers.Container) []model.DockerContainer {
	type result struct {
		sample stats.Sample
		exit   state.Sample
		err    error
	}
	results := make([]result, len(list))

	var wg sync.WaitGroup
	statsSlots := make(chan struct{}, maxParallel)
	detailSlots := make(chan struct{}, maxParallel)
	for i, ct := range list {
		slots := detailSlots
		if hasStats(ct.State) {
			slots = statsSlots
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				results[i].err = ctx.Err()
				return
			}
			defer func() { <-slots }()
			if hasStats(ct.State) {
				results[i].sample, results[i].err = c.readStats(ctx, ct.ID)
			} else {
				results[i].exit, results[i].err = c.readState(ctx, ct.ID)
			}
		}()
	}
	wg.Wait()

	out := make([]model.DockerContainer, len(list))
	listed := make(map[string]bool)
	running := make(map[string]bool)
	for i, ct := range list {
		out[i] = model.DockerContainer{ID: ct.ID, Name: ct.Name, Image: ct.Image, State: ct.State}
		listed[ct.ID] = true

		r := results[i]
		if r.err != nil {
			if !c.detailsFailing[ct.ID] {
				slog.Warn("Failed to read container details, publishing metadata only", "container", ct.Name, "state", ct.State, "error", r.err)
				c.detailsFailing[ct.ID] = true
			}
			out[i].Error = errorText(r.err)
			continue
		}
		if c.detailsFailing[ct.ID] {
			slog.Info("Container details readable again", "container", ct.Name)
			delete(c.detailsFailing, ct.ID)
		}

		if !hasStats(ct.State) {
			if e, ok := state.Exit(r.exit); ok {
				code := e.ExitCode
				out[i].ExitCode = &code
				out[i].FinishedAt = e.FinishedAt
			}
			continue
		}
		// Stopped between the list and the stats request; the next tick
		// lists it with its new state
		if !r.sample.Running {
			continue
		}
		running[ct.ID] = true
		c.fillStats(&out[i], r.sample)
	}

	// A container missing from running this tick starts CPU afresh next
	// time, so a failed read or a restart never differences across a gap
	for id := range c.prevStats {
		if !running[id] {
			delete(c.prevStats, id)
		}
	}
	for id := range c.detailsFailing {
		if !listed[id] {
			delete(c.detailsFailing, id)
		}
	}
	return out
}

// errorText is the published error for a failed request: a plain
// "timed out" for any deadline, rather than Go's full request error.
func errorText(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return errTimedOut
	}
	return err.Error()
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

func (c *Collector) readState(ctx context.Context, id string) (state.Sample, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	data, err := state.Read(ctx, c.client, id)
	if err != nil {
		return state.Sample{}, err
	}
	return state.Parse(data)
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
