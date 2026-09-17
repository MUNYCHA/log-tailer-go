// Package metrics publishes one combined snapshot of the server per interval.
//
// The collector here only orchestrates. Each metric lives in its own
// subpackage with the same three steps — read.go touches the server,
// parse.go turns raw text into numbers, calculate.go turns numbers into the
// published values — so this file never reads /proc or does arithmetic itself.
package metrics

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"log-tailer-go/config"
	"log-tailer-go/resources/cpu"
	"log-tailer-go/storage/disk"
	"log-tailer-go/resources/load"
	"log-tailer-go/resources/memory"
	"log-tailer-go/resources/network"
	"log-tailer-go/resources/uptime"
	"log-tailer-go/model"
)

// Publisher ships a batch of serialized events to a pub/sub channel in one
// pipelined round trip, returning how many were accepted.
type Publisher interface {
	PublishBatch(ctx context.Context, channel string, payloads [][]byte) int
}

type Collector struct {
	mounts    []string
	channel   string
	identity  config.IdentityConfig
	interval  time.Duration
	publisher Publisher

	// Previous CPU and network readings, differenced against the next tick to
	// get cpuPercent and the network rates. Nil until the first tick has been
	// taken. Only ever touched from Run's goroutine, so they need no lock.
	prevCPU   *cpu.Sample
	prevNet   network.Sample
	prevNetAt time.Time
}

func New(mounts []string, channel string, identity config.IdentityConfig, interval time.Duration, publisher Publisher) *Collector {
	return &Collector{
		mounts:    mounts,
		channel:   channel,
		identity:  identity,
		interval:  interval,
		publisher: publisher,
	}
}

// Run collects every metric each interval and publishes one combined event
// per tick. Returns when ctx is cancelled (graceful shutdown).
func (c *Collector) Run(ctx context.Context) {
	slog.Info("Starting metrics collector", "channel", c.channel, "interval", c.interval, "mounts", c.mounts)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

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
	uptimeSeconds, err := readUptime()
	if err != nil {
		slog.Error("Failed to read server uptime, skipping this tick", "error", err)
		return
	}

	event := model.MetricsEvent{
		SystemID:      c.identity.System.ID,
		SystemName:    c.identity.System.Name,
		ServerName:    c.identity.Server.Name,
		ServerIP:      c.identity.Server.IP,
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		UptimeSeconds: uptimeSeconds,
	}
	c.addLoad(&event)
	c.addMemory(&event)
	c.addCPUPercent(&event)
	c.addNetIO(&event)

	mounts := make([]model.MountUsage, len(c.mounts))
	for i, path := range c.mounts {
		mounts[i] = mountUsage(path)
	}

	event.Mounts = mounts

	payload, err := json.Marshal(event)
	if err != nil {
		slog.Error("Failed to serialize metrics event", "error", err)
		return
	}

	c.publisher.PublishBatch(ctx, c.channel, [][]byte{payload})
}

// readUptime is required, unlike every other metric: without it the tick is
// skipped entirely.
func readUptime() (int64, error) {
	data, err := uptime.Read()
	if err != nil {
		return 0, err
	}
	sample, err := uptime.Parse(data)
	if err != nil {
		return 0, err
	}
	return uptime.Seconds(sample), nil
}

// addLoad fills load1/5/15, leaving all three unset if /proc/loadavg is
// unreadable — a group is reported whole or not at all.
func (c *Collector) addLoad(event *model.MetricsEvent) {
	data, err := load.Read()
	if err == nil {
		var sample load.Sample
		if sample, err = load.Parse(data); err == nil {
			one, five, fifteen := load.Averages(sample)
			event.Load1 = ptr(one)
			event.Load5 = ptr(five)
			event.Load15 = ptr(fifteen)
			return
		}
	}
	slog.Warn("Failed to read load average, omitting from this tick", "path", load.Path, "error", err)
}

func (c *Collector) addMemory(event *model.MetricsEvent) {
	data, err := memory.Read()
	if err == nil {
		var sample memory.Sample
		if sample, err = memory.Parse(data); err == nil {
			usage := memory.ToBytes(sample)
			event.MemTotalBytes = ptr(usage.TotalBytes)
			event.MemAvailableBytes = ptr(usage.AvailableBytes)
			event.SwapTotalBytes = ptr(usage.SwapTotalBytes)
			event.SwapUsedBytes = ptr(usage.SwapUsedBytes)
			return
		}
	}
	slog.Warn("Failed to read memory info, omitting from this tick", "path", memory.Path, "error", err)
}

// addCPUPercent differences this tick's /proc/stat against the previous one,
// so the value is the mean over the whole interval rather than an instant.
// The first tick has nothing to difference against and omits the field.
func (c *Collector) addCPUPercent(event *model.MetricsEvent) {
	data, err := cpu.Read()
	if err != nil {
		slog.Warn("Failed to read CPU stats, omitting from this tick", "path", cpu.Path, "error", err)
		return
	}

	now, err := cpu.Parse(data)
	if err != nil {
		slog.Warn("Failed to parse CPU stats, omitting from this tick", "path", cpu.Path, "error", err)
		return
	}

	prev := c.prevCPU
	c.prevCPU = &now
	if prev == nil {
		return
	}
	if pct, ok := cpu.Percent(*prev, now); ok {
		event.CPUPercent = ptr(pct)
	}
}

// addNetIO differences this tick's /proc/net/dev against the previous one to
// get download (rx) and upload (tx) bytes per second. Like addCPUPercent, the
// first tick has no baseline and omits both fields.
func (c *Collector) addNetIO(event *model.MetricsEvent) {
	data, err := network.Read()
	if err != nil {
		slog.Warn("Failed to read network stats, omitting from this tick", "path", network.Path, "error", err)
		return
	}

	now, err := network.Parse(data)
	if err != nil {
		slog.Warn("Failed to parse network stats, omitting from this tick", "path", network.Path, "error", err)
		return
	}
	takenAt := time.Now()
	network.KeepPhysical(now)

	prev, prevAt := c.prevNet, c.prevNetAt
	c.prevNet, c.prevNetAt = now, takenAt
	if prev == nil {
		return
	}
	if rx, tx, ok := network.Rates(prev, now, takenAt.Sub(prevAt)); ok {
		event.NetRxBytesPerSec = ptr(rx)
		event.NetTxBytesPerSec = ptr(tx)
	}
}

// mountUsage reports one mount. A mount that can't be statted is still listed,
// with Error set, so the consumer sees it is down rather than missing.
func mountUsage(path string) model.MountUsage {
	stat, err := disk.Read(path)
	if err != nil {
		slog.Warn("Failed to stat mount, reporting as unavailable", "path", path, "error", err)
		return model.MountUsage{Path: path, Error: err.Error()}
	}

	usage := disk.ToBytes(disk.Parse(stat))
	return model.MountUsage{
		Path:        path,
		TotalBytes:  usage.TotalBytes,
		UsedBytes:   usage.UsedBytes,
		FreeBytes:   usage.FreeBytes,
		UsedPercent: usage.UsedPercent,
	}
}

func ptr[T any](v T) *T {
	return &v
}
