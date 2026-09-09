package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strconv"
	"syscall"
	"time"

	"log-tailer-go/config"
	"log-tailer-go/model"
)

const (
	uptimePath  = "/proc/uptime"
	loadavgPath = "/proc/loadavg"
	meminfoPath = "/proc/meminfo"
	statPath    = "/proc/stat"
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

	// Previous /proc/stat reading, differenced against the next one to get
	// cpuPercent. Nil until the first tick has been taken. Only ever touched
	// from Run's goroutine, so it needs no lock.
	prevCPU *cpuSample
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

// Run collects uptime and mount usage every interval and publishes one
// combined event per tick. Returns when ctx is cancelled (graceful shutdown).
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
	uptime, err := readUptime(uptimePath)
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
		UptimeSeconds: uptime,
	}
	c.addLoadAvg(&event)
	c.addMemInfo(&event)
	c.addCPUPercent(&event)

	mounts := make([]model.MountUsage, len(c.mounts))
	for i, path := range c.mounts {
		mounts[i] = statMount(path)
	}

	event.Mounts = mounts

	payload, err := json.Marshal(event)
	if err != nil {
		slog.Error("Failed to serialize metrics event", "error", err)
		return
	}

	c.publisher.PublishBatch(ctx, c.channel, [][]byte{payload})
}

// addLoadAvg fills load1/5/15, leaving all three unset if /proc/loadavg is
// unreadable — a group is reported whole or not at all.
func (c *Collector) addLoadAvg(event *model.MetricsEvent) {
	data, err := os.ReadFile(loadavgPath)
	if err == nil {
		var load loadAvg
		if load, err = parseLoadavg(data); err == nil {
			event.Load1 = ptr(load.one)
			event.Load5 = ptr(load.five)
			event.Load15 = ptr(load.fifteen)
			return
		}
	}
	slog.Warn("Failed to read load average, omitting from this tick", "path", loadavgPath, "error", err)
}

func (c *Collector) addMemInfo(event *model.MetricsEvent) {
	data, err := os.ReadFile(meminfoPath)
	if err == nil {
		var mem memInfo
		if mem, err = parseMeminfo(data); err == nil {
			event.MemTotalBytes = ptr(mem.totalBytes)
			event.MemAvailableBytes = ptr(mem.availableBytes)
			event.SwapTotalBytes = ptr(mem.swapTotalBytes)
			event.SwapUsedBytes = ptr(mem.swapUsedBytes)
			return
		}
	}
	slog.Warn("Failed to read memory info, omitting from this tick", "path", meminfoPath, "error", err)
}

// addCPUPercent differences this tick's /proc/stat against the previous one,
// so the value is the mean over the whole interval rather than an instant.
// The first tick has nothing to difference against and omits the field.
func (c *Collector) addCPUPercent(event *model.MetricsEvent) {
	data, err := os.ReadFile(statPath)
	if err != nil {
		slog.Warn("Failed to read CPU stats, omitting from this tick", "path", statPath, "error", err)
		return
	}

	now, err := parseStat(data)
	if err != nil {
		slog.Warn("Failed to parse CPU stats, omitting from this tick", "path", statPath, "error", err)
		return
	}

	prev := c.prevCPU
	c.prevCPU = &now
	if prev == nil {
		return
	}
	if pct, ok := cpuPercent(*prev, now); ok {
		event.CPUPercent = ptr(pct)
	}
}

func ptr[T any](v T) *T {
	return &v
}

func statMount(path string) model.MountUsage {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		slog.Warn("Failed to stat mount, reporting as unavailable", "path", path, "error", err)
		return model.MountUsage{Path: path, Error: err.Error()}
	}

	total := stat.Blocks * uint64(stat.Bsize)
	free := stat.Bavail * uint64(stat.Bsize)
	used := total - stat.Bfree*uint64(stat.Bsize)

	var usedPercent float64
	if total > 0 {
		usedPercent = float64(used) / float64(total) * 100
	}

	return model.MountUsage{
		Path:        path,
		TotalBytes:  total,
		UsedBytes:   used,
		FreeBytes:   free,
		UsedPercent: usedPercent,
	}
}

func readUptime(path string) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return parseUptimeLine(data)
}

// parseUptimeLine extracts the seconds-since-boot field from /proc/uptime's
// content ("<uptime> <idle>"), truncated to whole seconds.
func parseUptimeLine(data []byte) (int64, error) {
	fields := bytes.Fields(data)
	if len(fields) == 0 {
		return 0, os.ErrInvalid
	}
	seconds, err := strconv.ParseFloat(string(fields[0]), 64)
	if err != nil {
		return 0, err
	}
	return int64(seconds), nil
}
