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

const uptimePath = "/proc/uptime"

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

	mounts := make([]model.MountUsage, len(c.mounts))
	for i, path := range c.mounts {
		mounts[i] = statMount(path)
	}

	event := model.MetricsEvent{
		SystemID:      c.identity.System.ID,
		SystemName:    c.identity.System.Name,
		ServerName:    c.identity.Server.Name,
		ServerIP:      c.identity.Server.IP,
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		UptimeSeconds: uptime,
		Mounts:        mounts,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		slog.Error("Failed to serialize metrics event", "error", err)
		return
	}

	c.publisher.PublishBatch(ctx, c.channel, [][]byte{payload})
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
