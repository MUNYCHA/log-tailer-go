// Package storage publishes the server's total local storage and disk usage
// for the configured mounts per interval.
//
// The collector here only orchestrates. The disk and mounts subpackages have
// the same three steps as every metric — read.go touches the server, parse.go
// turns raw data into values, calculate.go turns values into what is
// published — so this file never calls statfs, reads the mount table or does
// arithmetic itself.
package storage

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"time"

	"log-tailer-go/config"
	"log-tailer-go/model"
	"log-tailer-go/storage/disk"
	"log-tailer-go/storage/mounts"
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

	// Local filesystems and configured mount paths that failed to stat on the
	// previous tick, so a failure is logged when it starts and when it clears
	// rather than on every tick. Kept apart because the same path can be both.
	// Only ever touched from Run's goroutine, so they need no lock.
	failing      map[string]bool
	mountFailing map[string]bool
}

func New(mounts []string, channel string, identity config.IdentityConfig, interval time.Duration, publisher Publisher) *Collector {
	return &Collector{
		mounts:       mounts,
		channel:      channel,
		identity:     identity,
		interval:     interval,
		publisher:    publisher,
		failing:      make(map[string]bool),
		mountFailing: make(map[string]bool),
	}
}

// Run reports every mount each interval and publishes one event per tick.
// Returns when ctx is cancelled (graceful shutdown).
func (c *Collector) Run(ctx context.Context) {
	slog.Info("Starting storage collector", "channel", c.channel, "interval", c.interval, "mounts", c.mounts)

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
	table := readMountTable()

	// Built non-nil so an empty mounts list publishes [] rather than null
	usage := make([]model.MountUsage, len(c.mounts))
	for i, path := range c.mounts {
		usage[i] = c.mountUsage(table, path)
	}

	event := model.StorageEvent{
		ServerID:  c.identity.ServerID,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Server:    c.serverStorage(table),
		Mounts:    usage,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		slog.Error("Failed to serialize storage event", "error", err)
		return
	}

	c.publisher.PublishBatch(ctx, c.channel, [][]byte{payload})
}

// readMountTable returns the parsed mount table, or nil when it can't be read.
// Read once per tick and shared by every configured path.
func readMountTable() []mounts.Entry {
	data, err := mounts.Read()
	if err != nil {
		slog.Warn("Failed to read mount table, reporting mounts without device, fsType or network check", "path", mounts.Path, "error", err)
		return nil
	}
	return mounts.Parse(data)
}

// serverStorage sums every local filesystem in the mount table, each counted
// once. A filesystem that can't be statted is left out and marks the total
// partial, naming it in MissingPaths so the event says what is absent without
// the reader having to find the log line; the total is omitted when none can
// be read. Network filesystems are never in the table's local set, so this
// never blocks on a dead remote.
func (c *Collector) serverStorage(table []mounts.Entry) *model.ServerStorage {
	var usages []disk.Usage
	var missing []string
	for _, fs := range mounts.LocalFilesystems(table) {
		stat, err := disk.Read(fs.Path)
		if err != nil {
			missing = append(missing, fs.Path)
			if !c.failing[fs.Path] {
				slog.Warn("Failed to stat local filesystem, server storage total is partial", "path", fs.Path, "device", fs.Device, "error", err)
				c.failing[fs.Path] = true
			}
			continue
		}
		if c.failing[fs.Path] {
			slog.Info("Local filesystem readable again", "path", fs.Path, "device", fs.Device)
			delete(c.failing, fs.Path)
		}

		usage := disk.ToBytes(disk.Parse(stat))
		if usage.TotalBytes == 0 {
			continue
		}
		usages = append(usages, usage)
	}

	if len(usages) == 0 {
		return nil
	}

	sort.Strings(missing)

	total := disk.Total(usages)
	return &model.ServerStorage{
		DiskUsage: model.DiskUsage{
			TotalBytes:    total.TotalBytes,
			UsedBytes:     total.UsedBytes,
			FreeBytes:     total.FreeBytes,
			ReservedBytes: total.ReservedBytes,
			UsedPercent:   total.UsedPercent,
		},
		Partial:      len(missing) > 0,
		MissingPaths: missing,
	}
}

// errNetworkFS is published for a configured path on a network filesystem.
const errNetworkFS = "network filesystem not supported"

// mountUsage reports one configured path. A path that can't be statted is
// still listed, with Error set, so the consumer sees it is down rather than
// missing; the failure is logged once when it starts and once when it clears.
// A path on a network filesystem is never statted: statfs there can block
// until the remote server answers, which would hold up the whole event.
func (c *Collector) mountUsage(table []mounts.Entry, path string) model.MountUsage {
	entry, found := mounts.Find(table, path)
	if found && mounts.IsNetwork(entry.FSType) {
		return model.MountUsage{Path: path, FSType: entry.FSType, Error: errNetworkFS}
	}

	stat, err := disk.Read(path)
	if err != nil {
		if !c.mountFailing[path] {
			slog.Warn("Failed to stat mount, reporting as unavailable", "path", path, "error", err)
			c.mountFailing[path] = true
		}
		return model.MountUsage{Path: path, Error: err.Error()}
	}
	if c.mountFailing[path] {
		slog.Info("Mount readable again", "path", path)
		delete(c.mountFailing, path)
	}

	usage := disk.ToBytes(disk.Parse(stat))
	return model.MountUsage{
		Path:   path,
		Device: entry.Device,
		FSType: entry.FSType,
		DiskUsage: &model.DiskUsage{
			TotalBytes:    usage.TotalBytes,
			UsedBytes:     usage.UsedBytes,
			FreeBytes:     usage.FreeBytes,
			ReservedBytes: usage.ReservedBytes,
			UsedPercent:   usage.UsedPercent,
		},
	}
}
