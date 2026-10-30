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
	"errors"
	"log/slog"
	"sort"
	"syscall"
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

	// stat is disk.Read, swapped in tests for one that blocks like a dead NAS.
	// pending holds the statfs calls that outlived statTimeout, by path, so a
	// path whose call is still stuck is not statted again. Like the maps
	// above, only touched from Run's goroutine.
	stat        func(path string) (syscall.Statfs_t, error)
	statTimeout time.Duration
	pending     map[string]chan statResult
}

type statResult struct {
	stat syscall.Statfs_t
	err  error
}

// statTimeout is how long one configured path's statfs is waited for. A
// local disk answers in microseconds; a network mount whose server is down
// may not answer at all.
const statTimeout = 2 * time.Second

func New(mounts []string, channel string, identity config.IdentityConfig, interval time.Duration, publisher Publisher) *Collector {
	return &Collector{
		mounts:       mounts,
		channel:      channel,
		identity:     identity,
		interval:     interval,
		publisher:    publisher,
		failing:      make(map[string]bool),
		mountFailing: make(map[string]bool),
		stat:         disk.Read,
		statTimeout:  statTimeout,
		pending:      make(map[string]chan statResult),
	}
}

// Run reports every mount each interval and publishes one event per tick.
// Returns when ctx is cancelled (graceful shutdown).
func (c *Collector) Run(ctx context.Context) {
	slog.Info("Starting storage collector", "channel", c.channel, "interval", c.interval, "mounts", c.mounts)

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
	table := readMountTable()

	event := model.StorageEvent{
		ServerID:  c.identity.ServerID,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Server:    c.serverStorage(table),
		Mounts:    c.mountUsages(table),
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
		slog.Warn("Failed to read mount table, reporting paths without device or fsType and wildcards unmatched", "path", mounts.Path, "error", err)
		return nil
	}
	return mounts.Parse(data)
}

// serverStorage sums every local filesystem in the mount table, each counted
// once. A filesystem that can't be statted is left out and marks the total
// partial, naming it in MissingPaths so the event says what is absent without
// the reader having to find the log line; the total is omitted when none can
// be read. Network filesystems are never in the table's local set, so this
// never touches a remote.
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

// Errors published for a configured path that has no reading.
const (
	errNoMatch      = "no mounts match"
	errNoMountTable = "mount table unreadable"
)

// errStatTimeout is a statfs that did not answer within statTimeout.
var errStatTimeout = errors.New("timeout")

// mountUsages reports every configured path in config order. A wildcard path
// expands to one entry per mount point it matches, each carrying the pattern;
// a mount point listed by more than one config entry is reported once, under
// the first.
func (c *Collector) mountUsages(table []mounts.Entry) []model.MountUsage {
	// Built non-nil so an empty mounts list publishes [] rather than null
	usage := make([]model.MountUsage, 0, len(c.mounts))
	seen := make(map[string]bool, len(c.mounts))
	add := func(u model.MountUsage) {
		if !seen[u.Path] {
			seen[u.Path] = true
			usage = append(usage, u)
		}
	}

	for _, path := range c.mounts {
		if !mounts.HasWildcard(path) {
			add(c.mountUsage(table, path))
			continue
		}

		// A pattern with nothing to show is still listed, so a share that
		// dropped off reads as missing rather than vanishing from the event
		if table == nil {
			add(model.MountUsage{Path: path, Pattern: path, Error: errNoMountTable})
			continue
		}
		matches := mounts.Match(table, path)
		if len(matches) == 0 {
			add(model.MountUsage{Path: path, Pattern: path, Error: errNoMatch})
			continue
		}
		for _, m := range matches {
			u := c.mountUsage(table, m.Path)
			u.Pattern = path
			add(u)
		}
	}
	return usage
}

// mountUsage reports one path. A path that can't be statted is still listed,
// with Error set, so the consumer sees it is down rather than missing; the
// failure is logged once when it starts and once when it clears. A timeout
// keeps device and fsType, naming the NAS that is down; any other error
// drops them, since a missing path would otherwise borrow the device of the
// mount above it (usually "/").
func (c *Collector) mountUsage(table []mounts.Entry, path string) model.MountUsage {
	entry, _ := mounts.Find(table, path)

	stat, err := c.statfs(path)
	if err != nil {
		if !c.mountFailing[path] {
			slog.Warn("Failed to stat mount, reporting as unavailable", "path", path, "device", entry.Device, "fsType", entry.FSType, "error", err)
			c.mountFailing[path] = true
		}
		failed := model.MountUsage{Path: path, Error: err.Error()}
		if errors.Is(err, errStatTimeout) {
			failed.Device, failed.FSType = entry.Device, entry.FSType
		}
		return failed
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

// statfs stats path but waits at most statTimeout. statfs has no deadline of
// its own: on a network mount whose server is down it blocks until the
// server answers, and nothing in Go can cancel it. So the call runs in its
// own goroutine; on timeout the tick reports errStatTimeout and moves on,
// leaving the call pending. Until that call returns, later ticks report the
// timeout at once instead of starting another, so a dead NAS holds one
// goroutine, not one per tick, and delays only the tick it first hangs on.
func (c *Collector) statfs(path string) (syscall.Statfs_t, error) {
	if ch, ok := c.pending[path]; ok {
		select {
		case <-ch:
			// Returned since an earlier tick; that reading is stale, take a new one
			delete(c.pending, path)
		default:
			return syscall.Statfs_t{}, errStatTimeout
		}
	}

	ch := make(chan statResult, 1) // buffered, so a late call never blocks sending
	go func() {
		stat, err := c.stat(path)
		ch <- statResult{stat, err}
	}()

	timer := time.NewTimer(c.statTimeout)
	defer timer.Stop()
	select {
	case r := <-ch:
		return r.stat, r.err
	case <-timer.C:
		c.pending[path] = ch
		return syscall.Statfs_t{}, errStatTimeout
	}
}
