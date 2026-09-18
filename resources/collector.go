// Package resources publishes one snapshot of the server's uptime, cpu,
// memory, swap and network per interval.
//
// The collector here only orchestrates. Each metric lives in its own
// subpackage with the same three steps — read.go touches the server,
// parse.go turns raw text into numbers, calculate.go turns numbers into the
// published values — so this file never reads /proc or does arithmetic itself.
package resources

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"log-tailer-go/config"
	"log-tailer-go/model"
	"log-tailer-go/resources/cpu"
	"log-tailer-go/resources/load"
	"log-tailer-go/resources/memory"
	"log-tailer-go/resources/network"
	"log-tailer-go/resources/uptime"
)

// Publisher ships a batch of serialized events to a pub/sub channel in one
// pipelined round trip, returning how many were accepted.
type Publisher interface {
	PublishBatch(ctx context.Context, channel string, payloads [][]byte) int
}

type Collector struct {
	channel   string
	identity  config.IdentityConfig
	interval  time.Duration
	publisher Publisher

	// Previous CPU and network readings, differenced against the next tick to
	// get cpu.usedPercent and the network rates. Nil until the first tick has
	// been taken. Only ever touched from Run's goroutine, so they need no lock.
	prevCPU   *cpu.Sample
	prevNet   network.Sample
	prevNetAt time.Time
}

func New(channel string, identity config.IdentityConfig, interval time.Duration, publisher Publisher) *Collector {
	return &Collector{
		channel:   channel,
		identity:  identity,
		interval:  interval,
		publisher: publisher,
	}
}

// Run collects every group each interval and publishes one event per tick.
// Returns when ctx is cancelled (graceful shutdown).
func (c *Collector) Run(ctx context.Context) {
	slog.Info("Starting resources collector", "channel", c.channel, "interval", c.interval)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	// Collect once immediately, then on the interval, so a restarted agent
	// reports within a second rather than after a whole interval of silence.
	// This first event is also the baseline for the two values that are
	// differences between ticks, so it carries no cpu.usedPercent and no
	// network — those arrive with the second event, as they always have.
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
	event := model.ResourcesEvent{
		ServerID:      c.identity.ServerID,
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		UptimeSeconds: readUptime(),
	}
	event.CPU = c.cpuGroup()
	event.Memory, event.Swap = memoryAndSwap()
	event.Network = c.networkGroup()

	payload, err := json.Marshal(event)
	if err != nil {
		slog.Error("Failed to serialize resources event", "error", err)
		return
	}

	c.publisher.PublishBatch(ctx, c.channel, [][]byte{payload})
}

// readUptime returns nil when /proc/uptime can't be read, so the field is
// omitted and the rest of the event still publishes.
func readUptime() *int64 {
	data, err := uptime.Read()
	if err == nil {
		var sample uptime.Sample
		if sample, err = uptime.Parse(data); err == nil {
			return ptr(uptime.Seconds(sample))
		}
	}
	slog.Warn("Failed to read server uptime, omitting from this tick", "path", uptime.Path, "error", err)
	return nil
}

// cpuGroup combines the CPU count, the busy percentage and the load averages.
// They come from different files, so each part is filled independently and the
// group is omitted only when none is available.
func (c *Collector) cpuGroup() *model.CPUGroup {
	var group model.CPUGroup
	c.addCPUStat(&group)
	addLoad(&group)

	if group.Count == nil && group.UsedPercent == nil && group.Load1 == nil {
		return nil
	}
	return &group
}

// addLoad fills load1/5/15, leaving all three unset if /proc/loadavg is
// unreadable — the set is reported whole or not at all.
func addLoad(group *model.CPUGroup) {
	data, err := load.Read()
	if err == nil {
		var sample load.Sample
		if sample, err = load.Parse(data); err == nil {
			one, five, fifteen := load.Averages(sample)
			group.Load1 = ptr(one)
			group.Load5 = ptr(five)
			group.Load15 = ptr(fifteen)
			return
		}
	}
	slog.Warn("Failed to read load average, omitting from this tick", "path", load.Path, "error", err)
}

// addCPUStat fills the CPU count and busy percentage from /proc/stat. The
// percentage differences this tick against the previous one, so it is the mean
// over the whole interval rather than an instant; the first tick has nothing
// to difference against and omits it. The count needs no baseline.
func (c *Collector) addCPUStat(group *model.CPUGroup) {
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

	if n, ok := cpu.Count(now); ok {
		group.Count = ptr(n)
	}

	prev := c.prevCPU
	c.prevCPU = &now
	if prev == nil {
		return
	}
	if pct, ok := cpu.Percent(*prev, now); ok {
		group.UsedPercent = ptr(pct)
	}
}

// memoryAndSwap reads both groups from one /proc/meminfo sample. An unreadable
// file omits both; otherwise each group is omitted only when its own lines are
// missing, so swap is still reported on a kernel memory can't be derived on.
func memoryAndSwap() (*model.MemoryGroup, *model.SwapGroup) {
	data, err := memory.Read()
	if err != nil {
		slog.Warn("Failed to read memory info, omitting from this tick", "path", memory.Path, "error", err)
		return nil, nil
	}
	sample, err := memory.Parse(data)
	if err != nil {
		slog.Warn("Failed to parse memory info, omitting from this tick", "path", memory.Path, "error", err)
		return nil, nil
	}

	var memGroup *model.MemoryGroup
	if usage, ok := memory.Memory(sample); ok {
		memGroup = &model.MemoryGroup{
			TotalBytes:     usage.TotalBytes,
			UsedBytes:      usage.UsedBytes,
			AvailableBytes: usage.AvailableBytes,
			UsedPercent:    usage.UsedPercent,
			Estimated:      usage.Estimated,
		}
	} else {
		slog.Warn("Memory lines missing, omitting memory from this tick", "path", memory.Path)
	}

	var swapGroup *model.SwapGroup
	if usage, ok := memory.Swap(sample); ok {
		swapGroup = &model.SwapGroup{
			TotalBytes:  usage.TotalBytes,
			UsedBytes:   usage.UsedBytes,
			UsedPercent: usage.UsedPercent,
		}
	} else {
		slog.Warn("Swap lines missing, omitting swap from this tick", "path", memory.Path)
	}

	return memGroup, swapGroup
}

// networkGroup differences this tick's /proc/net/dev against the previous one
// to get download (rx) and upload (tx) bytes per second. Like the CPU percentage,
// the first tick has no baseline and omits the group.
func (c *Collector) networkGroup() *model.NetworkGroup {
	data, err := network.Read()
	if err != nil {
		slog.Warn("Failed to read network stats, omitting from this tick", "path", network.Path, "error", err)
		return nil
	}

	now, err := network.Parse(data)
	if err != nil {
		slog.Warn("Failed to parse network stats, omitting from this tick", "path", network.Path, "error", err)
		return nil
	}
	takenAt := time.Now()
	network.KeepPhysical(now)

	prev, prevAt := c.prevNet, c.prevNetAt
	c.prevNet, c.prevNetAt = now, takenAt
	if prev == nil {
		return nil
	}
	rx, tx, ok := network.Rates(prev, now, takenAt.Sub(prevAt))
	if !ok {
		return nil
	}
	return &model.NetworkGroup{RxBytesPerSec: rx, TxBytesPerSec: tx}
}

func ptr[T any](v T) *T {
	return &v
}
