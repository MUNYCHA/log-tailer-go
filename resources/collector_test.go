package resources

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"log-tailer-go/config"
	"log-tailer-go/model"
	"log-tailer-go/resources/network"
)

// fakePublisher records published payloads. No Redis involved.
type fakePublisher struct {
	mu    sync.Mutex
	sent  []model.ResourcesEvent
	chans []string
}

func (p *fakePublisher) PublishBatch(_ context.Context, channel string, payloads [][]byte) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, payload := range payloads {
		var ev model.ResourcesEvent
		if err := json.Unmarshal(payload, &ev); err != nil {
			continue
		}
		p.sent = append(p.sent, ev)
		p.chans = append(p.chans, channel)
	}
	return len(payloads)
}

func (p *fakePublisher) events() []model.ResourcesEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]model.ResourcesEvent(nil), p.sent...)
}

func (p *fakePublisher) channels() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.chans...)
}

func TestCollector_PublishesIdentityAndUptime(t *testing.T) {
	pub := &fakePublisher{}
	identity := config.IdentityConfig{
		ServerID: "server-1",
	}
	c := New("resources-channel", identity, 10*time.Millisecond, pub)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	events := pub.events()
	if len(events) == 0 {
		t.Fatal("expected at least one resources event to be published")
	}
	if ch := pub.channels()[0]; ch != "resources-channel" {
		t.Fatalf("expected channel 'resources-channel', got %q", ch)
	}

	ev := events[0]
	if ev.ServerID != "server-1" {
		t.Fatalf("expected serverId 'server-1', got %q", ev.ServerID)
	}
	if ev.UptimeSeconds == nil || *ev.UptimeSeconds <= 0 {
		t.Fatalf("expected a positive uptimeSeconds, got %v", ev.UptimeSeconds)
	}
}

func TestCollector_PublishesCPUPercentFromTheFirstEvent(t *testing.T) {
	pub := &fakePublisher{}
	c := New("resources-channel", config.IdentityConfig{}, 10*time.Millisecond, pub)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	events := pub.events()
	if len(events) == 0 {
		t.Fatal("expected at least one event")
	}
	if events[0].CPU == nil {
		t.Fatal("expected the cpu group on the first event")
	}
	if events[0].CPU.Count == nil || *events[0].CPU.Count <= 0 {
		t.Fatalf("expected cpu.count on the first event, got %v", events[0].CPU.Count)
	}
	// The reading taken during the startup delay is the baseline, so the very first
	// published event already has a window to measure over
	if events[0].CPU.UsedPercent == nil {
		t.Fatal("expected cpu.usedPercent on the first event, the startup reading gives it a baseline")
	}
	if pct := *events[0].CPU.UsedPercent; pct < 0 || pct > 100 {
		t.Fatalf("expected cpu.usedPercent in [0,100], got %f", pct)
	}
	// Later events are not asserted: at this test's 10ms interval the jiffy
	// counters can be unchanged between two ticks, which legitimately omits
	// the field. The startup baseline is what this test is about.
}

func TestCollector_PublishesNetworkFromTheFirstEvent(t *testing.T) {
	hasPhysical := false
	if data, err := network.Read(); err == nil {
		if sample, err := network.Parse(data); err == nil {
			network.KeepPhysical(sample)
			hasPhysical = len(sample) > 0
		}
	}
	if !hasPhysical {
		t.Skip("no physical network interface on this host")
	}

	pub := &fakePublisher{}
	c := New("resources-channel", config.IdentityConfig{}, 10*time.Millisecond, pub)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	events := pub.events()
	if len(events) == 0 {
		t.Fatal("expected at least one event")
	}
	// The reading taken during the startup delay is the baseline for the rates too
	if events[0].Network == nil {
		t.Fatal("expected the network group on the first event, the startup reading gives it a baseline")
	}
	if events[0].Network.RxBytesPerSec < 0 || events[0].Network.TxBytesPerSec < 0 {
		t.Fatalf("expected non-negative rates, got rx %f tx %f", events[0].Network.RxBytesPerSec, events[0].Network.TxBytesPerSec)
	}
}

func TestCollector_PublishesLoadMemoryAndSwapFromRealProc(t *testing.T) {
	pub := &fakePublisher{}
	c := New("resources-channel", config.IdentityConfig{}, 10*time.Millisecond, pub)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	events := pub.events()
	if len(events) == 0 {
		t.Fatal("expected at least one resources event")
	}

	ev := events[0]
	if ev.CPU == nil || ev.CPU.Load1 == nil || ev.CPU.Load5 == nil || ev.CPU.Load15 == nil {
		t.Fatal("expected all three load figures to be present")
	}
	if ev.Memory == nil {
		t.Fatal("expected the memory group to be present")
	}
	if ev.Memory.TotalBytes == 0 {
		t.Fatal("expected a non-zero memory.totalBytes")
	}
	if ev.Memory.AvailableBytes > ev.Memory.TotalBytes {
		t.Fatalf("expected memory available <= total, got %d > %d", ev.Memory.AvailableBytes, ev.Memory.TotalBytes)
	}
	if ev.Memory.UsedBytes+ev.Memory.AvailableBytes != ev.Memory.TotalBytes {
		t.Fatalf("expected memory used + available = total, got %+v", *ev.Memory)
	}
	if ev.Memory.UsedPercent < 0 || ev.Memory.UsedPercent > 100 {
		t.Fatalf("expected memory.usedPercent in [0,100], got %f", ev.Memory.UsedPercent)
	}
	if ev.Swap == nil {
		t.Fatal("expected the swap group to be present")
	}
	if ev.Swap.UsedBytes > ev.Swap.TotalBytes {
		t.Fatalf("expected swap used <= total, got %d > %d", ev.Swap.UsedBytes, ev.Swap.TotalBytes)
	}
	if ev.Swap.UsedPercent < 0 || ev.Swap.UsedPercent > 100 {
		t.Fatalf("expected swap.usedPercent in [0,100], got %f", ev.Swap.UsedPercent)
	}
}

// Groups that can't be read must be absent from the JSON, not sent as zeros
func TestResourcesEvent_OmitsMissingGroupsInJSON(t *testing.T) {
	payload, err := json.Marshal(model.ResourcesEvent{})
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"uptimeSeconds", "cpu", "memory", "swap", "network"} {
		if _, ok := raw[key]; ok {
			t.Fatalf("expected %q omitted when nil, got %s", key, payload)
		}
	}
}

func TestCollector_PublishesOnStartWithoutWaitingTheInterval(t *testing.T) {
	pub := &fakePublisher{}
	// An interval far longer than the test: anything published can only be
	// the collect that runs before the ticker's first tick.
	c := New("resources-channel", config.IdentityConfig{ServerID: "server-1"}, time.Hour, pub)

	ctx, cancel := context.WithTimeout(context.Background(), config.StartupDelay+300*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	events := pub.events()
	if len(events) != 1 {
		t.Fatalf("expected exactly one event before the first tick, got %d", len(events))
	}
	// Complete despite arriving long before the first interval elapses: the
	// reading taken during the startup delay, not this event, is the baseline
	if events[0].CPU == nil || events[0].CPU.UsedPercent == nil {
		t.Fatal("expected cpu.usedPercent on the startup event")
	}
}

func TestCollector_StartupDelayDoesNotHoldUpShutdown(t *testing.T) {
	pub := &fakePublisher{}
	c := New("resources-channel", config.IdentityConfig{ServerID: "server-1"}, time.Hour, pub)

	// Cancelled well inside the startup delay: Run must return rather than
	// hold shutdown open for the rest of it
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	c.Run(ctx)
	if elapsed := time.Since(start); elapsed >= config.StartupDelay {
		t.Fatalf("expected Run to return when ctx was cancelled, took %s", elapsed)
	}
	if n := len(pub.events()); n != 0 {
		t.Fatalf("expected nothing published when cancelled during the startup delay, got %d", n)
	}
}
