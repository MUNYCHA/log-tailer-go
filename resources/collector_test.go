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
		System: config.SystemIdentity{ID: "prod-cluster", Name: "Production"},
		Server: config.ServerIdentity{Name: "server-1", IP: "10.0.0.5"},
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
	if ev.SystemID != "prod-cluster" {
		t.Fatalf("expected systemId 'prod-cluster', got %q", ev.SystemID)
	}
	if ev.SystemName != "Production" {
		t.Fatalf("expected systemName 'Production', got %q", ev.SystemName)
	}
	if ev.ServerName != "server-1" {
		t.Fatalf("expected serverName 'server-1', got %q", ev.ServerName)
	}
	if ev.ServerIP != "10.0.0.5" {
		t.Fatalf("expected serverIp '10.0.0.5', got %q", ev.ServerIP)
	}
	if ev.UptimeSeconds == nil || *ev.UptimeSeconds <= 0 {
		t.Fatalf("expected a positive uptimeSeconds, got %v", ev.UptimeSeconds)
	}
}

func TestCollector_OmitsCPUPercentOnFirstTickOnly(t *testing.T) {
	pub := &fakePublisher{}
	c := New("resources-channel", config.IdentityConfig{}, 10*time.Millisecond, pub)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	events := pub.events()
	if len(events) < 2 {
		t.Fatalf("expected at least 2 events to compare, got %d", len(events))
	}
	if events[0].CPU == nil {
		t.Fatal("expected the cpu group on the first tick (load is available)")
	}
	if events[0].CPU.Count == nil || *events[0].CPU.Count <= 0 {
		t.Fatalf("expected cpu.count on the first tick (it needs no baseline), got %v", events[0].CPU.Count)
	}
	if events[0].CPU.UsedPercent != nil {
		t.Fatalf("expected cpu.usedPercent omitted on the first tick, got %f", *events[0].CPU.UsedPercent)
	}
	if events[1].CPU == nil || events[1].CPU.UsedPercent == nil {
		t.Fatal("expected cpu.usedPercent on the second tick, got nil")
	}
	if pct := *events[1].CPU.UsedPercent; pct < 0 || pct > 100 {
		t.Fatalf("expected cpu.usedPercent in [0,100], got %f", pct)
	}
}

func TestCollector_OmitsNetworkOnFirstTickOnly(t *testing.T) {
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
	if len(events) < 2 {
		t.Fatalf("expected at least 2 events to compare, got %d", len(events))
	}
	if events[0].Network != nil {
		t.Fatal("expected the network group omitted on the first tick")
	}
	if events[1].Network == nil {
		t.Fatal("expected the network group on the second tick, got nil")
	}
	if events[1].Network.RxBytesPerSec < 0 || events[1].Network.TxBytesPerSec < 0 {
		t.Fatalf("expected non-negative rates, got rx %f tx %f", events[1].Network.RxBytesPerSec, events[1].Network.TxBytesPerSec)
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
	if ev.Swap == nil {
		t.Fatal("expected the swap group to be present")
	}
	if ev.Swap.UsedBytes > ev.Swap.TotalBytes {
		t.Fatalf("expected swap used <= total, got %d > %d", ev.Swap.UsedBytes, ev.Swap.TotalBytes)
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
