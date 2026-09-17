package metrics

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"log-tailer-go/config"
	"log-tailer-go/resources/network"
	"log-tailer-go/model"
)

// fakePublisher records published payloads. No Redis involved.
type fakePublisher struct {
	mu    sync.Mutex
	sent  []model.MetricsEvent
	chans []string
}

func (p *fakePublisher) PublishBatch(_ context.Context, channel string, payloads [][]byte) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, payload := range payloads {
		var ev model.MetricsEvent
		if err := json.Unmarshal(payload, &ev); err != nil {
			continue
		}
		p.sent = append(p.sent, ev)
		p.chans = append(p.chans, channel)
	}
	return len(payloads)
}

func (p *fakePublisher) events() []model.MetricsEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]model.MetricsEvent(nil), p.sent...)
}

func TestCollector_PublishesOneMixedGoodAndBadMount(t *testing.T) {
	pub := &fakePublisher{}
	identity := config.IdentityConfig{
		System: config.SystemIdentity{ID: "prod-cluster", Name: "Production"},
		Server: config.ServerIdentity{Name: "server-1", IP: "10.0.0.5"},
	}
	c := New([]string{"/", "/this/path/does/not/exist/hopefully"}, "metrics-channel", identity, 10*time.Millisecond, pub)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	events := pub.events()
	if len(events) == 0 {
		t.Fatal("expected at least one metrics event to be published")
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
	if ev.UptimeSeconds <= 0 {
		t.Fatalf("expected a positive uptimeSeconds, got %d", ev.UptimeSeconds)
	}
	if len(ev.Mounts) != 2 {
		t.Fatalf("expected 2 mounts in event, got %d", len(ev.Mounts))
	}
	if ev.Mounts[0].Error != "" || ev.Mounts[0].TotalBytes == 0 {
		t.Fatalf("expected / to have a reading and no error, got %+v", ev.Mounts[0])
	}
	if ev.Mounts[1].Error == "" {
		t.Fatal("expected the bad mount path to have an error set")
	}
	if ev.Mounts[1].TotalBytes != 0 {
		t.Fatalf("expected zero TotalBytes on error, got %d", ev.Mounts[1].TotalBytes)
	}
}

func TestCollector_OmitsCPUPercentOnFirstTickOnly(t *testing.T) {
	pub := &fakePublisher{}
	c := New([]string{"/"}, "metrics-channel", config.IdentityConfig{}, 10*time.Millisecond, pub)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	events := pub.events()
	if len(events) < 2 {
		t.Fatalf("expected at least 2 events to compare, got %d", len(events))
	}
	if events[0].CPUPercent != nil {
		t.Fatalf("expected cpuPercent omitted on the first tick, got %f", *events[0].CPUPercent)
	}
	if events[1].CPUPercent == nil {
		t.Fatal("expected cpuPercent on the second tick, got nil")
	}
	if pct := *events[1].CPUPercent; pct < 0 || pct > 100 {
		t.Fatalf("expected cpuPercent in [0,100], got %f", pct)
	}
}

func TestCollector_OmitsNetIOOnFirstTickOnly(t *testing.T) {
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
	c := New([]string{"/"}, "metrics-channel", config.IdentityConfig{}, 10*time.Millisecond, pub)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	events := pub.events()
	if len(events) < 2 {
		t.Fatalf("expected at least 2 events to compare, got %d", len(events))
	}
	if events[0].NetRxBytesPerSec != nil || events[0].NetTxBytesPerSec != nil {
		t.Fatal("expected network rates omitted on the first tick")
	}
	if events[1].NetRxBytesPerSec == nil || events[1].NetTxBytesPerSec == nil {
		t.Fatal("expected network rates on the second tick, got nil")
	}
	if *events[1].NetRxBytesPerSec < 0 || *events[1].NetTxBytesPerSec < 0 {
		t.Fatalf("expected non-negative rates, got rx %f tx %f", *events[1].NetRxBytesPerSec, *events[1].NetTxBytesPerSec)
	}
}

func TestCollector_PublishesLoadAndMemoryFromRealProc(t *testing.T) {
	pub := &fakePublisher{}
	c := New([]string{"/"}, "metrics-channel", config.IdentityConfig{}, 10*time.Millisecond, pub)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	events := pub.events()
	if len(events) == 0 {
		t.Fatal("expected at least one metrics event")
	}

	ev := events[0]
	if ev.Load1 == nil || ev.Load5 == nil || ev.Load15 == nil {
		t.Fatal("expected all three load figures to be present")
	}
	if ev.MemTotalBytes == nil || ev.MemAvailableBytes == nil {
		t.Fatal("expected memory figures to be present")
	}
	if *ev.MemTotalBytes == 0 {
		t.Fatal("expected a non-zero memTotalBytes")
	}
	if *ev.MemAvailableBytes > *ev.MemTotalBytes {
		t.Fatalf("expected memAvailable <= memTotal, got %d > %d", *ev.MemAvailableBytes, *ev.MemTotalBytes)
	}
	if ev.SwapTotalBytes == nil || ev.SwapUsedBytes == nil {
		t.Fatal("expected swap figures to be present")
	}
	if *ev.SwapUsedBytes > *ev.SwapTotalBytes {
		t.Fatalf("expected swapUsed <= swapTotal, got %d > %d", *ev.SwapUsedBytes, *ev.SwapTotalBytes)
	}
}
