package metrics

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"log-tailer-go/config"
	"log-tailer-go/model"
)

func TestParseUptimeLine(t *testing.T) {
	got, err := parseUptimeLine([]byte("12345.67 54321.00\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 12345 {
		t.Fatalf("expected 12345, got %d", got)
	}
}

func TestParseUptimeLine_Empty(t *testing.T) {
	if _, err := parseUptimeLine([]byte("")); err == nil {
		t.Fatal("expected error for empty input, got nil")
	}
}

func TestParseUptimeLine_NotANumber(t *testing.T) {
	if _, err := parseUptimeLine([]byte("not-a-number 0\n")); err == nil {
		t.Fatal("expected error for non-numeric input, got nil")
	}
}

func TestStatMount_RootFilesystem(t *testing.T) {
	usage := statMount("/")
	if usage.Error != "" {
		t.Fatalf("unexpected error statting /: %s", usage.Error)
	}
	if usage.TotalBytes == 0 {
		t.Fatal("expected TotalBytes > 0 for /")
	}
	if usage.UsedPercent < 0 || usage.UsedPercent > 100 {
		t.Fatalf("expected UsedPercent in [0,100], got %f", usage.UsedPercent)
	}
}

func TestStatMount_BadPath(t *testing.T) {
	usage := statMount("/this/path/does/not/exist/hopefully")
	if usage.Error == "" {
		t.Fatal("expected Error to be set for a nonexistent mount path")
	}
	if usage.TotalBytes != 0 {
		t.Fatalf("expected zero TotalBytes on error, got %d", usage.TotalBytes)
	}
}

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
	if len(ev.Mounts) != 2 {
		t.Fatalf("expected 2 mounts in event, got %d", len(ev.Mounts))
	}
	if ev.Mounts[0].Error != "" {
		t.Fatalf("expected / to have no error, got %q", ev.Mounts[0].Error)
	}
	if ev.Mounts[1].Error == "" {
		t.Fatal("expected the bad mount path to have an error set")
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
