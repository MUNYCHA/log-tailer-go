package storage

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"log-tailer-go/config"
	"log-tailer-go/model"
)

// fakePublisher records published payloads. No Redis involved.
type fakePublisher struct {
	mu    sync.Mutex
	raw   [][]byte
	chans []string
}

func (p *fakePublisher) PublishBatch(_ context.Context, channel string, payloads [][]byte) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, payload := range payloads {
		p.raw = append(p.raw, payload)
		p.chans = append(p.chans, channel)
	}
	return len(payloads)
}

func (p *fakePublisher) events(t *testing.T) []model.StorageEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	events := make([]model.StorageEvent, len(p.raw))
	for i, payload := range p.raw {
		if err := json.Unmarshal(payload, &events[i]); err != nil {
			t.Fatalf("published payload is not a storage event: %v", err)
		}
	}
	return events
}

func TestCollector_PublishesOneMixedGoodAndBadMount(t *testing.T) {
	pub := &fakePublisher{}
	identity := config.IdentityConfig{
		System: config.SystemIdentity{ID: "prod-cluster", Name: "Production"},
		Server: config.ServerIdentity{Name: "server-1", IP: "10.0.0.5"},
	}
	c := New([]string{"/", "/this/path/does/not/exist/hopefully"}, "storage-channel", identity, 10*time.Millisecond, pub)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	events := pub.events(t)
	if len(events) == 0 {
		t.Fatal("expected at least one storage event to be published")
	}
	if ch := pub.chans[0]; ch != "storage-channel" {
		t.Fatalf("expected channel 'storage-channel', got %q", ch)
	}

	ev := events[0]
	if ev.SystemID != "prod-cluster" || ev.SystemName != "Production" || ev.ServerName != "server-1" || ev.ServerIP != "10.0.0.5" {
		t.Fatalf("expected identity to be copied from config, got %+v", ev)
	}
	if ev.Timestamp == "" {
		t.Fatal("expected a timestamp")
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

// mounts is optional in config, so no mounts still publishes, with [] not null
func TestCollector_NoMountsPublishesEmptyList(t *testing.T) {
	pub := &fakePublisher{}
	c := New(nil, "storage-channel", config.IdentityConfig{}, 10*time.Millisecond, pub)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.raw) == 0 {
		t.Fatal("expected a storage event even with no mounts configured")
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(pub.raw[0], &raw); err != nil {
		t.Fatal(err)
	}
	if got := string(raw["mounts"]); got != "[]" {
		t.Fatalf("expected mounts to be [], got %s", got)
	}
}
