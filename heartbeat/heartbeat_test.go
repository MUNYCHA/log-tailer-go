package heartbeat

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"log-tailer-go/config"
	"log-tailer-go/model"
)

type fakePublisher struct {
	mu       sync.Mutex
	payloads [][]byte
	channels []string
	fail     bool
}

func (p *fakePublisher) PublishBatch(_ context.Context, channel string, payloads [][]byte) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.channels = append(p.channels, channel)
	p.payloads = append(p.payloads, payloads...)
	if p.fail {
		return 0
	}
	return len(payloads)
}

func (p *fakePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.payloads)
}

func identity() config.IdentityConfig {
	return config.IdentityConfig{
		System: config.SystemIdentity{ID: "dev-test", Name: "dev-test"},
		Server: config.ServerIdentity{Name: "dev-box", IP: "127.0.0.1"},
	}
}

func TestEmitter_PublishesIdentityPairOnFixedChannel(t *testing.T) {
	pub := &fakePublisher{}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
	defer cancel()
	New(identity(), 10*time.Millisecond, pub).Run(ctx)

	if pub.count() == 0 {
		t.Fatal("expected at least one beat")
	}
	if pub.channels[0] != "agent-heartbeat" {
		t.Fatalf("expected channel 'agent-heartbeat', got %q", pub.channels[0])
	}

	var beat model.HeartbeatEvent
	if err := json.Unmarshal(pub.payloads[0], &beat); err != nil {
		t.Fatalf("beat is not valid JSON: %v", err)
	}
	if beat.SystemID != "dev-test" || beat.ServerName != "dev-box" {
		t.Fatalf("expected the systemId/serverName pair, got %+v", beat)
	}

	// The payload carries identity and nothing else — no server id, no host data
	var raw map[string]any
	if err := json.Unmarshal(pub.payloads[0], &raw); err != nil {
		t.Fatalf("beat is not a JSON object: %v", err)
	}
	if len(raw) != 2 {
		t.Fatalf("expected exactly 2 fields in the beat, got %d: %v", len(raw), raw)
	}
}

func TestEmitter_KeepsBeatingWhenPublishFails(t *testing.T) {
	pub := &fakePublisher{fail: true}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Millisecond)
	defer cancel()
	New(identity(), 10*time.Millisecond, pub).Run(ctx)

	// A rejected publish is logged and dropped, never retried and never
	// allowed to stall the ticker
	if got := pub.count(); got < 2 {
		t.Fatalf("expected beats to continue after failures, got %d", got)
	}
}

func TestEmitter_StopsOnContextCancel(t *testing.T) {
	pub := &fakePublisher{}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		New(identity(), 10*time.Millisecond, pub).Run(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}
