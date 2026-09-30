package db

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
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
	p.raw = append(p.raw, payloads...)
	for range payloads {
		p.chans = append(p.chans, channel)
	}
	return len(payloads)
}

func (p *fakePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.raw)
}

// fakeEngine numbers its rounds and records each round's deadline
type fakeEngine struct {
	mu        sync.Mutex
	rounds    int
	deadlines []time.Duration
}

func (e *fakeEngine) Collect(ctx context.Context) any {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rounds++
	if d, ok := ctx.Deadline(); ok {
		e.deadlines = append(e.deadlines, time.Until(d))
	}
	return map[string]int{"round": e.rounds}
}

func (e *fakeEngine) roundCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.rounds
}

// The first round is read without publishing, as the baseline for counters,
// so the first published event is the engine's second round
func TestCollector_PrimesThenPublishesAfterStartup(t *testing.T) {
	engine := &fakeEngine{}
	pub := &fakePublisher{}
	c := New("fake", "db-channel", time.Hour, engine, pub)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(done)
	}()

	deadline := time.Now().Add(3 * time.Second)
	for pub.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	if pub.count() != 1 {
		t.Fatalf("expected exactly one event after the startup delay, got %d", pub.count())
	}
	if engine.roundCount() != 2 {
		t.Fatalf("expected a priming round and one published round, got %d rounds", engine.roundCount())
	}
	var got map[string]int
	if err := json.Unmarshal(pub.raw[0], &got); err != nil {
		t.Fatal(err)
	}
	if got["round"] != 2 || pub.chans[0] != "db-channel" {
		t.Fatalf("expected round 2 on db-channel, got %v on %s", got, pub.chans[0])
	}
}

func TestCollector_RoundIsBounded(t *testing.T) {
	engine := &fakeEngine{}
	c := New("fake", "db-channel", time.Hour, engine, &fakePublisher{})
	c.collect(context.Background())
	if len(engine.deadlines) != 1 || engine.deadlines[0] > maxRound {
		t.Fatalf("expected the round bounded by %v, got %v", maxRound, engine.deadlines)
	}
}

// Shutdown mid-round publishes nothing rather than a half-read event
func TestCollector_NoPublishAfterShutdown(t *testing.T) {
	pub := &fakePublisher{}
	c := New("fake", "db-channel", time.Minute, &fakeEngine{}, pub)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.collectAndPublish(ctx)
	if pub.count() != 0 {
		t.Fatalf("expected nothing published after shutdown, got %d", pub.count())
	}
}

func TestRoundBudget(t *testing.T) {
	cases := []struct {
		interval, want time.Duration
	}{
		{time.Hour, maxRound},
		{time.Minute, maxRound},
		{10 * time.Second, 10 * time.Second},
		{100 * time.Millisecond, minRound},
	}
	for _, tc := range cases {
		if got := roundBudget(tc.interval); got != tc.want {
			t.Fatalf("interval %v: expected %v, got %v", tc.interval, tc.want, got)
		}
	}
}
