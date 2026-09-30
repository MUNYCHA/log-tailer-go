// Package db publishes the health of a database engine running on this
// server, one event per interval.
//
// The collector here only runs the schedule: it asks its engine for an event
// each tick and publishes it. Each engine lives in its own subpackage
// (db/postgres) and does all the reading, so adding an engine never changes
// this file. Every engine gets its own Collector, channel and interval, so a
// slow or failing engine never holds up another one.
package db

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"log-tailer-go/config"
)

// A round is bounded by the interval, capped at maxRound, so an engine
// stuck on a slow database still gets its event out before the next tick.
// Never under minRound, so a very short interval leaves time to connect.
const (
	maxRound = 30 * time.Second
	minRound = time.Second
)

// Engine reads one database engine. Collect never fails as a whole: what it
// cannot read is reported inside the event it returns. It is called from one
// goroutine only, one round at a time.
type Engine interface {
	Collect(ctx context.Context) any
}

// Publisher ships a batch of serialized events to a pub/sub channel in one
// pipelined round trip, returning how many were accepted.
type Publisher interface {
	PublishBatch(ctx context.Context, channel string, payloads [][]byte) int
}

type Collector struct {
	name      string
	channel   string
	interval  time.Duration
	engine    Engine
	publisher Publisher
}

func New(name, channel string, interval time.Duration, engine Engine, publisher Publisher) *Collector {
	return &Collector{
		name:      name,
		channel:   channel,
		interval:  interval,
		engine:    engine,
		publisher: publisher,
	}
}

func roundBudget(interval time.Duration) time.Duration {
	return max(minRound, min(interval, maxRound))
}

// Run publishes one event per interval. Rounds run one after another on this
// goroutine, so a slow round delays the next instead of overlapping it.
// Returns when ctx is cancelled (graceful shutdown).
func (c *Collector) Run(ctx context.Context) {
	slog.Info("Starting database collector", "engine", c.name, "channel", c.channel, "interval", c.interval)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	// Read once without publishing, so the startup delay doubles as the
	// window the first event's counters are differenced over, then publish
	// once after the delay and on the interval, in step with the other
	// collectors.
	c.collect(ctx)
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

func (c *Collector) collect(ctx context.Context) any {
	round, cancel := context.WithTimeout(ctx, roundBudget(c.interval))
	defer cancel()
	return c.engine.Collect(round)
}

func (c *Collector) collectAndPublish(ctx context.Context) {
	event := c.collect(ctx)
	// Shutdown cancels the round mid-read; that is not the database failing
	if ctx.Err() != nil {
		return
	}

	payload, err := json.Marshal(event)
	if err != nil {
		slog.Error("Failed to serialize database event", "engine", c.name, "error", err)
		return
	}

	c.publisher.PublishBatch(ctx, c.channel, [][]byte{payload})
}
