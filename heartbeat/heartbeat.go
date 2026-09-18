// Package heartbeat publishes a fixed liveness beat on its own ticker.
//
// This is how a server is judged up, so it is deliberately the dumbest
// component in the agent: it reads no files, stats no mounts, and shares no
// state with the storage collector. If storage collection wedges on a stuck
// mount, the beat keeps going. A beat that can stop for any reason other than
// the agent being dead is worse than no beat at all.
package heartbeat

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"log-tailer-go/config"
	"log-tailer-go/model"
)

// Publisher ships a batch of serialized events to a pub/sub channel in one
// pipelined round trip, returning how many were accepted.
type Publisher interface {
	PublishBatch(ctx context.Context, channel string, payloads [][]byte) int
}

type Emitter struct {
	payload   []byte
	channel   string
	interval  time.Duration
	publisher Publisher
}

// New builds an emitter. channel comes from config and defaults to
// config.DefaultHeartbeatChannel; it must match what the consumer subscribes
// to, since a beat sent to an unwatched channel is discarded, not queued.
func New(identity config.IdentityConfig, channel string, interval time.Duration, publisher Publisher) *Emitter {
	// The payload never changes, and a struct of two strings cannot fail to
	// marshal, so it is built once here — a tick then does nothing but publish
	payload, _ := json.Marshal(model.HeartbeatEvent{
		ServerID: identity.ServerID,
	})

	return &Emitter{
		payload:   payload,
		channel:   channel,
		interval:  interval,
		publisher: publisher,
	}
}

// Run publishes one beat per interval until ctx is cancelled. Publishes are
// fire and forget: PublishBatch logs its own failures (throttled) and nothing
// is retried, so a Redis outage costs beats but never delays the next one.
func (e *Emitter) Run(ctx context.Context) {
	slog.Info("Starting heartbeat", "channel", e.channel, "interval", e.interval)

	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.publisher.PublishBatch(ctx, e.channel, [][]byte{e.payload})
		}
	}
}
