// Package heartbeat publishes a fixed liveness beat on its own ticker.
//
// This is how a server is judged up, so it is deliberately the dumbest
// component in the agent: it reads no files, stats no mounts, and shares no
// state with the metrics collector. If metrics collection wedges on a stuck
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

// Channel is fixed rather than configured: the consumer subscribes to this
// exact name, and a per-server override would only ever be a way to go silent.
const Channel = "agent-heartbeat"

// Publisher ships a batch of serialized events to a pub/sub channel in one
// pipelined round trip, returning how many were accepted.
type Publisher interface {
	PublishBatch(ctx context.Context, channel string, payloads [][]byte) int
}

type Emitter struct {
	payload   []byte
	interval  time.Duration
	publisher Publisher
}

func New(identity config.IdentityConfig, interval time.Duration, publisher Publisher) *Emitter {
	// The payload never changes, and a struct of two strings cannot fail to
	// marshal, so it is built once here — a tick then does nothing but publish
	payload, _ := json.Marshal(model.HeartbeatEvent{
		SystemID:   identity.System.ID,
		ServerName: identity.Server.Name,
	})

	return &Emitter{
		payload:   payload,
		interval:  interval,
		publisher: publisher,
	}
}

// Run publishes one beat per interval until ctx is cancelled. Publishes are
// fire and forget: PublishBatch logs its own failures (throttled) and nothing
// is retried, so a Redis outage costs beats but never delays the next one.
func (e *Emitter) Run(ctx context.Context) {
	slog.Info("Starting heartbeat", "channel", Channel, "interval", e.interval)

	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.publisher.PublishBatch(ctx, Channel, [][]byte{e.payload})
		}
	}
}
