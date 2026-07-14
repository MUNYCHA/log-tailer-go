package redis

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	dialTimeout = 5 * time.Second
	// A hung (not just dead) Redis can never block a tailer for more than
	// this per publish attempt
	rwTimeout = 3 * time.Second

	errLogInterval   = 5 * time.Second
	noSubLogInterval = time.Minute
)

// Publisher ships log events to Redis Pub/Sub channels.
// Safe for concurrent use by multiple tailer goroutines.
type Publisher struct {
	client *redis.Client

	mu         sync.Mutex
	failures   int64
	lastErrLog time.Time
	noSubs     int64
	lastSubLog time.Time
}

// New creates a Publisher and verifies the connection with a PING.
func New(addr, password string, db int) (*Publisher, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		DialTimeout:  dialTimeout,
		ReadTimeout:  rwTimeout,
		WriteTimeout: rwTimeout,
	})

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, err
	}

	return &Publisher{client: client}, nil
}

// PublishBatch sends payloads to the given channel in one pipelined round
// trip and returns how many Redis accepted. Failures are logged with
// throttling; failed lines are dropped — log loss is accepted over
// buffering, and go-redis reconnects automatically on the next call.
func (p *Publisher) PublishBatch(ctx context.Context, channel string, payloads [][]byte) int {
	pipe := p.client.Pipeline()
	cmds := make([]*redis.IntCmd, len(payloads))
	for i, payload := range payloads {
		cmds[i] = pipe.Publish(ctx, channel, payload)
	}
	_, _ = pipe.Exec(ctx) // per-command errors are inspected below

	var shipped, failed, noSubs int64
	var firstErr error
	for _, cmd := range cmds {
		if err := cmd.Err(); err != nil {
			failed++
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		shipped++
		if cmd.Val() == 0 {
			noSubs++
		}
	}
	if failed > 0 {
		p.logFailure(channel, failed, firstErr)
	}
	if noSubs > 0 {
		p.logNoSubscribers(channel, noSubs)
	}
	return int(shipped)
}

func (p *Publisher) Close() error {
	return p.client.Close()
}

func (p *Publisher) logFailure(channel string, count int64, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failures += count
	if time.Since(p.lastErrLog) >= errLogInterval {
		slog.Error("Redis publish failing",
			"channel", channel,
			"failures_since_last_log", p.failures,
			"error", err,
		)
		p.lastErrLog = time.Now()
		p.failures = 0
	}
}

// Pub/Sub has no persistence: a publish with zero subscribers silently
// discards the message, so a down consumer is only visible here.
func (p *Publisher) logNoSubscribers(channel string, count int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.noSubs += count
	if time.Since(p.lastSubLog) >= noSubLogInterval {
		slog.Warn("No subscribers on channel, messages are being discarded",
			"channel", channel,
			"publishes_since_last_log", p.noSubs,
		)
		p.lastSubLog = time.Now()
		p.noSubs = 0
	}
}
