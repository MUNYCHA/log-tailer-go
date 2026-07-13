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

// Publish sends payload to the given channel. Failures are logged with
// throttling and returned; the caller drops the line — log loss is accepted
// over buffering, and go-redis reconnects automatically on the next call.
func (p *Publisher) Publish(ctx context.Context, channel string, payload []byte) error {
	receivers, err := p.client.Publish(ctx, channel, payload).Result()
	if err != nil {
		p.logFailure(channel, err)
		return err
	}
	if receivers == 0 {
		p.logNoSubscribers(channel)
	}
	return nil
}

func (p *Publisher) Close() error {
	return p.client.Close()
}

func (p *Publisher) logFailure(channel string, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failures++
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
func (p *Publisher) logNoSubscribers(channel string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.noSubs++
	if time.Since(p.lastSubLog) >= noSubLogInterval {
		slog.Warn("No subscribers on channel, messages are being discarded",
			"channel", channel,
			"publishes_since_last_log", p.noSubs,
		)
		p.lastSubLog = time.Now()
		p.noSubs = 0
	}
}
